#!/usr/bin/env python3
"""Orchestrate the Discogs cache ETL pipeline.

Two modes of operation:

  Full pipeline from XML (steps 1-10):
    python scripts/run_pipeline.py \\
      --xml <releases.xml.gz> \\
      --library-artists <library_artists.txt> \\
      [--converter <path/to/discogs-xml-converter>] \\
      [--library-db <library.db>] \\
      [--wxyc-db-url <mysql://user:pass@host:port/db>] \\
      [--database-url <url>]

  Database build from pre-filtered CSVs (steps 4-10):
    python scripts/run_pipeline.py \\
      --csv-dir <path/to/filtered/> \\
      [--library-db <library.db>] \\
      [--database-url <url>] \\
      [--resume] [--state-file <path>]

Environment variables:
    DATABASE_URL  Default database URL when --database-url is not specified.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psycopg
from wxyc_etl.state import PipelineState

STEP_NAMES = [
    "create_schema",
    "import_csv",
    "create_indexes",
    "dedup",
    "import_tracks",
    "create_track_indexes",
    "prune",
    "vacuum",
    "set_logged",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
SCHEMA_DIR = SCRIPT_DIR.parent / "schema"

# Maximum seconds to wait for Postgres to become ready.
PG_CONNECT_TIMEOUT = 30

# Tables managed by the pipeline (shared by run_vacuum, set_tables_unlogged, set_tables_logged).
PIPELINE_TABLES = [
    "release",
    "release_artist",
    "release_label",
    "release_genre",
    "release_style",
    "release_track",
    "release_track_artist",
    "cache_metadata",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--xml",
        type=Path,
        metavar="PATH",
        help="Path to Discogs XML dump file or directory containing XML dumps "
        "(e.g. releases.xml.gz or a directory with artists.xml, labels.xml, releases.xml).",
    )
    source.add_argument(
        "--csv-dir",
        type=Path,
        metavar="DIR",
        help="Directory containing pre-filtered Discogs CSV files (skips steps 1-3).",
    )

    parser.add_argument(
        "--converter",
        type=str,
        default="discogs-xml-converter",
        metavar="PATH",
        help="Path to discogs-xml-converter binary (default: discogs-xml-converter on PATH).",
    )
    parser.add_argument(
        "--library-artists",
        type=Path,
        metavar="FILE",
        help="Path to library_artists.txt for artist filtering. "
        "Used with --xml to filter during conversion.",
    )
    parser.add_argument(
        "--library-db",
        type=Path,
        metavar="FILE",
        help="Path to library.db for KEEP/PRUNE classification "
        "(optional; if omitted, the prune step is skipped). "
        "Use --generate-library-db to generate it from MySQL via SSH.",
    )
    parser.add_argument(
        "--generate-library-db",
        action="store_true",
        default=False,
        help="Generate library.db from the WXYC catalog before running the pipeline. "
        "Requires --catalog-source and --catalog-db-url (or --wxyc-db-url). "
        "Uses wxyc-export-to-sqlite from the wxyc-catalog package. "
        "Conflicts with --library-db.",
    )
    parser.add_argument(
        "--wxyc-db-url",
        type=str,
        default=None,
        metavar="URL",
        help="MySQL connection URL for WXYC catalog database "
        "(e.g. mysql://user:pass@host:port/dbname). "
        "Alias for --catalog-source tubafrenzy --catalog-db-url <url>. "
        "Enriches library_artists.txt with alternate names and cross-references, "
        "and extracts label preferences for label-aware dedup. "
        "Requires --library-db.",
    )
    parser.add_argument(
        "--catalog-source",
        type=str,
        choices=["tubafrenzy", "backend-service"],
        default=None,
        metavar="SOURCE",
        help="Catalog source type: 'tubafrenzy' (MySQL) or 'backend-service' (PostgreSQL). "
        "Requires --catalog-db-url.",
    )
    parser.add_argument(
        "--catalog-db-url",
        type=str,
        default=None,
        metavar="URL",
        help="Database connection URL for the catalog source. Requires --catalog-source.",
    )
    parser.add_argument(
        "--database-url",
        type=str,
        default=os.environ.get("DATABASE_URL", "postgresql://localhost:5432/discogs"),
        help="PostgreSQL connection URL "
        "(default: DATABASE_URL env var or postgresql://localhost:5432/discogs).",
    )
    parser.add_argument(
        "--target-db-url",
        type=str,
        default=None,
        metavar="URL",
        help="Copy matched releases to a separate target database instead of "
        "pruning in place. Requires --library-db.",
    )
    parser.add_argument(
        "--library-labels",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to pre-generated library_labels.csv for label-aware dedup. "
        "If omitted but --wxyc-db-url is provided, labels are extracted "
        "automatically before dedup.",
    )
    parser.add_argument(
        "--label-hierarchy",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to label_hierarchy.csv from discogs-xml-converter. "
        "Enables sublabel resolution during label-aware dedup. "
        "In --xml mode with directory input, auto-detected from converter output.",
    )
    parser.add_argument(
        "--keep-csv",
        type=Path,
        default=None,
        metavar="DIR",
        help="Directory to store converted CSVs persistently (--xml mode only). "
        "When provided, CSVs are kept after the pipeline completes or fails, "
        "avoiding a full re-conversion on retry. When omitted, CSVs are "
        "written to a temporary directory that is deleted on exit.",
    )
    parser.add_argument(
        "--direct-pg",
        action="store_true",
        default=False,
        help="Stream releases directly into PostgreSQL from the converter, "
        "bypassing CSV import. Requires --xml mode. The converter writes "
        "releases via COPY, eliminating the CSV round-trip for release data. "
        "Supplementary CSVs (artist_alias.csv, label_hierarchy.csv) are "
        "still written to the output directory.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Resume a previously interrupted pipeline run. "
        "Skips steps that have already completed. Only valid with --csv-dir.",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path(".pipeline_state.json"),
        metavar="FILE",
        help="Path to pipeline state file for tracking/resuming progress "
        "(default: .pipeline_state.json).",
    )

    args = parser.parse_args(argv)

    # Validate --xml mode dependencies
    if args.xml is not None:
        if args.resume:
            parser.error("--resume is only valid with --csv-dir, not --xml")

    if args.direct_pg and args.xml is None:
        parser.error("--direct-pg requires --xml mode")

    if args.generate_library_db and args.library_db:
        parser.error("--generate-library-db and --library-db are mutually exclusive")
    if args.generate_library_db and not args.catalog_source:
        parser.error(
            "--generate-library-db requires --catalog-source and --catalog-db-url "
            "(or --wxyc-db-url)"
        )

    if args.catalog_source and not args.catalog_db_url:
        parser.error("--catalog-source requires --catalog-db-url")
    if args.catalog_db_url and not args.catalog_source:
        parser.error("--catalog-db-url requires --catalog-source")
    if args.wxyc_db_url and args.catalog_source:
        parser.error("--wxyc-db-url and --catalog-source are mutually exclusive")

    # Normalize: treat --wxyc-db-url as --catalog-source tubafrenzy --catalog-db-url <url>
    if args.wxyc_db_url and not args.catalog_source:
        args.catalog_source = "tubafrenzy"
        args.catalog_db_url = args.wxyc_db_url

    has_catalog = args.catalog_source is not None
    if has_catalog and not args.library_db and not args.generate_library_db:
        parser.error(
            "--library-db or --generate-library-db is required when using "
            "--wxyc-db-url or --catalog-source"
        )

    if args.target_db_url and not args.library_db and not args.generate_library_db:
        parser.error("--library-db or --generate-library-db is required when using --target-db-url")

    return args


def wait_for_postgres(db_url: str) -> None:
    """Poll Postgres until a connection succeeds or timeout is reached."""
    logger.info("Waiting for PostgreSQL at %s ...", db_url)
    deadline = time.monotonic() + PG_CONNECT_TIMEOUT
    delay = 0.5
    while True:
        try:
            conn = psycopg.connect(db_url, connect_timeout=5)
            conn.close()
            logger.info("PostgreSQL is ready.")
            return
        except psycopg.OperationalError:
            if time.monotonic() >= deadline:
                logger.error("Timed out waiting for PostgreSQL after %ds", PG_CONNECT_TIMEOUT)
                sys.exit(1)
            time.sleep(delay)
            delay = min(delay * 2, 3)


def run_sql_file(db_url: str, sql_file: Path, *, strip_concurrently: bool = False) -> None:
    """Execute a SQL file against the database using psycopg.

    Args:
        db_url: PostgreSQL connection URL.
        sql_file: Path to the .sql file.
        strip_concurrently: If True, remove CONCURRENTLY from CREATE INDEX
            statements (safe on a fresh database with no concurrent queries).
    """
    logger.info("Running %s ...", sql_file.name)

    sql = sql_file.read_text()
    if strip_concurrently:
        sql = sql.replace(" CONCURRENTLY", "")

    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    except psycopg.Error as exc:
        logger.error("SQL execution failed for %s: %s", sql_file.name, exc)
        conn.close()
        sys.exit(1)
    conn.close()
    logger.info("  done.")


def run_sql_statements_parallel(
    db_url: str,
    statements: list[str],
    description: str = "",
) -> None:
    """Execute independent SQL statements in parallel.

    Each statement runs on its own connection (autocommit=True) via
    ThreadPoolExecutor. Useful for creating multiple independent indexes
    concurrently.
    """
    if not statements:
        return

    if description:
        logger.info("Running %s (%d statements in parallel)...", description, len(statements))

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _execute_one(stmt: str) -> str:
        conn = psycopg.connect(db_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(stmt)
        finally:
            conn.close()
        return stmt

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=min(len(statements), 4)) as executor:
        futures = {executor.submit(_execute_one, s): s for s in statements}
        for future in as_completed(futures):
            stmt = futures[future]
            try:
                future.result()
            except psycopg.Error as exc:
                label = stmt[:60].strip()
                logger.error("Parallel SQL failed: %s: %s", label, exc)
                raise

    elapsed = time.monotonic() - start
    if description:
        logger.info("  %s done in %.1fs", description, elapsed)


def run_step(description: str, cmd: list[str], **kwargs) -> None:
    """Run a subprocess, streaming output line-by-line.

    Merges stderr into stdout to avoid threading complexity.  Each line
    is logged at INFO level as it arrives, giving real-time visibility
    into long-running steps like CSV import.
    """
    logger.info("Step: %s", description)
    start = time.monotonic()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        **kwargs,
    )
    for line in proc.stdout:
        text = line.rstrip("\n")
        # Strip env_logger timestamps (e.g. "[2026-03-09T14:13:57Z INFO  module]")
        # to avoid double-dating when wrapped by Python's logger.
        stripped = re.sub(r"^\[[\dT:Z -]+\s+\w+\s+\S+]\s*", "", text)
        logger.info("  %s", stripped)
    proc.wait()
    elapsed = time.monotonic() - start
    if proc.returncode != 0:
        logger.error("Step failed (exit %d) after %.1fs", proc.returncode, elapsed)
        sys.exit(1)
    logger.info("  completed in %.1fs", elapsed)


def run_vacuum(db_url: str) -> None:
    """Run VACUUM FULL on all pipeline tables in parallel.

    VACUUM FULL on independent tables does not conflict, so we use
    run_sql_statements_parallel (which opens a separate autocommit
    connection per statement) to vacuum all tables concurrently.
    """
    statements = [f"VACUUM FULL {table}" for table in PIPELINE_TABLES]
    run_sql_statements_parallel(db_url, statements, description="VACUUM FULL")


def set_tables_unlogged(db_url: str) -> None:
    """Set all pipeline tables to UNLOGGED to skip WAL writes during bulk import.

    FK ordering: child tables first (parallel), then the parent ``release``
    table, because PostgreSQL requires all tables in a FK relationship to
    share the same persistence mode.
    """
    child_tables = [t for t in PIPELINE_TABLES if t != "release"]
    child_stmts = [f"ALTER TABLE {t} SET UNLOGGED" for t in child_tables]
    run_sql_statements_parallel(db_url, child_stmts, description="SET UNLOGGED (children)")
    run_sql_statements_parallel(
        db_url, ["ALTER TABLE release SET UNLOGGED"], description="SET UNLOGGED (release)"
    )


def set_tables_logged(db_url: str) -> None:
    """Set all pipeline tables back to LOGGED for durable storage after import.

    FK ordering: parent ``release`` table first, then child tables (parallel),
    because PostgreSQL requires all tables in a FK relationship to share
    the same persistence mode.
    """
    run_sql_statements_parallel(
        db_url, ["ALTER TABLE release SET LOGGED"], description="SET LOGGED (release)"
    )
    child_tables = [t for t in PIPELINE_TABLES if t != "release"]
    child_stmts = [f"ALTER TABLE {t} SET LOGGED" for t in child_tables]
    run_sql_statements_parallel(db_url, child_stmts, description="SET LOGGED (children)")


def report_sizes(db_url: str) -> None:
    """Log final table row counts and sizes."""
    logger.info("Final database state:")
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT relname,
                   n_live_tup::bigint as row_count,
                   pg_size_pretty(pg_total_relation_size(relid)) as total_size
            FROM pg_stat_user_tables
            WHERE relname IN (
                'release', 'release_artist', 'release_label',
                'release_track', 'release_track_artist', 'cache_metadata'
            )
            ORDER BY pg_total_relation_size(relid) DESC
        """)
        for row in cur.fetchall():
            logger.info("  %-25s %10s rows   %s", row[0], f"{row[1]:,}", row[2])
    conn.close()


def convert_and_filter(
    xml_file: Path,
    output_dir: Path,
    converter: str,
    library_artists: Path | None = None,
    database_url: str | None = None,
) -> None:
    """Convert Discogs XML to CSV using discogs-xml-converter.

    Replaces the old three-step process (xml2db + fix_newlines + filter_csv)
    with a single call to the Rust binary.

    When database_url is provided, releases are streamed directly into
    PostgreSQL via COPY instead of being written to CSV files. Supplementary
    CSVs (artist_alias.csv, label_hierarchy.csv) are still written to
    output_dir.
    """
    cmd = [converter, str(xml_file), "--output-dir", str(output_dir)]
    if library_artists:
        cmd.extend(["--library-artists", str(library_artists)])
    if database_url:
        cmd.extend(["--database-url", database_url])
    description = (
        "Convert and import XML to PostgreSQL" if database_url else "Convert and filter XML to CSV"
    )
    run_step(description, cmd)


def enrich_library_artists(
    library_db: Path,
    library_artists_out: Path,
    wxyc_db_url: str | None = None,
    catalog_source: str | None = None,
    catalog_db_url: str | None = None,
) -> None:
    """Step 2.5: Enrich library_artists.txt with WXYC cross-reference data."""
    cmd = [
        "wxyc-enrich-library-artists",
        "--library-db",
        str(library_db),
        "--output",
        str(library_artists_out),
    ]
    if catalog_source and catalog_db_url:
        cmd.extend(["--catalog-source", catalog_source, "--catalog-db-url", catalog_db_url])
    elif wxyc_db_url:
        cmd.extend(["--wxyc-db-url", wxyc_db_url])
    run_step("Enrich library artists", cmd)


def _infer_pipeline_state(db_url: str, csv_dir: str) -> PipelineState:
    """Infer pipeline state from database structure.

    Inspects table existence, row counts, column presence, and index names
    to determine which pipeline steps have already completed. Steps that
    cannot be inferred (prune, vacuum) are left as pending since they are
    safe to re-run.
    """
    state = PipelineState(db_url=db_url, csv_dir=csv_dir, steps=STEP_NAMES)

    conn = psycopg.connect(db_url)
    try:
        with conn.cursor() as cur:
            # create_schema: release table exists?
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables"
                "  WHERE table_schema = 'public' AND table_name = 'release'"
                ")"
            )
            if not cur.fetchone()[0]:
                return state
            state.mark_completed("create_schema")

            # import_csv: release table has rows?
            cur.execute("SELECT EXISTS (SELECT 1 FROM release LIMIT 1)")
            if not cur.fetchone()[0]:
                return state
            state.mark_completed("import_csv")

            # create_indexes: base trigram indexes exist?
            cur.execute(
                "SELECT indexname FROM pg_indexes"
                " WHERE schemaname = 'public' AND indexname LIKE '%trgm%'"
            )
            indexes = {row[0] for row in cur.fetchall()}
            base_expected = {"idx_release_artist_name_trgm", "idx_release_title_trgm"}
            if not base_expected.issubset(indexes):
                return state
            state.mark_completed("create_indexes")

            # dedup: master_id column gone?
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.columns"
                "  WHERE table_name = 'release' AND column_name = 'master_id'"
                ")"
            )
            if cur.fetchone()[0]:
                return state
            state.mark_completed("dedup")

            # import_tracks: release_track has rows?
            cur.execute("SELECT EXISTS (SELECT 1 FROM release_track LIMIT 1)")
            if not cur.fetchone()[0]:
                return state
            state.mark_completed("import_tracks")

            # create_track_indexes: track trigram indexes exist?
            track_expected = {
                "idx_release_track_title_trgm",
                "idx_release_track_artist_name_trgm",
            }
            if not track_expected.issubset(indexes):
                return state
            state.mark_completed("create_track_indexes")
    finally:
        conn.close()

    # prune and vacuum cannot be inferred from database state
    return state


def _load_or_create_state(args: argparse.Namespace) -> PipelineState:
    """Load existing state for --resume, or create fresh state.

    When --resume is set and no state file exists, infers state from
    the database structure.
    """
    csv_dir_str = str(args.csv_dir.resolve())
    state_file = args.state_file

    if args.resume and state_file.exists():
        logger.info("Loading pipeline state from %s", state_file)
        state = PipelineState.load(str(state_file))
        state.validate_resume(db_url=args.database_url, csv_dir=csv_dir_str)
        return state

    if args.resume and not state_file.exists():
        logger.info("No state file found; inferring state from database")
        state = _infer_pipeline_state(args.database_url, csv_dir_str)
        completed = [s for s in STEP_NAMES if state.is_completed(s)]
        if completed:
            logger.info("  Inferred completed steps: %s", ", ".join(completed))
        else:
            logger.info("  No completed steps detected; starting from scratch")
        return state

    return PipelineState(db_url=args.database_url, csv_dir=csv_dir_str, steps=STEP_NAMES)


def generate_library_db(
    output_path: Path,
    catalog_source: str,
    catalog_db_url: str,
) -> None:
    """Step 0: Generate library.db from WXYC catalog via wxyc-catalog CLI."""
    run_step(
        "Generate library.db",
        [
            "wxyc-export-to-sqlite",
            "--catalog-source",
            catalog_source,
            "--catalog-db-url",
            catalog_db_url,
            "--output",
            str(output_path),
        ],
    )


def _run_xml_pipeline(
    args: argparse.Namespace,
    python: str,
    db_url: str,
) -> None:
    """Run the XML conversion + database build pipeline.

    When --keep-csv is provided, CSVs are written to that directory and
    persist after the pipeline. Otherwise, a TemporaryDirectory is used.
    """
    keep_csv_dir = args.keep_csv

    if keep_csv_dir is not None:
        keep_csv_dir.mkdir(parents=True, exist_ok=True)
        tmp = keep_csv_dir
        csv_dir = keep_csv_dir / "csv"
        logger.info("Using persistent CSV directory: %s", keep_csv_dir)
    else:
        # Create a TemporaryDirectory that auto-cleans on exit
        tmp = None
        csv_dir = None

    def _run_with_dirs(tmp_dir: Path, csv_out: Path) -> None:
        # -- enrich_artists
        library_artists_path = args.library_artists
        if args.library_db:
            enriched_artists = tmp_dir / "library_artists.txt"
            enrich_library_artists(
                args.library_db,
                enriched_artists,
                catalog_source=args.catalog_source,
                catalog_db_url=args.catalog_db_url,
            )
            library_artists_path = enriched_artists

        if args.direct_pg:
            # Direct-to-PG mode: create schema first, then converter writes
            # releases directly into PostgreSQL via COPY.
            wait_for_postgres(db_url)
            run_sql_file(db_url, SCHEMA_DIR / "create_database.sql")
            run_sql_file(db_url, SCHEMA_DIR / "create_functions.sql")

            # Truncate release tables so COPY doesn't hit unique violations
            # from a previous run. CASCADE removes child rows.
            logger.info("Truncating release tables...")
            conn = psycopg.connect(db_url, autocommit=True)
            with conn.cursor() as cur:
                cur.execute("TRUNCATE release CASCADE")
            conn.close()

            # Set tables UNLOGGED before the converter streams data via COPY.
            # This skips WAL writes during the bulk import phase.
            set_tables_unlogged(db_url)

            # Converter streams releases into PG; supplementary CSVs still
            # go to csv_out (artist_alias.csv, label_hierarchy.csv).
            convert_and_filter(
                args.xml,
                csv_out,
                args.converter,
                library_artists_path,
                database_url=db_url,
            )

            # Auto-detect label_hierarchy.csv
            hierarchy_csv = args.label_hierarchy
            if hierarchy_csv is None:
                auto_hierarchy = csv_out / "label_hierarchy.csv"
                if auto_hierarchy.exists():
                    logger.info("Auto-detected label_hierarchy.csv from converter output")
                    hierarchy_csv = auto_hierarchy

            # Skip import_csv steps (converter already loaded release data).
            # Continue with indexes, dedup, track indexes, prune, vacuum.
            _run_database_build_post_import(
                db_url,
                csv_out,
                args.library_db,
                python,
                library_labels=args.library_labels,
                label_hierarchy=hierarchy_csv,
                catalog_source=args.catalog_source,
                catalog_db_url=args.catalog_db_url,
            )
        else:
            # Standard CSV mode
            convert_and_filter(args.xml, csv_out, args.converter, library_artists_path)

            # Auto-detect label_hierarchy.csv
            hierarchy_csv = args.label_hierarchy
            if hierarchy_csv is None:
                auto_hierarchy = csv_out / "label_hierarchy.csv"
                if auto_hierarchy.exists():
                    logger.info("Auto-detected label_hierarchy.csv from converter output")
                    hierarchy_csv = auto_hierarchy

            # -- database build
            _run_database_build(
                db_url,
                csv_out,
                args.library_db,
                python,
                library_labels=args.library_labels,
                label_hierarchy=hierarchy_csv,
                catalog_source=args.catalog_source,
                catalog_db_url=args.catalog_db_url,
            )

    if keep_csv_dir is not None:
        _run_with_dirs(tmp, csv_dir)
    else:
        with tempfile.TemporaryDirectory(prefix="discogs_pipeline_") as tmpdir:
            tmp_path = Path(tmpdir)
            _run_with_dirs(tmp_path, tmp_path / "csv")


def main() -> None:
    args = parse_args()

    python = sys.executable
    db_url = args.database_url
    pipeline_start = time.monotonic()

    # Generate library.db if requested
    if args.generate_library_db:
        generated_db = Path(tempfile.mkdtemp(prefix="discogs_library_")) / "library.db"
        generate_library_db(generated_db, args.catalog_source, args.catalog_db_url)
        args.library_db = generated_db

    # Validate paths
    if args.xml is not None:
        if not args.xml.exists():
            logger.error("XML path not found: %s", args.xml)
            sys.exit(1)
        if args.library_artists and not args.library_artists.exists():
            logger.error("library_artists.txt not found: %s", args.library_artists)
            sys.exit(1)
    else:
        if not args.csv_dir.exists():
            logger.error("CSV directory not found: %s", args.csv_dir)
            sys.exit(1)

    if args.library_db and not args.library_db.exists():
        logger.error("library.db not found: %s", args.library_db)
        sys.exit(1)

    if args.library_labels and not args.library_labels.exists():
        logger.error("library_labels.csv not found: %s", args.library_labels)
        sys.exit(1)

    # Steps 1-3: XML conversion + filtering (only in --xml mode)
    if args.xml is not None:
        _run_xml_pipeline(args, python, db_url)
    else:
        # Database build only (--csv-dir mode)
        state = _load_or_create_state(args)
        _run_database_build(
            db_url,
            args.csv_dir,
            args.library_db,
            python,
            target_db_url=args.target_db_url,
            library_labels=args.library_labels,
            label_hierarchy=args.label_hierarchy,
            catalog_source=args.catalog_source,
            catalog_db_url=args.catalog_db_url,
            state=state,
            state_file=args.state_file,
        )

    total = time.monotonic() - pipeline_start
    logger.info("Pipeline complete in %.1f minutes.", total / 60)


def _run_database_build_post_import(
    db_url: str,
    csv_dir: Path,
    library_db: Path | None,
    python: str,
    *,
    library_labels: Path | None = None,
    label_hierarchy: Path | None = None,
    catalog_source: str | None = None,
    catalog_db_url: str | None = None,
) -> None:
    """Post-import database build for --direct-pg mode.

    Skips create_schema (already done), import_csv, and import_tracks
    (converter loaded all data directly). Tables are already UNLOGGED
    (set in _run_xml_pipeline before the converter). Runs create_indexes
    through SET LOGGED.
    """
    # -- create_indexes (base trigram indexes, run in parallel)
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    conn.close()

    run_sql_statements_parallel(
        db_url,
        [
            "CREATE INDEX IF NOT EXISTS idx_release_artist_name_trgm "
            "ON release_artist USING GIN (lower(f_unaccent(artist_name)) gin_trgm_ops)",
            "CREATE INDEX IF NOT EXISTS idx_release_title_trgm "
            "ON release USING GIN (lower(f_unaccent(title)) gin_trgm_ops)",
        ],
        description="base trigram indexes",
    )

    # -- dedup (deduplicate by master_id)
    labels_csv = library_labels
    if labels_csv is None and catalog_source is not None and catalog_db_url is not None:
        labels_csv = Path(tempfile.mkdtemp(prefix="discogs_labels_")) / "library_labels.csv"
        run_step(
            "Extract WXYC library labels",
            [
                "wxyc-extract-library-labels",
                "--catalog-source",
                catalog_source,
                "--catalog-db-url",
                catalog_db_url,
                "--output",
                str(labels_csv),
            ],
        )

    dedup_cmd = [python, str(SCRIPT_DIR / "dedup_releases.py")]
    if labels_csv is not None:
        dedup_cmd.extend(["--library-labels", str(labels_csv)])
    if label_hierarchy is not None:
        dedup_cmd.extend(["--label-hierarchy", str(label_hierarchy)])
    dedup_cmd.append(db_url)

    run_step("Deduplicate releases", dedup_cmd)

    # -- create_track_indexes (FK constraints, FK indexes, trigram indexes)
    # Level 1: FK constraints (parallel)
    run_sql_statements_parallel(
        db_url,
        [
            "DO $$ BEGIN "
            "ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release "
            "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
            "DO $$ BEGIN "
            "ALTER TABLE release_track_artist ADD CONSTRAINT fk_release_track_artist_release "
            "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
        ],
        description="track FK constraints",
    )
    # Level 2: FK indexes + trigram indexes (parallel)
    run_sql_statements_parallel(
        db_url,
        [
            "CREATE INDEX IF NOT EXISTS idx_release_track_release_id ON release_track(release_id)",
            "CREATE INDEX IF NOT EXISTS idx_release_track_artist_release_id "
            "ON release_track_artist(release_id)",
            "CREATE INDEX IF NOT EXISTS idx_release_track_title_trgm "
            "ON release_track USING GIN (lower(f_unaccent(title)) gin_trgm_ops)",
            "CREATE INDEX IF NOT EXISTS idx_release_track_artist_name_trgm "
            "ON release_track_artist USING GIN (lower(f_unaccent(artist_name)) gin_trgm_ops)",
        ],
        description="track indexes",
    )

    # -- prune (optional)
    if library_db:
        run_step(
            "Prune to library matches",
            [python, str(SCRIPT_DIR / "verify_cache.py"), "--prune", str(library_db), db_url],
        )
    else:
        logger.info("Skipping prune step (no library.db provided)")

    # -- vacuum
    run_vacuum(db_url)

    # -- set_tables_logged (restore WAL durability for consumers)
    set_tables_logged(db_url)

    # -- report
    report_sizes(db_url)


def _run_database_build(
    db_url: str,
    csv_dir: Path,
    library_db: Path | None,
    python: str,
    *,
    target_db_url: str | None = None,
    library_labels: Path | None = None,
    label_hierarchy: Path | None = None,
    catalog_source: str | None = None,
    catalog_db_url: str | None = None,
    state: PipelineState | None = None,
    state_file: Path | None = None,
) -> None:
    """Database build: create_schema through vacuum.

    When *state* is provided, completed steps are skipped and progress
    is saved to *state_file* after each step.

    When *target_db_url* is provided, matched releases are copied to the
    target database instead of pruning the source in place.

    When *library_labels* is provided, the CSV is passed to the dedup step
    for label-aware ranking.  When *catalog_source*/*catalog_db_url* are
    provided but *library_labels* is not, labels are extracted automatically
    before dedup.
    """

    def _save_state() -> None:
        if state is not None and state_file is not None:
            state.save(str(state_file))

    wait_for_postgres(db_url)

    # -- create_schema
    if state and state.is_completed("create_schema"):
        logger.info("Skipping create_schema (already completed)")
    else:
        run_sql_file(db_url, SCHEMA_DIR / "create_database.sql")
        run_sql_file(db_url, SCHEMA_DIR / "create_functions.sql")
        if state:
            state.mark_completed("create_schema")
            _save_state()

    # -- set_tables_unlogged (skip WAL writes during bulk import)
    set_tables_unlogged(db_url)

    # -- import_csv (base tables, artwork, cache_metadata, track counts)
    if state and state.is_completed("import_csv"):
        logger.info("Skipping import_csv (already completed)")
    else:
        run_step(
            "Import base CSVs",
            [python, str(SCRIPT_DIR / "import_csv.py"), "--base-only", str(csv_dir), db_url],
        )
        if state:
            state.mark_completed("import_csv")
            _save_state()

    # -- create_indexes (base trigram indexes, run in parallel)
    if state and state.is_completed("create_indexes"):
        logger.info("Skipping create_indexes (already completed)")
    else:
        # Ensure pg_trgm extension exists (idempotent, must be serial)
        run_sql_file(db_url, SCHEMA_DIR / "create_functions.sql")
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        conn.close()

        run_sql_statements_parallel(
            db_url,
            [
                "CREATE INDEX IF NOT EXISTS idx_release_artist_name_trgm "
                "ON release_artist USING GIN (lower(f_unaccent(artist_name)) gin_trgm_ops)",
                "CREATE INDEX IF NOT EXISTS idx_release_title_trgm "
                "ON release USING GIN (lower(f_unaccent(title)) gin_trgm_ops)",
            ],
            description="base trigram indexes",
        )
        if state:
            state.mark_completed("create_indexes")
            _save_state()

    # -- dedup (deduplicate by master_id)
    if state and state.is_completed("dedup"):
        logger.info("Skipping dedup (already completed)")
    else:
        # Resolve library labels CSV for label-aware dedup
        labels_csv = library_labels
        if labels_csv is None and catalog_source is not None and catalog_db_url is not None:
            labels_csv = Path(tempfile.mkdtemp(prefix="discogs_labels_")) / "library_labels.csv"
            run_step(
                "Extract WXYC library labels",
                [
                    "wxyc-extract-library-labels",
                    "--catalog-source",
                    catalog_source,
                    "--catalog-db-url",
                    catalog_db_url,
                    "--output",
                    str(labels_csv),
                ],
            )

        dedup_cmd = [python, str(SCRIPT_DIR / "dedup_releases.py")]
        if labels_csv is not None:
            dedup_cmd.extend(["--library-labels", str(labels_csv)])
        if label_hierarchy is not None:
            dedup_cmd.extend(["--label-hierarchy", str(label_hierarchy)])
        dedup_cmd.append(db_url)

        run_step("Deduplicate releases", dedup_cmd)
        if state:
            state.mark_completed("dedup")
            _save_state()

    # -- import_tracks (filtered to surviving release IDs)
    if state and state.is_completed("import_tracks"):
        logger.info("Skipping import_tracks (already completed)")
    else:
        run_step(
            "Import tracks",
            [python, str(SCRIPT_DIR / "import_csv.py"), "--tracks-only", str(csv_dir), db_url],
        )
        if state:
            state.mark_completed("import_tracks")
            _save_state()

    # -- create_track_indexes (FK constraints, FK indexes, trigram indexes)
    if state and state.is_completed("create_track_indexes"):
        logger.info("Skipping create_track_indexes (already completed)")
    else:
        # Level 1: FK constraints (parallel)
        run_sql_statements_parallel(
            db_url,
            [
                "DO $$ BEGIN "
                "ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE; "
                "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
                "DO $$ BEGIN "
                "ALTER TABLE release_track_artist ADD CONSTRAINT fk_release_track_artist_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE; "
                "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
            ],
            description="track FK constraints",
        )
        # Level 2: FK indexes + trigram indexes (parallel)
        run_sql_statements_parallel(
            db_url,
            [
                "CREATE INDEX IF NOT EXISTS idx_release_track_release_id "
                "ON release_track(release_id)",
                "CREATE INDEX IF NOT EXISTS idx_release_track_artist_release_id "
                "ON release_track_artist(release_id)",
                "CREATE INDEX IF NOT EXISTS idx_release_track_title_trgm "
                "ON release_track USING GIN (lower(f_unaccent(title)) gin_trgm_ops)",
                "CREATE INDEX IF NOT EXISTS idx_release_track_artist_name_trgm "
                "ON release_track_artist USING GIN (lower(f_unaccent(artist_name)) gin_trgm_ops)",
            ],
            description="track indexes",
        )
        if state:
            state.mark_completed("create_track_indexes")
            _save_state()

    # -- prune (or copy-to, optional)
    if state and state.is_completed("prune"):
        logger.info("Skipping prune/copy-to (already completed)")
    elif library_db and target_db_url:
        run_step(
            "Copy matched releases to target database",
            [
                python,
                str(SCRIPT_DIR / "verify_cache.py"),
                "--copy-to",
                target_db_url,
                str(library_db),
                db_url,
            ],
        )
        if state:
            state.mark_completed("prune")
            _save_state()
    elif library_db:
        run_step(
            "Prune to library matches",
            [python, str(SCRIPT_DIR / "verify_cache.py"), "--prune", str(library_db), db_url],
        )
        if state:
            state.mark_completed("prune")
            _save_state()
    else:
        logger.info("Skipping prune step (no library.db provided)")
        if state:
            state.mark_completed("prune")
            _save_state()

    # -- vacuum (on target DB if using copy-to, otherwise source)
    vacuum_db = target_db_url if target_db_url else db_url
    if state and state.is_completed("vacuum"):
        logger.info("Skipping vacuum (already completed)")
    else:
        run_vacuum(vacuum_db)
        if state:
            state.mark_completed("vacuum")
            _save_state()

    # -- set_tables_logged (restore WAL durability for consumers)
    if state and state.is_completed("set_logged"):
        logger.info("Skipping set_logged (already completed)")
    else:
        set_tables_logged(vacuum_db)
        if state:
            state.mark_completed("set_logged")
            _save_state()

    # -- report
    report_sizes(vacuum_db)


if __name__ == "__main__":
    main()
