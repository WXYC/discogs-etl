#!/usr/bin/env python3
"""Orchestrate the Discogs cache ETL pipeline.

Two modes of operation:

  Full pipeline from XML (steps 1-9):
    python scripts/run_pipeline.py \\
      --xml <releases.xml.gz> \\
      --library-artists <library_artists.txt> \\
      [--converter <path/to/discogs-xml-converter>] \\
      [--library-db <library.db>] \\
      [--wxyc-db-url <mysql://user:pass@host:port/db>] \\
      [--database-url <url>]

  Database build from pre-filtered CSVs (steps 4-9):
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
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).parent.parent))
from lib.pipeline_state import STEP_NAMES, PipelineState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
SCHEMA_DIR = SCRIPT_DIR.parent / "schema"

# Maximum seconds to wait for Postgres to become ready.
PG_CONNECT_TIMEOUT = 30


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
        help="Generate library.db from the WXYC MySQL catalog via SSH before "
        "running the pipeline. Requires LIBRARY_SSH_HOST, LIBRARY_SSH_USER, "
        "LIBRARY_DB_HOST, LIBRARY_DB_USER, LIBRARY_DB_PASSWORD, and "
        "LIBRARY_DB_NAME environment variables. Conflicts with --library-db.",
    )
    parser.add_argument(
        "--wxyc-db-url",
        type=str,
        default=None,
        metavar="URL",
        help="MySQL connection URL for WXYC catalog database "
        "(e.g. mysql://user:pass@host:port/dbname). "
        "Enriches library_artists.txt with alternate names and cross-references, "
        "and extracts label preferences for label-aware dedup. "
        "Requires --library-db.",
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

    if args.generate_library_db and args.library_db:
        parser.error("--generate-library-db and --library-db are mutually exclusive")

    if args.wxyc_db_url and not args.library_db and not args.generate_library_db:
        parser.error("--library-db or --generate-library-db is required when using --wxyc-db-url")

    if args.target_db_url and not args.library_db and not args.generate_library_db:
        parser.error(
            "--library-db or --generate-library-db is required when using --target-db-url"
        )

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
        logger.info("  %s", line.rstrip("\n"))
    proc.wait()
    elapsed = time.monotonic() - start
    if proc.returncode != 0:
        logger.error("Step failed (exit %d) after %.1fs", proc.returncode, elapsed)
        sys.exit(1)
    logger.info("  completed in %.1fs", elapsed)


def run_vacuum(db_url: str) -> None:
    """Run VACUUM FULL on all pipeline tables."""
    logger.info("Running VACUUM FULL ...")
    tables = [
        "release",
        "release_artist",
        "release_label",
        "release_track",
        "release_track_artist",
        "cache_metadata",
    ]
    conn = psycopg.connect(db_url, autocommit=True)
    for table in tables:
        logger.info("  VACUUM FULL %s ...", table)
        try:
            with conn.cursor() as cur:
                cur.execute(f"VACUUM FULL {table}")
        except psycopg.Error as exc:
            logger.warning("  VACUUM FULL %s failed: %s", table, exc)
    conn.close()
    logger.info("  VACUUM complete.")


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
) -> None:
    """Convert Discogs XML to CSV using discogs-xml-converter.

    Replaces the old three-step process (xml2db + fix_newlines + filter_csv)
    with a single call to the Rust binary.
    """
    cmd = [converter, str(xml_file), "--output-dir", str(output_dir)]
    if library_artists:
        cmd.extend(["--library-artists", str(library_artists)])
    run_step("Convert and filter XML to CSV", cmd)


def enrich_library_artists(
    library_db: Path,
    library_artists_out: Path,
    wxyc_db_url: str | None = None,
) -> None:
    """Step 2.5: Enrich library_artists.txt with WXYC cross-reference data."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "enrich_library_artists.py"),
        "--library-db",
        str(library_db),
        "--output",
        str(library_artists_out),
    ]
    if wxyc_db_url:
        cmd.extend(["--wxyc-db-url", wxyc_db_url])
    run_step("Enrich library artists", cmd)


def _load_or_create_state(args: argparse.Namespace) -> PipelineState:
    """Load existing state for --resume, or create fresh state.

    When --resume is set and no state file exists, infers state from
    the database structure.
    """
    csv_dir_str = str(args.csv_dir.resolve())
    state_file = args.state_file

    if args.resume and state_file.exists():
        logger.info("Loading pipeline state from %s", state_file)
        state = PipelineState.load(state_file)
        state.validate_resume(db_url=args.database_url, csv_dir=csv_dir_str)
        return state

    if args.resume and not state_file.exists():
        logger.info("No state file found; inferring state from database")
        from lib.db_introspect import infer_pipeline_state

        state = infer_pipeline_state(args.database_url)
        state.csv_dir = csv_dir_str
        completed = [s for s in STEP_NAMES if state.is_completed(s)]
        if completed:
            logger.info("  Inferred completed steps: %s", ", ".join(completed))
        else:
            logger.info("  No completed steps detected; starting from scratch")
        return state

    return PipelineState(db_url=args.database_url, csv_dir=csv_dir_str)


def generate_library_db(output_path: Path) -> None:
    """Step 0: Generate library.db from WXYC MySQL catalog via SSH."""
    run_step(
        "Generate library.db from MySQL",
        [sys.executable, str(SCRIPT_DIR / "export_to_sqlite.py")],
        env={**os.environ, "LIBRARY_DB_OUTPUT_PATH": str(output_path)},
    )


def main() -> None:
    args = parse_args()

    python = sys.executable
    db_url = args.database_url
    pipeline_start = time.monotonic()

    # Generate library.db if requested
    if args.generate_library_db:
        generated_db = Path(tempfile.mkdtemp(prefix="discogs_library_")) / "library.db"
        generate_library_db(generated_db)
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
        with tempfile.TemporaryDirectory(prefix="discogs_pipeline_") as tmpdir:
            tmp = Path(tmpdir)
            csv_dir = tmp / "csv"

            # -- enrich_artists: Generate/enrich library_artists.txt from library.db
            library_artists_path = args.library_artists
            if args.library_db:
                enriched_artists = tmp / "library_artists.txt"
                enrich_library_artists(args.library_db, enriched_artists, args.wxyc_db_url)
                library_artists_path = enriched_artists

            # -- convert_and_filter: XML to CSV (with optional artist filtering)
            convert_and_filter(args.xml, csv_dir, args.converter, library_artists_path)

            # Auto-detect label_hierarchy.csv from converter output
            hierarchy_csv = args.label_hierarchy
            if hierarchy_csv is None:
                auto_hierarchy = csv_dir / "label_hierarchy.csv"
                if auto_hierarchy.exists():
                    logger.info("Auto-detected label_hierarchy.csv from converter output")
                    hierarchy_csv = auto_hierarchy

            # -- database build (create_schema through vacuum)
            _run_database_build(
                db_url,
                csv_dir,
                args.library_db,
                python,
                library_labels=args.library_labels,
                label_hierarchy=hierarchy_csv,
                wxyc_db_url=args.wxyc_db_url,
            )
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
            wxyc_db_url=args.wxyc_db_url,
            state=state,
            state_file=args.state_file,
        )

    total = time.monotonic() - pipeline_start
    logger.info("Pipeline complete in %.1f minutes.", total / 60)


def _run_database_build(
    db_url: str,
    csv_dir: Path,
    library_db: Path | None,
    python: str,
    *,
    target_db_url: str | None = None,
    library_labels: Path | None = None,
    label_hierarchy: Path | None = None,
    wxyc_db_url: str | None = None,
    state: PipelineState | None = None,
    state_file: Path | None = None,
) -> None:
    """Database build: create_schema through vacuum.

    When *state* is provided, completed steps are skipped and progress
    is saved to *state_file* after each step.

    When *target_db_url* is provided, matched releases are copied to the
    target database instead of pruning the source in place.

    When *library_labels* is provided, the CSV is passed to the dedup step
    for label-aware ranking.  When *wxyc_db_url* is provided but
    *library_labels* is not, labels are extracted automatically before dedup.
    """

    def _save_state() -> None:
        if state is not None and state_file is not None:
            state.save(state_file)

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

    # -- create_indexes (base trigram indexes, strip CONCURRENTLY for fresh DB)
    if state and state.is_completed("create_indexes"):
        logger.info("Skipping create_indexes (already completed)")
    else:
        run_sql_file(db_url, SCHEMA_DIR / "create_indexes.sql", strip_concurrently=True)
        if state:
            state.mark_completed("create_indexes")
            _save_state()

    # -- dedup (deduplicate by master_id)
    if state and state.is_completed("dedup"):
        logger.info("Skipping dedup (already completed)")
    else:
        # Resolve library labels CSV for label-aware dedup
        labels_csv = library_labels
        if labels_csv is None and wxyc_db_url is not None:
            labels_csv = Path(tempfile.mkdtemp(prefix="discogs_labels_")) / "library_labels.csv"
            run_step(
                "Extract WXYC library labels",
                [
                    python,
                    str(SCRIPT_DIR / "extract_library_labels.py"),
                    "--wxyc-db-url",
                    wxyc_db_url,
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
        run_sql_file(db_url, SCHEMA_DIR / "create_track_indexes.sql", strip_concurrently=True)
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

    # -- report
    report_sizes(vacuum_db)


if __name__ == "__main__":
    main()
