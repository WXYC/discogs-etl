#!/usr/bin/env python3
"""Orchestrate the Discogs cache ETL pipeline.

Two modes of operation:

  Full pipeline from XML (steps 1-11):
    python scripts/run_pipeline.py \\
      --xml <releases.xml.gz> \\
      [--library-artists <library_artists.txt> | --library-db <library.db>] \\
      [--converter <path/to/discogs-xml-converter>] \\
      [--wxyc-db-url <mysql://user:pass@host:port/db>] \\
      [--database-url <url>]

  Database build from pre-filtered CSVs (steps 4-11):
    python scripts/run_pipeline.py \\
      --csv-dir <path/to/filtered/> \\
      [--library-db <library.db>] \\
      [--database-url <url>] \\
      [--resume] [--state-file <path>]

Environment variables:
    DATABASE_URL_DISCOGS  Preferred default for --database-url.
    DATABASE_URL          Deprecated fallback; emits a warning when used.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

import psycopg
from wxyc_etl.state import PipelineState

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402

STEP_NAMES = [
    "create_schema",
    "import_csv",
    "create_indexes",
    "dedup",
    "import_tracks",
    "create_track_indexes",
    "prune",
    "derive_va_release",
    "vacuum",
    "set_logged",
]

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
SCHEMA_DIR = SCRIPT_DIR.parent / "schema"

# Maximum seconds to wait for Postgres to become ready.
PG_CONNECT_TIMEOUT = 30

# Minimum fraction of ``release`` rows that must be covered by child-table
# rows for the reload invariant to hold (discogs-etl#298). Every Discogs
# release carries >=1 artist credit and (essentially) a tracklist, so a healthy
# WXYC-filtered cache sits near 100% coverage for both children. The documented
# catastrophe — a partial-rebuild abort that TRUNCATEd the children but never
# reloaded them — was 0.7%. This floor sits comfortably between, so it catches
# the empty-index state without false-positiving a legitimate rebuild (a false
# positive would hard-fail the whole monthly rebuild).
MIN_CHILD_COVERAGE_RATIO = 0.5

# Tables managed by the pipeline (shared by run_vacuum, set_tables_unlogged,
# set_tables_logged).
#
# Every table with a FK to ``release`` must be listed here. PostgreSQL prohibits
# an UNLOGGED table from being referenced by a LOGGED table (and vice versa),
# so any FK referrer of ``release`` that is missing from this list will cause
# ``ALTER TABLE release SET UNLOGGED`` to fail with ``could not change table
# "release" to unlogged because it references logged table "<missing>"`` (see
# #105). The ``test_pipeline_tables_covers_all_release_fk_referrers`` unit test
# enforces this invariant against ``schema/create_database.sql``.
DEFAULT_DATABASE_URL = "postgresql://localhost:5432/discogs"


def _resolve_database_url(cli_value: str | None) -> str:
    """Resolve the cache database URL.

    Priority: CLI flag > DATABASE_URL_DISCOGS env > DATABASE_URL env (deprecated)
    > built-in default. The generic DATABASE_URL fallback emits a stderr
    deprecation warning to nudge consumers to the service-specific name.
    """
    if cli_value is not None:
        return cli_value
    discogs_env = os.environ.get("DATABASE_URL_DISCOGS")
    if discogs_env:
        return discogs_env
    generic_env = os.environ.get("DATABASE_URL")
    if generic_env:
        print(
            "warning: DATABASE_URL is deprecated; set DATABASE_URL_DISCOGS instead.",
            file=sys.stderr,
        )
        return generic_env
    return DEFAULT_DATABASE_URL


PIPELINE_TABLES = [
    "release",
    "release_artist",
    "release_label",
    "release_genre",
    "release_style",
    "release_track",
    "release_track_artist",
    "release_video",
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
        "--xml-type",
        choices=["releases", "artists", "labels", "masters"],
        default=None,
        help="Forwarded to discogs-xml-converter --xml-type. Skips per-file "
        "root-element auto-detection. Required when --xml points at a FIFO "
        "(named pipe) — auto-detection's open/close cycle would SIGPIPE the "
        "upstream writer before the real scan starts.",
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
        default=None,
        help="PostgreSQL connection URL for the cache database "
        "(default: DATABASE_URL_DISCOGS env var, then DATABASE_URL with a "
        "deprecation warning, then postgresql://localhost:5432/discogs).",
    )
    parser.add_argument(
        "--target-db-url",
        type=str,
        default=None,
        metavar="URL",
        help="[DEPRECATED] Copy matched releases to a separate target database "
        "instead of pruning in place. Requires --library-db. Will be removed in "
        "a future release.",
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
    parser.add_argument(
        "--truncate-existing",
        action="store_true",
        help="Pass --truncate-existing to scripts/import_csv.py so the cache "
        "tables are wiped before COPY. Use when re-running a rebuild against "
        "a DB with stale rows from a prior failed attempt. Preserves the "
        "entity schema and alembic_version.",
    )
    parser.add_argument(
        "--fresh-rebuild",
        action="store_true",
        help="Drop and recreate the discogs-cache schema before importing "
        "(applies schema/drop_core_tables.sql + create_database.sql). "
        "Use when the intent is a from-scratch rebuild, e.g. recovering "
        "from cache corruption. Default is incremental: schema is "
        "preserved and LML-back-patched release.artwork_url + "
        "release.artwork_checked_at survive the rebuild via the "
        "staging-table UPSERT path (WXYC/discogs-etl#242).",
    )

    args = parser.parse_args(argv)

    args.database_url = _resolve_database_url(args.database_url)

    if args.target_db_url is not None:
        print(
            "warning: --target-db-url is deprecated; the cache convention is "
            "moving to a single --database-url. The flag still works for now.",
            file=sys.stderr,
        )

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

    if args.library_artists and args.library_db:
        parser.error(
            "--library-artists and --library-db are mutually exclusive. "
            "--library-artists yields a converter artist-only filter (~4M releases); "
            "--library-db yields the converter's pair-wise (artist, title) filter (~50K)."
        )

    return args


def _redact_dsn(db_url: str) -> str:
    """Strip userinfo from a connection string, keeping host/port/database.

    Same shape as scripts/resolve_collisions.py and derive_va_release.py's
    _describe_target. A DSN with no userinfo is returned unchanged.
    """
    return db_url.split("@")[-1]


def wait_for_postgres(db_url: str) -> None:
    """Poll Postgres until a connection succeeds or timeout is reached."""
    # Redacted: this logs on every run, success or failure, and
    # rebuild-cache-bootstrap.sh tees the pipeline log to the S3 log bucket.
    # Unredacted, it published the cache credential on the happy path (#361).
    logger.info("Waiting for PostgreSQL at %s ...", _redact_dsn(db_url))
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
        # Raise rather than sys.exit so any logger plugin that captures
        # SystemExit (Sentry-enabled run #3 swallowed it; #180) cannot
        # mask the failure. Default exception handler still exits non-zero.
        raise subprocess.CalledProcessError(proc.returncode, cmd)
    logger.info("  completed in %.1fs", elapsed)


def run_step_safe(description: str, cmd: list[str], **kwargs) -> None:
    """Run a step via run_step(), keeping a credentialed argv out of any
    exception that escapes a failed step (#361).

    dedup_releases.py, import_csv.py, and verify_cache.py all take the
    cache database URL positionally rather than via environment variable;
    the discogs-xml-converter binary and the wxyc-catalog CLIs take theirs
    as a flag. The distinction does not matter here, because
    CalledProcessError.__str__ embeds the *full* argv it was constructed
    with -- credentials and all. run_step() already logs the failure safely
    (exit code + elapsed time, no argv) before raising, but left uncaught
    that string is exactly what Python's default exception handler prints
    to stderr, and rebuild-cache-bootstrap.sh tees stdout/stderr straight
    to the rebuild log bucket on every step failure.

    Same precedent as run_derive_va_release_step below: log only the exit
    code, never str(exc) or cmd. Unlike that soft-fail step, these steps
    are not optional, so this re-raises rather than swallowing the failure
    -- but the re-raised CalledProcessError carries *description* (a plain
    label) as its ``cmd``, never the real argv, so its own __str__ stays
    credential-free however far up the call stack it is printed or logged.
    ``from None`` suppresses Python's implicit exception chaining, which
    would otherwise still print the original, argv-bearing exception as
    "the above exception" context.

    Scope, so the guarantee is not overread: this closes the *traceback*
    channel only. Two other channels still carry the DSN into the same log
    and are deliberately out of this function's reach --
    (1) the child scripts log their own connection string at startup, which
    belongs to #227 and needs those files edited; and (2) Sentry's
    excepthook integration captures frame locals by default, so the
    argv-bearing ``cmd`` local survives in the event payload even though
    ``from None`` correctly stops its chain walk. The durable fix for both
    is #361's own suggestion: have those scripts read DATABASE_URL_DISCOGS
    from a merged os.environ instead of argv.
    """
    try:
        run_step(description, cmd, **kwargs)
    except subprocess.CalledProcessError as exc:
        logger.error("%s failed (exit %d)", description, exc.returncode)
        raise subprocess.CalledProcessError(exc.returncode, description) from None


def run_derive_va_release_step(db_url: str, python: str) -> bool:
    """Best-effort derive_va_release step (#344), shared by both build paths.

    Passes ``--floor 0``: post-rebuild the pre-existing table holds
    pre-rebuild ids (stale garbage), so an honest empty derivation beats the
    floor guard's rollback to it — the floor protects the daily sync path,
    not this one.

    Soft-fail by design: the table is an ancillary derived artifact consumed
    only by LML's offline compilation scripts, and the xml/direct-pg paths
    have no resume state — a transient failure here must not discard a
    multi-hour rebuild or strand the pipeline tables UNLOGGED. On failure the
    stale pre-rebuild table is dropped best-effort (absent is honest and
    self-heals on the next daily sync; stale ids would silently mismatch the
    rebuilt cache) and the build continues. Returns True when the derivation
    succeeded (callers use this to decide whether to mark resume state).
    """
    cmd = [
        python,
        str(SCRIPT_DIR / "derive_va_release.py"),
        "--database-url",
        db_url,
        "--floor",
        "0",
    ]
    try:
        run_step("Derive va_release", cmd)
    except subprocess.CalledProcessError as exc:
        # Log only the exit code — str(exc) embeds the full argv, including
        # the credentialed --database-url, and this warning rides the
        # wxyc_etl/Sentry logging pipeline.
        logger.warning(
            "derive_va_release failed (exit %d); dropping the stale table and "
            "continuing (the next daily sync re-derives it)",
            exc.returncode,
        )
        try:
            # Same bounded lock wait as the derive script itself: this drop
            # contends for the same ACCESS EXCLUSIVE lock, and an unbounded
            # wait here would stall the rebuild at exactly the step whose
            # soft-fail exists to keep it moving.
            conn = psycopg.connect(db_url, autocommit=True, options="-c lock_timeout=30s")
            try:
                conn.execute("DROP TABLE IF EXISTS va_release")
            finally:
                conn.close()
        except Exception as drop_exc:
            logger.warning("could not drop stale va_release after derive failure: %s", drop_exc)
        return False
    return True


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
                'release_track', 'release_track_artist', 'cache_metadata',
                'master', 'master_artist'
            )
            ORDER BY pg_total_relation_size(relid) DESC
        """)
        for row in cur.fetchall():
            logger.info("  %-25s %10s rows   %s", row[0], f"{row[1]:,}", row[2])
    conn.close()


class ReloadInvariantResult(NamedTuple):
    """Outcome of the release-vs-children reload invariant (discogs-etl#298).

    A ``NamedTuple`` rather than a ``@dataclass`` on purpose: several test
    modules load ``run_pipeline`` via ``importlib`` without registering it in
    ``sys.modules``, and under ``from __future__ import annotations`` the
    dataclass machinery looks the module up there (``_is_type`` →
    ``sys.modules.get(cls.__module__)``) and blows up at class-creation time.
    ``NamedTuple`` resolves field names without that lookup, so it is robust to
    any loader.

    Attributes:
        ok: True when child coverage meets the floor (or the cache is empty).
        release_count: rows in ``release``.
        artist_coverage: distinct-release coverage of ``release_artist``
            (``distinct release_id / release_count``), or ``None`` when the
            cache is empty and coverage is undefined.
        track_coverage: distinct-release coverage of ``release_track``, or
            ``None`` when undefined.
        reason: human-readable explanation naming the offending child
            table(s); empty string when ``ok``.
    """

    ok: bool
    release_count: int
    artist_coverage: float | None
    track_coverage: float | None
    reason: str = ""


def evaluate_reload_invariant(
    *,
    release_count: int,
    artist_release_count: int,
    track_release_count: int,
    min_ratio: float = MIN_CHILD_COVERAGE_RATIO,
) -> ReloadInvariantResult:
    """Decide whether the ``release_*`` children cover enough of ``release``.

    A partial-rebuild abort can leave ``release`` fully populated while the
    child tables sit empty (they are TRUNCATEd up front, then reloaded across
    later steps). This compares each child's distinct-release coverage against
    ``release`` and flags the state when either falls below ``min_ratio``.

    An empty cache (``release_count <= 0``) is a legitimate state — a fresh or
    pre-import DB, or a ``--fresh-rebuild`` — so the invariant holds trivially.

    Args:
        release_count: rows in ``release``.
        artist_release_count: distinct ``release_id`` in ``release_artist``.
        track_release_count: distinct ``release_id`` in ``release_track``.
        min_ratio: minimum acceptable ``child_releases / release_count``.

    Returns:
        A ``ReloadInvariantResult``.
    """
    if release_count <= 0:
        return ReloadInvariantResult(
            ok=True, release_count=release_count, artist_coverage=None, track_coverage=None
        )

    artist_coverage = artist_release_count / release_count
    track_coverage = track_release_count / release_count

    problems: list[str] = []
    if artist_coverage < min_ratio:
        problems.append(
            f"release_artist covers {artist_coverage:.1%} of releases "
            f"({artist_release_count:,}/{release_count:,})"
        )
    if track_coverage < min_ratio:
        problems.append(
            f"release_track covers {track_coverage:.1%} of releases "
            f"({track_release_count:,}/{release_count:,})"
        )

    if problems:
        return ReloadInvariantResult(
            ok=False,
            release_count=release_count,
            artist_coverage=artist_coverage,
            track_coverage=track_coverage,
            reason=(
                "; ".join(problems)
                + f" — below the {min_ratio:.0%} floor. A partial-rebuild abort "
                "likely TRUNCATEd the release_* children without reloading them."
            ),
        )

    return ReloadInvariantResult(
        ok=True,
        release_count=release_count,
        artist_coverage=artist_coverage,
        track_coverage=track_coverage,
    )


def count_child_coverage(db_url: str) -> tuple[int, int, int]:
    """Return ``(release_count, artist_release_count, track_release_count)``.

    ``artist_release_count`` / ``track_release_count`` are the distinct
    ``release_id`` counts in ``release_artist`` / ``release_track``. When the
    ``release`` table does not exist yet (a fresh DB, before create_schema),
    returns ``(0, 0, 0)`` so the preflight can run unconditionally.

    Queries scan indexed columns on a ≤~260K-release cache — sub-second.
    """
    conn = psycopg.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('release')")
            if cur.fetchone()[0] is None:
                return (0, 0, 0)
            cur.execute("SELECT count(*) FROM release")
            release_count = cur.fetchone()[0]
            cur.execute("SELECT count(DISTINCT release_id) FROM release_artist")
            artist_release_count = cur.fetchone()[0]
            cur.execute("SELECT count(DISTINCT release_id) FROM release_track")
            track_release_count = cur.fetchone()[0]
            return (release_count, artist_release_count, track_release_count)
    finally:
        conn.close()


def write_keep_release_ids(db_url: str, output_path: Path) -> int:
    """Read WXYC's pinned-release overrides from lml_cache.library_release_override
    (if present) and write them as a newline-separated allowlist file consumed by
    dedup_releases.py and verify_cache.py's --keep-release-ids (discogs-etl#327).

    Read-only against lml_cache -- never creates, migrates, or truncates it
    (CLAUDE.md "entity.* schema ownership").

    The degrade-to-empty path is scoped precisely to the table or schema being
    absent (``UndefinedTable`` / ``InvalidSchemaName`` -- no LML bootstrap yet,
    or a local/CI DB): writes an empty file and returns 0, a no-op exemption.
    Every other error propagates and aborts the rebuild -- a column-name typo
    (``UndefinedColumn``) or a missing SELECT/USAGE grant
    (``InsufficientPrivilege``) must fail loudly here, not surface only as a
    parity failure at the next rebuild or, worse, silently evict every pinned
    release. (This is why we query the table directly and catch narrow errors
    rather than gate on ``to_regclass``, which returns NULL indistinguishably
    for "absent" and "not visible to this role".)

    A NULL ``discogs_release_id`` can't pin anything, so it is skipped rather
    than written as the literal ``"None"`` (which would ValueError at the
    consuming ``int()`` and abort the pipeline).
    """
    conn = psycopg.connect(db_url)
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    "SELECT DISTINCT discogs_release_id FROM lml_cache.library_release_override"
                )
                ids = [row[0] for row in cur.fetchall() if row[0] is not None]
            except (psycopg.errors.UndefinedTable, psycopg.errors.InvalidSchemaName):
                logger.info(
                    "lml_cache.library_release_override not found; "
                    "writing empty release-id allowlist"
                )
                output_path.write_text("")
                return 0
    finally:
        conn.close()

    output_path.write_text("\n".join(str(i) for i in ids))
    logger.info("Wrote %d pinned release_id override(s) to %s", len(ids), output_path)
    return len(ids)


def prune_audit_dump_args() -> list[str]:
    """Extra ``verify_cache.py`` args to capture the prune classification, or [].

    Opt-in instrumentation for the discogs-etl#217 coverage audit. When the
    ``PRUNE_AUDIT_DUMP_DIR`` environment variable is set, the prune step also
    dumps its keep/prune/review id sets (plus a second, no-ANV classification
    pass for the #305 uplift diff) under
    ``$PRUNE_AUDIT_DUMP_DIR/prune-audit-<UTC-date>``. Default off: a normal
    monthly rebuild leaves the env unset and ``verify_cache.py`` runs unchanged
    with no extra classification pass.

    The UTC date stamp (matching the rebuild's UTC scheduling) keeps a re-run on
    a later day from clobbering an earlier capture. The dump is read-only with
    respect to cache data — it only writes local artifact files.
    """
    base = os.environ.get("PRUNE_AUDIT_DUMP_DIR")
    if not base:
        return []
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return ["--dump-classification", str(Path(base) / f"prune-audit-{date}")]


def check_reload_invariant(
    db_url: str,
    *,
    raise_on_violation: bool,
    min_ratio: float = MIN_CHILD_COVERAGE_RATIO,
) -> ReloadInvariantResult:
    """Read child coverage and enforce the reload invariant (discogs-etl#298).

    ``raise_on_violation`` picks the failure mode:

    * ``False`` (preflight, before the truncating base step): a violation
      means a *prior* run left the ``release``-full / children-empty state.
      This run is about to repopulate, so we log a loud WARNING (operators
      see recovery is happening) and continue.
    * ``True`` (post-reload gate, before ``report_sizes`` / the success
      notification): a violation means *this* run finished with an empty
      index — e.g. an empty/missing tracks CSV COPYed as zero rows. Raise so
      the run fails before it reports success.
    """
    release_count, artist_release_count, track_release_count = count_child_coverage(db_url)
    result = evaluate_reload_invariant(
        release_count=release_count,
        artist_release_count=artist_release_count,
        track_release_count=track_release_count,
        min_ratio=min_ratio,
    )
    if result.ok:
        logger.info(
            "reload invariant healthy: release=%d artist_cov=%s track_cov=%s",
            result.release_count,
            f"{result.artist_coverage:.1%}" if result.artist_coverage is not None else "n/a",
            f"{result.track_coverage:.1%}" if result.track_coverage is not None else "n/a",
            extra={"step": "reload_invariant"},
        )
        return result

    if raise_on_violation:
        raise RuntimeError(f"[#298] cache reload invariant violated: {result.reason}")

    logger.warning(
        "[#298] cache reload invariant violated on entry (recovering this run): %s",
        result.reason,
        extra={"step": "reload_invariant"},
    )
    return result


def convert_and_filter(
    xml_file: Path,
    output_dir: Path,
    converter: str,
    library_artists: Path | None = None,
    library_db: Path | None = None,
    database_url: str | None = None,
    xml_type: str | None = None,
) -> None:
    """Convert Discogs XML to CSV using discogs-xml-converter.

    Replaces the old three-step process (xml2db + fix_newlines + filter_csv)
    with a single call to the Rust binary.

    When database_url is provided, releases are streamed directly into
    PostgreSQL via COPY (`import` subcommand) instead of being written to
    CSV files (`build` subcommand). Supplementary CSVs (artist_alias.csv,
    label_hierarchy.csv) are still written to output_dir in either mode.

    library_db, when set, is forwarded to the converter so the streaming
    scanner narrows release output to (artist, title) pairs in the WXYC
    library — ~50K releases instead of the artist-only ~4M. library_artists
    and library_db are mutually exclusive (the converter rejects both).

    xml_type, when set, skips the converter's per-file root-element
    auto-detection. Required for FIFO inputs (the rebuild-cache.sh
    monthly path) where the auto-detect open/close pattern would
    SIGPIPE the upstream curl writer before the real scan opens the file.
    """
    subcommand = "import" if database_url else "build"
    cmd = [converter, subcommand, str(xml_file), "--data-dir", str(output_dir)]
    if library_artists:
        cmd.extend(["--library-artists", str(library_artists)])
    if library_db:
        cmd.extend(["--library-db", str(library_db)])
    if database_url:
        cmd.extend(["--database-url", database_url])
    if xml_type:
        cmd.extend(["--xml-type", xml_type])
    description = (
        "Convert and import XML to PostgreSQL" if database_url else "Convert and filter XML to CSV"
    )
    # run_step_safe even though the converter takes --database-url as a flag
    # rather than positionally: CalledProcessError.__str__ embeds the whole
    # argv either way, and in --direct-pg mode that argv holds the cache DSN.
    run_step_safe(description, cmd)


def _infer_pipeline_state(db_url: str, csv_dir: str) -> PipelineState:
    """Infer pipeline state from database structure.

    Inspects table existence, row counts, index names, and constraint names
    to determine which pipeline steps have already completed. Steps that
    cannot be inferred (prune, derive_va_release, vacuum) are left as pending
    since they are safe to re-run.
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

            # dedup: post-swap FK constraint present?
            #
            # The dedup copy-swap does NOT drop release.master_id — the column is
            # in DEDUP_TABLES (scripts/dedup_releases.py) and is load-bearing for the
            # #317 masters phase (import_masters / --masters-only do
            # SELECT DISTINCT master_id FROM release), so it MUST persist. The old
            # "master_id column gone?" signal was a stale pre-#129 contract that never
            # fires now, wrongly forcing a resume to re-run dedup.
            #
            # The durable post-swap artifact is the NAMED FK fk_release_artist_release,
            # which add_base_constraints_and_indexes adds only after the swap
            # (scripts/dedup_releases.py). Before dedup the base schema's inline FK on
            # release_artist is auto-named release_artist_release_id_fkey (a different
            # name) and is dropped by the swap's DROP TABLE ... CASCADE, so this
            # constraint name is absent pre-dedup and present post-dedup.
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.table_constraints"
                "  WHERE constraint_name = 'fk_release_artist_release'"
                "    AND constraint_type = 'FOREIGN KEY'"
                ")"
            )
            if not cur.fetchone()[0]:
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

    # prune, derive_va_release, and vacuum cannot be inferred from database state
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
    """Step 0: Generate library.db from WXYC catalog via wxyc-catalog CLI.

    --catalog-db-url is a *second* credential (tubafrenzy MySQL or
    Backend-Service PostgreSQL), so this needs the same argv redaction as the
    cache-DSN steps.
    """
    run_step_safe(
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
        if args.direct_pg:
            # Direct-to-PG mode: create schema first, then converter writes
            # releases directly into PostgreSQL via COPY.
            wait_for_postgres(db_url)
            # create_functions.sql must run BEFORE create_database.sql:
            # create_database.sql's idx_master_title_trgm index expression
            # references f_unaccent (see #104).
            run_sql_file(db_url, SCHEMA_DIR / "create_functions.sql")
            run_sql_file(db_url, SCHEMA_DIR / "create_database.sql")

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
                library_artists=args.library_artists,
                library_db=args.library_db,
                database_url=db_url,
                xml_type=args.xml_type,
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
            # Standard CSV mode. The converter applies whichever filter the
            # operator picked: --library-artists for artist-only (~4M rows)
            # or --library-db for the pair-wise (artist, title) filter
            # (~50K rows). Both narrow the release.csv stream inside the
            # streaming scanner before any disk write.
            convert_and_filter(
                args.xml,
                csv_out,
                args.converter,
                library_artists=args.library_artists,
                library_db=args.library_db,
                xml_type=args.xml_type,
            )

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
                truncate_existing=args.truncate_existing,
                fresh_rebuild=args.fresh_rebuild,
            )

    if keep_csv_dir is not None:
        _run_with_dirs(tmp, csv_dir)
    else:
        with tempfile.TemporaryDirectory(prefix="discogs_pipeline_") as tmpdir:
            tmp_path = Path(tmpdir)
            _run_with_dirs(tmp_path, tmp_path / "csv")


def main() -> None:
    args = parse_args()

    init_logger(repo="discogs-etl", tool="discogs-etl run_pipeline")

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
            truncate_existing=args.truncate_existing,
            fresh_rebuild=args.fresh_rebuild,
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
    # -- read WXYC library pinned overrides once, threaded into every dedup
    # and verify_cache.py invocation below (discogs-etl#327)
    keep_release_ids_path = (
        Path(tempfile.mkdtemp(prefix="discogs_keep_release_ids_")) / "keep_release_ids.txt"
    )
    write_keep_release_ids(db_url, keep_release_ids_path)

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
        run_step_safe(
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
    dedup_cmd.extend(["--keep-release-ids", str(keep_release_ids_path)])
    dedup_cmd.append(db_url)

    run_step_safe("Deduplicate releases", dedup_cmd)

    # -- create_track_indexes (FK constraints, FK indexes, trigram indexes)
    # Level 0: Clean orphan track rows before FK validation (parallel).
    # The live library-metadata-lookup service inserts release_track +
    # release_track_artist rows on every Discogs API miss; during the dedup
    # swap window those land as orphans relative to the post-dedup release
    # table. The base-side parallel fix lives in dedup_releases.py and was
    # shipped in #211; this is the track-side parallel — surfaced by the
    # 2026-05-14 02:20 UTC failure on instance i-03a3040c5367faaf1 (#188).
    run_sql_statements_parallel(
        db_url,
        [
            "DELETE FROM release_track WHERE NOT EXISTS "
            "(SELECT 1 FROM release r WHERE r.id = release_track.release_id)",
            "DELETE FROM release_track_artist WHERE NOT EXISTS "
            "(SELECT 1 FROM release r WHERE r.id = release_track_artist.release_id)",
        ],
        description="clean orphan track rows before FK",
    )
    # Level 1: FK constraints (parallel, NOT VALID for race tolerance).
    # NOT VALID skips re-validation of existing rows so the ADD step can't
    # race-fail on orphans created between cleanup and constraint creation.
    # See dedup_releases.py::add_base_constraints_and_indexes for the
    # equivalent base-side pattern with full reasoning.
    run_sql_statements_parallel(
        db_url,
        [
            "DO $$ BEGIN "
            "ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release "
            "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
            "DO $$ BEGIN "
            "ALTER TABLE release_track_artist ADD CONSTRAINT fk_release_track_artist_release "
            "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID; "
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
        run_step_safe(
            "Prune to library matches",
            [
                python,
                str(SCRIPT_DIR / "verify_cache.py"),
                "--prune",
                str(library_db),
                db_url,
                "--keep-release-ids",
                str(keep_release_ids_path),
                *prune_audit_dump_args(),
            ],
        )
    else:
        logger.info("Skipping prune step (no library.db provided)")

    # -- derive_va_release (#344): re-derive the VA-compilation lookup table
    # now that dedup/prune have finalized release ids. Gated like prune: on a
    # build without library.db nothing consumes the table, and the unpruned
    # cache makes the derivation orders of magnitude more expensive. See
    # run_derive_va_release_step for the --floor 0 and soft-fail rationale.
    if library_db:
        run_derive_va_release_step(db_url, python)
    else:
        logger.info("Skipping derive_va_release step (no library.db provided)")

    # -- vacuum
    run_vacuum(db_url)

    # -- set_tables_logged (restore WAL durability for consumers)
    set_tables_logged(db_url)

    # -- reload-invariant gate (discogs-etl#298) — see _run_database_build.
    check_reload_invariant(db_url, raise_on_violation=True)

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
    truncate_existing: bool = False,
    fresh_rebuild: bool = False,
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

    # -- read WXYC library pinned overrides once, threaded into every dedup
    # and verify_cache.py invocation below (discogs-etl#327)
    keep_release_ids_path = (
        Path(tempfile.mkdtemp(prefix="discogs_keep_release_ids_")) / "keep_release_ids.txt"
    )
    write_keep_release_ids(db_url, keep_release_ids_path)

    # -- create_schema
    if state and state.is_completed("create_schema"):
        logger.info("Skipping create_schema (already completed)")
    else:
        # create_functions.sql must run BEFORE create_database.sql:
        # create_database.sql's idx_master_title_trgm index expression
        # references f_unaccent (see #104).
        if fresh_rebuild:
            # --fresh-rebuild: wipe the core-table subgraph before recreate
            # (WXYC/discogs-etl#242). Without this step create_database.sql
            # is a no-op against a populated DB (every CREATE is IF NOT
            # EXISTS), which is the incremental default that preserves
            # LML-back-patched artwork. drop_core_tables.sql does NOT drop
            # alembic_version or the entity schema (which holds LML-owned
            # identity / reconciliation state on both the artist and release
            # sides — entity.release_identity ships in alembic 0012, the
            # artist-side entity.identity / entity.reconciliation_log are
            # adopted into the chain by alembic 0013).
            logger.warning("--fresh-rebuild: dropping core-table subgraph")
            run_sql_file(db_url, SCHEMA_DIR / "drop_core_tables.sql")
        run_sql_file(db_url, SCHEMA_DIR / "create_functions.sql")
        run_sql_file(db_url, SCHEMA_DIR / "create_database.sql")
        if state:
            state.mark_completed("create_schema")
            _save_state()

    # -- reload-invariant preflight (discogs-etl#298)
    # The base step below TRUNCATEs the release_* children in a committed
    # transaction, then reloads them across several later steps. Before we do
    # that, check whether the *current* DB is already in the release-full /
    # children-empty state a prior partial-rebuild abort would have left. We
    # warn rather than raise — this run is about to repopulate — so operators
    # see that recovery is under way. Runs after create_schema so the tables
    # exist; count_child_coverage tolerates a not-yet-created schema anyway.
    check_reload_invariant(db_url, raise_on_violation=False)

    # -- set_tables_unlogged (skip WAL writes during bulk import)
    set_tables_unlogged(db_url)

    # -- import_csv (base tables, artwork, cache_metadata, track counts)
    if state and state.is_completed("import_csv"):
        logger.info("Skipping import_csv (already completed)")
    else:
        import_cmd = [python, str(SCRIPT_DIR / "import_csv.py"), "--base-only"]
        if truncate_existing:
            import_cmd.append("--truncate-existing")
        import_cmd.extend([str(csv_dir), db_url])
        run_step_safe("Import base CSVs", import_cmd)
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
            run_step_safe(
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
        dedup_cmd.extend(["--keep-release-ids", str(keep_release_ids_path)])
        dedup_cmd.append(db_url)

        run_step_safe("Deduplicate releases", dedup_cmd)
        if state:
            state.mark_completed("dedup")
            _save_state()

    # -- import_tracks (filtered to surviving release IDs)
    if state and state.is_completed("import_tracks"):
        logger.info("Skipping import_tracks (already completed)")
    else:
        # --truncate-existing is intentionally NOT forwarded here. In a full
        # pipeline run the base step already truncated + repopulated; the
        # tracks step runs AFTER dedup against that data. Truncating the
        # tracks subset again now would just wipe and rebuild empty track
        # tables. (import_csv's --tracks-only --truncate-existing is mode-
        # aware and only touches track tables for standalone callers — see
        # CACHE_TABLES_TO_TRUNCATE_TRACKS — but the orchestrator skips it
        # to make the no-op explicit.)
        tracks_cmd = [python, str(SCRIPT_DIR / "import_csv.py"), "--tracks-only"]
        tracks_cmd.extend([str(csv_dir), db_url])
        run_step_safe("Import tracks", tracks_cmd)
        if state:
            state.mark_completed("import_tracks")
            _save_state()

    # -- create_track_indexes (FK constraints, FK indexes, trigram indexes)
    if state and state.is_completed("create_track_indexes"):
        logger.info("Skipping create_track_indexes (already completed)")
    else:
        # Level 0: Clean orphan track rows before FK validation. Same race
        # as in the resumable path above; see comment there for details.
        run_sql_statements_parallel(
            db_url,
            [
                "DELETE FROM release_track WHERE NOT EXISTS "
                "(SELECT 1 FROM release r WHERE r.id = release_track.release_id)",
                "DELETE FROM release_track_artist WHERE NOT EXISTS "
                "(SELECT 1 FROM release r WHERE r.id = release_track_artist.release_id)",
            ],
            description="clean orphan track rows before FK",
        )
        # Level 1: FK constraints (parallel, NOT VALID for race tolerance).
        run_sql_statements_parallel(
            db_url,
            [
                "DO $$ BEGIN "
                "ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID; "
                "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
                "DO $$ BEGIN "
                "ALTER TABLE release_track_artist ADD CONSTRAINT fk_release_track_artist_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID; "
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
        run_step_safe(
            "Copy matched releases to target database",
            [
                python,
                str(SCRIPT_DIR / "verify_cache.py"),
                "--copy-to",
                target_db_url,
                str(library_db),
                db_url,
                "--keep-release-ids",
                str(keep_release_ids_path),
                *prune_audit_dump_args(),
            ],
        )
        if state:
            state.mark_completed("prune")
            _save_state()
    elif library_db:
        run_step_safe(
            "Prune to library matches",
            [
                python,
                str(SCRIPT_DIR / "verify_cache.py"),
                "--prune",
                str(library_db),
                db_url,
                "--keep-release-ids",
                str(keep_release_ids_path),
                *prune_audit_dump_args(),
            ],
        )
        if state:
            state.mark_completed("prune")
            _save_state()
    else:
        logger.info("Skipping prune step (no library.db provided)")
        if state:
            state.mark_completed("prune")
            _save_state()

    # The DB consumers read: the copy-to target when set, otherwise the
    # source. Shared by derive_va_release, vacuum, and set_logged below so
    # the three can never diverge on which database they finalize.
    consumer_db_url = target_db_url if target_db_url else db_url

    # -- derive_va_release (#344): re-derive the VA-compilation lookup table
    # now that dedup/prune have finalized release ids. Gated like prune; see
    # run_derive_va_release_step for the --floor 0 and soft-fail rationale.
    if state and state.is_completed("derive_va_release"):
        logger.info("Skipping derive_va_release (already completed)")
    elif not library_db:
        logger.info("Skipping derive_va_release step (no library.db provided)")
        if state:
            state.mark_completed("derive_va_release")
            _save_state()
    else:
        derived = run_derive_va_release_step(consumer_db_url, python)
        if state and derived:
            state.mark_completed("derive_va_release")
            _save_state()

    # -- vacuum (on target DB if using copy-to, otherwise source)
    vacuum_db = consumer_db_url
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

    # -- reload-invariant gate (discogs-etl#298)
    # Final check on the state consumers will read (the target DB in copy-to
    # mode, else the source). If release is populated but the children are
    # empty — e.g. an empty/missing tracks CSV COPYed as zero rows without
    # error — raise before report_sizes so the run fails loudly instead of
    # reporting success with an empty track/artist index. Always runs (even on
    # a fully-resumed build) as a last sanity gate.
    check_reload_invariant(vacuum_db, raise_on_violation=True)

    # -- report
    report_sizes(vacuum_db)


if __name__ == "__main__":
    main()
