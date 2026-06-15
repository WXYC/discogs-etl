#!/usr/bin/env python3
"""Deduplicate releases by master_id using CREATE TABLE AS + swap.

Instead of deleting 88% of rows (slow, huge WAL), copies the 12% we want
to keep into fresh tables, then swaps them in. Much faster for high
delete ratios.

Expects dedup_delete_ids table to already exist (from a previous run).
If not, creates it from the ROW_NUMBER query.

When --library-labels is provided, WXYC label preferences influence the
ranking: releases whose label matches WXYC's known pressing are preferred
over releases with more tracks but a different label.

Usage:
    python dedup_releases.py [database_url] [--library-labels <csv>]

    database_url defaults to postgresql:///discogs
"""

import argparse
import csv
import logging
import sys
import time
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402
from lib.pg_concurrent_ddl import (  # noqa: E402
    add_constraint_safely,
    add_index_concurrently_safely,
    group_concurrent_index_ddls_by_table,
)

logger = logging.getLogger(__name__)


def _track_count_table_exists(conn) -> bool:
    """Return True if the release_track_count pre-computed table exists."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'release_track_count'
            )
        """)
        return cur.fetchone()[0]


def _label_match_table_exists(conn) -> bool:
    """Return True if the release_label_match table exists."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'release_label_match'
            )
        """)
        return cur.fetchone()[0]


def load_library_labels(conn, csv_path: Path) -> int:
    """Load WXYC label preferences from CSV into an UNLOGGED table.

    Creates the ``wxyc_label_pref`` table with columns
    (artist_name, release_title, label_name) and bulk-loads the CSV.

    Args:
        conn: psycopg connection (autocommit=True).
        csv_path: Path to library_labels.csv.

    Returns:
        Number of rows loaded.
    """
    logger.info("Loading WXYC label preferences from %s", csv_path)

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS wxyc_label_pref")
        cur.execute("""
            CREATE UNLOGGED TABLE wxyc_label_pref (
                artist_name text NOT NULL,
                release_title text NOT NULL,
                label_name text NOT NULL
            )
        """)

    count = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        with conn.cursor() as cur:
            with cur.copy(
                "COPY wxyc_label_pref (artist_name, release_title, label_name) FROM STDIN"
            ) as copy:
                for row in reader:
                    copy.write_row((row["artist_name"], row["release_title"], row["label_name"]))
                    count += 1

    logger.info("Loaded %d label preferences", count)
    return count


def load_label_hierarchy(conn, csv_path: Path) -> int:
    """Load Discogs label hierarchy from CSV into an UNLOGGED table.

    Creates the ``label_hierarchy`` table with columns
    (label_id, label_name, parent_label_id, parent_label_name) and bulk-loads
    the CSV.

    Args:
        conn: psycopg connection (autocommit=True).
        csv_path: Path to label_hierarchy.csv.

    Returns:
        Number of rows loaded.
    """
    logger.info("Loading label hierarchy from %s", csv_path)

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS label_hierarchy")
        cur.execute("""
            CREATE UNLOGGED TABLE label_hierarchy (
                label_id integer NOT NULL,
                label_name text NOT NULL,
                parent_label_id integer NOT NULL,
                parent_label_name text NOT NULL
            )
        """)

    count = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        with conn.cursor() as cur:
            with cur.copy(
                "COPY label_hierarchy (label_id, label_name, parent_label_id, "
                "parent_label_name) FROM STDIN"
            ) as copy:
                for row in reader:
                    copy.write_row(
                        (
                            int(row["label_id"]),
                            row["label_name"],
                            int(row["parent_label_id"]),
                            row["parent_label_name"],
                        )
                    )
                    count += 1

    logger.info("Loaded %d label hierarchy entries", count)
    return count


def _label_hierarchy_table_exists(conn) -> bool:
    """Return True if the label_hierarchy table exists."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'label_hierarchy'
            )
        """)
        return cur.fetchone()[0]


def create_label_match_table(conn) -> int:
    """Create release_label_match table by joining Discogs labels to WXYC preferences.

    Marks which release_ids have a label matching WXYC's known pressing
    for that (artist, title) pair. Uses lowercased matching with optional
    sublabel resolution via the label_hierarchy table.

    Requires wxyc_label_pref table to exist (from load_library_labels).
    Optionally uses label_hierarchy table (from load_label_hierarchy) for
    bidirectional sublabel matching.

    Args:
        conn: psycopg connection (autocommit=True).

    Returns:
        Number of releases matched.
    """
    logger.info("Creating release_label_match table...")

    use_hierarchy = _label_hierarchy_table_exists(conn)
    if use_hierarchy:
        logger.info("  Label hierarchy loaded — enabling sublabel resolution")
        label_condition = """(
              lower(rl.label_name) = lower(wlp.label_name)
              OR EXISTS (
                  SELECT 1 FROM label_hierarchy lh
                  WHERE lower(lh.label_name) = lower(rl.label_name)
                    AND lower(lh.parent_label_name) = lower(wlp.label_name)
              )
              OR EXISTS (
                  SELECT 1 FROM label_hierarchy lh
                  WHERE lower(lh.parent_label_name) = lower(rl.label_name)
                    AND lower(lh.label_name) = lower(wlp.label_name)
              ))"""
    else:
        label_condition = "lower(rl.label_name) = lower(wlp.label_name)"

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS release_label_match")
        # All SQL is built from trusted internal constants, not user input
        cur.execute(f"""
            CREATE UNLOGGED TABLE release_label_match AS
            SELECT DISTINCT rl.release_id, 1 AS label_match
            FROM release_label rl
            JOIN release r ON r.id = rl.release_id
            JOIN release_artist ra ON ra.release_id = r.id AND ra.extra = 0
            JOIN wxyc_label_pref wlp
              ON lower(ra.artist_name) = lower(wlp.artist_name)
              AND lower(r.title) = lower(wlp.release_title)
              AND {label_condition}
            WHERE r.master_id IS NOT NULL
        """)
        cur.execute("ALTER TABLE release_label_match ADD PRIMARY KEY (release_id)")
        cur.execute("SELECT count(*) FROM release_label_match")
        count = int(cur.fetchone()[0])

    logger.info("Matched %d releases to WXYC label preferences", count)
    return count


def ensure_dedup_ids(conn) -> int:
    """Ensure dedup_delete_ids table exists. Create if needed.

    Uses release_track_count table for track counts if available (v2 pipeline),
    falling back to counting from release_track directly (v1 / standalone usage).

    Returns number of IDs to delete.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'dedup_delete_ids'
            )
        """)
        exists = cur.fetchone()[0]

    if exists:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM dedup_delete_ids")
            count = int(cur.fetchone()[0])
        logger.info(f"dedup_delete_ids already exists with {count:,} IDs")
        return count

    # Choose track count source: pre-computed table or live count from release_track
    use_precomputed = _track_count_table_exists(conn)

    if use_precomputed:
        logger.info(
            "Creating dedup_delete_ids from ROW_NUMBER query (using pre-computed track counts)..."
        )
        track_count_join = "JOIN release_track_count tc ON tc.release_id = r.id"
    else:
        logger.info(
            "Creating dedup_delete_ids from ROW_NUMBER query (counting from release_track)..."
        )
        track_count_join = (
            "JOIN ("
            "    SELECT release_id, COUNT(*) as track_count"
            "    FROM release_track"
            "    GROUP BY release_id"
            ") tc ON tc.release_id = r.id"
        )

    # Optional label matching: prefer releases whose label matches WXYC's pressing
    use_label_match = _label_match_table_exists(conn)
    if use_label_match:
        label_join = "LEFT JOIN release_label_match rlm ON rlm.release_id = r.id"
        order_by = (
            "COALESCE(rlm.label_match, 0) DESC, "
            "(r.country = 'US')::int DESC, "
            "tc.track_count DESC, r.id ASC"
        )
        logger.info("  Label matching enabled (release_label_match table found)")
    else:
        label_join = ""
        order_by = "(r.country = 'US')::int DESC, tc.track_count DESC, r.id ASC"

    with conn.cursor() as cur:
        # All SQL fragments are built from trusted internal constants, not user input
        cur.execute(f"""
            CREATE UNLOGGED TABLE dedup_delete_ids AS
            SELECT id AS release_id FROM (
                SELECT r.id, r.master_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY r.master_id, r.format
                           ORDER BY {order_by}
                       ) as rn
                FROM release r
                {track_count_join}
                {label_join}
                WHERE r.master_id IS NOT NULL
            ) ranked
            WHERE rn > 1
        """)
        cur.execute("ALTER TABLE dedup_delete_ids ADD PRIMARY KEY (release_id)")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dedup_delete_ids")
        count = int(cur.fetchone()[0])
    logger.info(f"Created dedup_delete_ids with {count:,} IDs")
    return count


# Tables copied during the dedup swap. Each tuple is
# (old_table, new_table, copied_columns, id_col_for_dedup_filter). This lives
# at module scope so tests/integration/test_dedup.py imports the same list the
# production main() uses, instead of maintaining a parallel copy that can
# silently drift (which is how WXYC/discogs-etl#129 hid for a release).
DEDUP_TABLES: list[tuple[str, str, str, str]] = [
    (
        "release",
        "new_release",
        "id, title, release_year, country, artwork_url, released, format, master_id, artwork_checked_at, not_found",
        "id",
    ),
    (
        "release_artist",
        "new_release_artist",
        "release_id, artist_id, artist_name, extra, role",
        "release_id",
    ),
    (
        "release_label",
        "new_release_label",
        "release_id, label_id, label_name, catno",
        "release_id",
    ),
    (
        "release_genre",
        "new_release_genre",
        "release_id, genre",
        "release_id",
    ),
    (
        "release_style",
        "new_release_style",
        "release_id, style",
        "release_id",
    ),
    (
        "cache_metadata",
        "new_cache_metadata",
        "release_id, cached_at, source, last_validated",
        "release_id",
    ),
]


# Column DEFAULTs to re-apply on each new_X table *before* swap_tables() makes
# it live. CTAS strips DEFAULTs along with NOT NULL/CHECK; restoring DEFAULT
# *before* the swap closes the race window where an LML cache-miss insert
# (which omits cached_at) could land NULL between the swap and the
# Level-2 SET DEFAULT, then trip the Level-2 SET NOT NULL. See #254.
PRE_SWAP_COLUMN_DEFAULTS: dict[str, dict[str, str]] = {
    "new_cache_metadata": {"cached_at": "now()"},
    "new_release_artist": {"extra": "0"},
    # LML#510: tombstone DEFAULT must be re-applied before swap so a
    # cache-miss INSERT that omits not_found never lands NULL between the
    # swap and Level-2 SET NOT NULL.
    "new_release": {"not_found": "false"},
}


# Columns that must be ``NOT NULL`` on each new_X table *before* the swap, so
# the post-swap ``ADD CONSTRAINT ... PRIMARY KEY USING INDEX`` step in
# add_base_constraints_and_indexes is a brief catalog flip rather than a
# hidden AccessExclusive full-table scan. CTAS strips NOT NULL alongside
# DEFAULT; applying it now (while the table isn't live) costs a scan that
# can't conflict with LML's writes. Without this prereq, the post-swap
# USING INDEX attach would still produce the correct end state but PG would
# internally run ``SET NOT NULL`` first, defeating the lock-conflict
# avoidance the helper is supposed to give us. See #286.
PRE_SWAP_NOT_NULL_COLUMNS: dict[str, tuple[str, ...]] = {
    "new_release": ("id",),
    "new_cache_metadata": ("release_id",),
}


def copy_table(conn, old_table: str, new_table: str, columns: str, id_col: str) -> int:
    """Copy rows NOT in dedup_delete_ids to a new table.

    Returns row count of new table.
    """
    start = time.time()
    logger.info(f"Copying {old_table} -> {new_table} (keeping non-duplicate rows)...")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {new_table}")
        cur.execute(f"""
            CREATE TABLE {new_table} AS
            SELECT {columns} FROM {old_table} t
            WHERE NOT EXISTS (
                SELECT 1 FROM dedup_delete_ids d WHERE d.release_id = t.{id_col}
            )
        """)
        for column, default_sql in PRE_SWAP_COLUMN_DEFAULTS.get(new_table, {}).items():
            cur.execute(f"ALTER TABLE {new_table} ALTER COLUMN {column} SET DEFAULT {default_sql}")
        for column in PRE_SWAP_NOT_NULL_COLUMNS.get(new_table, ()):
            cur.execute(f"ALTER TABLE {new_table} ALTER COLUMN {column} SET NOT NULL")
        cur.execute(f"SELECT count(*) FROM {new_table}")
        count = int(cur.fetchone()[0])
    conn.commit()

    elapsed = time.time() - start
    logger.info(f"  {new_table}: {count:,} rows in {elapsed:.1f}s")
    return count


def swap_tables(conn, old_table: str, new_table: str) -> None:
    """Swap old and new tables atomically.

    Uses CASCADE on DROP to remove FK constraints that reference the old table.
    Constraints are recreated by add_constraints_and_indexes() after all swaps.
    """
    bak = f"{old_table}_old"
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {old_table} RENAME TO {bak}")
        cur.execute(f"ALTER TABLE {new_table} RENAME TO {old_table}")
        cur.execute(f"DROP TABLE {bak} CASCADE")
    conn.commit()
    logger.info(f"  Swapped {new_table} -> {old_table}")


def _exec_one(db_url: str, stmt: str) -> None:
    """Execute a single SQL statement on its own connection (autocommit)."""
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(stmt)
    finally:
        conn.close()


def _add_constraint_one(db_url: str, ddl: str, lock_tables: tuple[str, ...]) -> None:
    """Open a connection and run ``add_constraint_safely`` against it.

    Used by the parallel constraint-add executors so each parallel worker has
    its own autocommit connection. See :mod:`lib.pg_concurrent_ddl` for the
    retry envelope and the parent-first lock ordering that makes the prune ↔
    LML deadlock structurally impossible. ``lock_tables`` MUST be in
    parent-first order — see #286 for the failure mode that reverses it.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        add_constraint_safely(conn, ddl, lock_tables=lock_tables)
    finally:
        conn.close()


def _add_index_concurrently_one(db_url: str, ddl: str) -> None:
    """Open an autocommit connection and run a CONCURRENTLY index build.

    Each call opens its own connection because ``CREATE INDEX CONCURRENTLY``
    must run outside a transaction block. The helper enforces that
    invariant.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        add_index_concurrently_safely(conn, ddl)
    finally:
        conn.close()


def add_base_constraints_and_indexes(conn, db_url: str | None = None) -> None:
    """Add PK, FK constraints and indexes to base tables (no track tables).

    Called after dedup copy-swap. Track constraints are added separately
    by create_track_indexes.sql after track import.

    Wraps every blocking-lock DDL in :func:`lib.pg_concurrent_ddl.
    add_constraint_safely` (parent-first ``LOCK TABLE ... IN ACCESS EXCLUSIVE
    MODE`` with bounded ``lock_timeout`` + retry on ``55P03``) and builds
    every index with :func:`lib.pg_concurrent_ddl.
    add_index_concurrently_safely` (CONCURRENTLY + INVALID-index
    precleanup). See #286 for the prune-side outage that drove this fix
    shape; dedup adopts the same shape to prevent regression here when
    the prune helper is hardened further.

    Args:
        conn: psycopg connection in autocommit mode. Used for serial setup
            calls and orphan cleanup; parallel constraint/index workers each
            open their own short-lived connection against ``db_url``.
        db_url: PostgreSQL connection URL for parallel workers. If None,
            falls back to conn.info.dsn (which may omit password).
    """
    logger.info("Adding base constraints and indexes...")
    start = time.time()

    from concurrent.futures import ThreadPoolExecutor, as_completed

    if db_url is None:
        db_url = conn.info.dsn

    def _exec_parallel(stmts: list[str], label: str) -> None:
        if not stmts:
            return
        logger.info(f"  {label} ({len(stmts)} statements)...")
        level_start = time.time()
        with ThreadPoolExecutor(max_workers=min(len(stmts), 4)) as executor:
            futures = {executor.submit(_exec_one, db_url, s): s for s in stmts}
            for future in as_completed(futures):
                future.result()
        logger.info(f"    done in {time.time() - level_start:.1f}s")

    def _exec_constraints_parallel(
        ops: list[tuple[str, tuple[str, ...]]],
        label: str,
    ) -> None:
        """Run a batch of (ddl, lock_tables) constraint ops concurrently.

        Each worker opens its own autocommit connection and goes through
        :func:`add_constraint_safely`, so they share the same retry envelope
        and parent-first lock ordering. Multiple workers can serialize on
        ``LOCK TABLE release`` (the common parent) but that's correct
        behavior — the locks are exclusive by definition. Order across
        workers is not preserved.
        """
        if not ops:
            return
        logger.info(f"  {label} ({len(ops)} constraints)...")
        level_start = time.time()
        with ThreadPoolExecutor(max_workers=min(len(ops), 4)) as executor:
            futures = {
                executor.submit(_add_constraint_one, db_url, ddl, lock_tables): ddl
                for ddl, lock_tables in ops
            }
            for future in as_completed(futures):
                future.result()
        logger.info(f"    done in {time.time() - level_start:.1f}s")

    def _exec_indexes_concurrently_parallel(ddls: list[str], label: str) -> None:
        """Run a batch of CONCURRENTLY index builds, parallel across distinct
        target tables and serial within each target table.

        Two ``CREATE INDEX CONCURRENTLY`` calls on the **same** table will
        deadlock against each other inside Postgres' wait-for-snapshot
        phase — they each take ShareUpdateExclusive on the table while
        also waiting on the other's virtual transaction. PG documents
        this as a one-CONCURRENTLY-per-table limit. Two CONCURRENTLY
        builds on **different** tables run cleanly in parallel.

        We group ``ddls`` by parsed target table and dispatch one worker
        per table — within the worker the per-table DDLs run sequentially.
        Falls back to serial if a DDL can't be parsed (defensive).
        """
        if not ddls:
            return
        logger.info(f"  {label} ({len(ddls)} indexes)...")
        level_start = time.time()

        groups = group_concurrent_index_ddls_by_table(ddls)

        def _run_serial_per_table(ddl_list: list[str]) -> None:
            for ddl in ddl_list:
                _add_index_concurrently_one(db_url, ddl)

        with ThreadPoolExecutor(max_workers=min(len(groups), 4)) as executor:
            futures = {
                executor.submit(_run_serial_per_table, ddl_list): table
                for table, ddl_list in groups.items()
            }
            for future in as_completed(futures):
                future.result()
        logger.info(f"    done in {time.time() - level_start:.1f}s")

    # Level 1: PK on release. Build via CONCURRENTLY + USING INDEX so the
    # index scan takes only ShareUpdateExclusive — no conflict with LML's
    # RowExclusive DML. The post-swap ``release.id`` column was already
    # set NOT NULL pre-swap (see PRE_SWAP_NOT_NULL_COLUMNS in copy_table),
    # so the ADD CONSTRAINT USING INDEX step is a brief catalog flip.
    # See #286 "Prereq for the brief AccessExclusive claim."
    logger.info("  [Level 1] PK on release (CONCURRENTLY + USING INDEX)...")
    pk_start = time.time()
    _add_index_concurrently_one(
        db_url,
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS release_pkey ON release(id)",
    )
    _add_constraint_one(
        db_url,
        "ALTER TABLE release ADD CONSTRAINT release_pkey PRIMARY KEY USING INDEX release_pkey",
        ("release",),
    )
    logger.info(f"    done in {time.time() - pk_start:.1f}s")

    # Level 1.5: Clean orphan child rows (parallel).
    #
    # The live library-metadata-lookup service inserts release + release_label
    # + release_artist + cache_metadata rows for every Discogs API miss. During
    # the dedup copy-swap window, LML can produce child rows referencing
    # release ids that are NOT in the post-dedup release table. Those orphans
    # would cause the FK constraint validation at Level 2 to fail with
    # ``ForeignKeyViolation``, aborting the entire rebuild — exactly what
    # happened in the 2026-05-13 23:42 UTC run (instance i-03e2afe2410ad43f8,
    # see WXYC/discogs-etl#188 comment thread).
    #
    # Combined with the NOT VALID modifier in Level 2 below, this gives a
    # race-tolerant constraint creation: existing orphans are removed here,
    # and the small window of new orphans landing between this cleanup and
    # the NOT VALID constraint creation is tolerated because NOT VALID skips
    # re-validation of existing rows. Future LML inserts are blocked by the
    # FK at INSERT time, which is the durable correct behavior.
    _exec_parallel(
        [
            "DELETE FROM release_artist "
            "WHERE NOT EXISTS (SELECT 1 FROM release r WHERE r.id = release_artist.release_id)",
            "DELETE FROM release_label "
            "WHERE NOT EXISTS (SELECT 1 FROM release r WHERE r.id = release_label.release_id)",
            "DELETE FROM release_genre "
            "WHERE NOT EXISTS (SELECT 1 FROM release r WHERE r.id = release_genre.release_id)",
            "DELETE FROM release_style "
            "WHERE NOT EXISTS (SELECT 1 FROM release r WHERE r.id = release_style.release_id)",
            "DELETE FROM cache_metadata "
            "WHERE NOT EXISTS (SELECT 1 FROM release r WHERE r.id = cache_metadata.release_id)",
        ],
        "Level 1.5: Clean orphan child rows before FK validation",
    )

    # Level 1.75: Backfill race-window NULLs before SET NOT NULL runs.
    #
    # ``PRE_SWAP_COLUMN_DEFAULTS`` is supposed to seal the race by setting
    # the DEFAULT on ``new_X`` before swap_tables(), so any LML insert that
    # lands after the swap inherits ``cached_at = now()``. This UPDATE is
    # the belt-and-suspenders: any row that slipped through (e.g. an LML
    # transaction that started before the pre-swap ALTER landed and
    # committed against the renamed table) gets a non-NULL stamp before
    # Level-2 SET NOT NULL would reject it. See #254.
    _exec_one(
        db_url,
        "UPDATE cache_metadata SET cached_at = now() WHERE cached_at IS NULL",
    )

    # Level 2A: FK constraints (parallel, parent-first locks via helper).
    #
    # Each ``ALTER TABLE ... ADD CONSTRAINT FOREIGN KEY ... REFERENCES
    # release(id) NOT VALID`` takes ShareRowExclusiveLock on **both** the
    # child (target of the ALTER) and the parent (FK target). NOT VALID
    # skips existing-row scanning but does NOT skip lock acquisition. PG's
    # FK creation acquires the child first then the parent — opposite of
    # LML's parent-first order in ``write_release``. Without the helper's
    # explicit parent-first ``LOCK TABLE`` envelope, this deadlocks with
    # an LML cache-miss writer (see #286 — the bug surfaced production-side
    # in the verify_cache prune; the dedup site has the same shape and
    # would have surfaced eventually).
    _exec_constraints_parallel(
        [
            (
                "ALTER TABLE release_artist ADD CONSTRAINT fk_release_artist_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                ("release", "release_artist"),
            ),
            (
                "ALTER TABLE release_label ADD CONSTRAINT fk_release_label_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                ("release", "release_label"),
            ),
            (
                "ALTER TABLE release_genre ADD CONSTRAINT fk_release_genre_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                ("release", "release_genre"),
            ),
            (
                "ALTER TABLE release_style ADD CONSTRAINT fk_release_style_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                ("release", "release_style"),
            ),
            (
                "ALTER TABLE cache_metadata ADD CONSTRAINT fk_cache_metadata_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                ("release", "cache_metadata"),
            ),
        ],
        "Level 2A: FK constraints (parent-first LOCK TABLE)",
    )

    # Level 2B: PK on cache_metadata via CONCURRENTLY + USING INDEX.
    # ``new_cache_metadata.release_id`` was set NOT NULL pre-swap.
    logger.info("  [Level 2B] PK on cache_metadata (CONCURRENTLY + USING INDEX)...")
    pk2_start = time.time()
    _add_index_concurrently_one(
        db_url,
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS cache_metadata_pkey "
        "ON cache_metadata(release_id)",
    )
    _add_constraint_one(
        db_url,
        "ALTER TABLE cache_metadata ADD CONSTRAINT cache_metadata_pkey "
        "PRIMARY KEY USING INDEX cache_metadata_pkey",
        ("cache_metadata",),
    )
    logger.info(f"    done in {time.time() - pk2_start:.1f}s")

    # Level 2C: FK indexes built CONCURRENTLY. ShareUpdateExclusive instead
    # of Share — no conflict with LML's DML.
    _exec_indexes_concurrently_parallel(
        [
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_artist_release_id "
            "ON release_artist(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_label_release_id "
            "ON release_label(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_genre_release_id "
            "ON release_genre(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_style_release_id "
            "ON release_style(release_id)",
        ],
        "Level 2C: FK indexes (CONCURRENTLY)",
    )

    # Level 2D: NOT NULL restoration. Each takes AccessExclusive on a
    # single table and performs an implicit full-table scan — wrap via the
    # helper so it shares the retry envelope and brief-lock semantics.
    #
    # ``SET NOT NULL`` here re-asserts the constraints that ``CREATE TABLE
    # new_X AS SELECT ...`` strips silently (CTAS carries column types
    # forward but not NOT NULL / DEFAULT / CHECK). The corresponding
    # DEFAULTs are re-applied earlier — in copy_table() via
    # ``PRE_SWAP_COLUMN_DEFAULTS`` — so LML's cache-miss INSERT (which
    # omits ``cached_at``) never lands a NULL during the swap window.
    # Pinned by ``tests/integration/test_copy_swap_preserves_not_null.py``.
    _exec_constraints_parallel(
        [
            ("ALTER TABLE release ALTER COLUMN title SET NOT NULL", ("release",)),
            ("ALTER TABLE release ALTER COLUMN not_found SET NOT NULL", ("release",)),
            (
                "ALTER TABLE release_artist ALTER COLUMN release_id SET NOT NULL",
                ("release_artist",),
            ),
            (
                "ALTER TABLE release_artist ALTER COLUMN artist_name SET NOT NULL",
                ("release_artist",),
            ),
            (
                "ALTER TABLE release_label ALTER COLUMN release_id SET NOT NULL",
                ("release_label",),
            ),
            (
                "ALTER TABLE release_label ALTER COLUMN label_name SET NOT NULL",
                ("release_label",),
            ),
            (
                "ALTER TABLE release_genre ALTER COLUMN release_id SET NOT NULL",
                ("release_genre",),
            ),
            ("ALTER TABLE release_genre ALTER COLUMN genre SET NOT NULL", ("release_genre",)),
            (
                "ALTER TABLE release_style ALTER COLUMN release_id SET NOT NULL",
                ("release_style",),
            ),
            ("ALTER TABLE release_style ALTER COLUMN style SET NOT NULL", ("release_style",)),
            (
                "ALTER TABLE cache_metadata ALTER COLUMN cached_at SET NOT NULL",
                ("cache_metadata",),
            ),
            (
                "ALTER TABLE cache_metadata ALTER COLUMN source SET NOT NULL",
                ("cache_metadata",),
            ),
        ],
        "Level 2D: NOT NULL restoration",
    )

    # Level 3: GIN trigram + cache metadata indexes (CONCURRENTLY, parallel).
    _exec_indexes_concurrently_parallel(
        [
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_artist_name_trgm "
            "ON release_artist USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_title_trgm "
            "ON release USING gin (lower(f_unaccent(title)) gin_trgm_ops)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cache_metadata_cached_at "
            "ON cache_metadata(cached_at)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cache_metadata_source "
            "ON cache_metadata(source)",
        ],
        "Level 3: GIN trigram + metadata indexes (CONCURRENTLY)",
    )

    elapsed = time.time() - start
    logger.info(f"Base constraints and indexes added in {elapsed:.1f}s")


def add_track_constraints_and_indexes(conn, db_url: str | None = None) -> None:
    """Add FK constraints and indexes to track tables.

    Called after track import (post-dedup). Equivalent to running
    create_track_indexes.sql.

    Wraps blocking-lock DDL via :func:`lib.pg_concurrent_ddl.
    add_constraint_safely` and indexes via :func:`lib.pg_concurrent_ddl.
    add_index_concurrently_safely`. See the docstring of
    :func:`add_base_constraints_and_indexes` and #286 for the full
    rationale; this is the same fix shape applied to the track tables.

    Args:
        conn: psycopg connection (unused but kept for API consistency).
        db_url: PostgreSQL connection URL for parallel workers. If None,
            falls back to conn.info.dsn (which may omit password).
    """
    logger.info("Adding track constraints and indexes...")
    start = time.time()

    from concurrent.futures import ThreadPoolExecutor, as_completed

    if db_url is None:
        db_url = conn.info.dsn

    def _exec_parallel(stmts: list[str], label: str) -> None:
        if not stmts:
            return
        logger.info(f"  {label} ({len(stmts)} statements)...")
        level_start = time.time()
        with ThreadPoolExecutor(max_workers=min(len(stmts), 4)) as executor:
            futures = {executor.submit(_exec_one, db_url, s): s for s in stmts}
            for future in as_completed(futures):
                future.result()
        logger.info(f"    done in {time.time() - level_start:.1f}s")

    def _exec_constraints_parallel(
        ops: list[tuple[str, tuple[str, ...]]],
        label: str,
    ) -> None:
        if not ops:
            return
        logger.info(f"  {label} ({len(ops)} constraints)...")
        level_start = time.time()
        with ThreadPoolExecutor(max_workers=min(len(ops), 4)) as executor:
            futures = {
                executor.submit(_add_constraint_one, db_url, ddl, lock_tables): ddl
                for ddl, lock_tables in ops
            }
            for future in as_completed(futures):
                future.result()
        logger.info(f"    done in {time.time() - level_start:.1f}s")

    def _exec_indexes_concurrently_parallel(ddls: list[str], label: str) -> None:
        """Run CONCURRENTLY index builds, parallel across distinct tables and
        serial within each table. See the docstring on the version in
        :func:`add_base_constraints_and_indexes` for the PG-side rationale
        (two CONCURRENTLY builds on the same table deadlock against each
        other).
        """
        if not ddls:
            return
        logger.info(f"  {label} ({len(ddls)} indexes)...")
        level_start = time.time()

        groups = group_concurrent_index_ddls_by_table(ddls)

        def _run_serial_per_table(ddl_list: list[str]) -> None:
            for ddl in ddl_list:
                _add_index_concurrently_one(db_url, ddl)

        with ThreadPoolExecutor(max_workers=min(len(groups), 4)) as executor:
            futures = {
                executor.submit(_run_serial_per_table, ddl_list): table
                for table, ddl_list in groups.items()
            }
            for future in as_completed(futures):
                future.result()
        logger.info(f"    done in {time.time() - level_start:.1f}s")

    # Level 0: Clean orphan track rows (parallel). LML writes release_track
    # + release_track_artist rows on every Discogs API miss; during the dedup
    # swap window those land as orphans. Parallel to the base-side cleanup
    # in add_base_constraints_and_indexes. See #211 for the original fix
    # and #188 for the 2026-05-14 02:20 UTC failure that surfaced this site.
    _exec_parallel(
        [
            "DELETE FROM release_track WHERE NOT EXISTS "
            "(SELECT 1 FROM release r WHERE r.id = release_track.release_id)",
            "DELETE FROM release_track_artist WHERE NOT EXISTS "
            "(SELECT 1 FROM release r WHERE r.id = release_track_artist.release_id)",
        ],
        "Level 0: Clean orphan track rows before FK validation",
    )

    # Level 1: FK constraints (parallel, parent-first locks via helper).
    # Same shape as add_base_constraints_and_indexes Level 2A — the
    # ``ADD CONSTRAINT ... NOT VALID`` step takes ShareRowExclusive on
    # both child and parent and would otherwise race-deadlock against an
    # open LML ``write_release`` transaction (see #286).
    _exec_constraints_parallel(
        [
            (
                "ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                ("release", "release_track"),
            ),
            (
                "ALTER TABLE release_track_artist ADD CONSTRAINT "
                "fk_release_track_artist_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                ("release", "release_track_artist"),
            ),
        ],
        "Level 1: FK constraints (parent-first LOCK TABLE)",
    )

    # Level 2: FK indexes + GIN trigram indexes (CONCURRENTLY, parallel).
    _exec_indexes_concurrently_parallel(
        [
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_release_id "
            "ON release_track(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_artist_release_id "
            "ON release_track_artist(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_title_trgm "
            "ON release_track USING gin (lower(f_unaccent(title)) gin_trgm_ops)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_artist_name_trgm "
            "ON release_track_artist USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)",
        ],
        "Level 2: FK indexes + GIN trigram (CONCURRENTLY)",
    )

    elapsed = time.time() - start
    logger.info(f"Track constraints and indexes added in {elapsed:.1f}s")


def add_constraints_and_indexes(conn, db_url: str | None = None) -> None:
    """Add PK, FK constraints and indexes to all tables.

    Convenience function that calls both base and track versions.
    Used for backward compatibility (standalone dedup with all tables present).
    """
    add_base_constraints_and_indexes(conn, db_url=db_url)
    add_track_constraints_and_indexes(conn, db_url=db_url)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "database_url",
        nargs="?",
        default="postgresql:///discogs",
        help="PostgreSQL connection URL (default: postgresql:///discogs).",
    )
    parser.add_argument(
        "--library-labels",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to library_labels.csv with WXYC label preferences. "
        "When provided, dedup ranking prefers releases whose label "
        "matches WXYC's known pressing.",
    )
    parser.add_argument(
        "--label-hierarchy",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to label_hierarchy.csv from discogs-xml-converter. "
        "Enables sublabel resolution during label matching "
        "(e.g., Parlophone matches EMI).",
    )
    return parser.parse_args(argv)


def main():
    args = parse_args()
    db_url = args.database_url

    init_logger(repo="discogs-etl", tool="discogs-etl dedup_releases")

    logger.info(f"Connecting to {db_url}")
    conn = psycopg.connect(db_url, autocommit=True)

    # Step 0 (optional): Load WXYC label preferences for label-aware ranking
    if args.library_labels:
        if not args.library_labels.exists():
            logger.error("Library labels file not found: %s", args.library_labels)
            sys.exit(1)
        load_library_labels(conn, args.library_labels)

        # Load label hierarchy for sublabel resolution (optional)
        if args.label_hierarchy:
            if not args.label_hierarchy.exists():
                logger.error("Label hierarchy file not found: %s", args.label_hierarchy)
                sys.exit(1)
            load_label_hierarchy(conn, args.label_hierarchy)

        create_label_match_table(conn)

    # Step 1: Ensure dedup IDs exist
    delete_count = ensure_dedup_ids(conn)
    if delete_count == 0:
        logger.info("No duplicates found, nothing to do")
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS release_track_count")
            cur.execute("DROP TABLE IF EXISTS wxyc_label_pref")
            cur.execute("DROP TABLE IF EXISTS release_label_match")
        conn.close()
        return

    total_start = time.time()

    # Step 2: Copy each table (keeping only non-duplicate rows)
    # Only base tables + cache_metadata (tracks are imported after dedup)
    for old, new, cols, id_col in DEDUP_TABLES:
        copy_table(conn, old, new, cols, id_col)

    # Step 3: Drop old FK constraints before swap
    logger.info("Dropping FK constraints on old tables...")
    with conn.cursor() as cur:
        for stmt in [
            "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
            "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
            "ALTER TABLE release_genre DROP CONSTRAINT IF EXISTS fk_release_genre_release",
            "ALTER TABLE release_style DROP CONSTRAINT IF EXISTS fk_release_style_release",
            "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
        ]:
            cur.execute(stmt)

    # Step 4: Swap tables
    logger.info("Swapping tables...")
    for old, new, _, _ in DEDUP_TABLES:
        swap_tables(conn, old, new)

    # Step 5: Add base constraints and indexes
    add_base_constraints_and_indexes(conn, db_url=db_url)

    # Step 6: Cleanup
    logger.info("Cleaning up...")
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
        cur.execute("DROP TABLE IF EXISTS release_track_count")
        cur.execute("DROP TABLE IF EXISTS wxyc_label_pref")
        cur.execute("DROP TABLE IF EXISTS release_label_match")
        cur.execute("DROP TABLE IF EXISTS label_hierarchy")

    # Step 7: Report
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM release")
        count = cur.fetchone()[0]

    total_elapsed = time.time() - total_start
    logger.info(f"Done! Final release count: {count:,} ({total_elapsed / 60:.1f} min total)")

    # Table sizes
    with conn.cursor() as cur:
        cur.execute("""
            SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) as total_size
            FROM pg_stat_user_tables
            WHERE relname IN ('release', 'release_artist', 'release_label',
                              'release_track', 'release_track_artist', 'cache_metadata')
            ORDER BY pg_total_relation_size(relid) DESC
        """)
        logger.info("Table sizes:")
        for row in cur.fetchall():
            logger.info(f"  {row[0]}: {row[1]}")

    conn.close()


if __name__ == "__main__":
    main()
