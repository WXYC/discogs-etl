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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
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

    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((row["artist_name"], row["release_title"], row["label_name"]))

    if rows:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO wxyc_label_pref (artist_name, release_title, label_name) "
                "VALUES (%s, %s, %s)",
                rows,
            )

    logger.info("Loaded %d label preferences", len(rows))
    return len(rows)


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

    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                (
                    int(row["label_id"]),
                    row["label_name"],
                    int(row["parent_label_id"]),
                    row["parent_label_name"],
                )
            )

    if rows:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO label_hierarchy (label_id, label_name, parent_label_id, "
                "parent_label_name) VALUES (%s, %s, %s, %s)",
                rows,
            )

    logger.info("Loaded %d label hierarchy entries", len(rows))
    return len(rows)


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
                           PARTITION BY r.master_id
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


def add_base_constraints_and_indexes(conn) -> None:
    """Add PK, FK constraints and indexes to base tables (no track tables).

    Called after dedup copy-swap. Track constraints are added separately
    by create_track_indexes.sql after track import.

    Parallelizes independent index/constraint creation:
    - Level 1: PK on release (must be first, FK constraints depend on it)
    - Level 2: FK constraints + FK indexes (parallel, all depend on PK only)
    - Level 3: GIN trigram indexes + cache metadata indexes (parallel, independent)
    """
    logger.info("Adding base constraints and indexes...")
    start = time.time()

    from concurrent.futures import ThreadPoolExecutor, as_completed

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

    # Level 1: PK on release (serial, must be first)
    logger.info("  [Level 1] ALTER TABLE release ADD PRIMARY KEY...")
    pk_start = time.time()
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE release ADD PRIMARY KEY (id)")
    conn.commit()
    logger.info(f"    done in {time.time() - pk_start:.1f}s")

    # Level 2: FK constraints + PK on cache_metadata + FK indexes (parallel)
    _exec_parallel(
        [
            "ALTER TABLE release_artist ADD CONSTRAINT fk_release_artist_release "
            "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE",
            "ALTER TABLE release_label ADD CONSTRAINT fk_release_label_release "
            "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE",
            "ALTER TABLE cache_metadata ADD CONSTRAINT fk_cache_metadata_release "
            "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE",
            "ALTER TABLE cache_metadata ADD PRIMARY KEY (release_id)",
            "CREATE INDEX idx_release_artist_release_id ON release_artist(release_id)",
            "CREATE INDEX idx_release_label_release_id ON release_label(release_id)",
        ],
        "Level 2: FK constraints + FK indexes",
    )

    # Level 3: GIN trigram indexes + cache metadata indexes (parallel)
    _exec_parallel(
        [
            "CREATE INDEX idx_release_artist_name_trgm ON release_artist "
            "USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)",
            "CREATE INDEX idx_release_title_trgm ON release "
            "USING gin (lower(f_unaccent(title)) gin_trgm_ops)",
            "CREATE INDEX idx_cache_metadata_cached_at ON cache_metadata(cached_at)",
            "CREATE INDEX idx_cache_metadata_source ON cache_metadata(source)",
        ],
        "Level 3: GIN trigram + metadata indexes",
    )

    elapsed = time.time() - start
    logger.info(f"Base constraints and indexes added in {elapsed:.1f}s")


def add_track_constraints_and_indexes(conn) -> None:
    """Add FK constraints and indexes to track tables.

    Called after track import (post-dedup). Equivalent to running
    create_track_indexes.sql.

    Parallelizes independent statements:
    - Level 1: FK constraints (parallel, both depend on release PK only)
    - Level 2: FK indexes + GIN trigram indexes (parallel, independent)
    """
    logger.info("Adding track constraints and indexes...")
    start = time.time()

    from concurrent.futures import ThreadPoolExecutor, as_completed

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

    # Level 1: FK constraints (parallel)
    _exec_parallel(
        [
            "ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release "
            "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE",
            "ALTER TABLE release_track_artist ADD CONSTRAINT fk_release_track_artist_release "
            "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE",
        ],
        "Level 1: FK constraints",
    )

    # Level 2: FK indexes + GIN trigram indexes (parallel)
    _exec_parallel(
        [
            "CREATE INDEX idx_release_track_release_id ON release_track(release_id)",
            "CREATE INDEX idx_release_track_artist_release_id ON release_track_artist(release_id)",
            "CREATE INDEX idx_release_track_title_trgm ON release_track "
            "USING gin (lower(f_unaccent(title)) gin_trgm_ops)",
            "CREATE INDEX idx_release_track_artist_name_trgm ON release_track_artist "
            "USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)",
        ],
        "Level 2: FK indexes + GIN trigram indexes",
    )

    elapsed = time.time() - start
    logger.info(f"Track constraints and indexes added in {elapsed:.1f}s")


def add_constraints_and_indexes(conn) -> None:
    """Add PK, FK constraints and indexes to all tables.

    Convenience function that calls both base and track versions.
    Used for backward compatibility (standalone dedup with all tables present).
    """
    add_base_constraints_and_indexes(conn)
    add_track_constraints_and_indexes(conn)


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
    tables = [
        ("release", "new_release", "id, title, release_year, country, artwork_url", "id"),
        (
            "release_artist",
            "new_release_artist",
            "release_id, artist_id, artist_name, extra",
            "release_id",
        ),
        ("release_label", "new_release_label", "release_id, label_name", "release_id"),
        (
            "cache_metadata",
            "new_cache_metadata",
            "release_id, cached_at, source, last_validated",
            "release_id",
        ),
    ]

    for old, new, cols, id_col in tables:
        copy_table(conn, old, new, cols, id_col)

    # Step 3: Drop old FK constraints before swap
    logger.info("Dropping FK constraints on old tables...")
    with conn.cursor() as cur:
        for stmt in [
            "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
            "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
            "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
        ]:
            cur.execute(stmt)

    # Step 4: Swap tables
    logger.info("Swapping tables...")
    for old, new, _, _ in tables:
        swap_tables(conn, old, new)

    # Step 5: Add base constraints and indexes
    add_base_constraints_and_indexes(conn)

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
