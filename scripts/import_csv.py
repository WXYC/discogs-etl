#!/usr/bin/env python3
"""Import Discogs CSV files into PostgreSQL with proper multiline handling.

Imports only the columns needed by the schema (see 04-create-database.sql).
The release_image.csv is processed separately to populate artwork_url on release.
"""

from __future__ import annotations

import csv
import logging
import os
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TypedDict

import psycopg

try:
    from wxyc_etl.import_utils import DedupSet

    _HAS_WXYC_ETL = True
except ImportError:
    _HAS_WXYC_ETL = False

sys.path.insert(0, str(Path(__file__).parent.parent))
from lib.format_normalization import normalize_format  # noqa: E402
from lib.observability import init_logger  # noqa: E402
from lib.pg_text import strip_pg_null_bytes  # noqa: E402

logger = logging.getLogger(__name__)

# Table configs: (csv_filename, table_name, csv_columns, db_columns, required_columns, transforms)
#
# csv_columns: columns to read from the CSV (in CSV header order)
# db_columns: corresponding column names in the DB table
# required_columns: CSV column names that must not be null
# transforms: dict mapping csv_column -> callable for value transformation
#
# When csv_columns != db_columns, values are mapped positionally.

YEAR_RE = re.compile(r"^[0-9]{4}")


def extract_year(released: str | None) -> str | None:
    """Extract 4-digit year from a Discogs 'released' text field."""
    if released and YEAR_RE.match(released):
        return released[:4]
    return None


def count_tracks_from_csv(csv_path: Path) -> dict[int, int]:
    """Count tracks per release_id from a release_track CSV file.

    Returns a dict mapping release_id -> track count.
    Uses csv.reader with positional indexing instead of csv.DictReader
    to avoid dict creation overhead on 100M+ row files.
    """
    counts: dict[int, int] = {}
    with open(csv_path, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        release_id_idx = header.index("release_id")
        for row in reader:
            try:
                release_id = int(row[release_id_idx])
            except (ValueError, IndexError):
                continue
            counts[release_id] = counts.get(release_id, 0) + 1
    return counts


class TableConfig(TypedDict, total=False):
    csv_file: str
    table: str
    csv_columns: list[str]
    db_columns: list[str]
    required: list[str]
    transforms: dict[str, Callable[[str | None], str | None]]
    unique_key: list[str]


BASE_TABLES: list[TableConfig] = [
    {
        "csv_file": "release.csv",
        "table": "release",
        "csv_columns": ["id", "title", "country", "released", "format", "master_id"],
        "db_columns": ["id", "title", "country", "released", "format", "master_id"],
        "required": ["id", "title"],
        "transforms": {"format": normalize_format},
        "unique_key": ["id"],
    },
    {
        "csv_file": "release_artist.csv",
        "table": "release_artist",
        "csv_columns": ["release_id", "artist_id", "artist_name", "extra", "role"],
        "db_columns": ["release_id", "artist_id", "artist_name", "extra", "role"],
        "required": ["release_id", "artist_name"],
        "transforms": {},
        "unique_key": ["release_id", "artist_name"],
    },
    {
        "csv_file": "release_label.csv",
        "table": "release_label",
        "csv_columns": ["release_id", "label", "catno"],
        "db_columns": ["release_id", "label_name", "catno"],
        "required": ["release_id", "label"],
        "transforms": {},
        "unique_key": ["release_id", "label"],
    },
    {
        "csv_file": "release_genre.csv",
        "table": "release_genre",
        "csv_columns": ["release_id", "genre"],
        "db_columns": ["release_id", "genre"],
        "required": ["release_id", "genre"],
        "transforms": {},
        "unique_key": ["release_id", "genre"],
    },
    {
        "csv_file": "release_style.csv",
        "table": "release_style",
        "csv_columns": ["release_id", "style"],
        "db_columns": ["release_id", "style"],
        "required": ["release_id", "style"],
        "transforms": {},
        "unique_key": ["release_id", "style"],
    },
]

TRACK_TABLES: list[TableConfig] = [
    {
        "csv_file": "release_track.csv",
        "table": "release_track",
        "csv_columns": ["release_id", "sequence", "position", "title", "duration"],
        "db_columns": ["release_id", "sequence", "position", "title", "duration"],
        "required": ["release_id", "title"],
        "transforms": {},
        "unique_key": ["release_id", "sequence"],
    },
    {
        "csv_file": "release_track_artist.csv",
        "table": "release_track_artist",
        "csv_columns": ["release_id", "track_sequence", "artist_name"],
        "db_columns": ["release_id", "track_sequence", "artist_name"],
        "required": ["release_id", "track_sequence"],
        "transforms": {},
        "unique_key": ["release_id", "track_sequence", "artist_name"],
    },
]

VIDEO_TABLES: list[TableConfig] = [
    {
        "csv_file": "release_video.csv",
        "table": "release_video",
        "csv_columns": ["release_id", "sequence", "src", "title", "duration", "embed"],
        "db_columns": ["release_id", "sequence", "src", "title", "duration", "embed"],
        "required": ["release_id", "src"],
        "transforms": {},
        "unique_key": ["release_id", "sequence"],
    },
]

ARTIST_TABLES: list[TableConfig] = [
    {
        "csv_file": "artist_alias.csv",
        "table": "artist_alias",
        "csv_columns": ["artist_id", "alias_name"],
        "db_columns": ["artist_id", "alias_name"],
        "required": ["artist_id", "alias_name"],
        "transforms": {},
        "unique_key": ["artist_id", "alias_name"],
    },
    {
        "csv_file": "artist_member.csv",
        "table": "artist_member",
        "csv_columns": ["group_artist_id", "member_artist_id", "member_name"],
        "db_columns": ["artist_id", "member_id", "member_name"],
        "required": ["group_artist_id", "member_artist_id", "member_name"],
        "transforms": {},
        "unique_key": ["group_artist_id", "member_artist_id"],
    },
]

MASTER_TABLES: list[TableConfig] = [
    {
        "csv_file": "master.csv",
        "table": "master",
        "csv_columns": ["id", "title", "main_release_id", "year"],
        "db_columns": ["id", "title", "main_release_id", "year"],
        "required": ["id", "title"],
        "transforms": {},
        "unique_key": ["id"],
    },
    {
        "csv_file": "master_artist.csv",
        "table": "master_artist",
        "csv_columns": ["master_id", "artist_id", "artist_name"],
        "db_columns": ["master_id", "artist_id", "artist_name"],
        "required": ["master_id", "artist_name"],
        "transforms": {},
        "unique_key": ["master_id", "artist_id"],
    },
]

TABLES: list[TableConfig] = BASE_TABLES + TRACK_TABLES + VIDEO_TABLES


def import_csv(
    conn,
    csv_path: Path,
    table: str,
    csv_columns: list[str],
    db_columns: list[str],
    required_columns: list[str],
    transforms: dict,
    unique_key: list[str] | None = None,
    release_id_filter: set[int] | None = None,
    id_filter: set[int] | None = None,
    id_filter_column: str | None = None,
) -> int:
    """Import a CSV file into a table, selecting only needed columns.

    Reads the CSV header to find column indices, extracts only the columns
    listed in csv_columns, applies any transforms, and writes to the DB
    using the corresponding db_columns names.

    If unique_key is provided, duplicate rows (by those CSV columns) are
    skipped, keeping the first occurrence.

    If release_id_filter is provided, only rows whose release_id is in the
    set are imported. The CSV must have a 'release_id' or 'id' column.

    If id_filter and id_filter_column are provided, only rows where the
    specified column's integer value is in id_filter are imported.
    """
    logger.info(f"Importing {csv_path.name} into {table}...")

    db_col_list = ", ".join(db_columns)

    # Build unique key column indices for dedup
    unique_key_indices: list[int] | None = None
    if unique_key:
        unique_key_indices = [csv_columns.index(col) for col in unique_key]

    with open(csv_path, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            logger.warning(f"  No header found in {csv_path.name}, skipping")
            return 0

        # Verify required CSV columns exist in header
        missing = sorted(set(csv_columns) - set(header))
        if missing:
            logger.error(f"  Missing columns in {csv_path.name}: {missing}")
            return 0

        # Build column index mapping for positional access
        col_idx = {col: header.index(col) for col in csv_columns}

        # Find indices of required columns for null checking
        required_set = set(required_columns)
        if _HAS_WXYC_ETL and not os.environ.get("WXYC_ETL_NO_RUST"):
            seen = DedupSet()
        else:
            seen: set[tuple[str | None, ...]] = set()

        # Determine release_id column index for filtering
        release_id_idx: int | None = None
        if release_id_filter is not None:
            for col_name in ("release_id", "id"):
                if col_name in col_idx:
                    release_id_idx = col_idx[col_name]
                    break

        # Determine generic id_filter column index
        id_filter_idx: int | None = None
        if id_filter is not None and id_filter_column is not None:
            if id_filter_column in header:
                id_filter_idx = header.index(id_filter_column)

        with conn.cursor() as cur:
            with cur.copy(f"COPY {table} ({db_col_list}) FROM STDIN") as copy:
                count = 0
                skipped = 0
                filtered = 0
                dupes = 0
                for row in reader:
                    # Filter by release_id if specified
                    if release_id_filter is not None and release_id_idx is not None:
                        try:
                            rid = int(row[release_id_idx])
                        except (ValueError, IndexError):
                            filtered += 1
                            continue
                        if rid not in release_id_filter:
                            filtered += 1
                            continue

                    # Filter by generic id column if specified
                    if id_filter is not None and id_filter_idx is not None:
                        try:
                            fid = int(row[id_filter_idx])
                        except (ValueError, IndexError):
                            filtered += 1
                            continue
                        if fid not in id_filter:
                            filtered += 1
                            continue

                    # Extract only the columns we need
                    values: list[str | None] = []
                    skip = False
                    for csv_col in csv_columns:
                        val = row[col_idx[csv_col]]
                        if val == "":
                            val = None

                        # Apply transform if defined
                        if csv_col in transforms:
                            val = transforms[csv_col](val)

                        # Check required columns
                        if csv_col in required_set and val is None:
                            skip = True
                            break

                        # Strip U+0000 at PG TEXT boundary (WXYC/docs#18 policy).
                        # PostgreSQL TEXT rejects NUL bytes; stripping is idempotent.
                        val = strip_pg_null_bytes(val)

                        values.append(val)

                    if skip:
                        skipped += 1
                        continue

                    # Dedup by unique key
                    if unique_key_indices is not None:
                        key = tuple(values[i] for i in unique_key_indices)
                        if key in seen:
                            dupes += 1
                            continue
                        seen.add(key)

                    copy.write_row(values)
                    count += 1

                    if count % 500000 == 0:
                        logger.info(f"  {table}: {count:,} rows...")

            logger.info(f"  {table}: COPY complete, committing...")
    conn.commit()
    logger.info(f"  {table}: committed")
    parts = [f"Imported {count:,} rows"]
    if skipped > 0:
        parts.append(f"skipped {skipped:,} with null required fields")
    if filtered > 0:
        parts.append(f"filtered {filtered:,} by release_id")
    if dupes > 0:
        parts.append(f"skipped {dupes:,} duplicates")
    logger.info(f"  {', '.join(parts)}")
    return count


def populate_cache_metadata(conn) -> int:
    """Populate cache_metadata for all releases via COPY.

    Much faster than INSERT...SELECT with ON CONFLICT for large tables.
    Assumes cache_metadata is empty (schema freshly created).

    Returns the number of rows inserted.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM release")
        release_ids = [row[0] for row in cur.fetchall()]

    count = len(release_ids)
    with conn.cursor() as cur:
        with cur.copy("COPY cache_metadata (release_id, source) FROM STDIN") as copy:
            for rid in release_ids:
                copy.write_row((rid, "bulk_import"))
    conn.commit()
    logger.info(f"  Populated cache_metadata with {count:,} rows")
    return count


def populate_release_year(conn) -> int:
    """Populate release_year from the released text field.

    Extracts the 4-digit year prefix from the 'released' column and stores it
    in the 'release_year' smallint column for efficient filtering.
    """
    logger.info("Populating release_year from released text...")
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE release SET release_year = CAST(LEFT(released, 4) AS smallint)
            WHERE released IS NOT NULL
              AND released ~ '^[0-9]{4}'
              AND release_year IS NULL
        """)
        count = cur.rowcount
    conn.commit()
    logger.info(f"  Populated release_year for {count:,} releases")
    return count


def create_track_count_table(conn, csv_dir: Path) -> int:
    """Pre-compute track counts from CSV and store in release_track_count table.

    Creates an unlogged table with (release_id, track_count) that dedup uses
    to rank releases by track count before tracks are imported.

    Returns the number of releases with track counts.
    """
    csv_path = csv_dir / "release_track.csv"
    if not csv_path.exists():
        logger.warning("release_track.csv not found, skipping track count table")
        return 0

    logger.info("Computing track counts from release_track.csv...")
    counts = count_tracks_from_csv(csv_path)

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS release_track_count")
        cur.execute("""
            CREATE UNLOGGED TABLE release_track_count (
                release_id integer PRIMARY KEY,
                track_count integer NOT NULL
            )
        """)
        with cur.copy("COPY release_track_count (release_id, track_count) FROM STDIN") as copy:
            for release_id, track_count in counts.items():
                copy.write_row((release_id, track_count))
    conn.commit()
    logger.info(f"  Created release_track_count with {len(counts):,} rows")
    return len(counts)


def import_artwork(conn, csv_dir: Path) -> int:
    """Populate release.artwork_url from release_image.csv.

    Reads the release_image CSV and updates the release table with the URI
    of each release's primary image. Only 'primary' type images are used;
    if none exists, the first image is used as fallback.
    """
    csv_path = csv_dir / "release_image.csv"
    if not csv_path.exists():
        logger.warning("release_image.csv not found, skipping artwork import")
        return 0

    logger.info("Importing artwork URLs from release_image.csv...")

    # Collect primary image URIs (one per release)
    artwork: dict[int, str] = {}
    fallback: dict[int, str] = {}

    with open(csv_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                release_id = int(row["release_id"])
            except (ValueError, KeyError):
                continue

            uri = row.get("uri", "")
            if not uri:
                continue

            img_type = row.get("type", "")
            if img_type == "primary" and release_id not in artwork:
                artwork[release_id] = uri
            elif release_id not in fallback:
                fallback[release_id] = uri

    # Merge: prefer primary, fall back to first image
    for release_id, uri in fallback.items():
        if release_id not in artwork:
            artwork[release_id] = uri

    if not artwork:
        logger.info("  No artwork URLs found")
        return 0

    # Batch update using a temp table for efficiency
    logger.info(f"  Updating {len(artwork):,} releases with artwork URLs...")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE _artwork (
                release_id integer PRIMARY KEY,
                artwork_url text NOT NULL
            )
        """)

        with cur.copy("COPY _artwork (release_id, artwork_url) FROM STDIN") as copy:
            for release_id, uri in artwork.items():
                copy.write_row((release_id, strip_pg_null_bytes(uri)))

        cur.execute("""
            UPDATE release r
            SET artwork_url = a.artwork_url
            FROM _artwork a
            WHERE r.id = a.release_id
        """)

        cur.execute("DROP TABLE _artwork")

    conn.commit()
    logger.info(f"  Updated {len(artwork):,} releases with artwork URLs")
    return len(artwork)


def _import_tables(
    conn,
    csv_dir: Path,
    table_list: list[TableConfig],
    release_id_filter: set[int] | None = None,
    artist_id_filter: set[int] | None = None,
) -> int:
    """Import a list of table configs, returning total row count.

    If artist_id_filter is provided, rows are filtered by the first column
    in csv_columns that contains 'artist_id' (e.g., 'artist_id' or
    'group_artist_id').
    """
    total = 0
    for table_config in table_list:
        csv_path = csv_dir / table_config["csv_file"]
        if not csv_path.exists():
            logger.warning(f"Skipping {table_config['csv_file']} (not found)")
            continue

        # Determine artist_id filter column for this table
        id_filter = None
        id_filter_column = None
        if artist_id_filter is not None:
            for col in table_config["csv_columns"]:
                if "artist_id" in col:
                    id_filter = artist_id_filter
                    id_filter_column = col
                    break

        count = import_csv(
            conn,
            csv_path,
            table_config["table"],
            table_config["csv_columns"],
            table_config["db_columns"],
            table_config["required"],
            table_config["transforms"],
            unique_key=table_config.get("unique_key"),
            release_id_filter=release_id_filter,
            id_filter=id_filter,
            id_filter_column=id_filter_column,
        )
        total += count
    return total


def _import_tables_parallel(
    db_url: str,
    csv_dir: Path,
    parent_tables: list[TableConfig],
    child_tables: list[TableConfig],
    release_id_filter: set[int] | None = None,
) -> int:
    """Import parent tables sequentially, then child tables concurrently.

    Each child table import runs on its own connection via ThreadPoolExecutor.
    Parent tables must be imported first to satisfy FK constraints.

    Returns total row count across all tables.
    """
    from concurrent.futures import ThreadPoolExecutor

    total = 0

    # Import parent tables sequentially (on a shared connection)
    conn = psycopg.connect(db_url)
    for table_config in parent_tables:
        csv_path = csv_dir / table_config["csv_file"]
        if not csv_path.exists():
            logger.warning(f"Skipping {table_config['csv_file']} (not found)")
            continue
        count = import_csv(
            conn,
            csv_path,
            table_config["table"],
            table_config["csv_columns"],
            table_config["db_columns"],
            table_config["required"],
            table_config["transforms"],
            unique_key=table_config.get("unique_key"),
            release_id_filter=release_id_filter,
        )
        total += count
    conn.close()

    # Import child tables concurrently (each on its own connection)
    def _import_child(table_config: TableConfig) -> int:
        csv_path = csv_dir / table_config["csv_file"]
        if not csv_path.exists():
            logger.warning(f"Skipping {table_config['csv_file']} (not found)")
            return 0
        child_conn = psycopg.connect(db_url)
        count = import_csv(
            child_conn,
            csv_path,
            table_config["table"],
            table_config["csv_columns"],
            table_config["db_columns"],
            table_config["required"],
            table_config["transforms"],
            unique_key=table_config.get("unique_key"),
            release_id_filter=release_id_filter,
        )
        child_conn.close()
        return count

    if child_tables:
        with ThreadPoolExecutor(max_workers=len(child_tables)) as executor:
            futures = [executor.submit(_import_child, tc) for tc in child_tables]
            for future in futures:
                total += future.result()

    return total


def import_artist_details(conn, csv_dir: Path) -> int:
    """Import artist detail tables from CSV.

    Creates stub artist rows from release_artist data, then imports
    artist_alias and artist_member CSVs.

    Returns total rows imported.
    """
    # Create stub artist rows from release_artist (id + name only)
    logger.info("Creating stub artist rows from release_artist...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO artist (id, name)
            SELECT DISTINCT artist_id, artist_name
            FROM release_artist
            WHERE artist_id IS NOT NULL
            ON CONFLICT (id) DO NOTHING
        """)
        count = cur.rowcount
    conn.commit()
    logger.info(f"  Created {count:,} stub artist rows")

    total = count

    # Update artist profiles from artist.csv (if present)
    artist_csv = csv_dir / "artist.csv"
    if artist_csv.exists():
        logger.info("Updating artist profiles from artist.csv...")
        import csv as csv_mod

        profile_count = 0
        with open(artist_csv, newline="", encoding="utf-8") as f:
            reader = csv_mod.DictReader(f)
            with conn.cursor() as cur:
                for row in reader:
                    artist_id = row.get("artist_id")
                    profile = row.get("profile", "").strip()
                    if artist_id and profile:
                        cur.execute(
                            "UPDATE artist SET profile = %s WHERE id = %s",
                            (strip_pg_null_bytes(profile), int(artist_id)),
                        )
                        profile_count += cur.rowcount
        conn.commit()
        logger.info(f"  Updated {profile_count:,} artist profiles")
        total += profile_count
    else:
        logger.info("No artist.csv found, skipping profile import")

    # Query known artist IDs for filtering artist_alias and artist_member
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM artist")
        artist_ids = {row[0] for row in cur.fetchall()}
    logger.info(f"  Filtering artist tables to {len(artist_ids):,} known artists")

    total += _import_tables(conn, csv_dir, ARTIST_TABLES, artist_id_filter=artist_ids)
    return total


def import_masters(conn, csv_dir: Path) -> int:
    """Import master tables from CSV.

    Imports master.csv first (parent), then master_artist.csv (child).
    Returns total rows imported.
    """
    return _import_tables(conn, csv_dir, MASTER_TABLES)


def main():
    import argparse

    init_logger(repo="discogs-etl", tool="discogs-etl import_csv")

    parser = argparse.ArgumentParser(description="Import Discogs CSV files into PostgreSQL")
    parser.add_argument("csv_dir", type=Path, help="Directory containing CSV files")
    parser.add_argument(
        "db_url",
        nargs="?",
        default="postgresql:///discogs",
        help="PostgreSQL connection URL",
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--base-only",
        action="store_true",
        help="Import only base tables (release, release_artist, release_label) "
        "plus artwork, cache_metadata, and track counts",
    )
    mode.add_argument(
        "--tracks-only",
        action="store_true",
        help="Import only track tables, filtered to surviving release IDs",
    )

    args = parser.parse_args()
    csv_dir = args.csv_dir
    db_url = args.db_url

    if not csv_dir.exists():
        logger.error(f"CSV directory not found: {csv_dir}")
        sys.exit(1)

    logger.info(f"Connecting to {db_url}")
    conn = psycopg.connect(db_url)

    if args.tracks_only:
        # Query surviving release IDs from the database
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release")
            release_ids = {row[0] for row in cur.fetchall()}
        conn.close()
        logger.info(f"Filtering tracks to {len(release_ids):,} surviving releases")
        # release_track, release_track_artist, and release_video are independent — import in parallel
        total = _import_tables_parallel(
            db_url,
            csv_dir,
            parent_tables=[],
            child_tables=TRACK_TABLES + VIDEO_TABLES,
            release_id_filter=release_ids,
        )
    elif args.base_only:
        conn.close()
        # release is parent; release_artist and release_label are independent children
        total = _import_tables_parallel(
            db_url, csv_dir, parent_tables=BASE_TABLES[:1], child_tables=BASE_TABLES[1:]
        )
        conn = psycopg.connect(db_url)
        logger.info("Populating artwork URLs...")
        import_artwork(conn, csv_dir)
        logger.info("Artwork URLs complete")
        populate_release_year(conn)
        logger.info("Populating cache_metadata via COPY...")
        populate_cache_metadata(conn)
        logger.info("cache_metadata complete")
        logger.info("Creating track count table...")
        create_track_count_table(conn, csv_dir)
        logger.info("Track count table complete")
        logger.info("Importing artist details...")
        import_artist_details(conn, csv_dir)
        logger.info("Artist details complete")
        logger.info("Importing masters...")
        import_masters(conn, csv_dir)
        logger.info("Masters complete")
        conn.close()
    else:
        total = _import_tables(conn, csv_dir, TABLES)
        logger.info("Populating artwork URLs...")
        import_artwork(conn, csv_dir)
        logger.info("Artwork URLs complete")
        populate_release_year(conn)
        logger.info("Populating cache_metadata via COPY...")
        populate_cache_metadata(conn)
        logger.info("cache_metadata complete")
        logger.info("Importing artist details...")
        import_artist_details(conn, csv_dir)
        logger.info("Artist details complete")
        logger.info("Importing masters...")
        import_masters(conn, csv_dir)
        logger.info("Masters complete")
        conn.close()

    logger.info(f"Total: {total:,} rows imported")


if __name__ == "__main__":
    main()
