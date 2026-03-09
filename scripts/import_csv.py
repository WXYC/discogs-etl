#!/usr/bin/env python3
"""Import Discogs CSV files into PostgreSQL with proper multiline handling.

Imports only the columns needed by the optimized schema (see 04-create-database.sql).
Dropped tables (release_genre, release_style, artist) are skipped.
The release_image.csv is processed separately to populate artwork_url on release.
"""

from __future__ import annotations

import csv
import logging
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TypedDict

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
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
    """
    counts: dict[int, int] = {}
    with open(csv_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                release_id = int(row["release_id"])
            except (ValueError, KeyError):
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
        "csv_columns": ["id", "title", "country", "released", "master_id"],
        "db_columns": ["id", "title", "country", "release_year", "master_id"],
        "required": ["id", "title"],
        "transforms": {"released": extract_year},
    },
    {
        "csv_file": "release_artist.csv",
        "table": "release_artist",
        "csv_columns": ["release_id", "artist_id", "artist_name", "extra"],
        "db_columns": ["release_id", "artist_id", "artist_name", "extra"],
        "required": ["release_id", "artist_name"],
        "transforms": {},
        "unique_key": ["release_id", "artist_name"],
    },
    {
        "csv_file": "release_label.csv",
        "table": "release_label",
        "csv_columns": ["release_id", "label"],
        "db_columns": ["release_id", "label_name"],
        "required": ["release_id", "label"],
        "transforms": {},
        "unique_key": ["release_id", "label"],
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

TABLES: list[TableConfig] = BASE_TABLES + TRACK_TABLES


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
) -> int:
    """Import a CSV file into a table, selecting only needed columns.

    Reads the CSV header to find column indices, extracts only the columns
    listed in csv_columns, applies any transforms, and writes to the DB
    using the corresponding db_columns names.

    If unique_key is provided, duplicate rows (by those CSV columns) are
    skipped, keeping the first occurrence.

    If release_id_filter is provided, only rows whose release_id is in the
    set are imported. The CSV must have a 'release_id' or 'id' column.
    """
    logger.info(f"Importing {csv_path.name} into {table}...")

    db_col_list = ", ".join(db_columns)

    # Build unique key column indices for dedup
    unique_key_indices: list[int] | None = None
    if unique_key:
        unique_key_indices = [csv_columns.index(col) for col in unique_key]

    with open(csv_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        if not header:
            logger.warning(f"  No header found in {csv_path.name}, skipping")
            return 0

        # Verify required CSV columns exist in header
        missing = [c for c in csv_columns if c not in header]
        if missing:
            logger.error(f"  Missing columns in {csv_path.name}: {missing}")
            return 0

        # Find indices of required columns for null checking
        required_set = set(required_columns)
        seen: set[tuple[str | None, ...]] = set()

        # Determine release_id column name for filtering
        release_id_col: str | None = None
        if release_id_filter is not None:
            for col_name in ("release_id", "id"):
                if col_name in csv_columns:
                    release_id_col = col_name
                    break

        with conn.cursor() as cur:
            with cur.copy(f"COPY {table} ({db_col_list}) FROM STDIN") as copy:
                count = 0
                skipped = 0
                filtered = 0
                dupes = 0
                for row in reader:
                    # Filter by release_id if specified
                    if release_id_filter is not None and release_id_col is not None:
                        try:
                            rid = int(row.get(release_id_col, ""))
                        except (ValueError, TypeError):
                            filtered += 1
                            continue
                        if rid not in release_id_filter:
                            filtered += 1
                            continue

                    # Extract only the columns we need
                    values: list[str | None] = []
                    skip = False
                    for csv_col in csv_columns:
                        val = row.get(csv_col, "")
                        if val == "":
                            val = None

                        # Apply transform if defined
                        if csv_col in transforms:
                            val = transforms[csv_col](val)

                        # Check required columns
                        if csv_col in required_set and val is None:
                            skip = True
                            break

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

    conn.commit()
    parts = [f"Imported {count:,} rows"]
    if skipped > 0:
        parts.append(f"skipped {skipped:,} with null required fields")
    if filtered > 0:
        parts.append(f"filtered {filtered:,} by release_id")
    if dupes > 0:
        parts.append(f"skipped {dupes:,} duplicates")
    logger.info(f"  {', '.join(parts)}")
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
                copy.write_row((release_id, uri))

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
) -> int:
    """Import a list of table configs, returning total row count."""
    total = 0
    for table_config in table_list:
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


def main():
    import argparse

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
        # release_track and release_track_artist are independent — import in parallel
        total = _import_tables_parallel(
            db_url,
            csv_dir,
            parent_tables=[],
            child_tables=TRACK_TABLES,
            release_id_filter=release_ids,
        )
    elif args.base_only:
        conn.close()
        # release is parent; release_artist and release_label are independent children
        total = _import_tables_parallel(
            db_url, csv_dir, parent_tables=BASE_TABLES[:1], child_tables=BASE_TABLES[1:]
        )
        conn = psycopg.connect(db_url)
        import_artwork(conn, csv_dir)
        logger.info("Populating cache_metadata...")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cache_metadata (release_id, source)
                SELECT id, 'bulk_import'
                FROM release
                ON CONFLICT (release_id) DO NOTHING
            """)
        conn.commit()
        create_track_count_table(conn, csv_dir)
        conn.close()
    else:
        total = _import_tables(conn, csv_dir, TABLES)
        import_artwork(conn, csv_dir)
        logger.info("Populating cache_metadata...")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cache_metadata (release_id, source)
                SELECT id, 'bulk_import'
                FROM release
                ON CONFLICT (release_id) DO NOTHING
            """)
        conn.commit()
        conn.close()

    logger.info(f"Total: {total:,} rows imported")


if __name__ == "__main__":
    main()
