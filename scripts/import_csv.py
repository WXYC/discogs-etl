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
from wxyc_etl.pg import to_pg_text_form  # noqa: E402

from lib.format_normalization import normalize_format  # noqa: E402
from lib.observability import init_logger  # noqa: E402

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
    # ``optional_csv_columns``: column names that the loader includes in
    # the COPY if (and only if) they appear in the CSV header. Their DB
    # column names must match the CSV column names. Used for forward-
    # compatibility with new converter columns whose absence in older
    # CSVs should fall through to the DB-side default — e.g. ``extra``
    # and ``role`` on ``release_track_artist`` per WXYC/discogs-etl#218.
    optional_csv_columns: list[str]
    # ``optional_unique_key``: optional columns that should join the dedup
    # ``unique_key`` *only when present in the CSV header* (i.e. a subset of
    # ``optional_csv_columns`` that are key-bearing). Lets a table widen its
    # dedup key for new converter output without crashing legacy CSVs that
    # lack the column — ``csv_columns.index(col)`` would raise. See
    # WXYC/discogs-etl#293 and the converter's ``WideTrackArtistDedup``
    # (discogs-xml-converter#74).
    optional_unique_key: list[str]


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
        "csv_columns": ["release_id", "artist_id", "artist_name", "extra"],
        "db_columns": ["release_id", "artist_id", "artist_name", "extra"],
        "required": ["release_id", "artist_name"],
        "transforms": {},
        # ``extra`` is in the dedup key so a person who is both the main
        # artist (``extra=0``) and a same-release extra credit (``extra=1``,
        # e.g. ``Written-By``) keeps *both* rows — the converter writes the
        # ``extra=0`` row first, and a key without ``extra`` would drop the
        # role-bearing ``extra=1`` row (WXYC/discogs-etl#293). Safe as a
        # static key because ``extra`` is always in ``csv_columns`` above
        # (unlike ``release_track_artist``, where it is optional). Converges
        # with the converter's ``WideArtistDedup`` (discogs-xml-converter#74).
        "unique_key": ["release_id", "artist_name", "extra"],
        # The converter now emits the source ``<role>`` for release-level
        # extra credits (writer/composer/producer). ``role`` is listed as
        # OPTIONAL — not in the required ``csv_columns`` above — so the loader
        # reads it when present and tolerates its absence in older CSVs
        # (PG default ``role=NULL``) rather than bailing with "Missing
        # columns" and writing zero rows (the #204 failure mode). Mirrors
        # ``release_track_artist`` (WXYC/discogs-etl#218); populates
        # ``release_artist.role`` for the release-level composer fallback
        # (WXYC/library-metadata-lookup#699).
        "optional_csv_columns": ["role"],
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
        # ``extra`` and ``role`` were added per WXYC/discogs-etl#218 so
        # downstream consumers can filter to main-artist credits
        # (``WHERE extra = 0``) and inspect the source-side role string.
        # Listed as optional so the loader tolerates both the new 5-col
        # converter output and pre-#55 3-col CSVs without bouncing the
        # import. PG defaults (``extra=0``, ``role=NULL``) cover absent
        # columns, which matches the legacy "everything was main credits"
        # interpretation under which existing consumers were operating.
        "optional_csv_columns": ["extra", "role"],
        # Widen the dedup key with ``extra`` so a same-track main+extra credit
        # for one person keeps both rows (WXYC/discogs-etl#293), but *only*
        # when the column is present — a static ``extra`` key would crash a
        # legacy 3-column CSV at ``csv_columns.index("extra")``. Mirrors the
        # converter's ``WideTrackArtistDedup`` (discogs-xml-converter#74).
        "optional_unique_key": ["extra"],
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
        # WXYC/discogs-xml-converter#54 splits Discogs `<namevariations>`
        # (spelling variants) out of artist_alias.csv into their own file.
        # Without this entry the table would stay empty after every rebuild
        # and LML's fuzzy-name path would query an empty table; see #215.
        "csv_file": "artist_name_variation.csv",
        "table": "artist_name_variation",
        "csv_columns": ["artist_id", "name"],
        "db_columns": ["artist_id", "name"],
        "required": ["artist_id", "name"],
        "transforms": {},
        "unique_key": ["artist_id", "name"],
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
    {
        # WXYC/discogs-xml-converter#68 extends the converter to extract
        # Discogs `<urls>` (Wikipedia, official sites, social) into
        # artist_url.csv. Without this entry the file would silently drop on
        # the floor at rebuild time, leaving `artist_url` empty and the LML
        # `cache_service.get_artist_details` join returning no external URLs.
        # Step 3 of WXYC/library-metadata-lookup#497.
        "csv_file": "artist_url.csv",
        "table": "artist_url",
        "csv_columns": ["artist_id", "url"],
        "db_columns": ["artist_id", "url"],
        "required": ["artist_id", "url"],
        "transforms": {},
        "unique_key": ["artist_id", "url"],
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


# Cache tables wiped by --truncate-existing, partitioned by mode. The base
# set wipes the full cache; the tracks set wipes only the tables that
# --tracks-only writes to. Two exclusions in either set:
#
#   - the entire entity schema (entity.identity / entity.reconciliation_log
#     are WXYC-side artist identity; entity.release_identity /
#     entity.release_reconciliation_log are the release-side counterpart
#     added by alembic 0012 for LML#526). The rebuild must NOT touch any
#     of these — they hold mint state owned by LML.
#   - alembic_version — migration history must persist across rebuilds.
#
# Mode-awareness matters: in a full run_pipeline.py invocation the base
# step runs first, then dedup, then the tracks step. If --tracks-only
# wiped base tables, the tracks step would erase the just-deduped data
# and find zero release IDs to filter against. The tracks subset only
# touches track-domain tables, so a tracks-only rerun against an
# already-populated cache only refreshes the tracks.
#
# `release` stays in this list for --truncate-existing only. The default
# incremental path goes through import_release_via_upsert instead.
CACHE_TABLES_TO_TRUNCATE_BASE: list[str] = [
    "release",
    "release_artist",
    "release_label",
    "release_genre",
    "release_style",
    "release_track",
    "release_track_artist",
    "release_video",
    "artist",
    "artist_alias",
    "artist_member",
    "artist_name_variation",
    "artist_url",
    "master",
    "master_artist",
    "cache_metadata",
]

CACHE_TABLES_TO_TRUNCATE_TRACKS: list[str] = [
    "release_track",
    "release_track_artist",
    "release_video",
]


def _validate_truncate_lists() -> None:
    """Enforce the 'preserves the entire entity schema' promise at import time.

    The exclusion works because both truncate lists hold bare public-schema
    table names — any schema-qualified entry (the only way to reach entity.*
    via TRUNCATE) trips this guard before any pipeline run can do damage.
    A loose bare-prefix check would overshoot onto legitimate public tables
    that happen to start with "entity" (e.g. a future ``entity_log``
    analytics table), so the contract is narrowed to schema qualifiers only.

    Wrapped in a function so the loop variable doesn't leak into the module
    namespace and so an empty truncate list (e.g. during a future refactor)
    doesn't raise ``NameError`` from a stray ``del``.
    """
    for table in (*CACHE_TABLES_TO_TRUNCATE_BASE, *CACHE_TABLES_TO_TRUNCATE_TRACKS):
        if "." in table:
            raise RuntimeError(
                f"--truncate-existing list must not include schema-qualified "
                f"names; found {table!r}. Cross-schema TRUNCATE is the only "
                f"path that could reach LML-owned entity.* state — see "
                f"comment above CACHE_TABLES_TO_TRUNCATE_BASE."
            )


_validate_truncate_lists()


def _truncate_tables(conn, table_names: list[str]) -> None:
    """Wipe the named tables with a single TRUNCATE ... CASCADE.

    One statement keeps the operation atomic (either all empty or none).
    CASCADE handles FK dependencies for any table not in the explicit list
    (e.g., release_image's FK to release if release_image is ever added to
    the schema but not yet to CACHE_TABLES_TO_TRUNCATE).

    Commits immediately. Otherwise parallel workers — which open fresh
    connections via ``_import_tables_parallel`` — would still see the
    pre-TRUNCATE rows under MVCC isolation and the COPY would fail with
    the same duplicate-key violation we're trying to avoid.
    """
    if not table_names:
        return
    quoted = ", ".join(f'"{t}"' for t in table_names)
    sql = f"TRUNCATE {quoted} CASCADE"
    logger.warning(f"--truncate-existing: TRUNCATE {len(table_names)} cache tables")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


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
    optional_csv_columns: list[str] | None = None,
    optional_unique_key: list[str] | None = None,
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

    If ``optional_csv_columns`` is provided, those column names are
    included in the COPY only when they appear in the CSV header. The DB
    column name is assumed to match the CSV column name. Absent columns
    fall through to the DB-side default (``DEFAULT`` clause / NULL). Used
    for forward-compatibility with new converter columns (e.g. ``extra``
    / ``role`` on ``release_track_artist`` per WXYC/discogs-etl#218).

    If ``optional_unique_key`` is provided, those columns are appended to
    ``unique_key`` for the dedup — but only the ones that are actually
    present in this CSV's header (a subset of ``optional_csv_columns``).
    This lets a table widen its dedup key for new converter output without
    crashing on a legacy CSV that lacks the column (WXYC/discogs-etl#293).
    """
    logger.info(f"Importing {csv_path.name} into {table}...")

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

        # Detect which optional columns are present in this CSV header.
        # When the producer (discogs-xml-converter) ships the new
        # columns, they're appended to csv_columns / db_columns for
        # the duration of this call. Older converters that don't emit
        # them get the legacy behavior (PG defaults populate the
        # absent columns).
        present_optional = [col for col in (optional_csv_columns or []) if col in header]
        if present_optional:
            csv_columns = list(csv_columns) + present_optional
            db_columns = list(db_columns) + present_optional
            # Widen the dedup key with any optional key-columns that showed up
            # this run (WXYC/discogs-etl#293). Scoped to present_optional so a
            # CSV lacking the column keeps the static key and never hits the
            # ValueError from csv_columns.index() on a missing column.
            extra_key_cols = [c for c in (optional_unique_key or []) if c in present_optional]
            if extra_key_cols and unique_key is not None:
                unique_key = list(unique_key) + extra_key_cols

        db_col_list = ", ".join(db_columns)

        # Build unique key column indices for dedup
        unique_key_indices: list[int] | None = None
        if unique_key:
            unique_key_indices = [csv_columns.index(col) for col in unique_key]

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
                        val = to_pg_text_form(val)

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
    """Populate cache_metadata for all releases.

    Uses INSERT ... SELECT ... ON CONFLICT DO NOTHING rather than COPY
    because cache_metadata is concurrently written by the live
    library-metadata-lookup service: on every Discogs API miss, LML's
    ``discogs/cache_service.py`` inserts a row with source='api_fetch'.
    During a rebuild, those concurrent writes race the bulk populate
    and cause duplicate-key violations on COPY (which has no upsert
    semantics). See WXYC/discogs-etl#188 (2026-05-13 21:32 UTC run, 52
    'api_fetch' rows appeared in the 34-second window between TRUNCATE
    and the populate step).

    Returns the number of rows actually inserted; ON CONFLICT skips
    are excluded, which is the right denominator for "how much did this
    populate step add?". Rows already present from concurrent
    'api_fetch' writes keep their original source.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cache_metadata (release_id, source)
            SELECT id, 'bulk_import' FROM release
            ON CONFLICT (release_id) DO NOTHING
        """)
        count = cur.rowcount
    conn.commit()
    logger.info(f"  Populated cache_metadata with {count:,} rows (ON CONFLICT skips excluded)")
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


# Tables wiped by import_release_via_upsert before re-COPY. Derived from
# the existing config lists so a future child table added to BASE_TABLES /
# TRACK_TABLES / VIDEO_TABLES is picked up automatically. cache_metadata is
# the lone non-config entry — it's populated by populate_cache_metadata
# rather than COPY, but it's still derivative of the current rebuild and
# stale rows would mismatch release after the upsert's DELETE prunes.
_RELEASE_CHILD_TABLES: list[str] = [
    t["table"] for t in BASE_TABLES[1:] + TRACK_TABLES + VIDEO_TABLES
] + ["cache_metadata"]


# Prune of releases absent from the incoming dump's staging table. It MUST use
# NOT EXISTS (a Hash Anti Join), never NOT IN (SELECT ...): PostgreSQL cannot
# plan NOT IN as an anti-join (SQL NULL semantics), so it falls back to an
# O(n*m) per-row Materialized SubPlan. At ~682K releases that SubPlan ran
# 2h20m on-CPU before Railway admin-killed the connection mid-rebuild
# (2026-07-06), aborting after the child-table TRUNCATE committed and leaving
# release_track / release_artist empty. release.id and release_staging.id are
# both NOT NULL, so the two forms delete identical rows. See
# WXYC/discogs-etl#298 / #302. TestPruneStaleReleasesPlan pins the plan shape
# so a regression back to NOT IN fails loudly instead of re-stalling a rebuild.
PRUNE_STALE_RELEASES_SQL = (
    "DELETE FROM release r WHERE NOT EXISTS (SELECT 1 FROM release_staging s WHERE s.id = r.id)"
)


def import_release_via_upsert(conn, csv_dir: Path) -> int:
    """Reload ``release`` from CSV while preserving artwork columns.

    Staging-table COPY + UPSERT with ``artwork_url`` and
    ``artwork_checked_at`` excluded from the SET list, plus a DELETE step
    pruning releases not in the new dump. Child tables of ``release`` are
    TRUNCATEd first so the subsequent COPYs don't append duplicates.

    Returns the staging row count (number of rows COPYed from release.csv).
    """
    release_config = next(t for t in BASE_TABLES if t["table"] == "release")
    csv_path = csv_dir / release_config["csv_file"]
    if not csv_path.exists():
        logger.warning(f"Skipping {release_config['csv_file']} (not found)")
        return 0

    logger.info("Truncating child tables of release ahead of upsert...")
    with conn.cursor() as cur:
        quoted = ", ".join(f'"{t}"' for t in _RELEASE_CHILD_TABLES)
        cur.execute(f"TRUNCATE {quoted} CASCADE")
    conn.commit()

    logger.info("Creating release_staging temp table...")
    with conn.cursor() as cur:
        # ON COMMIT PRESERVE ROWS (the default): import_csv() commits
        # internally, so ON COMMIT DROP would wipe the staging table out
        # from under the subsequent INSERT/DELETE.
        cur.execute("CREATE TEMP TABLE release_staging (LIKE release INCLUDING DEFAULTS)")

    rows = import_csv(
        conn,
        csv_path=csv_path,
        table="release_staging",
        csv_columns=release_config["csv_columns"],
        db_columns=release_config["db_columns"],
        required_columns=release_config["required"],
        transforms=release_config["transforms"],
        unique_key=release_config.get("unique_key"),
    )

    # Safety floor: refuse to apply an empty rebuild against a populated
    # cache. A truncated / mid-write release.csv would otherwise DELETE
    # every release downstream. Operators who *want* an empty cache go
    # through --fresh-rebuild + DROP CASCADE.
    if rows == 0:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE release_staging")
        conn.commit()
        raise RuntimeError(
            f"release_staging is empty after COPY from {csv_path.name}; "
            "refusing to DELETE every release. If you intended an empty "
            "cache, rerun with --fresh-rebuild."
        )

    # Index the staging table on id so the prune's anti-join below runs as a
    # hash/merge anti-join instead of a per-row scan of an unindexed temp table
    # (release_staging is created LIKE release INCLUDING DEFAULTS — no index).
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX ON release_staging (id)")
        cur.execute("ANALYZE release_staging")

    logger.info(f"Upserting {rows:,} releases (preserving artwork columns)...")
    with conn.cursor() as cur:
        # artwork_url, artwork_checked_at intentionally NOT in the SET
        # list — the contract of this function.
        #
        # not_found IS in the SET list (LML#510): a fresh dump is
        # authoritative, so any prior LML 404 tombstone clears here.
        # Without this, a tombstoned id would survive every rebuild and
        # stay unreachable until LML's admin recovery endpoint deletes it.
        cur.execute(
            """
            INSERT INTO release (id, title, country, released, format, master_id, not_found)
            SELECT id, title, country, released, format, master_id, FALSE FROM release_staging
            ON CONFLICT (id) DO UPDATE SET
                title     = EXCLUDED.title,
                country   = EXCLUDED.country,
                released  = EXCLUDED.released,
                format    = EXCLUDED.format,
                master_id = EXCLUDED.master_id,
                not_found = EXCLUDED.not_found
            """
        )
        # Prune releases absent from the new dump. NOT EXISTS, not NOT IN — see
        # PRUNE_STALE_RELEASES_SQL for the incident rationale (WXYC/discogs-etl#298).
        cur.execute(PRUNE_STALE_RELEASES_SQL)
        pruned = cur.rowcount
        cur.execute("DROP TABLE release_staging")
    conn.commit()
    logger.info(f"  Upsert complete; pruned {pruned:,} releases not in new dump")
    return rows


def import_artwork(conn, csv_dir: Path) -> int:
    """Populate release.artwork_url + stamp release.artwork_checked_at from release_image.csv.

    Only 'primary' type images are used; the first image is the fallback.
    Stamping artwork_checked_at matches LML's runtime write_release semantics
    so dump-imported rows are treated as cached. See
    docs/architecture.md → "artwork_checked_at Column Lifecycle".
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
                copy.write_row((release_id, to_pg_text_form(uri)))

        cur.execute("""
            UPDATE release r
            SET artwork_url = a.artwork_url,
                artwork_checked_at = now()
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
            optional_csv_columns=table_config.get("optional_csv_columns"),
            optional_unique_key=table_config.get("optional_unique_key"),
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
            optional_csv_columns=table_config.get("optional_csv_columns"),
            optional_unique_key=table_config.get("optional_unique_key"),
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
            optional_csv_columns=table_config.get("optional_csv_columns"),
            optional_unique_key=table_config.get("optional_unique_key"),
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

    Steps, in order:

    1. Stub-INSERT artist (id, name) from release_artist (ON CONFLICT DO
       NOTHING) so subsequent steps have a stable set of artist IDs.
    2. Snapshot `SELECT id FROM artist` into `artist_ids`. Used to gate
       both the profile UPDATE and the child-table loads, so the rebuild
       doesn't bother staging artists outside the WXYC-filtered set.
    3. If `artist.csv` is present, UPDATE artist.profile via a temp
       staging table populated from a Python-deduplicated dict (last-
       value-wins on duplicate artist_id). Pre-filters CSV rows to those
       whose artist_id is in `artist_ids`.
    4. Load `artist_alias`, `artist_name_variation`, and `artist_member`
       via _import_tables, filtered to `artist_ids`.

    Contract on `_artist_profile`: the staging table has no PRIMARY KEY;
    uniqueness is guaranteed by the caller-side dict, which is
    load-bearing — a future refactor that swaps the dict for an iterator
    must restore the PK or the UPDATE FROM JOIN's choice over duplicates
    becomes implementation-defined.

    Returns total rows imported (stubs + profile updates + child rows).
    """
    # Create stub artist rows from release_artist (id + name only)
    #
    # ON CONFLICT clears any prior LML#510 tombstone (`not_found = TRUE`).
    # `WHERE artist.not_found = TRUE` narrows the rewrite to actual
    # tombstones so non-tombstone rows aren't disturbed. Without this
    # branch a tombstoned id would survive every rebuild.
    logger.info("Creating stub artist rows from release_artist...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO artist (id, name, not_found)
            SELECT DISTINCT artist_id, artist_name, FALSE
            FROM release_artist
            WHERE artist_id IS NOT NULL
            ON CONFLICT (id) DO UPDATE SET
                not_found = FALSE
            WHERE artist.not_found = TRUE
        """)
        count = cur.rowcount
    conn.commit()
    logger.info(f"  Created or refreshed {count:,} stub artist rows")

    total = count

    # The set of stub-artist IDs gates both the profile UPDATE below (so we
    # don't COPY the entire Discogs artist dump's ~3-4 M rows just to UPDATE
    # the ~50 K rows that survived release_artist filtering) and the child-
    # table loads (artist_alias / NV / member) further down.
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM artist")
        artist_ids = {row[0] for row in cur.fetchall()}
    logger.info(f"  Filtering artist tables to {len(artist_ids):,} known artists")

    # Update artist profiles from artist.csv (if present).
    # COPY to a temp staging table + single UPDATE FROM JOIN avoids the
    # millions of libpq round-trips a per-row UPDATE loop would incur once
    # the rebuild starts producing the full Discogs artist dump.
    artist_csv = csv_dir / "artist.csv"
    if artist_csv.exists():
        logger.info("Updating artist profiles from artist.csv...")
        import csv as csv_mod

        # Read + filter + dedup the CSV BEFORE opening the COPY stream so
        # a file-IO error doesn't abort an in-flight COPY, and so the
        # staging table never holds rows the JOIN would drop anyway. The
        # dict de-dup is last-value-wins (matches the v1 per-row UPDATE
        # behavior, which is the opposite of import_artwork's
        # first-value-wins); since Discogs's dump has unique artist IDs
        # this only matters if a corrupt dump ever ships dups, in which
        # case we want to land *something* rather than abort.
        profiles: dict[int, str] = {}
        skipped_non_int = 0
        skipped_unknown_artist = 0
        with open(artist_csv, newline="", encoding="utf-8") as f:
            for row in csv_mod.DictReader(f):
                artist_id_str = row.get("artist_id")
                profile = row.get("profile", "").strip()
                if not (artist_id_str and profile):
                    continue
                try:
                    artist_id = int(artist_id_str)
                except ValueError:
                    skipped_non_int += 1
                    continue
                if artist_id not in artist_ids:
                    skipped_unknown_artist += 1
                    continue
                profiles[artist_id] = to_pg_text_form(profile)
        if skipped_non_int:
            logger.warning(
                f"  Skipped {skipped_non_int:,} artist.csv rows with non-integer artist_id"
            )
        if skipped_unknown_artist:
            logger.info(
                f"  Skipped {skipped_unknown_artist:,} artist.csv rows for artists not in release_artist"
            )

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE _artist_profile (
                    artist_id integer NOT NULL,
                    profile text NOT NULL
                ) ON COMMIT DROP
            """)
            with cur.copy("COPY _artist_profile (artist_id, profile) FROM STDIN") as copy:
                for artist_id, profile in profiles.items():
                    copy.write_row((artist_id, profile))
            cur.execute("""
                UPDATE artist a
                SET profile = p.profile
                FROM _artist_profile p
                WHERE a.id = p.artist_id
            """)
            profile_count = cur.rowcount
        conn.commit()
        logger.info(f"  Updated {profile_count:,} artist profiles")
        total += profile_count
    else:
        logger.info("No artist.csv found, skipping profile import")

    total += _import_tables(conn, csv_dir, ARTIST_TABLES, artist_id_filter=artist_ids)
    return total


def import_masters(conn, csv_dir: Path) -> int:
    """Import ``master`` + ``master_artist`` from CSV, scoped to the library.

    The converter does not pre-filter masters (``process_masters`` writes every
    master in the dump — ~2.3M rows), so library scope is applied here: only
    masters referenced by ``release.master_id`` are loaded — the same
    library-artist scope the ``release`` table already enforces. This mirrors
    how ``import_artist_details`` scopes the artist child tables to the artist
    ids that survived release filtering (WXYC/discogs-etl#317).

    Truncate-and-reload of the two masters tables, so a monthly rerun is
    idempotent (a plain COPY would hit ``master``'s PK on the second run). The
    TRUNCATE is safe: ``release.master_id`` is a plain integer, NOT a foreign
    key, so it cannot cascade into ``release``; ``master_artist`` references
    ``master(id) ON DELETE CASCADE`` and nothing references ``master_artist``.
    So ``TRUNCATE master, master_artist CASCADE`` touches exactly those two
    tables.

    The truncate is NOT committed on its own: it shares a transaction with the
    ``master`` COPY (``import_csv`` commits at the end of that load), so a
    reload that raises rolls the truncate back and the previously-loaded masters
    survive. Without this, a mid-load failure (e.g. an out-of-range ``year`` in
    the dump) would leave the tables committed-empty. See WXYC/discogs-etl#317.

    Two guards keep the destructive truncate from silently wiping populated
    tables:

    * Absent ``master.csv`` → no truncate, no-op, so a rebuild that didn't
      fetch a masters dump leaves the tables untouched ("additive, safe by
      omission").
    * An empty library scope (no ``release.master_id`` references) → skip the
      truncate, so running against a half-built cache (or one whose release
      rows all have NULL ``master_id``) doesn't wipe a populated masters table.

    A present-but-empty/malformed ``master.csv`` that yields zero rows despite a
    non-empty scope still truncates (the reload can't tell "legitimately zero"
    from "malformed"), so that case is surfaced with a warning for the
    post-run observability path rather than passing silently.

    Returns total rows imported.
    """
    master_config = next(t for t in MASTER_TABLES if t["table"] == "master")
    master_artist_config = next(t for t in MASTER_TABLES if t["table"] == "master_artist")

    master_csv = csv_dir / master_config["csv_file"]
    if not master_csv.exists():
        logger.warning("No master.csv found, skipping masters import (tables unchanged)")
        return 0

    # Library scope: the master ids referenced by the (already library-filtered)
    # release table. Computed BEFORE the truncate — the truncate touches only
    # the masters tables, never release.
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT master_id FROM release WHERE master_id IS NOT NULL")
        library_master_ids = {row[0] for row in cur.fetchall()}

    if not library_master_ids:
        logger.warning(
            "No masters referenced by release.master_id (scope is empty); skipping "
            "masters import and leaving master/master_artist unchanged, to avoid "
            "wiping populated tables when run against a half-built cache. See #317."
        )
        return 0

    logger.info(
        f"  Filtering masters to {len(library_master_ids):,} referenced by release.master_id"
    )

    # No standalone commit here: the TRUNCATE folds into the master COPY's
    # transaction (import_csv commits at the end of that load), so a raising
    # reload rolls the truncate back and preserves the prior masters. See #317.
    with conn.cursor() as cur:
        cur.execute("TRUNCATE master, master_artist CASCADE")

    total = import_csv(
        conn,
        master_csv,
        master_config["table"],
        master_config["csv_columns"],
        master_config["db_columns"],
        master_config["required"],
        master_config["transforms"],
        unique_key=master_config.get("unique_key"),
        id_filter=library_master_ids,
        id_filter_column="id",
        optional_csv_columns=master_config.get("optional_csv_columns"),
        optional_unique_key=master_config.get("optional_unique_key"),
    )

    # Filter master_artist to the master ids ACTUALLY loaded (not the raw
    # release set): a release.master_id can point at a master absent from this
    # month's dump, and COPYing its master_artist rows would violate the FK.
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM master")
        imported_master_ids = {row[0] for row in cur.fetchall()}

    if not imported_master_ids:
        # Scope was non-empty (guarded above) yet 0 masters loaded — the dump's
        # master.csv is empty/malformed, or none of the referenced masters are
        # in this month's dump. The tables are now empty; surface it loudly so
        # the run's observability (report_sizes) isn't a silent success. See #317.
        logger.warning(
            "0 master rows loaded despite a non-empty library scope "
            f"({len(library_master_ids):,} referenced) — master.csv may be empty or "
            "malformed; master/master_artist are now empty. See #317."
        )

    master_artist_csv = csv_dir / master_artist_config["csv_file"]
    if master_artist_csv.exists():
        total += import_csv(
            conn,
            master_artist_csv,
            master_artist_config["table"],
            master_artist_config["csv_columns"],
            master_artist_config["db_columns"],
            master_artist_config["required"],
            master_artist_config["transforms"],
            unique_key=master_artist_config.get("unique_key"),
            id_filter=imported_master_ids,
            id_filter_column="master_id",
            optional_csv_columns=master_artist_config.get("optional_csv_columns"),
            optional_unique_key=master_artist_config.get("optional_unique_key"),
        )
    else:
        logger.warning("No master_artist.csv found, skipping master_artist import")

    return total


def _import_masters_best_effort(conn, csv_dir: Path) -> None:
    """Run ``import_masters`` on the monthly (``--base-only`` / default) paths
    without ever letting a masters failure abort the rebuild.

    ``import_masters`` is the last step of the import phase, which in
    ``run_pipeline.py`` precedes the ``dedup`` and ``prune`` steps. A raise here
    would exit the ``import_csv.py`` subprocess non-zero and abort the pipeline
    *after* the (idempotent) release upsert already committed but *before*
    dedup/prune run — leaving the cache serving un-deduped, un-pruned releases.
    Masters is best-effort (see WXYC/discogs-etl#317), so a masters problem must
    not have that blast radius: catch it, roll back the aborted transaction (the
    truncate shares it, so existing masters are preserved), log, and continue.

    ``--masters-only`` deliberately does NOT use this wrapper — an explicit
    operator refresh should surface its errors.
    """
    try:
        import_masters(conn, csv_dir)
    except Exception:
        conn.rollback()
        logger.exception(
            "Masters import failed; continuing without updating masters "
            "(best-effort, existing masters preserved). See #317."
        )


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
    mode.add_argument(
        "--masters-only",
        action="store_true",
        help="Import only master + master_artist, scoped to masters referenced "
        "by release.master_id. Idempotent (import_masters truncates + reloads "
        "just those two tables), so it's safe to run against a live cache "
        "without disturbing release/artist/label data. Used by the one-time "
        "prod import and any ad-hoc masters refresh (WXYC/discogs-etl#317).",
    )
    parser.add_argument(
        "--truncate-existing",
        action="store_true",
        help="TRUNCATE the cache tables before COPY. Use when re-running a "
        "rebuild against a DB with stale rows from a prior failed attempt. "
        "Preserves the entity schema and alembic_version. Without this flag, "
        "a duplicate-key violation on the first table aborts the pipeline.",
    )

    args = parser.parse_args()
    csv_dir = args.csv_dir
    db_url = args.db_url

    if not csv_dir.exists():
        logger.error(f"CSV directory not found: {csv_dir}")
        sys.exit(1)

    logger.info(f"Connecting to {db_url}")
    conn = psycopg.connect(db_url)

    # --masters-only self-truncates its two tables inside import_masters, so it
    # must NOT trigger the base wipe here (which includes `release`). Guarding
    # keeps an accidental `--masters-only --truncate-existing` safe. See #317.
    if args.truncate_existing and not args.masters_only:
        truncate_set = (
            CACHE_TABLES_TO_TRUNCATE_TRACKS if args.tracks_only else CACHE_TABLES_TO_TRUNCATE_BASE
        )
        _truncate_tables(conn, truncate_set)

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
    elif args.masters_only:
        # Masters-only: import_masters reads release.master_id, self-truncates
        # the two masters tables, and reloads the filtered set. Nothing that
        # writes release/artist/label runs. See #317.
        logger.info("Importing masters...")
        total = import_masters(conn, csv_dir)
        logger.info("Masters complete")
        conn.close()
    elif args.base_only:
        # --truncate-existing wipes release; default path upserts to preserve LML back-patches.
        if args.truncate_existing:
            conn.close()
            total = _import_tables_parallel(
                db_url, csv_dir, parent_tables=BASE_TABLES[:1], child_tables=BASE_TABLES[1:]
            )
        else:
            conn.close()
            upsert_conn = psycopg.connect(db_url)
            upsert_rows = import_release_via_upsert(upsert_conn, csv_dir)
            upsert_conn.close()
            total = upsert_rows + _import_tables_parallel(
                db_url, csv_dir, parent_tables=[], child_tables=BASE_TABLES[1:]
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
        _import_masters_best_effort(conn, csv_dir)
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
        _import_masters_best_effort(conn, csv_dir)
        logger.info("Masters complete")
        conn.close()

    logger.info(f"Total: {total:,} rows imported")


if __name__ == "__main__":
    main()
