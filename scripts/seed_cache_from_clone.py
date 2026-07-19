#!/usr/bin/env python3
"""Additively seed a target discogs-cache with an artist-scoped release row-set
copied from a full (unfiltered) Discogs clone.

Motivation (WXYC/Backend-Service#1631): the Apple-Music-URL backfill stalls on a
"cold tail" of non-library albums whose Discogs releases are absent from the
library-filtered prod discogs-cache. On a miss LML falls through to the live
Discogs API + cold path (>15s), times out, and trips the health watchdog. A full
local clone already holds those releases in the right shape; this script copies
exactly the tail artists' releases clone -> prod, additively, so LML resolves
them as fast cache hits.

Design (see plans/bs1631-tail-cache-seed.md):

  * Filter unit is the *artist* -- callers pass the release_id set already
    selected via LML's ``lower(f_unaccent(artist_name)) % $artist`` trigram
    predicate (schema/create_indexes.sql). This script does the copy, not the
    selection.
  * Additive only. ``release`` (id PK) and ``cache_metadata`` (release_id PK)
    use ``ON CONFLICT DO NOTHING``. The arbiter-less child tables
    (release_artist/label/genre/style/track/track_artist/video) have no
    PK/UNIQUE, so idempotency is driven off the *parent*: children are inserted
    only for the "new-release-id set" = (requested ids) - (ids already on the
    target). A release_id is in that set only if its parent was absent, so its
    children were necessarily absent too -- no duplicates, fully re-runnable.
  * Column lists come from ``verify_cache.COPY_TABLE_SPEC`` (the canonical source
    of truth) intersected with the columns actually present on both databases,
    so clone-vs-prod column drift (the clone predates
    ``release.artwork_checked_at`` / ``release.not_found`` and has no
    ``release_video`` table) is tolerated -- prod-only columns simply default.
  * ``release_image`` is NOT a table (it is a transient import CSV feeding
    ``release.artwork_url``, which already travels in the ``release`` copy).

Never runs ``verify_cache.py`` against the seeded rows: it prunes non-library
releases, which is exactly what this seeds. Post-seed verification is a separate
read-only check.

Usage:
    python seed_cache_from_clone.py \
        --source postgresql://localhost:5432/discogs \
        --target "$DATABASE_URL_DISCOGS" \
        --ids-file tail_release_ids.txt \
        [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import psycopg

_SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPTS_DIR.parent))
sys.path.insert(0, str(_SCRIPTS_DIR))

from verify_cache import COPY_TABLE_SPEC  # noqa: E402

from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)

# release_image is deliberately absent from COPY_TABLE_SPEC (not a table). The
# spec is the single source of truth for release-family column lists; see the
# module docstring.
_RELEASE_SPEC = COPY_TABLE_SPEC

# Artist family (COPY_TABLE_SPEC covers only the release side). Only the two
# tables the plan names are seeded: the id-PK parent and its arbiter-less
# name-variation child. Columns mirror schema/create_database.sql; prod-only
# columns absent on the clone (artist.not_found) fall out in the intersection.
_ARTIST_SPEC = [
    ("artist", "id", ["id", "name", "profile", "image_url", "fetched_at", "not_found"]),
    ("artist_name_variation", "artist_id", ["artist_id", "name"]),
]

# Tables whose primary key lets us use ON CONFLICT ... DO NOTHING as a
# belt-and-suspenders guard against a concurrent seed. Everything else is an
# arbiter-less child, gated on the new-parent-id set.
_CONFLICT_TARGET = {"release": "id", "cache_metadata": "release_id", "artist": "id"}


def _table_exists(conn: psycopg.Connection, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        return cur.fetchone()[0] is not None


def _columns(conn: psycopg.Connection, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        return {r[0] for r in cur.fetchall()}


def _compute_new_ids(
    target: str | psycopg.Connection, table: str, id_col: str, candidate_ids
) -> set[int]:
    """Return the subset of ``candidate_ids`` NOT already present in
    ``target.{table}.{id_col}`` -- the new-parent-id set that drives
    parent-gated idempotency. Accepts a connection URL or an open connection."""
    candidates = list({int(x) for x in candidate_ids})
    if not candidates:
        return set()

    own_conn = isinstance(target, str)
    conn = psycopg.connect(target) if own_conn else target
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {id_col} FROM {table} WHERE {id_col} = ANY(%s::integer[])",
                (candidates,),
            )
            existing = {r[0] for r in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()
    return set(candidates) - existing


def compute_new_release_ids(target: str | psycopg.Connection, candidate_ids) -> set[int]:
    """Release-family wrapper over :func:`_compute_new_ids` (``release.id``)."""
    return _compute_new_ids(target, "release", "id", candidate_ids)


def intersect_columns(spec_columns, source_cols, target_cols) -> list[str]:
    """Columns from ``spec_columns`` present on BOTH the source and target,
    preserving spec order. Pure (no DB) so it is unit-testable. Prod-only columns
    (absent on the clone) and clone-only columns both fall out here; the surviving
    set is what the COPY names, and any prod column not named simply defaults."""
    src = set(source_cols)
    tgt = set(target_cols)
    return [c for c in spec_columns if c in src and c in tgt]


def _copy_columns(source_conn, target_conn, table: str, spec_columns: list[str]) -> list[str]:
    """DB-bound wrapper over :func:`intersect_columns`."""
    return intersect_columns(
        spec_columns, _columns(source_conn, table), _columns(target_conn, table)
    )


def _load_seed_ids(source_conn, new_ids: set[int]) -> None:
    """Materialize the new-parent-id set into a temp table on the source, so each
    table's COPY filter is an indexed join rather than a giant IN-list. The temp
    column is named neutrally (``seed_id``) since it holds release_ids or
    artist_ids depending on the family being seeded."""
    with source_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS _seed_ids")
        cur.execute("CREATE TEMP TABLE _seed_ids (seed_id integer PRIMARY KEY)")
        with cur.copy("COPY _seed_ids (seed_id) FROM STDIN") as copy:
            for rid in new_ids:
                copy.write_row((rid,))
    source_conn.commit()


def _copy_table(source_conn, target_conn, table: str, filter_col: str, columns: list[str]) -> int:
    """Stream one table's rows (for the loaded ``_seed_ids`` set) source -> target
    staging, then INSERT ... SELECT into the real table. Returns rows inserted."""
    col_list = ", ".join(columns)
    stage = f"_seed_stage_{table}"

    with target_conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {stage} AS SELECT {col_list} FROM {table} WHERE false")

    select_query = (
        f"COPY (SELECT {col_list} FROM {table} "
        f"WHERE {filter_col} IN (SELECT seed_id FROM _seed_ids)) TO STDOUT"
    )
    with source_conn.cursor() as src_cur:
        with src_cur.copy(select_query) as src_copy:
            with target_conn.cursor() as tgt_cur:
                with tgt_cur.copy(f"COPY {stage} ({col_list}) FROM STDIN") as tgt_copy:
                    for data in src_copy:
                        tgt_copy.write(data)

    conflict = _CONFLICT_TARGET.get(table)
    on_conflict = f" ON CONFLICT ({conflict}) DO NOTHING" if conflict else ""
    with target_conn.cursor() as cur:
        cur.execute(f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM {stage}{on_conflict}")
        inserted = cur.rowcount
        cur.execute(f"DROP TABLE {stage}")
    return inserted


def _dry_run_counts(source_conn, spec) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table, filter_col, _cols in spec:
        if not _table_exists(source_conn, table):
            counts[table] = 0
            continue
        with source_conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {table} "
                f"WHERE {filter_col} IN (SELECT seed_id FROM _seed_ids)"
            )
            counts[table] = cur.fetchone()[0]
    return counts


def _seed_family(
    source_url: str,
    target_url: str,
    spec,
    pk_table: str,
    pk_col: str,
    candidate_ids,
    *,
    family: str,
    dry_run: bool,
) -> dict[str, int]:
    """Generic additive, parent-gated copy of one table family (release or
    artist) from the source clone into the target. Only parent ids absent from
    the target are copied; existing rows are never touched."""
    new_ids = _compute_new_ids(target_url, pk_table, pk_col, candidate_ids)
    result = {table: 0 for table, _f, _c in spec}

    if not new_ids:
        logger.info(
            "No new %s to seed (all %d candidates already present)",
            family,
            len({int(x) for x in candidate_ids}),
        )
        return result

    logger.info(
        "Seeding %s new %s (%s candidates, rest already present)",
        f"{len(new_ids):,}",
        family,
        f"{len({int(x) for x in candidate_ids}):,}",
    )

    source_conn = psycopg.connect(source_url)
    try:
        _load_seed_ids(source_conn, new_ids)

        if dry_run:
            counts = _dry_run_counts(source_conn, spec)
            for table, n in counts.items():
                logger.info("  [dry-run] %s: would copy %s rows", table, f"{n:,}")
            return counts

        target_conn = psycopg.connect(target_url)
        try:
            for table, filter_col, spec_cols in spec:
                if not _table_exists(source_conn, table):
                    logger.info("  %s: absent on source clone, skipping", table)
                    continue
                columns = _copy_columns(source_conn, target_conn, table, spec_cols)
                if not columns:
                    logger.info("  %s: no shared columns, skipping", table)
                    continue
                inserted = _copy_table(source_conn, target_conn, table, filter_col, columns)
                result[table] = inserted
                logger.info("  %s: inserted %s rows", table, f"{inserted:,}")
            target_conn.commit()
        except Exception:
            target_conn.rollback()
            raise
        finally:
            target_conn.close()
    finally:
        source_conn.close()

    logger.info("Seed complete (%s): %s", family, {k: v for k, v in result.items() if v})
    return result


def seed_releases_additive(
    source_url: str,
    target_url: str,
    release_ids,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Additively copy the artist-scoped release row-set (``release_ids`` and its
    children) from the source clone into the target discogs-cache.

    Only release_ids absent from the target are copied; existing rows are never
    touched. Returns a per-table count dict: rows inserted (or, for ``dry_run``,
    rows that *would* be copied)."""
    return _seed_family(
        source_url,
        target_url,
        _RELEASE_SPEC,
        "release",
        "id",
        release_ids,
        family="releases",
        dry_run=dry_run,
    )


def seed_artists_additive(
    source_url: str,
    target_url: str,
    artist_ids,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Additively copy tail ``artist`` rows + their ``artist_name_variation``
    children from the source clone into the target discogs-cache, for LML
    enrichment JOINs. ``artist`` (id PK) uses ON CONFLICT; the arbiter-less
    ``artist_name_variation`` is parent-gated on the new-artist-id set. Only
    artist ids absent from the target are copied; existing rows are untouched."""
    return _seed_family(
        source_url,
        target_url,
        _ARTIST_SPEC,
        "artist",
        "id",
        artist_ids,
        family="artists",
        dry_run=dry_run,
    )


def _read_ids_file(path: Path) -> list[int]:
    ids: list[int] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            ids.append(int(line))
    return ids


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="Source clone DSN (read-only).")
    parser.add_argument("--target", required=True, help="Target discogs-cache DSN.")
    parser.add_argument(
        "--ids-file",
        required=True,
        type=Path,
        help="File of release_ids to seed, one per line (selected upstream via LML's "
        "trigram predicate over the tail artists).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report per-table row-set sizes without writing to the target.",
    )
    args = parser.parse_args()

    init_logger(repo="discogs-etl", tool="discogs-etl seed_cache_from_clone")

    release_ids = _read_ids_file(args.ids_file)
    logger.info("Loaded %s release_ids from %s", f"{len(release_ids):,}", args.ids_file)

    counts = seed_releases_additive(args.source, args.target, release_ids, dry_run=args.dry_run)
    logger.info("%s: %s", "Planned" if args.dry_run else "Seeded", counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
