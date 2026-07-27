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
  * ``--source`` is genuinely read-only: the source connection only ever runs
    ``SELECT`` / ``COPY ... TO STDOUT``. The new-parent-id set is embedded
    directly into each COPY's filter as an ``ANY(ARRAY[...]::integer[])``
    literal instead of being staged into a source-side temp table.
  * A transaction-scoped advisory lock (``pg_advisory_xact_lock``, key 330001 --
    see ``CLAUDE.md``'s "Advisory lock keys" table) on the target spans the
    whole read (new-id computation) -> write window of a real run, so two
    concurrent invocations against the same target can't both compute the same
    parent as "new" and double-insert the arbiter-less child rows.

Never runs ``verify_cache.py`` against the seeded rows: it prunes non-library
releases, which is exactly what this seeds. Post-seed verification is a separate
read-only check.

Usage:
    python seed_cache_from_clone.py \
        --source postgresql://localhost:5432/discogs \
        --target "$DATABASE_URL_DISCOGS" \
        --ids-file tail_release_ids.txt \
        [--seed-artists] \
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

from lib.keep_release_ids import parse_keep_release_ids  # noqa: E402
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

# Freshness tables that are seeded FRESH rather than copied from the clone: the
# clone's cache_metadata.cached_at is nullable and carries NULLs, which would
# violate prod's NOT NULL on a straight copy — and a vintage cached_at would
# misreport freshness anyway. Each newly-seeded release instead gets a fresh
# ``(release_id, source='bulk_import', cached_at=now())`` row (prod defaults),
# matching this repo's bulk-import convention (see verify_cache column tests /
# test_copy_to_target._fresh_import).
_FRESH_SEED = {"cache_metadata"}

# Columns that must default to now() on the target when NULL on the source,
# because the target's NOT NULL constraint would otherwise reject a straight
# copy. Mirrors the treatment cache_metadata.cached_at gets via _FRESH_SEED
# (a full fresh-row reseed), but at the single-column level: artist rows
# otherwise copy straight through unmodified, so a full reseed isn't needed.
_COALESCE_NOW_COLUMNS = {("artist", "fetched_at")}

# pg_advisory_xact_lock key for the read (new-id computation) -> write critical
# section of a real (non-dry-run) _seed_family call. Database-global on the
# shared discogs-cache PG, so it must stay registered in CLAUDE.md's "Advisory
# lock keys (shared-PG registry)" table. 330001 = discogs-etl PR#330, first key
# allocated by this repo (<issue-number> * 1000 + sequence convention).
_ADVISORY_LOCK_KEY = 330001


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


def _int_array_literal(ids) -> str:
    """Format a validated integer id set as a literal ``ARRAY[...]::integer[]``
    SQL fragment, for embedding directly in a ``COPY (SELECT ...) TO STDOUT``
    filter. COPY doesn't support bind parameters for the query it wraps, which
    is why this is a literal rather than a placeholder -- safe here because
    every id has already round-tripped through ``int()`` (see
    ``_compute_new_ids``) before reaching this function, so there is no
    injection surface in formatting them as a comma-separated integer literal.
    At the real backfill's scale (~17k ids) a single ARRAY[] literal this size
    is fine for Postgres; no batching needed."""
    return "ARRAY[" + ",".join(str(int(i)) for i in sorted(ids)) + "]::integer[]"


def _copy_table(
    source_conn, target_conn, table: str, filter_col: str, columns: list[str], ids: set[int]
) -> int:
    """Stream one table's rows (for ``ids``) source -> target staging, then
    INSERT ... SELECT into the real table. Returns rows inserted.

    The source-side SELECT never writes anything (no temp table, no COPY FROM
    STDIN against source_conn) -- ``--source`` only ever needs SELECT / COPY ...
    TO STDOUT, matching the runbook's read-only contract."""
    col_list = ", ".join(columns)
    select_exprs = ", ".join(
        f"COALESCE({c}, now())" if (table, c) in _COALESCE_NOW_COLUMNS else c for c in columns
    )
    stage = f"_seed_stage_{table}"
    array_literal = _int_array_literal(ids)

    with target_conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {stage} AS SELECT {col_list} FROM {table} WHERE false")

    select_query = (
        f"COPY (SELECT {select_exprs} FROM {table} "
        f"WHERE {filter_col} = ANY({array_literal})) TO STDOUT"
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


def _seed_fresh_cache_metadata(target_conn, new_ids: set[int]) -> int:
    """Insert a fresh cache_metadata row per newly-seeded release (source
    bulk_import, cached_at defaulted to now()), instead of copying the clone's
    unreliable/nullable freshness rows. Idempotent via the release_id PK."""
    with target_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO cache_metadata (release_id, source) "
            "SELECT id, 'bulk_import' FROM release WHERE id = ANY(%s::integer[]) "
            "ON CONFLICT (release_id) DO NOTHING",
            (list(new_ids),),
        )
        return cur.rowcount


def _dry_run_counts(source_conn, spec, new_ids: set[int], pk_table: str) -> dict[str, int]:
    array_literal = _int_array_literal(new_ids)
    counts: dict[str, int] = {}
    for table, filter_col, _cols in spec:
        if table in _FRESH_SEED:
            continue  # filled in below, mirroring pk_table's real source-side count
        if not _table_exists(source_conn, table):
            counts[table] = 0
            continue
        with source_conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table} WHERE {filter_col} = ANY({array_literal})")
            counts[table] = cur.fetchone()[0]
    for table, _f, _c in spec:
        if table in _FRESH_SEED:
            # Seeded fresh -- one row per new PARENT actually present on the
            # source. Mirror pk_table's real source-side count for this id set
            # rather than echoing len(new_ids), which overstates whenever a
            # candidate id is absent from source (new_ids is computed purely
            # against the target, so it can contain ids source doesn't have).
            counts[table] = counts.get(pk_table, 0)
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
    # Materialize once -- candidate_ids is referenced multiple times below (id
    # computation, then log-message length calcs); a one-shot iterable (e.g. a
    # generator) must not be silently exhausted by the first use.
    candidate_ids = sorted({int(x) for x in candidate_ids})
    result = {table: 0 for table, _f, _c in spec}

    if not candidate_ids:
        logger.info("No %s candidates supplied", family)
        return result

    if dry_run:
        new_ids = _compute_new_ids(target_url, pk_table, pk_col, candidate_ids)
        if not new_ids:
            logger.info(
                "No new %s to seed (all %d candidates already present)",
                family,
                len(candidate_ids),
            )
            return result
        logger.info(
            "Seeding %s new %s (%s candidates, rest already present)",
            f"{len(new_ids):,}",
            family,
            f"{len(candidate_ids):,}",
        )
        source_conn = psycopg.connect(source_url)
        try:
            counts = _dry_run_counts(source_conn, spec, new_ids, pk_table)
            for table, n in counts.items():
                logger.info("  [dry-run] %s: would seed %s rows", table, f"{n:,}")
            return counts
        finally:
            source_conn.close()

    # Real (writing) run. Hold a transaction-scoped advisory lock on the target
    # for the whole read (new-id computation) -> write window: two concurrent
    # invocations against the same target must not both compute the same
    # release/artist as "new" and double-insert the arbiter-less child rows
    # (release_artist/label/genre/style/track/track_artist, artist_name_variation
    # -- see the module docstring). Key 330001 = discogs-etl PR#330, first key
    # allocated; registered in CLAUDE.md's "Advisory lock keys" table.
    target_conn = psycopg.connect(target_url)
    try:
        with target_conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (_ADVISORY_LOCK_KEY,))

        new_ids = _compute_new_ids(target_conn, pk_table, pk_col, candidate_ids)
        if not new_ids:
            logger.info(
                "No new %s to seed (all %d candidates already present)",
                family,
                len(candidate_ids),
            )
            target_conn.rollback()  # releases the advisory lock; nothing to write
            return result

        # Last visual sanity check before the write below. A --source/--target
        # swap (two DSNs pointing at the same or an unexpectedly-similar
        # database) typically makes "new" look like ~all or ~none of the
        # candidates -- an operator scanning this line catches what DSN-string
        # comparison alone cannot (two genuinely different DSNs that were still
        # transposed).
        logger.info(
            "Seeding %s new %s (%s candidates, rest already present) -- verify this "
            "matches expectations before the write below",
            f"{len(new_ids):,}",
            family,
            f"{len(candidate_ids):,}",
        )

        source_conn = psycopg.connect(source_url)
        try:
            for table, filter_col, spec_cols in spec:
                if table in _FRESH_SEED:
                    continue  # seeded fresh after the copy loop
                if not _table_exists(source_conn, table):
                    logger.info("  %s: absent on source clone, skipping", table)
                    continue
                columns = _copy_columns(source_conn, target_conn, table, spec_cols)
                if not columns:
                    logger.info("  %s: no shared columns, skipping", table)
                    continue
                inserted = _copy_table(
                    source_conn, target_conn, table, filter_col, columns, new_ids
                )
                result[table] = inserted
                logger.info("  %s: inserted %s rows", table, f"{inserted:,}")
            for table, _f, _c in spec:
                if table == "cache_metadata":
                    n = _seed_fresh_cache_metadata(target_conn, new_ids)
                    result[table] = n
                    logger.info("  %s: seeded %s fresh rows", table, f"{n:,}")
            target_conn.commit()
        except Exception:
            target_conn.rollback()
            raise
        finally:
            source_conn.close()
    finally:
        target_conn.close()

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


def _validate_distinct_dsns(source: str, target: str) -> None:
    """Refuse an identical --source/--target pair -- the cheap, robust half of
    catching a transposed CLI invocation. A transposed pair where source and
    target are the *same* DSN would otherwise silently no-op (every candidate
    id already "present" on the target), exiting 0 with no error.

    A swap between two genuinely different DSNs can't be caught by string
    comparison alone; the mitigant for that case is the prominent
    candidate/new-id count logged just before the real (non-dry-run) write in
    _seed_family, which an operator should sanity-check before the write
    proceeds."""
    if source == target:
        raise ValueError(
            "--source and --target resolve to the identical DSN -- refusing. This "
            "almost always means the two arguments were transposed."
        )


def _read_ids_file(path: Path) -> list[int]:
    """Read a newline-separated id allowlist file (release_ids, or artist_ids
    with --seed-artists). Delegates line-parsing to the shared
    lib.keep_release_ids parser rather than reimplementing it. Unlike that
    parser's "missing file -> empty set" convention, --ids-file is a required,
    operator-supplied path here, so a missing file is a loud failure rather
    than a silent empty seed."""
    if not path.exists():
        raise FileNotFoundError(f"--ids-file not found: {path}")
    return sorted(parse_keep_release_ids(path))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="Source clone DSN (read-only).")
    parser.add_argument("--target", required=True, help="Target discogs-cache DSN.")
    parser.add_argument(
        "--ids-file",
        required=True,
        type=Path,
        help="File of release_ids (or, with --seed-artists, artist_ids) to seed, one per "
        "line (selected upstream via LML's trigram predicate over the tail artists).",
    )
    parser.add_argument(
        "--seed-artists",
        action="store_true",
        help="Seed artist rows (and artist_name_variation children) from --ids-file, "
        "instead of the default release family.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report per-table row-set sizes without writing to the target.",
    )
    args = parser.parse_args()

    try:
        _validate_distinct_dsns(args.source, args.target)
    except ValueError as exc:
        parser.error(str(exc))

    init_logger(repo="discogs-etl", tool="discogs-etl seed_cache_from_clone")

    ids = _read_ids_file(args.ids_file)
    id_label = "artist_ids" if args.seed_artists else "release_ids"
    logger.info("Loaded %s %s from %s", f"{len(ids):,}", id_label, args.ids_file)

    seed_fn = seed_artists_additive if args.seed_artists else seed_releases_additive
    counts = seed_fn(args.source, args.target, ids, dry_run=args.dry_run)
    logger.info("%s: %s", "Planned" if args.dry_run else "Seeded", counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
