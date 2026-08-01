"""Derive the ``va_release`` lookup table from ``release`` ⋈ ``release_artist``.

``va_release`` (``id``, ``title``, ``norm_title = lower(title)``) is the
VA-compilation subset of the cache, consumed by library-metadata-lookup's
compilation-matching scripts (``match_compilations.py``,
``canonicalize_compilations.py``, and through them the recall-index build
``build_compilation_track_location.py`` — see #344). It is derived data, not
durable schema: release ids drift across cache rebuilds, so every invocation
performs a full transactional re-derivation (DROP + CREATE TABLE AS + index
inside one transaction — an atomic swap for readers).

The predicate keeps main-artist Various credits only, matching on the
canonical Discogs Various artist id (194) OR the literal name ``'Various'``
(``release_artist.artist_id`` is NULL on API-fetched rows LML inserts at
runtime, and 194 carries ANV name variants the name alone would miss).

Divergences from the ad-hoc local table this replaces, all deliberate:
LOGGED (crash recovery must not empty it), no FK to ``release`` (snapshot
semantics; also keeps it out of the pipeline's LOGGED/UNLOGGED flip lists),
and no btree ``idx_va_release_norm_exact`` (no consumer queries ``norm_title``
by equality — both exact-match paths build Python-side ``title.lower()`` maps).

Row-count floor guard: when ``va_release`` already existed before the
transaction and the fresh derivation lands below ``--floor`` (default 1,
``VA_RELEASE_FLOOR``; 0 opts out), the swap is rolled back and the script
exits non-zero — a transiently-thin derivation (e.g. the daily sync colliding
with a mid-rebuild truncated ``release`` table) must never replace a populated
table with an empty one. On the first run (no pre-existing table) the floor is
skipped: an empty table still beats the ``UndefinedTableError`` crash.

Usage::

    python scripts/derive_va_release.py \\
        [--database-url "$DATABASE_URL_DISCOGS"] [--floor 1]

Falls back to ``DATABASE_URL_DISCOGS`` then ``DATABASE_URL`` when
``--database-url`` is omitted; errors out when none are set (deliberately no
localhost default — a silent local-DB fallback is the local-artifact trap
#344 cleans up after).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)

# Discogs' canonical "Various" artist. Validated 2026-08-01 against the full
# 18.9M-release dump: 1,320,795 main credits carry this id, 1,090 of them
# under ANV names != 'Various'; zero rows have the name without the id — the
# name disjunct exists solely for API-fetched rows whose artist_id is NULL.
VARIOUS_ARTIST_ID = 194

DEFAULT_FLOOR = 1

_DERIVE_SQL = f"""
    CREATE TABLE va_release AS
    SELECT DISTINCT r.id, r.title, lower(r.title) AS norm_title
    FROM release r
    JOIN release_artist ra ON ra.release_id = r.id
    WHERE (ra.artist_id = {VARIOUS_ARTIST_ID} OR ra.artist_name = 'Various')
      AND ra.extra = 0
"""

_TRGM_INDEX_SQL = (
    "CREATE INDEX idx_va_release_norm_trgm ON va_release USING gin (norm_title gin_trgm_ops)"
)


class FloorViolation(RuntimeError):
    """Derivation produced fewer rows than the floor; the swap was rolled back."""


def derive_va_release(conn: psycopg.Connection, *, floor: int = DEFAULT_FLOOR) -> int:
    """Re-derive ``va_release`` in one transaction; return the derived row count.

    Raises :class:`FloorViolation` (after rolling back, preserving the
    pre-existing table) when the table existed before the transaction and the
    fresh derivation has fewer than ``floor`` rows.
    """
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute("SELECT to_regclass('public.va_release') IS NOT NULL")
        pre_existed = bool(cur.fetchone()[0])
        cur.execute("DROP TABLE IF EXISTS va_release")
        cur.execute(_DERIVE_SQL)
        cur.execute("SELECT count(*) FROM va_release")
        count = int(cur.fetchone()[0])
        if pre_existed and count < floor:
            conn.rollback()
            raise FloorViolation(
                f"derived {count} va_release rows, below floor {floor}; "
                "rolled back to the pre-existing table"
            )
        cur.execute(_TRGM_INDEX_SQL)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("ANALYZE va_release")
    conn.commit()
    logger.info(
        "Derived va_release: %d rows (pre-existing table: %s)",
        count,
        "replaced" if pre_existed else "none",
    )
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--database-url",
        default=None,
        help=(
            "PostgreSQL URL for the discogs-cache. Falls back to "
            "DATABASE_URL_DISCOGS, then DATABASE_URL."
        ),
    )
    parser.add_argument(
        "--floor",
        type=int,
        default=int(os.environ.get("VA_RELEASE_FLOOR", DEFAULT_FLOOR)),
        help=(
            "Minimum derived row count required to replace a pre-existing "
            "va_release (rolled back below it). 0 opts out. Skipped when no "
            f"table pre-exists. Default: VA_RELEASE_FLOOR or {DEFAULT_FLOOR}."
        ),
    )
    args = parser.parse_args(argv)

    init_logger(repo="discogs-etl", tool="discogs-etl derive_va_release")

    database_url = (
        args.database_url
        or os.environ.get("DATABASE_URL_DISCOGS")
        or os.environ.get("DATABASE_URL")
    )
    if not database_url:
        print(
            "error: --database-url not provided and DATABASE_URL_DISCOGS/DATABASE_URL not set.",
            file=sys.stderr,
        )
        return 2

    try:
        with psycopg.connect(database_url) as conn:
            derive_va_release(conn, floor=args.floor)
    except FloorViolation as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
