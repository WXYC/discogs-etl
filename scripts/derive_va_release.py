"""Derive the ``va_release`` lookup table from ``release`` ⋈ ``release_artist``.

``va_release`` (``id``, ``title``, ``norm_title = lower(title)``) is the
VA-compilation subset of the cache, consumed by library-metadata-lookup's
compilation-matching scripts (``match_compilations.py``,
``canonicalize_compilations.py``, and through them the recall-index build
``build_compilation_track_location.py`` — see #344). It is derived data, not
durable schema: release ids drift across cache rebuilds, so every invocation
performs a full transactional re-derivation (DROP + CREATE TABLE AS + index +
ANALYZE inside one transaction — an atomic swap for readers). A plain table,
not a (materialized) view, on purpose: the dedup copy-swap
(``dedup_releases.py``: ``ALTER TABLE release RENAME`` + ``DROP ... CASCADE``)
would CASCADE-drop any view defined on ``release`` mid-rebuild.

The predicate keeps main-artist Various credits only, matching on the
canonical Discogs Various artist id (194) OR the literal name ``'Various'``
(``release_artist.artist_id`` is NULL on API-fetched rows LML inserts at
runtime, and 194 carries ANV name variants the name alone would miss). Note
this deliberately differs from the *name heuristics* elsewhere in the org —
``wxyc_etl.text.is_compilation_artist`` (WXYC shelf-name strings) and LML's
runtime ``'Various'`` string gates — which classify library-side names, not
Discogs credits; keep the three in mind if any of them is ever hardened.

``norm_title`` is PostgreSQL ``lower(title)`` under the database collation.
For a handful of title classes (Turkish dotted İ, Greek final sigma) that
differs from Python ``str.lower()``; both consumers compare ``norm_title``
against PG-side ``lower($1)`` in SQL (the exact-match paths lowercase
``title`` in Python and never read ``norm_title``), so PG semantics are the
contract — do not "fix" it to match Python.

Divergences from the ad-hoc local table this replaces, all deliberate:
LOGGED (crash recovery must not empty it), no FK to ``release`` (snapshot
semantics; also keeps it out of the pipeline's LOGGED/UNLOGGED flip lists),
and no btree ``idx_va_release_norm_exact`` (no consumer queries ``norm_title``
by equality).

Row-count floor guard: when the pre-existing ``va_release`` has rows and the
fresh derivation lands below ``--floor`` (default 1, ``VA_RELEASE_FLOOR``;
0 opts out), the swap is rolled back and the script exits non-zero — a
transiently-thin derivation (e.g. the daily sync colliding with a mid-rebuild
truncated ``release`` table) must never replace a populated table with an
empty one. When the pre-existing table is absent or empty there is nothing to
protect, so even an empty derivation commits (the first-run backfill, and the
honest empty state after a broken rebuild, both land without a red loop).

Usage::

    python scripts/derive_va_release.py \\
        [--database-url "$DATABASE_URL_DISCOGS"] [--floor 1]

Falls back to ``DATABASE_URL_DISCOGS`` then ``DATABASE_URL`` when
``--database-url`` is omitted; errors out when none are set (deliberately no
localhost default — a silent local-DB fallback is the local-artifact trap
#344 cleans up after). The generic ``DATABASE_URL`` fallback logs a warning,
and the resolved target host/database is always logged before any DDL runs:
this is the first DDL writer on the daily-sync path, so which database it
touched must never be a mystery.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import urllib.parse
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

# Bound the wait for the ACCESS EXCLUSIVE lock on va_release and the shared
# locks on release/release_artist. Without it, a daily-sync derivation that
# collides with an in-flight monthly rebuild (which holds long exclusive
# locks on release) would stall for the rebuild's full wall-clock instead of
# degrading into the sync's soft-fail WARN.
LOCK_TIMEOUT = "30s"

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
    pre-existing table) when the pre-existing table has at least one row and
    the fresh derivation has fewer than ``floor`` rows.

    The connection must NOT be in autocommit mode: the atomic swap and the
    floor guard's rollback both depend on the statements sharing one
    transaction (on an autocommit connection the DROP would already be
    durable by the time the floor check runs).
    """
    if conn.autocommit:
        raise ValueError(
            "derive_va_release requires a transactional connection; "
            "autocommit would make the DROP durable before the floor guard runs"
        )
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
        # Unqualified on purpose, so the probe resolves through the same
        # search_path as the unqualified DDL below and can never disagree
        # with it about which table is being replaced.
        cur.execute("SELECT to_regclass('va_release') IS NOT NULL")
        pre_existed = bool(cur.fetchone()[0])
        pre_rows = 0
        if pre_existed:
            cur.execute("SELECT count(*) FROM va_release")
            pre_rows = int(cur.fetchone()[0])
        cur.execute("DROP TABLE IF EXISTS va_release")
        cur.execute(_DERIVE_SQL)
        cur.execute("SELECT count(*) FROM va_release")
        count = int(cur.fetchone()[0])
        if pre_rows > 0 and count < floor:
            conn.rollback()
            raise FloorViolation(
                f"derived {count} va_release rows, below floor {floor}; "
                f"rolled back to the pre-existing table ({pre_rows} rows)"
            )
        cur.execute(_TRGM_INDEX_SQL)
        cur.execute("ANALYZE va_release")
    conn.commit()
    if count == 0:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM release LIMIT 1)")
            release_populated = bool(cur.fetchone()[0])
        conn.commit()
        if release_populated:
            logger.warning(
                "Derived an EMPTY va_release from a populated release table — "
                "the VA predicate matched nothing. Downstream compilation "
                "matching will find no VA releases until this is investigated."
            )
    logger.info(
        "Derived va_release: %d rows (pre-existing table: %s)",
        count,
        f"replaced ({pre_rows} rows)" if pre_existed else "none",
    )
    return count


def resolve_floor(cli_value: int | None) -> int:
    """Resolve the floor: explicit ``--floor`` wins; else ``VA_RELEASE_FLOOR``.

    Resolved lazily (never at argparse-construction time) so a malformed env
    var cannot crash a caller that passed an explicit ``--floor``. An empty
    env value is treated as unset, matching the shell siblings'
    ``${VAR:-default}`` tolerance.
    """
    if cli_value is not None:
        return cli_value
    raw = os.environ.get("VA_RELEASE_FLOOR", "").strip()
    if not raw:
        return DEFAULT_FLOOR
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"error: VA_RELEASE_FLOOR must be an integer, got {raw!r}") from None


def _describe_target(database_url: str) -> str:
    """Loggable host/database of the target URL, never including credentials."""
    parsed = urllib.parse.urlparse(database_url)
    host = parsed.hostname or "<local socket>"
    port = f":{parsed.port}" if parsed.port else ""
    dbname = parsed.path.lstrip("/") or "<default db>"
    return f"{host}{port}/{dbname}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
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
        default=None,
        help=(
            "Minimum derived row count required to replace a pre-existing "
            "non-empty va_release (rolled back below it). 0 opts out. "
            f"Default: VA_RELEASE_FLOOR or {DEFAULT_FLOOR}."
        ),
    )
    args = parser.parse_args(argv)

    init_logger(repo="discogs-etl", tool="discogs-etl derive_va_release")
    floor = resolve_floor(args.floor)

    database_url = args.database_url or os.environ.get("DATABASE_URL_DISCOGS")
    if not database_url:
        database_url = os.environ.get("DATABASE_URL")
        if database_url:
            logger.warning(
                "Using the generic DATABASE_URL fallback for a DDL operation; "
                "prefer --database-url or DATABASE_URL_DISCOGS so the target "
                "cache is explicit"
            )
    if not database_url:
        print(
            "error: --database-url not provided and DATABASE_URL_DISCOGS/DATABASE_URL not set.",
            file=sys.stderr,
        )
        return 2

    logger.info("Deriving va_release on %s", _describe_target(database_url))
    try:
        with psycopg.connect(database_url) as conn:
            derive_va_release(conn, floor=floor)
    except FloorViolation as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
