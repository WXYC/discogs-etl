"""Parity check for the v2 ``wxyc_library`` hook against the legacy two-table model.

Implements the §4.1.4 extended parity query from
https://github.com/WXYC/wiki/blob/main/plans/library-hook-canonicalization.md#414-homebrew-discogs-port-5432-full-62-gb-cache--last

This is the dual-write-window audit helper for the **full Homebrew Discogs cache
(port 5432, 62 GB)**. During the ≥30 day dual-run before flipping
``LML_USE_NEW_HOOK_DISCOGS_FULL=true``, run this against ``DATABASE_URL_DISCOGS``
to confirm every row in the legacy ``wxyc_release_match`` table maps to a row
in the consolidated ``wxyc_library``.

Two modes (auto-selected by inspecting ``pg_proc``):

- **``wxyc_norm_artist`` mode** — preferred; uses the Postgres analog of the
  canonical normalization function (E3 step 4 / §3.3.5). High fidelity; matches
  the cross-cache-identity contract.
- **legacy-text mode** — fallback when ``wxyc_norm_artist()`` has not yet been
  deployed to this cache. Compares ``wrm.discogs_artist = wl.artist_name``
  literally; lower fidelity (no diacritic / case fold) but unblocks early
  audits before the function ships. The wiki §4.1.4 sequencing note
  explicitly authorizes this fallback.

The script also gracefully reports when the legacy ``wxyc_release_match``
table is absent (e.g. Docker dev cache, fresh test DB), so it can be run
against any cache as a smoke test.

Usage::

    python scripts/wxyc_library_parity_check.py \\
        --database-url $DATABASE_URL_DISCOGS

Exit codes:

- ``0`` — query ran cleanly. Inspect the ``unmatched_legacy_rows`` count.
- ``2`` — argument / environment error.
- ``3`` — database error or required table absent.

Note that exit ``0`` does **not** mean parity passed; the read of
``unmatched_legacy_rows`` is the operator's call. The 7-consecutive-days
audit-pass criterion in §4.2 is enforced by humans / Slack alerts during
dual-run, not by this script's exit code.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)


ParityMode = Literal["wxyc_norm_artist", "legacy_text", "unavailable"]


@dataclass(frozen=True)
class ParityResult:
    """Outcome of a single parity check run.

    Mirrors the column list from the §4.1.4 extended parity query, plus a
    discriminator for which join condition was used.
    """

    mode: ParityMode
    legacy_artists: int | None
    new_artists: int | None
    unmatched_legacy_rows: int | None
    note: str | None = None


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = %s",
        (table,),
    )
    return cur.fetchone() is not None


def _function_exists(cur, function_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM pg_proc WHERE proname = %s",
        (function_name,),
    )
    return cur.fetchone() is not None


# §4.1.4 query, ``wxyc_norm_artist`` mode (preferred, requires E3 step 4).
_QUERY_NORM_MODE = """
SELECT
    (SELECT COUNT(DISTINCT discogs_artist) FROM wxyc_release_match) AS legacy_artists,
    (SELECT COUNT(DISTINCT artist_name) FROM wxyc_library) AS new_artists,
    (SELECT COUNT(*) FROM wxyc_release_match wrm
       LEFT JOIN wxyc_library wl
         ON wl.norm_artist = wxyc_norm_artist(wrm.discogs_artist)
       WHERE wl.library_id IS NULL) AS unmatched_legacy_rows
"""

# §4.1.4 fallback query, legacy-text mode. Lower fidelity (no diacritic fold),
# but doesn't depend on ``wxyc_norm_artist()`` being deployed.
_QUERY_LEGACY_MODE = """
SELECT
    (SELECT COUNT(DISTINCT discogs_artist) FROM wxyc_release_match) AS legacy_artists,
    (SELECT COUNT(DISTINCT artist_name) FROM wxyc_library) AS new_artists,
    (SELECT COUNT(*) FROM wxyc_release_match wrm
       LEFT JOIN wxyc_library wl
         ON wrm.discogs_artist = wl.artist_name
       WHERE wl.library_id IS NULL) AS unmatched_legacy_rows
"""


def run_parity_check(database_url: str) -> ParityResult:
    """Execute the §4.1.4 parity check against ``database_url``.

    Returns a ``ParityResult`` describing the outcome. Raises on connection
    error so the caller can decide how to surface it (CLI exits 3; tests
    catch).
    """
    import psycopg

    with psycopg.connect(database_url) as conn, conn.cursor() as cur:
        if not _table_exists(cur, "wxyc_library"):
            return ParityResult(
                mode="unavailable",
                legacy_artists=None,
                new_artists=None,
                unmatched_legacy_rows=None,
                note=(
                    "wxyc_library not present in this database. Run "
                    "`alembic upgrade head` against DATABASE_URL_DISCOGS first."
                ),
            )

        if not _table_exists(cur, "wxyc_release_match"):
            # Legacy table is loaded out-of-band on the prod full cache. On a
            # Docker dev cache or test fixture it won't exist; the parity
            # check is then trivially "no legacy rows to compare" and we
            # report mode=unavailable rather than crashing.
            return ParityResult(
                mode="unavailable",
                legacy_artists=None,
                new_artists=None,
                unmatched_legacy_rows=None,
                note=(
                    "wxyc_release_match not present in this database. The "
                    "legacy two-table model is loaded out-of-band on the "
                    "prod full cache only; this check is a no-op elsewhere."
                ),
            )

        if _function_exists(cur, "wxyc_norm_artist"):
            mode: ParityMode = "wxyc_norm_artist"
            cur.execute(_QUERY_NORM_MODE)
        else:
            mode = "legacy_text"
            cur.execute(_QUERY_LEGACY_MODE)

        row = cur.fetchone()
        assert row is not None  # COUNT(*) always returns a row
        legacy_artists, new_artists, unmatched = row

        return ParityResult(
            mode=mode,
            legacy_artists=int(legacy_artists or 0),
            new_artists=int(new_artists or 0),
            unmatched_legacy_rows=int(unmatched or 0),
            note=(
                "wxyc_norm_artist() in use — high-fidelity audit per §3.3.5."
                if mode == "wxyc_norm_artist"
                else (
                    "Falling back to legacy text comparison (wrm.discogs_artist "
                    "= wl.artist_name). Lower fidelity; deploy wxyc_norm_artist() "
                    "from §3.3 step 4 for the canonical audit."
                )
            ),
        )


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Run the §4.1.4 extended parity check (wxyc_release_match vs "
            "wxyc_library) against the full Homebrew Discogs cache."
        ),
    )
    p.add_argument(
        "--database-url",
        default=None,
        help=(
            "PostgreSQL URL for the discogs-cache. Falls back to "
            "DATABASE_URL_DISCOGS, then DATABASE_URL."
        ),
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON object on stdout (machine-readable).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    init_logger(repo="discogs-etl", tool="discogs-etl wxyc_library_parity_check")

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
        result = run_parity_check(database_url)
    except Exception as e:  # pragma: no cover (live-DB path only)
        logger.exception("parity check failed")
        print(f"error: parity check failed: {e}", file=sys.stderr)
        return 3

    if args.json:
        print(json.dumps(asdict(result), indent=2))
    else:
        print(f"mode: {result.mode}")
        if result.note:
            print(f"note: {result.note}")
        if result.legacy_artists is not None:
            print(f"legacy_artists:        {result.legacy_artists:>10}")
            print(f"new_artists:           {result.new_artists:>10}")
            print(f"unmatched_legacy_rows: {result.unmatched_legacy_rows:>10}")

    logger.info(
        "parity check complete",
        extra={
            "step": "wxyc_library_parity_check",
            "mode": result.mode,
            "legacy_artists": result.legacy_artists,
            "new_artists": result.new_artists,
            "unmatched_legacy_rows": result.unmatched_legacy_rows,
        },
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
