#!/usr/bin/env python3
"""Post-backfill parity gate for the one-time LML-override backfill (discogs-etl#329).

Runs the per-source parity query from discogs-etl#327's issue body and exits
non-zero unless every source's ``missing_from_cache`` count is 0 -- so an
operator (or a script) can verify backfill success without re-deriving the
SQL. Read-only.

Usage::

    python scripts/check_override_parity.py --database-url "$DATABASE_URL_DISCOGS"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import psycopg

_SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPTS_DIR.parent))

from lib.library_release_overrides import (  # noqa: E402
    OverrideSourceSummary,
    fetch_override_summary,
    format_summary_table,
)
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)


def evaluate_parity(rows: list[OverrideSourceSummary]) -> bool:
    """True when every source has ``missing_from_cache == 0`` (vacuously True
    for an empty override table). Pure -- unit-tested independent of the DB
    query."""
    return all(row.missing_from_cache == 0 for row in rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL URL for the discogs-cache. Falls back to "
        "DATABASE_URL_DISCOGS, then DATABASE_URL.",
    )
    args = parser.parse_args(argv)

    init_logger(repo="discogs-etl", tool="discogs-etl check_override_parity")

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

    conn = psycopg.connect(database_url)
    try:
        summary = fetch_override_summary(conn)
    finally:
        conn.close()

    print(format_summary_table(summary))

    if not summary:
        # Vacuously healthy (see evaluate_parity), but an empty table is also
        # what a wrong-DB target looks like -- lml_cache.library_release_override
        # present yet holding no pins. fetch_override_summary already raises on a
        # genuinely-absent table, so this is the "present but empty" case: warn
        # loudly rather than let exit 0 read as a verified backfill.
        logger.warning(
            "no override pins found (empty lml_cache.library_release_override); "
            "parity is vacuously OK -- confirm this is the intended discogs-cache."
        )
        return 0

    if evaluate_parity(summary):
        logger.info("parity OK: every source has missing_from_cache == 0")
        return 0

    logger.warning("parity FAILED: at least one source has missing_from_cache > 0")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
