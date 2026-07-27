#!/usr/bin/env python3
"""Regenerate the ids-file for the one-time LML-override backfill (discogs-etl#329).

Queries the live discogs-cache for every ``discogs_release_id`` that LML's
``lml_cache.library_release_override`` pins but that is currently absent from
``release`` (the query in discogs-etl#327's issue body), and writes them to a
newline-separated file consumable by ``scripts/seed_cache_from_clone.py --ids-file``.

Read-only: this script never writes to any table (see
``lib/library_release_overrides.py`` for the shared query definitions and
the missing-table failure behavior).

Usage::

    python scripts/query_missing_override_release_ids.py \\
        --database-url "$DATABASE_URL_DISCOGS" \\
        --output missing_override_release_ids.txt
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
    fetch_missing_release_ids,
    fetch_override_summary,
    format_summary_table,
)
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)


def write_ids_file(path: Path, ids: list[int]) -> None:
    """Write *ids*, one per line, preceded by a traceability comment header.

    The header line starts with ``#`` so it is skipped by
    ``seed_cache_from_clone.py``'s ``_read_ids_file`` (blank lines and
    ``#``-prefixed lines are ignored there). Pure I/O, no DB -- unit-tested
    independent of the query functions.
    """
    lines = [f"# {len(ids)} release_id(s) missing from discogs-cache (discogs-etl#329)"]
    lines.extend(str(i) for i in ids)
    path.write_text("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL URL for the discogs-cache. Falls back to "
        "DATABASE_URL_DISCOGS, then DATABASE_URL.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Path to write the missing-release-ids file (seed_cache_from_clone.py --ids-file).",
    )
    args = parser.parse_args(argv)

    init_logger(repo="discogs-etl", tool="discogs-etl query_missing_override_release_ids")

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
        missing_ids = fetch_missing_release_ids(conn)
    finally:
        conn.close()

    print(format_summary_table(summary))
    write_ids_file(args.output, missing_ids)
    logger.info(
        "Wrote %d missing release_id(s) to %s",
        len(missing_ids),
        args.output,
        extra={"step": "query_missing_override_release_ids"},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
