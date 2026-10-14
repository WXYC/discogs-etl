#!/usr/bin/env python3
"""Restore an ``api_fetch`` export into the discogs-cache through the pipeline's own restore.

For a run that kept no ``*_api_fetch_snap`` snapshot (``PRESERVE_API_FETCH=0``, a
standalone ``run_pipeline.py``, ``--target-db-url``, the ``rebuild-cache.yml``
fallback, or a snapshot dropped by hand), restore the ``api_fetch`` releases from an
export taken before it. The export's ``api_fetch`` release family is staged on
``--target`` as the pipeline's own scratch tables
(``lib.api_fetch_preservation.stage_snapshot_from``) and then restored by
``restore_api_fetch_releases``, so the manual and the in-pipeline restore share one
rule set: the dump wins, hollow pins (#424) are rehydrated, the full
``cache_metadata`` row comes back, and the ``release`` lock is bounded.

``--source`` is only read. A real run holds the rebuild lock (key 354001, see
``CLAUDE.md``'s "Advisory lock keys" table) on ``--target`` throughout and exits 75
without writing if another session has it; ``--dry-run`` takes no lock and writes
nothing. A failed restore keeps the staged tables, which the next rebuild restores.

Usage:
    python scripts/restore_api_fetch_from_export.py \
        --source postgresql://localhost:5432/discogs_export \
        --target "$DATABASE_URL_DISCOGS" \
        [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.api_fetch_preservation import (  # noqa: E402
    ApiFetchRestoreError,
    plan_restore_from,
    restore_api_fetch_releases,
    stage_snapshot_from,
)
from lib.observability import init_logger  # noqa: E402
from lib.rebuild_lock import (  # noqa: E402
    REBUILD_LOCK_BOWED_OUT_EXIT_CODE,
    REBUILD_LOCK_KEY,
    release_rebuild_lock,
    try_acquire_rebuild_lock,
)

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--source", required=True, help="Export DSN (read-only).")
    parser.add_argument("--target", required=True, help="Target discogs-cache DSN.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be preserved, rehydrated and superseded; write nothing.",
    )
    args = parser.parse_args(argv)
    if args.source == args.target:
        parser.error("--source and --target are the identical DSN; the arguments look transposed")

    init_logger(repo="discogs-etl", tool="discogs-etl restore_api_fetch_from_export")

    if args.dry_run:
        logger.info("[dry-run] %s", plan_restore_from(args.source, args.target).format())
        return 0

    lock_conn = try_acquire_rebuild_lock(args.target)
    if lock_conn is None:
        # EX_TEMPFAIL is an expected outcome (retry later): WARNING, so it opens no Sentry event.
        logger.warning(
            "Rebuild lock %d is held on --target (a rebuild, dedup_artist_children, a seed or "
            "an operator session); nothing written. Bowing out with %d.",
            REBUILD_LOCK_KEY,
            REBUILD_LOCK_BOWED_OUT_EXIT_CODE,
        )
        return REBUILD_LOCK_BOWED_OUT_EXIT_CODE
    try:
        stage_snapshot_from(args.source, args.target)
        if restore_api_fetch_releases(args.target) is None:
            logger.warning("Nothing was restored: --target has no snapshot or no release table")
    except ApiFetchRestoreError:
        # Already logged by the restore; the staged tables stay for the next rebuild's recovery.
        return 1
    finally:
        release_rebuild_lock(lock_conn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
