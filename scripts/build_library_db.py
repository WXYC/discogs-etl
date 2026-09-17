#!/usr/bin/env python3
"""Build a ``library.db`` from Backend-Service.

WXYC/discogs-etl#346 retires tubafrenzy's ``wxycmusic`` MySQL as the catalog
source. Until then the only way to run the Backend producer was
``catalog_parity_diff.py``, which builds *two* databases in order to diff
them; the daily sync needs one, with no second side to compare against.

Usage:

    scripts/build_library_db.py --source https://api.wxyc.org --output library.db

Credentials come from the environment, never the command line -- an argv
password is readable by any ``ps`` and is echoed by ``set -x`` and by GitHub
Actions command traces. Supply either ``$BACKEND_CATALOG_TOKEN``, or the
``$BACKEND_CATALOG_EMAIL`` / ``$BACKEND_CATALOG_PASSWORD`` pair.

Exit codes are the ones ``catalog_parity_diff.py`` already established, so a
failed daily-sync run reads the same way as a failed parity soak:

    0  built
    2  usage error
    3  the library.db could not be built

``--output`` must not already exist. The producer builds beside the target and
renames on success, so a failed run leaves nothing behind for the next one to
trip over.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.backend_library_source import (  # noqa: E402
    SourceError,
    build_library_db_from_backend,
)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a library.db from the Backend-Service catalog export.",
    )
    parser.add_argument(
        "--source",
        required=True,
        metavar="URL",
        help=(
            "Backend base URL, e.g. https://api.wxyc.org. Must be https except for loopback hosts."
        ),
    )
    parser.add_argument(
        "--output",
        required=True,
        metavar="PATH",
        help="Where to write the SQLite database. Must not already exist.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    try:
        build_library_db_from_backend(args.source, args.output)
    except SourceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:  # noqa: BLE001 - see module docstring on exit 3
        # Deliberately broad. The caller is a shell script whose only question
        # is "is there a library.db at that path?", and a traceback with exit 1
        # would answer it in a different vocabulary than the SourceError above.
        print(f"error: unexpected failure building {args.output}: {exc}", file=sys.stderr)
        return 3
    print(f"built {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
