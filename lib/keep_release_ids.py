"""Shared parser for the release_id allowlist file consumed by
scripts/dedup_releases.py and scripts/verify_cache.py's --keep-release-ids
flag, to exempt WXYC library pinned overrides (lml_cache.library_release_override)
from the dedup and prune seams.

File format: plain text, one release_id (integer) per line. Blank lines and
lines starting with '#' are ignored. No header, no CSV quoting -- chosen as
the simplest transport for a flat integer list; also trivially parseable by
a future Rust discogs-xml-converter allowlist consumer.
"""

from __future__ import annotations

from pathlib import Path


def parse_keep_release_ids(path: Path) -> set[int]:
    """Parse a newline-separated release_id allowlist file.

    Returns an empty set if *path* does not exist, so callers can treat
    "no override file" and "empty override file" identically.
    """
    if not path.exists():
        return set()
    ids: set[int] = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            ids.add(int(line))
    return ids
