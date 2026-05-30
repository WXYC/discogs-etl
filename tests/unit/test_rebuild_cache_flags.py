"""Pin the flags `scripts/rebuild-cache.sh` passes to `run_pipeline.py`.

The monthly rebuild's pipeline invocation is operator config that's easy to
diverge from the in-tree semantics. Two pins in this file:

1. `--truncate-existing` must NOT be in the invocation. Per #252:
   the flag forces the legacy destructive code path, defeating the
   artwork-preservation upsert added in #242. The first prod rebuild
   after #242 wiped the 46,047 LML-back-patched artwork URLs because the
   script still passed this flag. Pin the absence.

2. `--library-db` and `--xml` must be in the invocation. These are the
   load-bearing arguments — without them the run_pipeline.py call goes
   off the canonical path entirely. (Defensive pin to catch refactors.)
"""

from __future__ import annotations

import shlex
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache.sh"


def _run_pipeline_invocation_tokens() -> list[str]:
    """Return the tokens of the ``run_pipeline.py`` invocation in rebuild-cache.sh.

    Joins continuation lines (``\\``) so a multi-line invocation parses as a
    single shell command, then tokenizes via shlex so quoted arguments and
    bare flags are handled the same way bash would handle them.
    """
    source = SCRIPT_PATH.read_text()
    # Strip backslash-continuations so the call spans one logical line.
    joined = source.replace("\\\n", " ")
    for line in joined.splitlines():
        stripped = line.lstrip()
        if "scripts/run_pipeline.py" in stripped and stripped.startswith("python"):
            return shlex.split(stripped)
    raise AssertionError("Could not find run_pipeline.py invocation in rebuild-cache.sh")


class TestRebuildCacheFlags:
    def test_does_not_pass_truncate_existing(self) -> None:
        # See #252: passing --truncate-existing defeats #242's artwork
        # preservation by forcing the destructive code path. The default
        # is the upsert path; the flag must stay off.
        tokens = _run_pipeline_invocation_tokens()
        assert "--truncate-existing" not in tokens, (
            "rebuild-cache.sh must not pass --truncate-existing; see #252 / #242."
        )

    def test_passes_xml_dump_path(self) -> None:
        tokens = _run_pipeline_invocation_tokens()
        assert "--xml" in tokens, "rebuild-cache.sh must pass --xml to run_pipeline.py."

    def test_passes_library_db_path(self) -> None:
        tokens = _run_pipeline_invocation_tokens()
        assert "--library-db" in tokens, "rebuild-cache.sh must pass --library-db."
