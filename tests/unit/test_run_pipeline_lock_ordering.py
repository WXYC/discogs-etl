"""Pin the fork-ordering invariant of run_pipeline.py's advisory-lock guard
(discogs-etl#354).

Advisory locks are cooperative: a session lock held by ``main()`` blocks none
of the children ``run_step`` forks via ``subprocess.Popen`` (each opens its
own fresh connection). The guard only functions as a mutex if the lock
acquisition happens exactly once, at the earliest point in ``main()``,
*before* the first subprocess is spawned -- a lock taken after
``generate_library_db()`` (the earliest fork in ``main()``, gated by
``--generate-library-db``) or after either build-path dispatcher would leave
a window where a losing peer's earlier forks have already run.

These are static-structural tests -- they parse ``scripts/run_pipeline.py``
and assert the relevant fragments exist in the required order, the same
convention ``tests/unit/test_rebuild_cache_alembic_upgrade.py`` uses for
``scripts/rebuild-cache.sh``. The live acquire/refuse/release behavior is
covered by ``tests/integration/test_rebuild_lock.py`` (pg-marked).
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_pipeline.py"


@pytest.fixture(scope="module")
def script_lines() -> list[str]:
    return SCRIPT_PATH.read_text().splitlines()


def _main_body_bounds(lines: list[str]) -> tuple[int, int]:
    """Return (start, end) line indices spanning ``def main() -> None:``'s body.

    ``end`` is the index of the next top-level (column-0) ``def`` after
    ``main``, or ``len(lines)`` if ``main`` is the last top-level def.
    """
    start = None
    for i, line in enumerate(lines):
        if line.startswith("def main() -> None:"):
            start = i
            break
    if start is None:
        raise AssertionError(f"Could not find 'def main() -> None:' in {SCRIPT_PATH}")
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("def "):
            return start, j
    return start, len(lines)


def _first_index_in_range(lines: list[str], needle: str, start: int, end: int) -> int:
    for i in range(start, end):
        if needle in lines[i]:
            return i
    raise AssertionError(f"{needle!r} not found in main() body ({SCRIPT_PATH}:{start}-{end})")


class TestAdvisoryLockAcquiredFirst:
    def test_main_imports_the_lock_helpers(self, script_lines: list[str]) -> None:
        source = "\n".join(script_lines)
        assert "from lib.rebuild_lock import" in source, (
            "run_pipeline.py must import the advisory-lock helpers from "
            "lib.rebuild_lock (discogs-etl#354)."
        )

    def test_lock_acquired_before_generate_library_db_call(self, script_lines: list[str]) -> None:
        """generate_library_db() is the earliest possible subprocess.Popen
        fork in main() (gated by --generate-library-db, via run_step ->
        wxyc-export-to-sqlite). The lock must be acquired before it."""
        start, end = _main_body_bounds(script_lines)
        lock_idx = _first_index_in_range(script_lines, "try_acquire_rebuild_lock(", start, end)
        fork_idx = _first_index_in_range(script_lines, "generate_library_db(", start, end)
        assert lock_idx < fork_idx, (
            f"try_acquire_rebuild_lock (line {lock_idx + 1}) must precede the "
            f"generate_library_db() call (line {fork_idx + 1}) -- that is the "
            "earliest subprocess fork reachable from main(). See discogs-etl#354."
        )

    def test_lock_acquired_before_xml_pipeline_dispatch(self, script_lines: list[str]) -> None:
        start, end = _main_body_bounds(script_lines)
        lock_idx = _first_index_in_range(script_lines, "try_acquire_rebuild_lock(", start, end)
        dispatch_idx = _first_index_in_range(script_lines, "_run_xml_pipeline(", start, end)
        assert lock_idx < dispatch_idx, (
            f"try_acquire_rebuild_lock (line {lock_idx + 1}) must precede the "
            f"_run_xml_pipeline() dispatch (line {dispatch_idx + 1}), which "
            "forks the converter and every database-build subprocess."
        )

    def test_lock_acquired_before_database_build_dispatch(self, script_lines: list[str]) -> None:
        start, end = _main_body_bounds(script_lines)
        lock_idx = _first_index_in_range(script_lines, "try_acquire_rebuild_lock(", start, end)
        dispatch_idx = _first_index_in_range(script_lines, "_run_database_build(", start, end)
        assert lock_idx < dispatch_idx, (
            f"try_acquire_rebuild_lock (line {lock_idx + 1}) must precede the "
            f"_run_database_build() dispatch (line {dispatch_idx + 1})."
        )

    def test_bow_out_uses_the_distinct_exit_code(self, script_lines: list[str]) -> None:
        start, end = _main_body_bounds(script_lines)
        lock_idx = _first_index_in_range(script_lines, "try_acquire_rebuild_lock(", start, end)
        exit_idx = _first_index_in_range(
            script_lines, "REBUILD_LOCK_BOWED_OUT_EXIT_CODE", start, end
        )
        assert lock_idx < exit_idx, (
            "the bow-out branch (sys.exit(REBUILD_LOCK_BOWED_OUT_EXIT_CODE)) "
            "must appear after the acquisition attempt in main()."
        )
        # Must not be a bare sys.exit(0) or sys.exit(1) standing in for it --
        # that is exactly the #352 false-positive-success trap the ticket
        # warns about (see scripts/rebuild-cache.sh's own test file).
        bow_out_line = script_lines[exit_idx]
        assert "sys.exit(" in "\n".join(script_lines[exit_idx : exit_idx + 3]), (
            "REBUILD_LOCK_BOWED_OUT_EXIT_CODE must be passed straight to "
            f"sys.exit(...) near line {exit_idx + 1}."
        )
        assert bow_out_line.strip() != "sys.exit(0)"

    def test_lock_released_in_main(self, script_lines: list[str]) -> None:
        start, end = _main_body_bounds(script_lines)
        # Must not raise -- release_rebuild_lock must appear somewhere in main().
        _first_index_in_range(script_lines, "release_rebuild_lock(", start, end)
