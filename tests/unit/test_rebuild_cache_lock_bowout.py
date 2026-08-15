"""Pin scripts/rebuild-cache.sh's handling of run_pipeline.py's advisory-lock
bow-out (discogs-etl#354).

The trap: implemented literally ("exit 0, matching #311's semantics"), a
lock-loser's run_pipeline.py would exit 0, ``set -e`` would see no error, the
script would fall through to its "6. Drift watchdog" step, and post
":white_check_mark: rebuilt successfully" to Slack for a run that never
touched the cache -- exactly the false-positive-success defect class
incident #352 exists to eliminate. run_pipeline.py instead exits with the
distinct ``REBUILD_LOCK_BOWED_OUT_EXIT_CODE`` (lib/rebuild_lock.py), and this
script must branch on that exact value before it ever reaches the drift
watchdog or the success notification.

These are static-structural tests -- they parse the script and assert the
relevant fragments exist in the required order, the same convention already
used for this file's siblings (test_rebuild_cache_alembic_upgrade.py,
test_rebuild_cache_flags.py, test_rebuild_cache_curl_resilience.py). They do
not execute bash; the live end-to-end behavior (a real lock-loser process
exiting with this code and touching nothing) is covered by
tests/integration/test_rebuild_lock.py's TestRunPipelineBowsOutEndToEnd
(pg-marked).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache.sh"
LOCK_MODULE_PATH = REPO_ROOT / "lib" / "rebuild_lock.py"


@pytest.fixture(scope="module")
def script_lines() -> list[str]:
    return SCRIPT_PATH.read_text().splitlines()


@pytest.fixture(scope="module")
def script_text() -> str:
    return SCRIPT_PATH.read_text()


def _non_comment_lines(lines: list[str]) -> list[str]:
    return [line for line in lines if not line.lstrip().startswith("#")]


def _first_index(lines: list[str], needle: str) -> int:
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if needle in line:
            return i
    raise AssertionError(f"{needle!r} not found in non-comment lines of {SCRIPT_PATH}")


def _python_sentinel_value() -> int:
    source = LOCK_MODULE_PATH.read_text()
    match = re.search(r"^REBUILD_LOCK_BOWED_OUT_EXIT_CODE\s*=\s*(\d+)", source, re.MULTILINE)
    assert match is not None, (
        "Could not find REBUILD_LOCK_BOWED_OUT_EXIT_CODE = <int> in "
        f"{LOCK_MODULE_PATH}. This test cross-checks rebuild-cache.sh's "
        "literal against that constant."
    )
    return int(match.group(1))


def test_captures_run_pipeline_exit_code_without_tripping_set_e(script_text: str) -> None:
    """The script must inspect run_pipeline.py's exit code itself rather
    than letting `set -e` abort on any non-zero return -- otherwise it can
    never distinguish "bowed out" from "crashed" (both would just kill the
    script via the ERR trap)."""
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert re.search(r"PIPELINE_EXIT_CODE\s*=\s*\$\?", code), (
        "rebuild-cache.sh must capture run_pipeline.py's exit status into "
        "$PIPELINE_EXIT_CODE (via 'set +e' / '$?' / 'set -e') so it can "
        "branch on the advisory-lock bow-out code instead of letting the "
        "ERR trap treat every non-zero exit as a crash."
    )


def test_bow_out_literal_matches_python_sentinel(script_text: str) -> None:
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    expected = _python_sentinel_value()
    assert re.search(rf"REBUILD_LOCK_BOWED_OUT_EXIT_CODE\s*=\s*{expected}\b", code), (
        f"rebuild-cache.sh's REBUILD_LOCK_BOWED_OUT_EXIT_CODE literal must "
        f"equal {expected}, matching lib/rebuild_lock.py's "
        "REBUILD_LOCK_BOWED_OUT_EXIT_CODE. A silent drift between the two "
        "would make the bow-out branch below dead code."
    )


def test_bow_out_branch_precedes_drift_watchdog(script_lines: list[str]) -> None:
    pipeline_idx = _first_index(script_lines, "run_pipeline.py")
    bowout_check_idx = _first_index(script_lines, "REBUILD_LOCK_BOWED_OUT_EXIT_CODE")
    watchdog_idx = _first_index(script_lines, "check_cache_drift.py")
    assert pipeline_idx < bowout_check_idx < watchdog_idx, (
        "the bow-out exit-code check must sit between the run_pipeline.py "
        f"invocation (line {pipeline_idx + 1}) and the drift watchdog "
        f"(line {watchdog_idx + 1}); found the check at line "
        f"{bowout_check_idx + 1}."
    )


def test_bow_out_branch_precedes_success_notification(script_lines: list[str]) -> None:
    bowout_check_idx = _first_index(script_lines, "REBUILD_LOCK_BOWED_OUT_EXIT_CODE")
    success_idx = _first_index(script_lines, "rebuilt successfully")
    assert bowout_check_idx < success_idx, (
        "the bow-out branch must be checked before the "
        "':white_check_mark: rebuilt successfully' notification can be "
        "reached, so a lock-loser can never be reported as a completed "
        "rebuild (incident #352)."
    )


def test_bow_out_branch_exits_before_reaching_success_path(script_lines: list[str]) -> None:
    """The bow-out branch must itself `exit` -- otherwise execution falls
    through into the drift watchdog / success notify even after correctly
    detecting the bow-out."""
    bowout_check_idx = _first_index(script_lines, "REBUILD_LOCK_BOWED_OUT_EXIT_CODE")
    watchdog_idx = _first_index(script_lines, "check_cache_drift.py")
    between = script_lines[bowout_check_idx:watchdog_idx]
    assert any(re.search(r"\bexit\s+0\b", line) for line in between), (
        "the REBUILD_LOCK_BOWED_OUT_EXIT_CODE branch must call 'exit 0' "
        "before the drift watchdog -- matching #311's existing 'bow out "
        "cleanly, not a failure' semantics -- so a lock-loser never falls "
        "through to the success path."
    )


def test_bow_out_notifies_slack_with_a_distinct_message(script_lines: list[str]) -> None:
    bowout_check_idx = _first_index(script_lines, "REBUILD_LOCK_BOWED_OUT_EXIT_CODE")
    watchdog_idx = _first_index(script_lines, "check_cache_drift.py")
    between = "\n".join(script_lines[bowout_check_idx:watchdog_idx])
    assert "notify_slack" in between, (
        "the bow-out branch must call notify_slack with a message distinct "
        "from ':white_check_mark: rebuilt successfully', so an operator "
        "watching Slack can tell a bow-out apart from a real completed "
        "rebuild."
    )
    assert "rebuilt successfully" not in between, (
        "the bow-out branch's Slack message must not reuse the success "
        "wording -- that would defeat the whole point of distinguishing "
        "the two outcomes."
    )


def test_non_bow_out_non_zero_exit_still_treated_as_failure(script_lines: list[str]) -> None:
    """A real crash (any exit code other than 0 or the bow-out sentinel)
    must still be surfaced as a failure, not silently swallowed by the new
    branch."""
    bowout_check_idx = _first_index(script_lines, "REBUILD_LOCK_BOWED_OUT_EXIT_CODE")
    watchdog_idx = _first_index(script_lines, "check_cache_drift.py")
    between = "\n".join(script_lines[bowout_check_idx:watchdog_idx])
    assert "on_error" in between, (
        "a run_pipeline.py exit code that is neither 0 nor the bow-out "
        "sentinel must still route through on_error (or an equivalent "
        "failure path) so a genuine crash keeps failing loudly."
    )
