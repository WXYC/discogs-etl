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

Two layers of test live here.

The module-level tests are static-structural -- they parse the script and
assert the relevant fragments exist in the required order, the same
convention already used for this file's siblings
(test_rebuild_cache_alembic_upgrade.py, test_rebuild_cache_flags.py,
test_rebuild_cache_curl_resilience.py).

``TestBowOutBranchActuallyExecutes`` runs the region under a real bash. That
layer exists because the static one is provably not enough: the first cut of
this change bracketed the invocation in ``set +e`` / ``set -e``, which reads
correctly and satisfies every structural assertion, but does not work. ``set
+e`` suppresses errexit only -- bash fires an ERR trap for any failing simple
command regardless of errexit state -- so the ERR trap installed above still
fired at the invocation line, posted ":warning: failed at line N (exit 75)",
and exited 75, leaving the entire bow-out branch unreachable. Text-only tests
cannot see that; executing the region can. Verified against bash 3.2.57 and
5.2.37.

The live end-to-end behavior on the Python side (a real lock-loser process
exiting with this code and touching nothing) is covered by
tests/integration/test_rebuild_lock.py's TestRunPipelineBowsOutEndToEnd
(pg-marked).
"""

from __future__ import annotations

import re
import shutil
import subprocess
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


# ---------------------------------------------------------------------------
# Executable layer — run the real region under a real bash.
# ---------------------------------------------------------------------------


def _extract_function(source: str, name: str) -> str:
    """Lift ``name() { ... }`` out of the script verbatim."""
    match = re.search(rf"^{re.escape(name)}\(\) \{{\n.*?^\}}$", source, re.MULTILINE | re.DOTALL)
    assert match is not None, f"could not find the {name}() definition in {SCRIPT_PATH}"
    return match.group(0)


def _extract_err_traps(source: str) -> list[str]:
    """Every ``trap '...' ERR`` line, in order. The last one installed is the
    one armed by the time the pipeline runs, which is what this region has to
    survive."""
    traps = re.findall(r"^trap\s+'[^']+'\s+ERR$", source, re.MULTILINE)
    assert traps, f"no ERR trap found in {SCRIPT_PATH}"
    return traps


def _extract_exit_code_region(lines: list[str], stub_rc_var: str) -> str:
    """Return the pipeline-invocation + exit-code-branch region of the script,
    with the ``run_pipeline.py`` invocation swapped for a stub exiting
    ``$stub_rc_var`` and everything else -- the capture construct, the branch
    conditions, the notify calls, the ``exit``s -- kept verbatim.

    Anchored on ``PIPELINE_EXIT_CODE=0`` through the closing ``fi`` rather
    than on line numbers, so the region tracks edits to the script.
    """
    start = next(i for i, line in enumerate(lines) if line.strip() == "PIPELINE_EXIT_CODE=0")
    end = next(i for i in range(start, len(lines)) if lines[i].strip() == "fi")

    region = lines[start : end + 1]
    invoke_start = next(i for i, line in enumerate(region) if "scripts/run_pipeline.py" in line)
    invoke_end = invoke_start
    while region[invoke_end].rstrip().endswith("\\"):
        invoke_end += 1

    tail = region[invoke_end].split("\\")[-1]
    # Preserve whatever the real invocation appends after the command itself
    # (today: `|| PIPELINE_EXIT_CODE=$?`). That suffix is the thing under test.
    suffix = tail.split('"', 2)[-1] if '"' in tail else tail
    stub = f'sh -c "exit ${stub_rc_var}"{suffix}'
    return "\n".join([*region[:invoke_start], stub, *region[invoke_end + 1 :]])


def _build_harness(stub_rc: int) -> str:
    source = SCRIPT_PATH.read_text()
    lines = source.splitlines()
    return "\n".join(
        [
            "set -euo pipefail",
            'LOG_FILE="/dev/null"',
            'WORK_DIR="/nonexistent"',
            'REPO_DIR="/nonexistent"',
            f"STUB_RC={stub_rc}",
            # Stand-in for the real notify_slack (which curls a webhook);
            # everything else below is the script's own code, verbatim.
            'notify_slack() { echo "NOTIFY $1 $2"; }',
            _extract_function(source, "on_error"),
            *_extract_err_traps(source),
            _extract_exit_code_region(lines, "STUB_RC"),
            'echo "FELL THROUGH TO DRIFT WATCHDOG"',
        ]
    )


def _run_harness(stub_rc: int, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    bash = shutil.which("bash")
    assert bash is not None, "bash is required to execute this region"
    harness = tmp_path / "harness.sh"
    harness.write_text(_build_harness(stub_rc))
    return subprocess.run(
        [bash, str(harness)], capture_output=True, text=True, timeout=30, cwd=tmp_path
    )


class TestBowOutBranchActuallyExecutes:
    """The regression these guard: any capture construct that leaves the ERR
    trap armed (notably `set +e` / `set -e` bracketing, which suppresses
    errexit but NOT the trap) makes every line of the bow-out branch dead
    code while still passing every static assertion above."""

    def test_bow_out_reaches_its_branch_and_exits_zero(self, tmp_path: Path) -> None:
        result = _run_harness(_python_sentinel_value(), tmp_path)
        assert result.returncode == 0, (
            "a bow-out is not a failure; the script must exit 0.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        assert "NOTIFY :no_entry:" in result.stdout, (
            "the bow-out branch must be reached and must post its distinct "
            "Slack message. If ':warning:' appears instead, the ERR trap beat "
            "the branch to the exit code -- see this module's docstring.\n"
            f"stdout:\n{result.stdout}"
        )
        assert "NOTIFY :warning:" not in result.stdout, (
            "a bow-out must not be reported as a failure"
        )
        assert "FELL THROUGH TO DRIFT WATCHDOG" not in result.stdout, (
            "the bow-out branch must exit before the drift watchdog and the "
            "':white_check_mark: rebuilt successfully' notification (incident #352)"
        )

    def test_a_real_crash_still_fails_loudly(self, tmp_path: Path) -> None:
        result = _run_harness(1, tmp_path)
        assert result.returncode == 1, (
            f"a non-bow-out non-zero exit must propagate.\nstdout:\n{result.stdout}"
        )
        assert "NOTIFY :warning:" in result.stdout
        assert "NOTIFY :no_entry:" not in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" not in result.stdout

    def test_a_clean_run_falls_through_to_the_rest_of_the_script(self, tmp_path: Path) -> None:
        result = _run_harness(0, tmp_path)
        assert result.returncode == 0
        assert "FELL THROUGH TO DRIFT WATCHDOG" in result.stdout, (
            "a successful pipeline run must continue to the drift watchdog"
        )
        assert "NOTIFY" not in result.stdout, "a clean run must not notify from this region"


WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "rebuild-cache.yml"


def test_workflow_bow_out_literal_matches_python_sentinel() -> None:
    """The manual-dispatch GHA path invokes run_pipeline.py directly, so it is
    a third consumer of the sentinel and needs the same literal.

    It matters more here than it looks: that step feeds the dump through a
    FIFO with curl parked on the write end, and a bow-out exits before the
    converter ever opens the read end. Without this branch the job blocks on
    `wait` until its 350-minute timeout. A drifted literal silently restores
    that ~5.8h hang.
    """
    expected = _python_sentinel_value()
    workflow = WORKFLOW_PATH.read_text()
    assert re.search(rf'\[ "\$PY_RC" -eq {expected} \]', workflow), (
        f"{WORKFLOW_PATH} must branch on PY_RC == {expected}, matching "
        "lib/rebuild_lock.py's REBUILD_LOCK_BOWED_OUT_EXIT_CODE."
    )


def test_workflow_bow_out_reaps_curl_before_waiting() -> None:
    expected = _python_sentinel_value()
    lines = WORKFLOW_PATH.read_text().splitlines()
    branch_idx = next(i for i, line in enumerate(lines) if f"-eq {expected} ]" in line)
    wait_idx = next(i for i, line in enumerate(lines) if 'wait "$CURL_PID" || true' in line)
    assert branch_idx < wait_idx, (
        "the bow-out branch must come before the unconditional `wait` on curl"
    )
    between = "\n".join(lines[branch_idx:wait_idx])
    assert 'kill "$CURL_PID"' in between, (
        "the bow-out branch must kill the backgrounded curl, which is blocked "
        "in open() on a FIFO no reader will ever open, before exiting"
    )
