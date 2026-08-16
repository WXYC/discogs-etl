"""Pin scripts/rebuild-cache.sh's handling of the `flock` single-instance
bow-out (discogs-etl#395 follow-up).

A cross-PR integration analysis on #395 found that PR newly dangerous for a
pre-existing gap: the `flock -n` guard sat ABOVE `notify_slack()`'s
definition, so its loser branch could only `echo` and `exit 0` -- it had no
function to call even if someone tried to add a notification there. #395's
own bootstrap log line tells operators that a bare `exit 0` from
rebuild-cache.sh is now ambiguous (rebuilt, or bowed out on the advisory
lock) and to disambiguate by reading the Slack message -- but the flock path
never sent one, so that instruction pointed at a message this path could
never emit. A bow-out reported as success is the exact defect class incident
#352 exists to eliminate; #267 is the standing reminder of what it costs.

The fix moves the flock block below `notify_slack()`'s definition and gives
its bow-out a Slack message of its own -- distinct from the advisory-lock
bow-out's `:no_entry:` message, because the cause is different: a concurrent
run on the *same host*, not a peer holding the destination database's
lock. `LOCK_FILE` defaults under `$LOG_DIR`, which on the ephemeral EC2
model is per-instance, so this guard cannot see (and its message must not
claim to guard against) a peer on a different host or account.

Two layers of test live here, mirroring test_rebuild_cache_lock_bowout.py:

Static-structural tests assert the flock block now sits below
`notify_slack()`, and that its message is textually distinct from the
advisory-lock bow-out's and does not overclaim cross-host protection.

``TestFlockBowOutBranchActuallyExecutes`` runs the real region -- verbatim,
including the real `notify_slack()` -- under a real bash, with only `flock`
and `curl` stubbed. That layer exists for the same reason the sibling file's
does: a structural assertion that the block sits "below" notify_slack in
line-number terms cannot prove the call inside it actually resolves and
runs. Executing it can.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache.sh"


@pytest.fixture(scope="module")
def script_lines() -> list[str]:
    return SCRIPT_PATH.read_text().splitlines()


@pytest.fixture(scope="module")
def script_text() -> str:
    return SCRIPT_PATH.read_text()


def _index_of(lines: list[str], stripped_needle: str) -> int:
    for i, line in enumerate(lines):
        if line.strip() == stripped_needle:
            return i
    raise AssertionError(
        f"{stripped_needle!r} not found (as a full stripped line) in {SCRIPT_PATH}"
    )


def test_flock_block_sits_below_notify_slack_definition(script_lines: list[str]) -> None:
    """The bug: `LOCK_FD=200` (start of the flock guard) used to precede
    `notify_slack() {`, so the guard's bow-out branch had no notify_slack
    to call. Pin the corrected order directly."""
    notify_def_idx = _index_of(script_lines, "notify_slack() {")
    lock_fd_idx = _index_of(script_lines, "LOCK_FD=200")
    assert notify_def_idx < lock_fd_idx, (
        "the flock single-instance-lock block (LOCK_FD=200 ...) must sit "
        f"below notify_slack()'s definition (line {notify_def_idx + 1}); "
        f"found LOCK_FD=200 at line {lock_fd_idx + 1}. A bow-out branch "
        "above notify_slack() cannot call it."
    )


def test_flock_bowout_calls_notify_slack(script_lines: list[str]) -> None:
    lock_fd_idx = _index_of(script_lines, "LOCK_FD=200")
    end_idx = next(
        i for i in range(lock_fd_idx, len(script_lines)) if script_lines[i].strip() == "fi"
    )
    between = "\n".join(script_lines[lock_fd_idx : end_idx + 1])
    assert "notify_slack" in between, (
        "the flock bow-out branch must call notify_slack so an operator "
        "reading Slack (per rebuild-cache-bootstrap.sh's terminal log line) "
        "can actually see a same-host bow-out, not just the log file."
    )
    assert "exit 0" in between, "a flock bow-out is not a failure; it must exit 0."


def test_flock_bowout_message_is_distinct_from_advisory_lock_message(script_text: str) -> None:
    """The two bow-outs have different causes -- a concurrent run on the same
    host (flock) versus a peer holding the destination DB's advisory lock --
    and must be distinguishable from Slack alone, without reading the log."""
    # Pull each notify_slack call's (emoji, message) pair out in source order.
    calls = re.findall(r'notify_slack\s+"(:[a-z_]+:)"\s+"([^"]*)"', script_text)
    assert calls, "expected at least one notify_slack call in rebuild-cache.sh"

    advisory_lock_call = next((c for c in calls if "discogs-cache advisory lock" in c[1]), None)
    assert advisory_lock_call is not None, (
        "could not find the advisory-lock bow-out notify_slack call"
    )

    flock_calls = [c for c in calls if c is not advisory_lock_call and "flock" in c[1].lower()]
    assert flock_calls, (
        "expected a notify_slack call whose message mentions 'flock' to "
        "distinguish a same-host bow-out from the advisory-lock bow-out"
    )
    flock_call = flock_calls[0]

    assert flock_call[0] != advisory_lock_call[0], (
        f"the flock bow-out emoji {flock_call[0]!r} must differ from the "
        f"advisory-lock bow-out emoji {advisory_lock_call[0]!r} so the two "
        "are visually distinguishable in a Slack channel."
    )
    assert flock_call[1] != advisory_lock_call[1], "the two bow-out messages must not be identical"
    assert "rebuilt successfully" not in flock_call[1]


def test_flock_bowout_message_does_not_overclaim_cross_host_protection(script_text: str) -> None:
    """LOCK_FILE defaults under $LOG_DIR, which is per-instance on the
    ephemeral EC2 model -- this guard cannot see a peer on another host or
    AWS account. The message must not imply it can."""
    calls = re.findall(r'notify_slack\s+"(:[a-z_]+:)"\s+"([^"]*)"', script_text)
    flock_call = next((c for c in calls if "flock" in c[1].lower()), None)
    assert flock_call is not None, "expected to find the flock bow-out's notify_slack call"
    message = flock_call[1].lower()

    for overclaim in ("every instance", "across instances", "any account", "any region", "cluster"):
        assert overclaim not in message, (
            f"the flock bow-out message must not claim cross-host protection "
            f"it doesn't provide (found {overclaim!r} in {flock_call[1]!r})"
        )
    assert "this host" in message or "same host" in message, (
        "the flock bow-out message should scope itself to this host, since "
        f"that is all the guard can see. Got: {flock_call[1]!r}"
    )


# ---------------------------------------------------------------------------
# Executable layer — run the real region (real notify_slack included) under a
# real bash, with only `flock` and `curl` stubbed.
# ---------------------------------------------------------------------------


def _extract_notify_slack_through_flock_block(lines: list[str]) -> str:
    """Slice the script from notify_slack()'s definition through the end of
    the flock single-instance-lock block, verbatim. This deliberately
    includes the real notify_slack (and on_error/trap/fail, which sit
    between the two in the fixed script) rather than a stub, so a regression
    that moves the flock block back above notify_slack's definition breaks
    this extraction (and the test below) instead of silently passing."""
    start = _index_of(lines, "notify_slack() {")
    lock_fd_idx = _index_of(lines, "LOCK_FD=200")
    assert lock_fd_idx > start, (
        "notify_slack() must precede the flock block for this extraction to "
        "make sense; see test_flock_block_sits_below_notify_slack_definition"
    )
    end = next(i for i in range(lock_fd_idx, len(lines)) if lines[i].strip() == "fi")
    return "\n".join(lines[start : end + 1])


def _build_harness(flock_rc: int, tmp_path: Path) -> str:
    lines = SCRIPT_PATH.read_text().splitlines()
    region = _extract_notify_slack_through_flock_block(lines)
    log_file = tmp_path / "run.log"
    lock_file = tmp_path / "discogs-rebuild.lock"
    return "\n".join(
        [
            "set -euo pipefail",
            f'LOG_FILE="{log_file}"',
            f'LOG_DIR="{tmp_path}"',
            f'LOCK_FILE="{lock_file}"',
            'SLACK_MONITORING_WEBHOOK="http://stub-webhook.invalid"',
            f"FLOCK_RC={flock_rc}",
            # Stub flock: real bash has no portable `flock` on every dev
            # machine (notably macOS), and we only care about its exit
            # status here, not real file-locking semantics -- those are the
            # OS's job, already exercised in production.
            'flock() { return "$FLOCK_RC"; }',
            # Stub curl so the REAL notify_slack (pulled in verbatim below)
            # doesn't hit the network; it still runs its own body, proving
            # the function is defined and callable at this point.
            'curl() { echo "CURL_CALLED $*"; return 0; }',
            region,
            'echo "FELL THROUGH PAST LOCK GUARD"',
        ]
    )


def _run_harness(flock_rc: int, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    bash = shutil.which("bash")
    assert bash is not None, "bash is required to execute this region"
    harness = tmp_path / "harness.sh"
    harness.write_text(_build_harness(flock_rc, tmp_path))
    return subprocess.run(
        [bash, str(harness)], capture_output=True, text=True, timeout=30, cwd=tmp_path
    )


class TestFlockBowOutBranchActuallyExecutes:
    """The regression this guards against: a flock bow-out branch that sits
    above notify_slack()'s definition looks fine to a human reader and can
    even satisfy naive static assertions (e.g. "the word notify_slack
    appears somewhere in the file"), but the call itself is unreachable
    dead code -- calling an undefined function under `set -u`/`set -e`."""

    def test_contended_lock_notifies_and_exits_zero(self, tmp_path: Path) -> None:
        result = _run_harness(flock_rc=1, tmp_path=tmp_path)
        assert result.returncode == 0, (
            "a same-host bow-out is not a failure; the block must exit 0.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        assert "CURL_CALLED" in result.stdout, (
            "notify_slack's real body (which shells out to curl) must have "
            "run, proving the function was defined and callable at this "
            "point in the script. If this is missing, the flock block is "
            "still above notify_slack()'s definition.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        assert "FELL THROUGH PAST LOCK GUARD" not in result.stdout, (
            "a contended lock must exit before the rest of the script runs"
        )

    def test_uncontended_lock_falls_through_without_notifying(self, tmp_path: Path) -> None:
        result = _run_harness(flock_rc=0, tmp_path=tmp_path)
        assert result.returncode == 0
        assert "FELL THROUGH PAST LOCK GUARD" in result.stdout, (
            "an uncontended lock must let the rest of the script proceed.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        assert "CURL_CALLED" not in result.stdout, "an uncontended lock must not notify Slack"
