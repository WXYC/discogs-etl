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

import ast
import json
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


def _branch_index(lines: list[str]) -> int:
    """The post-loop ``-eq`` test of the exit code (the constant's definition and
    the loop's ``-ne`` exit test sit above it)."""
    return _first_index(lines, '-eq "$REBUILD_LOCK_BOWED_OUT_EXIT_CODE"')


def _python_constant(name: str) -> str:
    match = re.search(rf'^{name}\s*=\s*(\d+|"[^"]*")', LOCK_MODULE_PATH.read_text(), re.M)
    assert match is not None, f"could not find {name} = <literal> in {LOCK_MODULE_PATH}"
    return match.group(1).strip('"')


def _python_free_exit_code() -> int:
    return int(_python_constant("REBUILD_LOCK_HOLDER_FREE_EXIT_CODE"))


REBUILD_HOLDER = "discogs-etl-rebuild"


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
    bowout_check_idx = _branch_index(script_lines)
    watchdog_idx = _first_index(script_lines, "check_cache_drift.py")
    assert pipeline_idx < bowout_check_idx < watchdog_idx, (
        "the bow-out exit-code check must sit between the run_pipeline.py "
        f"invocation (line {pipeline_idx + 1}) and the drift watchdog "
        f"(line {watchdog_idx + 1}); found the check at line "
        f"{bowout_check_idx + 1}."
    )


def test_bow_out_branch_precedes_success_notification(script_lines: list[str]) -> None:
    bowout_check_idx = _branch_index(script_lines)
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
    bowout_check_idx = _branch_index(script_lines)
    watchdog_idx = _first_index(script_lines, "check_cache_drift.py")
    between = script_lines[bowout_check_idx:watchdog_idx]
    assert any(re.search(r"\bexit\s+0\b", line) for line in between), (
        "the REBUILD_LOCK_BOWED_OUT_EXIT_CODE branch must call 'exit 0' "
        "before the drift watchdog -- matching #311's existing 'bow out "
        "cleanly, not a failure' semantics -- so a lock-loser never falls "
        "through to the success path."
    )


def test_bow_out_notifies_slack_with_a_distinct_message(script_lines: list[str]) -> None:
    bowout_check_idx = _branch_index(script_lines)
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
    bowout_check_idx = _branch_index(script_lines)
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


def _extract_lock_wait_region(lines: list[str]) -> str:
    """Return the retry loop and the exit-code branches after it, verbatim.

    Anchored on the constants block through the ``fi`` that closes the
    ``on_error`` branch rather than on line numbers, so the region tracks edits
    to the script. Nothing in it is replaced: the harness stubs ``python``,
    ``sleep``, ``notify_slack`` and ``report_outcome`` as shell functions, so
    the capture construct, the loop, the branch conditions and the ``exit``s are
    the script's own text.
    """
    start = next(
        i for i, line in enumerate(lines) if line.startswith("REBUILD_LOCK_BOWED_OUT_EXIT_CODE=")
    )
    on_error = next(i for i in range(start, len(lines)) if "on_error " in lines[i])
    end = next(i for i in range(on_error, len(lines)) if lines[i].strip() == "fi")
    return "\n".join(lines[start : end + 1])


# Replies for the stubbed holder CLI: a name, "" for an unnamed holder, FREE for
# "nobody holds it" (the CLI's exit 3) and ERROR for a failed lookup.
FREE = "@free"
ERROR = "@error"


def _build_harness(
    pipeline_rcs: list[int],
    holders: list[str],
    wait_minutes: int | str | None,
    free_exit_code: int,
    real_notify: bool = False,
    stubs: tuple[str, ...] = (),
    pipeline_cost: int = 0,
    lookups: list[str] | None = None,
    lookup_file: Path | None = None,
) -> str:
    source = SCRIPT_PATH.read_text()
    lines = source.splitlines()
    rcs = " ".join(str(rc) for rc in pipeline_rcs)
    replies = " ".join(f"'{holder}'" for holder in holders)
    return "\n".join(
        [
            "set -euo pipefail",
            'LOG_FILE="/dev/null"',
            'WORK_DIR="/nonexistent"',
            'REPO_DIR="/nonexistent"',
            'PRESERVE_API_FETCH_ARG=""',
            *([f"REBUILD_LOCK_WAIT_MINUTES='{wait_minutes}'"] if wait_minutes is not None else []),
            f"PIPELINE_RCS=({rcs})",
            f"HOLDER_REPLIES=({replies})",
            "ATTEMPT=0",
            # With lookups given, holder-CLI call N answers LOOKUP_REPLIES[N-1]
            # (repeating the last), counted apart from the pipeline attempts so
            # a lookup retried within one attempt can be scripted.
            f"LOOKUP_REPLIES=({' '.join(repr(r) for r in (lookups or []))})",
            f'LOOKUP_FILE="{lookup_file}"',
            # Seconds each stubbed pipeline attempt takes, by advancing bash's
            # own SECONDS clock.
            f"PIPELINE_COST={pipeline_cost}",
            # Stand-in for the real notify_slack (which curls a webhook);
            # everything else below is the script's own code, verbatim. With
            # real_notify the script's own notify_slack runs and only curl is
            # stubbed, printing the JSON body it was handed.
            *(
                [
                    'SLACK_MONITORING_WEBHOOK="https://hooks.invalid"',
                    'curl() { while [ $# -gt 0 ]; do [ "$1" = "-d" ] && echo "PAYLOAD $2"; '
                    "shift; done; }",
                    _extract_function(source, "notify_slack"),
                ]
                if real_notify
                else ['notify_slack() { echo "NOTIFY $1 $2"; }']
            ),
            # Likewise for report_outcome (#424), which shells out to
            # scripts/report_rebuild_outcome.py. on_error() is extracted
            # verbatim below and calls it, so it has to exist here; the marker's
            # own behavior is pinned in test_rebuild_outcome_marker.py.
            'report_outcome() { echo "OUTCOME $1 ${2:-}"; }',
            'sleep() { echo "SLEEP $1"; }',
            *stubs,
            # One stub for both python entry points the region calls. The
            # pipeline's attempt N exits PIPELINE_RCS[N-1]; the holder CLI,
            # called right after attempt N, answers HOLDER_REPLIES[N-1]. Both
            # arrays repeat their last entry.
            "python() {",
            "    local n=$ATTEMPT",
            '    case "$*" in',
            "        *scripts/run_pipeline.py*)",
            "            ATTEMPT=$((ATTEMPT + 1)); n=$ATTEMPT",
            '            echo "PIPELINE attempt $n"',
            "            SECONDS=$((SECONDS + PIPELINE_COST))",
            "            [ $n -gt ${#PIPELINE_RCS[@]} ] && n=${#PIPELINE_RCS[@]}",
            '            return "${PIPELINE_RCS[$((n - 1))]}" ;;',
            "        *'-m lib.rebuild_lock --describe-holder'*)",
            "            local reply",
            "            if [ ${#LOOKUP_REPLIES[@]} -gt 0 ]; then",
            '                n=$(($(cat "$LOOKUP_FILE" 2>/dev/null || echo 0) + 1)); echo $n >"$LOOKUP_FILE"',
            "                [ $n -gt ${#LOOKUP_REPLIES[@]} ] && n=${#LOOKUP_REPLIES[@]}",
            '                reply="${LOOKUP_REPLIES[$((n - 1))]}"',
            "            else",
            "                [ $n -gt ${#HOLDER_REPLIES[@]} ] && n=${#HOLDER_REPLIES[@]}",
            '                reply="${HOLDER_REPLIES[$((n - 1))]}"',
            "            fi",
            f'            [ "$reply" = "{FREE}" ] && return {free_exit_code}',
            '            [ "$reply" = "' + ERROR + '" ] && return 1',
            '            echo "$reply"; return 0 ;;',
            '        *) echo "unexpected python call: $*" >&2; return 99 ;;',
            "    esac",
            "}",
            _extract_function(source, "on_error"),
            *_extract_err_traps(source),
            _extract_lock_wait_region(lines),
            'echo "FELL THROUGH TO DRIFT WATCHDOG"',
        ]
    )


def _run_harness(
    pipeline_rcs: int | list[int],
    tmp_path: Path,
    *,
    holders: list[str] | None = None,
    wait_minutes: int | str | None = None,
    real_notify: bool = False,
    stubs: tuple[str, ...] = (),
    pipeline_cost: int = 0,
    lookups: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    bash = shutil.which("bash")
    assert bash is not None, "bash is required to execute this region"
    rcs = [pipeline_rcs] if isinstance(pipeline_rcs, int) else pipeline_rcs
    harness = tmp_path / "harness.sh"
    harness.write_text(
        _build_harness(
            rcs,
            holders or [REBUILD_HOLDER],
            wait_minutes,
            _python_free_exit_code(),
            real_notify,
            stubs,
            pipeline_cost,
            lookups,
            tmp_path / "lookups",
        )
    )
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

    def test_a_peer_rebuild_holder_bows_out_at_once(self, tmp_path: Path) -> None:
        result = _run_harness([75], tmp_path, holders=[REBUILD_HOLDER])
        assert result.returncode == 0, result.stdout
        assert result.stdout.count("PIPELINE attempt") == 1
        assert "SLEEP" not in result.stdout
        assert "hourglass" not in result.stdout
        assert "NOTIFY :no_entry:" in result.stdout
        assert f"Holder: {REBUILD_HOLDER}." in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" not in result.stdout

    @pytest.mark.parametrize(
        ("holder", "label"),
        [
            ("discogs-etl-seed", "discogs-etl-seed"),
            ("discogs-etl-dedup", "discogs-etl-dedup"),
            ("", "an unnamed session"),
            ("psql", "psql"),
        ],
    )
    def test_a_non_rebuild_holder_is_waited_out_then_the_rebuild_runs(
        self, tmp_path: Path, holder: str, label: str
    ) -> None:
        result = _run_harness([75, 75, 0], tmp_path, holders=[holder], wait_minutes=180)
        assert result.returncode == 0, result.stdout + result.stderr
        out = result.stdout.splitlines()
        assert out.count("SLEEP 300") == 2
        assert result.stdout.count("PIPELINE attempt") == 3
        hourglass = [line for line in out if line.startswith("NOTIFY :hourglass_flowing_sand:")]
        assert len(hourglass) == 1, "the wait is announced once, not once per retry"
        assert "354001" in hourglass[0]
        assert label in hourglass[0]
        assert re.search(r"retrying every 5 min until \w{3} \d\d:\d\d P[SD]T", hourglass[0])
        assert "NOTIFY :warning:" not in result.stdout
        assert "NOTIFY :no_entry:" not in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" in result.stdout

    def test_a_non_rebuild_holder_past_the_deadline_bows_out_naming_it(
        self, tmp_path: Path
    ) -> None:
        result = _run_harness([75], tmp_path, holders=["discogs-etl-seed"], wait_minutes=10)
        assert result.returncode == 0, result.stdout
        assert result.stdout.count("PIPELINE attempt") == 3  # t=0, t=5, t=10 min
        assert result.stdout.count("SLEEP 300") == 2
        final = next(ln for ln in result.stdout.splitlines() if ln.startswith("NOTIFY :no_entry:"))
        assert "discogs-etl-seed" in final
        assert "could not take the lock in the whole 10-minute wait" in final
        outcome = next(
            ln for ln in result.stdout.splitlines() if ln.startswith("OUTCOME bowed_out")
        )
        assert "discogs-etl-seed" in outcome
        assert "FELL THROUGH TO DRIFT WATCHDOG" not in result.stdout

    def test_the_holder_becoming_a_peer_rebuild_mid_wait_bows_out_then(
        self, tmp_path: Path
    ) -> None:
        result = _run_harness([75, 75, 75], tmp_path, holders=["discogs-etl-dedup", REBUILD_HOLDER])
        assert result.stdout.count("PIPELINE attempt") == 2
        assert result.stdout.count("SLEEP 300") == 1
        assert "NOTIFY :no_entry:" in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" not in result.stdout

    def test_a_lock_released_between_attempt_and_lookup_is_retried_at_once(
        self, tmp_path: Path
    ) -> None:
        result = _run_harness([75, 0], tmp_path, holders=[FREE])
        assert result.stdout.count("PIPELINE attempt") == 2
        assert "SLEEP" not in result.stdout
        assert "hourglass" not in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" in result.stdout

    def test_a_failed_lookup_then_a_named_holder_is_waited_out(self, tmp_path: Path) -> None:
        result = _run_harness(
            [75, 0], tmp_path, lookups=[ERROR, "discogs-etl-seed"], wait_minutes=180
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "holder lookup 1 of 3 failed (exit 1)" in result.stderr
        out = result.stdout.splitlines()
        assert out.count("SLEEP 30") == 1
        assert out.count("SLEEP 300") == 1
        assert result.stdout.count("PIPELINE attempt") == 2
        assert "NOTIFY :no_entry:" not in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" in result.stdout

    def test_a_failed_lookup_then_a_peer_rebuild_bows_out_at_once(self, tmp_path: Path) -> None:
        result = _run_harness([75, 0], tmp_path, lookups=[ERROR, REBUILD_HOLDER], wait_minutes=180)
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stdout.count("PIPELINE attempt") == 1
        assert f"Holder: {REBUILD_HOLDER}." in result.stdout
        assert "SLEEP 300" not in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" not in result.stdout

    def test_every_lookup_failing_bows_out_after_the_configured_attempts(
        self, tmp_path: Path
    ) -> None:
        """A holder that cannot be named might be a peer rebuild, so the
        pipeline is never re-run to retry the lookup."""
        result = _run_harness([75, 0], tmp_path, lookups=[ERROR], wait_minutes=180)
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stdout.count("PIPELINE attempt") == 1
        assert result.stderr.count("holder lookup") == 3
        assert [ln for ln in result.stdout.splitlines() if ln.startswith("SLEEP")] == [
            "SLEEP 30",
            "SLEEP 30",
        ]
        final = next(ln for ln in result.stdout.splitlines() if ln.startswith("NOTIFY :no_entry:"))
        assert "Holder: an unknown session (the holder lookup failed 3 times)." in final
        assert "hourglass" not in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" not in result.stdout

    def test_a_lookup_failing_past_the_deadline_bows_out_without_sleeping(
        self, tmp_path: Path
    ) -> None:
        """An attempt that overran the wait leaves no time for a lookup retry; a
        negative sleep would fire the ERR trap on the instance, so none runs."""
        result = _run_harness([75], tmp_path, lookups=[ERROR], wait_minutes=5, pipeline_cost=400)
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stderr.count("holder lookup") == 1
        assert [ln for ln in result.stdout.splitlines() if ln.startswith("SLEEP")] == []
        final = next(ln for ln in result.stdout.splitlines() if ln.startswith("NOTIFY :no_entry:"))
        assert "the holder lookup failed" in final

    def test_lookup_retry_sleeps_count_against_the_wait_and_stop_at_the_deadline(
        self, tmp_path: Path
    ) -> None:
        # 1 min of wait, 45 s between lookups: lookup 1 fails, sleep 45;
        # lookup 2 fails, and the next sleep is clamped to the 15 s left.
        result = _run_harness(
            [75],
            tmp_path,
            lookups=[ERROR],
            wait_minutes=1,
            stubs=("REBUILD_LOCK_LOOKUP_RETRY_SECONDS=45",),
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert [ln for ln in result.stdout.splitlines() if ln.startswith("SLEEP")] == [
            "SLEEP 45",
            "SLEEP 15",
        ]
        assert result.stdout.count("PIPELINE attempt") == 1
        assert "the holder lookup failed 3 times" in result.stdout

    def test_a_lock_that_keeps_reading_free_still_ends_at_the_deadline(
        self, tmp_path: Path
    ) -> None:
        """75 then "free", over and over, must not spin: one immediate retry,
        then each retry sleeps and counts against the wait."""
        result = _run_harness([75], tmp_path, holders=[FREE], wait_minutes=10)
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stdout.count("PIPELINE attempt") == 4  # once at once, then t=0, 5, 10
        assert result.stdout.count("SLEEP 300") == 2
        final = next(ln for ln in result.stdout.splitlines() if ln.startswith("NOTIFY :no_entry:"))
        assert "could not take the lock in the whole 10-minute wait" in final
        assert "FELL THROUGH TO DRIFT WATCHDOG" not in result.stdout

    def test_a_quote_or_backslash_in_the_holder_name_keeps_the_slack_json_valid(
        self, tmp_path: Path
    ) -> None:
        result = _run_harness(
            [75], tmp_path, holders=['ops "ddl" \\x'], wait_minutes=5, real_notify=True
        )
        assert result.returncode == 0, result.stdout + result.stderr
        payloads = [
            ln.removeprefix("PAYLOAD ")
            for ln in result.stdout.splitlines()
            if ln.startswith("PAYLOAD ")
        ]
        assert len(payloads) == 2  # the hourglass post and the bow-out
        texts = [json.loads(p)["text"] for p in payloads]
        assert ":hourglass_flowing_sand:" in texts[0]
        assert "Holder at the deadline: ops ddl x;" in texts[1]

    def test_the_last_sleep_stops_at_the_deadline(self, tmp_path: Path) -> None:
        result = _run_harness([75], tmp_path, holders=["discogs-etl-seed"], wait_minutes=7)
        assert result.returncode == 0, result.stdout + result.stderr
        assert [ln for ln in result.stdout.splitlines() if ln.startswith("SLEEP")] == [
            "SLEEP 300",
            "SLEEP 120",
        ]
        assert "could not take the lock in the whole 7-minute wait" in result.stdout

    def test_time_spent_before_an_immediate_retry_counts_against_the_wait(
        self, tmp_path: Path
    ) -> None:
        """The free-lock retry skips the sleep, not the clock: the attempt and
        lookup before it still count, so a slow 75-then-free cycle cannot
        stretch the wait past its budget."""
        result = _run_harness(
            [75],
            tmp_path,
            holders=[FREE, "discogs-etl-seed"],
            wait_minutes=10,
            pipeline_cost=400,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        # 400 s before the immediate retry plus 400 s for the second attempt
        # already passes the 600 s budget, so nothing is left to sleep.
        assert [ln for ln in result.stdout.splitlines() if ln.startswith("SLEEP")] == ["SLEEP 0"]
        assert "NOTIFY :no_entry:" in result.stdout

    def test_time_spent_in_attempts_counts_against_the_wait(self, tmp_path: Path) -> None:
        """Slow attempts (connect retries) must not stretch the wait past its
        budget, which the instance-lifetime test relies on."""
        result = _run_harness(
            [75], tmp_path, holders=["discogs-etl-seed"], wait_minutes=10, pipeline_cost=400
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stdout.count("PIPELINE attempt") == 2
        assert [ln for ln in result.stdout.splitlines() if ln.startswith("SLEEP")] == ["SLEEP 200"]
        assert "NOTIFY :no_entry:" in result.stdout

    @pytest.mark.parametrize("wait_minutes", ["90m", "3h", " 5"])
    def test_a_wait_that_is_not_whole_minutes_falls_back_to_the_default(
        self, tmp_path: Path, wait_minutes: str
    ) -> None:
        """A bad value must not abort the script outside the ERR trap, which
        would leave no outcome marker and no Slack post."""
        result = _run_harness(
            [75, 0], tmp_path, holders=["discogs-etl-seed"], wait_minutes=wait_minutes
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "not a whole number of minutes" in result.stderr
        assert "SLEEP 300" in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" in result.stdout

    def test_a_leading_zero_is_read_as_decimal(self, tmp_path: Path) -> None:
        result = _run_harness([75], tmp_path, holders=["discogs-etl-seed"], wait_minutes="08")
        assert result.returncode == 0, result.stdout + result.stderr
        assert "could not take the lock in the whole 8-minute wait" in result.stdout

    def test_a_date_that_cannot_format_the_deadline_still_waits(self, tmp_path: Path) -> None:
        """The deadline is display-only; it must not fail the run."""
        failing_date = 'date() { case "$*" in *"%a"*) return 1 ;; *) command date "$@" ;; esac; }'
        result = _run_harness(
            [75, 0], tmp_path, holders=["discogs-etl-seed"], stubs=(failing_date,)
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "NOTIFY :warning:" not in result.stdout
        assert "until 180 min from now" in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" in result.stdout

    def test_a_crash_while_waiting_still_fails_loudly(self, tmp_path: Path) -> None:
        result = _run_harness([75, 1], tmp_path, holders=["discogs-etl-seed"])
        assert result.returncode == 1
        assert "NOTIFY :warning:" in result.stdout
        assert "FELL THROUGH TO DRIFT WATCHDOG" not in result.stdout

    def test_zero_minutes_restores_the_immediate_bow_out(self, tmp_path: Path) -> None:
        result = _run_harness([75], tmp_path, holders=["discogs-etl-seed"], wait_minutes=0)
        assert result.returncode == 0
        assert result.stdout.count("PIPELINE attempt") == 1
        assert "SLEEP" not in result.stdout
        assert "hourglass" not in result.stdout
        assert "Holder:" not in result.stdout, "no lookup is made when the wait is off"
        assert "NOTIFY :no_entry:" in result.stdout


WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "rebuild-cache.yml"
DEPLOY_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "deploy-ephemeral-rebuild.yml"


class TestLockWaitConstants:
    """The shell's literals for the wait (#465), pinned to the Python side."""

    def test_rebuild_holder_name_matches_python(self, script_text: str) -> None:
        code = "\n".join(_non_comment_lines(script_text.splitlines()))
        expected = _python_constant("REBUILD_LOCK_HOLDER_REBUILD")
        assert re.search(rf'REBUILD_LOCK_REBUILD_HOLDER="{re.escape(expected)}"', code)

    def test_free_exit_code_matches_python(self, script_text: str) -> None:
        code = "\n".join(_non_comment_lines(script_text.splitlines()))
        expected = _python_free_exit_code()
        assert re.search(rf"REBUILD_LOCK_HOLDER_FREE_EXIT_CODE={expected}\b", code)

    def test_wait_fits_inside_the_instance_lifetime(self, script_text: str) -> None:
        """The wait, about 30 min of setup and a 90-minute rebuild must finish
        before the sweeper's earliest kill (exactly MaxInstanceAgeHours), with
        an hour to spare for a rebuild that runs long. The deployed limit is
        the workflow's: SAM keeps a parameter's previous value unless it is
        passed, so the template default alone does not reach the stack."""
        default = re.search(r"REBUILD_LOCK_WAIT_MINUTES:-(\d+)", script_text)
        assert default is not None, "REBUILD_LOCK_WAIT_MINUTES needs a literal default"
        age = re.search(r"MaxInstanceAgeHours=(\d+)", DEPLOY_WORKFLOW_PATH.read_text())
        assert age is not None, f"no MaxInstanceAgeHours= override in {DEPLOY_WORKFLOW_PATH}"
        wait, max_age_minutes = int(default.group(1)), 60 * int(age.group(1))
        assert wait + 120 <= max_age_minutes - 60, (
            f"a {wait}-minute lock wait + 120 minutes of setup and rebuild must fit in the "
            f"{max_age_minutes}-minute instance limit with an hour of margin"
        )


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


class TestBowOutWordingIsHolderNeutral:
    """The lock (354001) is also taken by dedup, a real seed and operator
    sessions (#433, #454), so the bow-out text must not assert a peer rebuild
    holds it (#459)."""

    @pytest.mark.parametrize(
        "relpath",
        [
            "scripts/rebuild-cache.sh",
            "scripts/run_pipeline.py",
            ".github/workflows/rebuild-cache.yml",
        ],
    )
    def test_no_peer_only_claim(self, relpath: str) -> None:
        text = (REPO_ROOT / relpath).read_text().lower()
        assert "another rebuild holds the discogs-cache advisory lock" not in text
        assert "another rebuild holds the advisory lock" not in text
        assert "another rebuild already holds" not in text

    def test_shell_region_emits_every_holder(self, tmp_path: Path) -> None:
        stdout = _run_harness(_python_sentinel_value(), tmp_path).stdout.splitlines()
        _assert_names_every_holder(_only_line(stdout, r"NOTIFY :no_entry: "))
        _assert_names_every_holder(_only_line(stdout, r"\[[0-9:]+Z\] bowed out: "))
        # The outcome marker's detail is a short cause, not a list of holders.
        outcome = _only_line(stdout, r"OUTCOME bowed_out ").lower()
        assert "held by another session" in outcome
        assert "another rebuild" not in outcome

    def test_workflow_warning_names_every_holder(self) -> None:
        lines = WORKFLOW_PATH.read_text().splitlines()
        sentinel = _python_sentinel_value()
        branch_idx = next(i for i, line in enumerate(lines) if f"-eq {sentinel} ]" in line)
        _assert_names_every_holder(next(ln for ln in lines[branch_idx:] if "::warning::" in ln))

    def test_run_pipeline_warning_names_every_holder(self) -> None:
        tree = ast.parse((REPO_ROOT / "scripts" / "run_pipeline.py").read_text())
        templates = [
            node.args[0].value
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "warning"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
            and "bowing out" in node.args[0].value
        ]
        assert len(templates) == 1, f"expected one bow-out warning, found {templates!r}"
        _assert_names_every_holder(templates[0] % _python_lock_key())

    @pytest.mark.parametrize(
        "relpath",
        [
            "scripts/seed_cache_from_clone.py",
            "scripts/restore_api_fetch_from_export.py",
            "scripts/dedup_artist_children.py",
        ],
    )
    def test_holder_scripts_refuse_through_the_naming_helper(self, relpath: str) -> None:
        """These CLIs name the actual holder (lib.rebuild_lock.held_rebuild_lock, #490)
        instead of listing candidates, so no hand-written holder list can drift here."""
        tree = ast.parse((REPO_ROOT / relpath).read_text())
        called = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert "held_rebuild_lock" in called, f"{relpath} must refuse through held_rebuild_lock"
        assert "try_acquire_rebuild_lock" not in called, (
            f"{relpath} takes the lock by hand again; its refusal would need a holder list"
        )
        texts = [
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        ]
        assert not [t for t in texts if "operator session" in t], (
            f"{relpath} carries a hand-written holder list"
        )

    def test_automation_doc_quotes_the_slack_text(self, tmp_path: Path) -> None:
        """docs/automation.md's failure item 5 is found by searching Slack's
        text, so its heading must quote what the script posts today."""
        stdout = _run_harness(_python_sentinel_value(), tmp_path).stdout.splitlines()
        posted = _only_line(stdout, r"NOTIFY :no_entry: ").removeprefix("NOTIFY ")
        quoted = posted.split("; no cache write")[0]
        doc = (REPO_ROOT / "docs" / "automation.md").read_text()
        assert f"**Slack posts `{quoted}`**" in doc


# How each REBUILD_LOCK_HOLDER_* constant in lib/rebuild_lock.py reads in a
# bow-out message. test_every_holder_constant_has_a_phrase keeps this complete,
# so a new holder cannot be registered without the messages being made to name it.
_HOLDER_PHRASES = {
    "REBUILD_LOCK_HOLDER_REBUILD": "a rebuild",
    "REBUILD_LOCK_HOLDER_DEDUP": "a dedup",
    "REBUILD_LOCK_HOLDER_SEED": "a seed",
    "REBUILD_LOCK_HOLDER_RESTORE_API_FETCH": "an api_fetch export restore",
}
# Every registered 354001 holder: one phrase per constant, plus an operator
# session holding the lock by hand (which has no constant).
_LOCK_HOLDERS = (*_HOLDER_PHRASES.values(), "an operator session")


def _holder_constant_names() -> set[str]:
    tree = ast.parse(LOCK_MODULE_PATH.read_text())
    return {
        target.id
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
        for target in node.targets
        if isinstance(target, ast.Name) and target.id.startswith("REBUILD_LOCK_HOLDER_")
    }


def test_every_holder_constant_has_a_phrase() -> None:
    assert _holder_constant_names() == set(_HOLDER_PHRASES)


def _python_lock_key() -> int:
    match = re.search(r"^REBUILD_LOCK_KEY\s*=\s*(\d+)", LOCK_MODULE_PATH.read_text(), re.M)
    assert match is not None, f"could not find REBUILD_LOCK_KEY = <int> in {LOCK_MODULE_PATH}"
    return int(match.group(1))


def _only_line(lines: list[str], prefix_pattern: str) -> str:
    matches = [line for line in lines if re.match(prefix_pattern, line)]
    assert len(matches) == 1, f"expected one line matching {prefix_pattern!r}, got {lines!r}"
    return matches[0]


def _assert_names_every_holder(message: str) -> None:
    lowered = message.lower()
    assert str(_python_lock_key()) in message, f"must name the lock key: {message!r}"
    assert "another rebuild" not in lowered, f"must not claim a peer rebuild: {message!r}"
    assert "held by another session" in lowered, message
    _assert_names_every_phrase(message)


def _assert_names_every_phrase(message: str) -> None:
    missing = [holder for holder in _LOCK_HOLDERS if holder not in message.lower()]
    assert not missing, f"bow-out text omits {missing}: {message!r}"
