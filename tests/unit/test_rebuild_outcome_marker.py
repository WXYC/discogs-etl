"""Pin the rebuild's terminal-outcome marker (discogs-etl#424, criterion 3).

On 2026-09-04 the monthly rebuild aborted at 06:48:41 UTC and nobody learned
for 14 days. Every alerting path was open-circuit at once:
``SLACK_MONITORING_WEBHOOK`` was unset, so every ``notify_slack`` call in
``rebuild-cache.sh`` was a bare ``return 0``; the drift watchdog never ran
because the pipeline died before it; and the only durable traces were three
Sentry issues raised by the *pipeline's* own Python, which no alert rule
watched. A successful run and an aborted one left the same artifacts behind.

``scripts/report_rebuild_outcome.py`` closes that by writing a
``99-outcome.txt`` marker into the log directory the bootstrap already syncs to
S3, so the outcome is legible from a bucket *listing*. Three properties make the
marker trustworthy, and all three are pinned here:

1. **Every terminal path reports, and the canonical marker only ever describes
   the run holding the lock.** The lock holder stamps ``outcome=running`` on
   acquiring, so the readings are total: no marker means no run ever acquired
   the lock; ``running`` means one did and never reached a terminal path
   (SIGKILL, OOM, host loss); anything else is that run's real outcome. A
   bystander tick writes its own distinctly-named file, because ``$LOG_DIR`` and
   the bootstrap's S3 prefix are shared with the live run -- see
   ``TestMarkerCannotBeForgedByABystanderTick``.
2. **The marker never depends on the venv, and never on the logger working.**
   The import is deferred AND the ``init_logger`` call is inside the same
   ``try``: a valid-looking but unreachable ``SENTRY_DSN`` raises from the call,
   not the import, and letting that escape would write no marker at all while
   ``report_outcome``'s ``|| true`` hid the failure. The flock bow-out fires at
   ``LOCK_FD=200`` / ``flock -n``, roughly 20 lines ABOVE the
   ``source "$REPO_DIR/.venv/bin/activate"`` in step 1, so at that point
   ``lib.observability`` (which pulls in ``wxyc_etl`` and ``sentry_sdk``) is not
   importable. If the reporter died on that import, the earliest terminal path
   in the script -- a same-host double-tick, the single most likely benign
   bow-out -- would be exactly the one that reports nothing, and would read as
   a lost host. The marker write is stdlib-only and must survive an
   unimportable logger; only the Sentry leg is lost, and a bow-out logs at INFO
   there anyway, raising nothing.
3. **The marker and the logger are independent channels.** Neither failing may
   take the other down, so an unwritable ``$LOG_DIR`` still raises its own
   ERROR rather than losing both signals behind ``|| true``.

Test layers follow the convention of this file's siblings
(``test_rebuild_cache_flock_bowout.py``, ``test_rebuild_cache_lock_bowout.py``):
static-structural assertions over the script text, plus a layer that executes a
region under a real bash. That second layer is not belt-and-braces here for the
reason #354's tests document -- ``set +e`` suppresses errexit but NOT the ERR
trap, so a shell change can satisfy every structural assertion while being
unreachable dead code. Text cannot see that; execution can.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache.sh"
REPORTER_PATH = REPO_ROOT / "scripts" / "report_rebuild_outcome.py"

sys.path.insert(0, str(REPO_ROOT))

from scripts.report_rebuild_outcome import (  # noqa: E402
    FAILURE_STATUSES,
    OUTCOME_FILENAME,
    STATUSES,
    main,
    render_outcome,
    run,
)


@pytest.fixture
def script_text() -> str:
    return SCRIPT_PATH.read_text()


@pytest.fixture
def script_lines() -> list[str]:
    return SCRIPT_PATH.read_text().splitlines()


def _index_of(lines: list[str], needle: str) -> int:
    for i, line in enumerate(lines):
        if needle in line:
            return i
    raise AssertionError(f"{needle!r} not found in {SCRIPT_PATH}")


# ---------------------------------------------------------------------------
# The reporter itself
# ---------------------------------------------------------------------------


class TestStatusTaxonomy:
    def test_bowed_out_is_not_a_failure(self) -> None:
        """A bow-out is a CORRECT no-op run (a peer held the flock or the #354
        advisory lock), not a failure. Incident #352 is the standing reminder
        that conflating the two is what makes a silent non-rebuild look like a
        success -- and the inverse, paging on a benign double-tick, is how an
        alert gets muted and stops working for the case it was built for."""
        assert "bowed_out" in STATUSES
        assert "bowed_out" not in FAILURE_STATUSES

    def test_smoke_ok_is_a_distinct_non_failure_status(self) -> None:
        """``REBUILD_SMOKE=1`` exits 0 before writing anything to the cache, so
        it is neither a rebuild that succeeded nor one that failed. It still
        has to report: absence of the marker is how a lost host is detected, so
        a silent smoke run would forge that signal."""
        assert "smoke_ok" in STATUSES
        assert "smoke_ok" not in FAILURE_STATUSES

    def test_failed_is_the_only_failure_status(self) -> None:
        assert FAILURE_STATUSES == ("failed",)


class TestMarkerBody:
    def test_body_is_greppable_key_value_lines(self) -> None:
        body = render_outcome(
            status="failed",
            detail="line 445 (exit 1)",
            run_log="/log/x.log",
            utc_now="2026-10-04T06:48:41Z",
        )
        assert body.endswith("\n")
        assert "outcome=failed" in body
        assert "utc=2026-10-04T06:48:41Z" in body
        assert "detail=line 445 (exit 1)" in body
        assert "log=/log/x.log" in body

    def test_optional_fields_are_omitted_not_blank(self) -> None:
        body = render_outcome(
            status="success", detail="", run_log="", utc_now="2026-10-04T07:10:00Z"
        )
        assert "detail=" not in body
        assert "log=" not in body


class TestLogLevelByStatus:
    """A failure must log at ERROR so ``lib.observability`` forwards it to
    Sentry and an alert rule can target ``step=rebuild_outcome``; a non-failure
    must log at INFO and raise nothing, or the alert is noise from day one."""

    def test_failure_logs_at_error(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO):
            run(status="failed", detail="boom", log_dir=tmp_path, run_log="")
        assert [r.levelno for r in caplog.records] == [logging.ERROR]

    @pytest.mark.parametrize("status", ["success", "bowed_out", "smoke_ok"])
    def test_non_failure_logs_at_info(
        self, status: str, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO):
            run(status=status, detail="", log_dir=tmp_path, run_log="")
        assert [r.levelno for r in caplog.records] == [logging.INFO]


class TestMarkerSurvivesAnUnimportableLogger:
    """The venv-independence property, at the level that matters: ``main()``.

    ``run()`` is stdlib-only and trivially survives; the risk is ``main()``
    dying on the logger import BEFORE it ever calls ``run()``, which is exactly
    what would happen on the pre-venv flock bow-out path.
    """

    def test_main_writes_the_marker_when_the_logger_cannot_be_imported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # ``None`` in sys.modules makes ``from lib.observability import ...``
        # raise ImportError, which is the shape an absent venv produces (the
        # real failure is wxyc_etl/sentry_sdk missing, one layer down).
        monkeypatch.setitem(sys.modules, "lib.observability", None)
        rc = main(["--status", "bowed_out", "--log-dir", str(tmp_path)])
        assert rc == 0, "a missing logger must not fail the reporter"
        marker = tmp_path / OUTCOME_FILENAME
        assert marker.exists(), (
            "the marker write is stdlib-only and must not depend on the venv; "
            "the flock bow-out fires ~20 lines above the venv activation"
        )
        assert "outcome=bowed_out" in marker.read_text()

    def test_main_writes_the_marker_when_init_logger_itself_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Guarding the IMPORT is not enough — the call has to be guarded too.

        A malformed or unreachable ``SENTRY_DSN``, a ``sentry_sdk.init``
        failure, or any error inside ``wxyc_etl.logger`` raises from
        ``init_logger`` itself, on a host where the import succeeds perfectly.
        If that escapes, ``main()`` dies before ``run()`` and NO marker is
        written -- while ``report_outcome``'s ``|| true`` in rebuild-cache.sh
        swallows the non-zero exit. The S3 listing would then show a run that
        terminated cleanly as "never reached a terminal path (SIGKILL, host
        loss)": the reporter forging the exact contract it exists to provide.
        """
        fake = types.ModuleType("lib.observability")

        def boom(**_kwargs: object) -> None:
            raise RuntimeError("Invalid Sentry DSN")

        fake.init_logger = boom  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "lib.observability", fake)
        rc = main(["--status", "failed", "--detail", "line 445", "--log-dir", str(tmp_path)])
        assert rc == 0, "a broken logger must not fail the reporter"
        marker = tmp_path / OUTCOME_FILENAME
        assert marker.exists(), (
            "the marker must survive init_logger raising, not just failing to import"
        )
        assert "outcome=failed" in marker.read_text()


# ---------------------------------------------------------------------------
# Static-structural: the shell wiring
# ---------------------------------------------------------------------------


class TestEveryTerminalPathReports:
    """Absence of a marker means "never reached a terminal path". That reading
    is only sound if every terminal path reports, so each one is pinned by
    name. A new terminal path added without a report_outcome call silently
    degrades the signal for every other path."""

    def test_helper_is_defined_above_the_first_terminal_path(self, script_lines: list[str]) -> None:
        helper = _index_of(script_lines, "report_outcome() {")
        first_terminal = _index_of(script_lines, "LOCK_FD=200")
        assert helper < first_terminal, (
            "report_outcome() must be defined before the flock bow-out, the "
            "earliest terminal path; calling an undefined function under "
            "set -u/-e is unreachable dead code (the #267 defect class)"
        )

    def test_err_trap_reports_failed(self, script_text: str) -> None:
        body = re.search(r"^on_error\(\) \{(.*?)^\}", script_text, re.S | re.M)
        assert body is not None, "on_error() not found"
        assert "report_outcome failed" in body.group(1)

    def test_fail_helper_reports_failed(self, script_text: str) -> None:
        body = re.search(r"^fail\(\) \{(.*?)^\}", script_text, re.S | re.M)
        assert body is not None, "fail() not found"
        assert "report_outcome failed" in body.group(1)

    def test_flock_bowout_reports_bowed_out(self, script_lines: list[str]) -> None:
        start = _index_of(script_lines, "LOCK_FD=200")
        end = next(i for i in range(start, len(script_lines)) if script_lines[i].strip() == "fi")
        region = "\n".join(script_lines[start : end + 1])
        assert "report_outcome bowed_out" in region

    def test_advisory_lock_bowout_reports_bowed_out(self, script_text: str) -> None:
        assert re.search(
            r"REBUILD_LOCK_BOWED_OUT_EXIT_CODE.*?report_outcome bowed_out", script_text, re.S
        ), "the #354 advisory-lock bow-out branch must report bowed_out"

    def test_smoke_exit_reports_smoke_ok(self, script_text: str) -> None:
        assert re.search(
            r"smoke test passed.*?report_outcome smoke_ok", script_text, re.S
        ) or re.search(r"report_outcome smoke_ok.*?smoke test passed", script_text, re.S), (
            "the REBUILD_SMOKE terminal path must report smoke_ok, not stay "
            "silent and not claim success"
        )

    def test_success_path_reports_success(self, script_text: str) -> None:
        assert "report_outcome success" in script_text


class TestMarkerCannotBeForgedByABystanderTick:
    """$LOG_DIR is shared with whichever run currently holds the lock.

    The flock is same-host only and LOCK_FILE defaults under $LOG_DIR, so a
    second cron tick landing on a live rebuild shares that directory AND the
    bootstrap's S3 prefix. If the bystander wrote the canonical marker, then a
    later SIGKILL of the real run (OOM, host loss) would leave the bystander's
    ``outcome=bowed_out`` as the newest thing the ``trap EXIT`` sync uploads --
    a lost host reading as a benign no-op, which is precisely the false-signal
    class incident #352 exists to eliminate.

    Two rules close it, and together they strengthen the contract rather than
    just patching the hole:

    * a bystander writes its own distinctly-named marker and never the
      canonical one;
    * the run that DOES own the lock stamps ``outcome=running`` immediately on
      acquiring it, so the canonical marker always describes the lock holder.

    The resulting readings are total: no canonical marker means no run ever
    acquired the lock; ``running`` means one did and never reached a terminal
    path; anything else is that run's real outcome. The stamp also overwrites
    any stale marker from a previous run, which the log trim at the end of the
    script would not have removed -- it matches only ``*.log``.
    """

    def test_running_is_a_valid_non_failure_status(self) -> None:
        assert "running" in STATUSES
        assert "running" not in FAILURE_STATUSES

    def test_marker_name_is_overridable(self, tmp_path: Path) -> None:
        rc = main(
            ["--status", "bowed_out", "--log-dir", str(tmp_path), "--marker-name", "99-bowout.txt"]
        )
        assert rc == 0
        assert (tmp_path / "99-bowout.txt").exists()
        assert not (tmp_path / OUTCOME_FILENAME).exists(), (
            "a bystander must not write the canonical marker"
        )

    def test_flock_bowout_writes_a_distinct_marker_name(self, script_lines: list[str]) -> None:
        start = _index_of(script_lines, "LOCK_FD=200")
        end = next(i for i in range(start, len(script_lines)) if script_lines[i].strip() == "fi")
        region = "\n".join(script_lines[start : end + 1])
        assert "report_outcome bowed_out" in region
        assert OUTCOME_FILENAME not in region, (
            "the bow-out must pass a marker name that is NOT the canonical one"
        )
        assert re.search(r"report_outcome bowed_out [^\n]*99-", region), (
            "the bow-out must pass an explicit, distinct marker name as its 3rd arg"
        )

    def test_lock_holder_stamps_running_after_acquiring(self, script_lines: list[str]) -> None:
        lock_end = next(
            i
            for i in range(_index_of(script_lines, "LOCK_FD=200"), len(script_lines))
            if script_lines[i].strip() == "fi"
        )
        after = "\n".join(script_lines[lock_end : lock_end + 12])
        assert "report_outcome running" in after, (
            "the lock holder must stamp outcome=running immediately after "
            "acquiring, so the canonical marker always describes it -- and so a "
            "stale marker from a previous run is overwritten"
        )


class TestReportingCannotBreakTheRebuild:
    def test_helper_body_swallows_its_own_failure(self, script_text: str) -> None:
        """The reporter runs python. Under ``set -e`` plus the ERR trap, a
        reporter that exits non-zero inside ``on_error`` would re-enter
        ``on_error`` -- an infinite mutual recursion on the one path that most
        needs to terminate. The helper must swallow its own failure."""
        body = re.search(r"^report_outcome\(\) \{(.*?)^\}", script_text, re.S | re.M)
        assert body is not None, "report_outcome() not found"
        assert "|| true" in body.group(1), (
            "report_outcome must not be able to fail its caller; on_error "
            "calls it, and a failure there would recurse into on_error"
        )

    def test_helper_uses_python3_not_python(self, script_text: str) -> None:
        """``python`` does not exist on the rebuild AMI before
        ``.venv/bin/activate`` runs, and the flock bow-out precedes it.
        ``python3`` resolves both before activation (system) and after (the
        venv puts its own python3 first on PATH)."""
        body = re.search(r"^report_outcome\(\) \{(.*?)^\}", script_text, re.S | re.M)
        assert body is not None
        assert re.search(r"\bpython3\b", body.group(1)), "must invoke python3"
        assert not re.search(r"(?<!3)\bpython\b(?!3)", body.group(1)), (
            "bare `python` is absent from the AMI pre-venv"
        )


# ---------------------------------------------------------------------------
# Execution layer: the region actually runs and actually writes the marker
# ---------------------------------------------------------------------------


def _extract_notify_slack_through_flock_block(lines: list[str]) -> str:
    start = _index_of(lines, "notify_slack() {")
    lock_fd_idx = _index_of(lines, "LOCK_FD=200")
    assert lock_fd_idx > start
    end = next(i for i in range(lock_fd_idx, len(lines)) if lines[i].strip() == "fi")
    return "\n".join(lines[start : end + 1])


def _build_harness(flock_rc: int, tmp_path: Path) -> str:
    region = _extract_notify_slack_through_flock_block(SCRIPT_PATH.read_text().splitlines())
    return "\n".join(
        [
            "set -euo pipefail",
            f'LOG_FILE="{tmp_path / "run.log"}"',
            f'LOG_DIR="{tmp_path}"',
            f'LOCK_FILE="{tmp_path / "discogs-rebuild.lock"}"',
            f'REPO_DIR="{REPO_ROOT}"',
            'SLACK_MONITORING_WEBHOOK="http://stub-webhook.invalid"',
            f"FLOCK_RC={flock_rc}",
            'flock() { return "$FLOCK_RC"; }',
            'curl() { echo "CURL_CALLED $*"; return 0; }',
            # Stub python3 to record the argv the helper would invoke, and to
            # write the marker itself -- so this layer proves the SHELL reaches
            # the reporter with the right status, independent of the reporter's
            # own (separately tested) behavior.
            'python3() { echo "PYTHON3_CALLED $*"; }',
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


class TestFlockBowOutActuallyReports:
    def test_contended_lock_reports_bowed_out_and_exits_zero(self, tmp_path: Path) -> None:
        result = _run_harness(flock_rc=1, tmp_path=tmp_path)
        assert result.returncode == 0, (
            f"a same-host bow-out is not a failure.\n{result.stdout}\n{result.stderr}"
        )
        assert "PYTHON3_CALLED" in result.stdout, (
            "the flock bow-out must reach report_outcome. If this is missing, "
            "the helper is defined below this block and the call is dead code."
            f"\n{result.stdout}\n{result.stderr}"
        )
        assert "--status bowed_out" in result.stdout, (
            f"must report bowed_out, not success or failed.\n{result.stdout}"
        )
        assert "FELL THROUGH PAST LOCK GUARD" not in result.stdout

    def test_uncontended_lock_reports_nothing(self, tmp_path: Path) -> None:
        result = _run_harness(flock_rc=0, tmp_path=tmp_path)
        assert result.returncode == 0
        assert "FELL THROUGH PAST LOCK GUARD" in result.stdout
        assert "PYTHON3_CALLED" not in result.stdout, (
            "an uncontended lock is not a terminal path and must not report"
        )


class TestReporterIsExecutableStandalone:
    """End-to-end of the venv-independence property, as a real subprocess.

    Running the reporter in place would prove nothing: ``sys.executable`` is the
    venv, so ``lib.observability`` imports fine and the deferred-import path is
    never taken. So the reporter is COPIED to a directory whose parent holds no
    ``lib`` package at all -- which is what ``sys.path.insert(0, parent.parent)``
    then finds, making the import genuinely fail the way an un-provisioned host
    does. The marker is still required.
    """

    def _run_copy(
        self, tmp_path: Path, *args: str
    ) -> tuple[subprocess.CompletedProcess[str], Path]:
        sandbox = tmp_path / "opt" / "scripts"
        sandbox.mkdir(parents=True)
        shutil.copy(REPORTER_PATH, sandbox / REPORTER_PATH.name)
        log_dir = tmp_path / "logs"
        result = subprocess.run(
            [sys.executable, str(sandbox / REPORTER_PATH.name), "--log-dir", str(log_dir), *args],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=tmp_path,
        )
        return result, log_dir / OUTCOME_FILENAME

    def test_marker_is_written_with_no_lib_package_importable(self, tmp_path: Path) -> None:
        result, marker = self._run_copy(
            tmp_path, "--status", "failed", "--detail", "line 445 (exit 1)"
        )
        assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        assert marker.exists(), (
            "the marker must survive an unimportable logger; the flock bow-out "
            f"reports from above the venv activation.\nstderr:\n{result.stderr}"
        )
        body = marker.read_text()
        assert "outcome=failed" in body
        assert "detail=line 445 (exit 1)" in body

    def test_marker_only_mode_says_so_on_stderr(self, tmp_path: Path) -> None:
        """Silence here would be its own trap: an operator reading a marker with
        no Sentry issue beside it needs to know the Sentry leg was never wired,
        rather than concluding the alert rule is broken."""
        result, _ = self._run_copy(tmp_path, "--status", "bowed_out")
        assert "logger unavailable" in result.stderr, (
            f"expected a marker-only notice on stderr.\nstderr:\n{result.stderr}"
        )

    def test_log_dir_is_created_when_absent(self, tmp_path: Path) -> None:
        """The reporter may be the first thing to touch LOG_DIR on a host whose
        provisioning died early."""
        _, marker = self._run_copy(tmp_path, "--status", "failed")
        assert marker.exists()
