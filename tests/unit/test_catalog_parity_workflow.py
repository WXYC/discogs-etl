"""Wiring tests for ``.github/workflows/catalog-parity.yml`` (discogs-etl#378).

The scheduled soak is the consumer that gives ``catalog_parity_diff.py
--fail-on-drift``'s exit 4 a meaning, and it is the mechanism behind
[wiki#89](https://github.com/WXYC/wiki/issues/89) AC#4 -- seven consecutive
clean parity days before the 2026-08-31 tubafrenzy turndown. Nobody watches a
workflow that runs at 09:37 UTC daily, so the ways it can rot silently are
what this file pins:

1. **The artifact must survive a failing run.** Exit 4 is the *interesting*
   case -- it is what every run produces until BS#2108 and the 28 residual
   field mismatches clear. If the upload step is skipped on failure (the
   default), the only days with an audit trail are the days nobody needs to
   audit.
2. **The exit code must reach the summary renderer.** GitHub aborts the job
   at the first failing step, so the harness step has to capture its own
   status rather than fail on it -- otherwise the taxonomy in
   ``parity_run_summary.py`` never runs and exit 4 renders as a bare red X.
3. **The run must not collide with ``sync-library.yml``.** Both scan the same
   Kattare MySQL, whose HikariCP pool maxes at 5 connections; overlapping
   full-catalog scans are how that host wedges (2026-05-24). The margin is
   checked against this workflow's own ``timeout-minutes``, so raising the
   timeout past the gap fails here instead of in production.
4. **The MySQL password must never enter the DSN.** The DSN reaches the
   harness on argv, visible to ``ps`` for the whole run and echoed by any
   ``set -x``. ``$LIBRARY_DB_PASSWORD`` -> ``MYSQL_PWD`` is the only path.
5. **The scratch paths must be fresh.** The harness refuses to overwrite an
   existing ``--*-db`` by design, so a cached scratch directory turns every
   run after the first into exit 3.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "catalog-parity.yml"
SYNC_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "sync-library.yml"


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def _triggers(doc: dict[str, Any]) -> dict[str, Any]:
    """The ``on:`` block.

    YAML 1.1 -- which PyYAML implements -- resolves an unquoted ``on`` key to
    the boolean ``True``, so ``doc["on"]`` is a KeyError on every GitHub
    Actions file that spells the key the normal way.
    """
    return doc.get("on", doc.get(True))  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def workflow() -> dict[str, Any]:
    assert WORKFLOW_PATH.exists(), f"{WORKFLOW_PATH.relative_to(REPO_ROOT)} does not exist"
    return _load(WORKFLOW_PATH)


@pytest.fixture(scope="module")
def source() -> str:
    return WORKFLOW_PATH.read_text()


@pytest.fixture(scope="module")
def job(workflow: dict[str, Any]) -> dict[str, Any]:
    jobs = workflow["jobs"]
    assert len(jobs) == 1, "expected exactly one job in the parity soak"
    return next(iter(jobs.values()))


def _steps(job: dict[str, Any]) -> list[dict[str, Any]]:
    return job["steps"]


def _step(job: dict[str, Any], needle: str) -> dict[str, Any]:
    for step in _steps(job):
        if needle.lower() in str(step.get("name", "")).lower():
            return step
        if needle.lower() in str(step.get("uses", "")).lower():
            return step
    raise AssertionError(
        f"no step matching {needle!r}; have {[s.get('name') for s in _steps(job)]}"
    )


def _cron_minutes(cron: str) -> int:
    """Minutes past midnight UTC for a daily ``M H * * *`` expression."""
    minute, hour, dom, month, dow = cron.split()
    assert (dom, month, dow) == ("*", "*", "*"), f"expected a daily cron, got {cron!r}"
    return int(hour) * 60 + int(minute)


class TestTriggers:
    def test_runs_on_a_schedule_and_on_demand(self, workflow) -> None:
        triggers = _triggers(workflow)
        assert "schedule" in triggers
        assert "workflow_dispatch" in triggers

    def test_exactly_one_schedule_entry(self, workflow) -> None:
        """Two crons against the same Kattare MySQL is the #353 shape."""
        assert len(_triggers(workflow)["schedule"]) == 1

    def test_run_finishes_before_sync_library_starts(self, workflow, job) -> None:
        parity = _cron_minutes(_triggers(workflow)["schedule"][0]["cron"])
        sync = _cron_minutes(_triggers(_load(SYNC_WORKFLOW_PATH))["schedule"][0]["cron"])

        timeout = job.get("timeout-minutes")
        assert timeout, "the job needs timeout-minutes for this margin to mean anything"

        gap = (sync - parity) % (24 * 60)
        assert gap > timeout, (
            f"the parity soak starts {gap} min before sync-library but may run for "
            f"{timeout} min; overlapping full-catalog scans wedge Kattare's 5-connection pool"
        )

    def test_job_is_time_bounded(self, job) -> None:
        """A wedged tunnel would otherwise burn the 6-hour runner default."""
        assert 0 < job["timeout-minutes"] <= 60

    def test_runs_are_serialized(self, workflow) -> None:
        """A manual dispatch during the scheduled run is two concurrent
        full-catalog scans against the same MySQL."""
        concurrency = workflow.get("concurrency")
        assert concurrency, "the workflow needs a concurrency group"
        assert concurrency.get("cancel-in-progress") is not True, (
            "cancelling mid-run leaves the SSH tunnel and MySQL scan orphaned"
        )


class TestPermissions:
    def test_token_scope_is_read_only(self, workflow) -> None:
        """This workflow pushes nothing. `contents: read` is the floor and
        the ceiling -- no id-token, no packages, no contents: write."""
        assert workflow["permissions"] == {"contents": "read"}


class TestKattareAccess:
    def test_installs_the_mariadb_client(self, source) -> None:
        """The Homebrew/Oracle MySQL 9.x client segfaults (exit 139) against
        tubafrenzy's MySQL 5.1.56; mariadb-client is what works."""
        assert "mariadb-client" in source

    def test_tunnel_failure_is_not_silent(self, source) -> None:
        """``ssh -f -N -L`` backgrounds successfully even when the local
        forward cannot bind; the mysql client then connects to nothing and
        the run fails with a confusing producer error instead of a tunnel
        error. ExitOnForwardFailure is what makes the tunnel step fail."""
        assert "ExitOnForwardFailure=yes" in source

    def test_host_key_is_pinned_by_keyscan(self, source) -> None:
        assert "ssh-keyscan" in source


class TestHarnessInvocation:
    @pytest.fixture(scope="class")
    def run_step(self, job) -> dict[str, Any]:
        return _step(job, "Run catalog parity")

    def test_passes_fail_on_drift(self, run_step) -> None:
        assert "--fail-on-drift" in run_step["run"]

    def test_emits_json(self, run_step) -> None:
        assert "--json" in run_step["run"]

    def test_builds_both_sides_from_live_sources(self, run_step) -> None:
        assert "--mysql-source" in run_step["run"]
        assert "--backend-source" in run_step["run"]

    def test_residue_ledger_is_left_at_its_default(self, run_step) -> None:
        """The default resolves relative to the script, not the cwd, and is
        the pinned vendored ledger. Passing 'none' would also make
        --fail-on-drift exit 2."""
        assert "--residue-ledger" not in run_step["run"]

    def test_scratch_paths_are_fresh_per_run(self, run_step, source) -> None:
        """``_require_absent`` refuses to overwrite either --*-db, so a
        cached or repo-relative path bricks every run after the first."""
        assert "mktemp -d" in run_step["run"]
        assert "actions/cache" not in source

    def test_mysql_password_is_not_in_the_dsn(self, run_step) -> None:
        """The DSN is argv: visible to ps, echoed by set -x. The password
        goes through $LIBRARY_DB_PASSWORD, which the harness reads into
        MYSQL_PWD."""
        run = run_step["run"]
        assert "mysql://" in run
        dsn_line = next(line for line in run.splitlines() if "mysql://" in line)
        assert "PASSWORD" not in dsn_line.upper(), f"password material in the DSN: {dsn_line!r}"
        assert "LIBRARY_DB_PASSWORD" in run_step["env"]

    def test_exit_code_is_captured_not_raised(self, run_step) -> None:
        """GitHub skips every later step once one fails. If this step fails
        on exit 4, the artifact never uploads and the taxonomy never renders
        -- the two things that make a red run legible."""
        run = run_step["run"]
        assert "set +e" in run, "the harness invocation must not abort the step"
        assert "$?" in run, "the exit code must be captured to a sentinel"
        assert "GITHUB_ENV" in run, "the sentinel must outlive the step"


class TestFailureLegibility:
    def test_report_step_runs_even_when_earlier_steps_failed(self, job) -> None:
        assert _step(job, "parity verdict").get("if") == "always()"

    def test_report_step_renders_through_the_summary_script(self, job) -> None:
        run = _step(job, "parity verdict")["run"]
        assert "scripts/parity_run_summary.py" in run
        assert "GITHUB_STEP_SUMMARY" in run

    def test_report_step_receives_the_exit_code(self, job) -> None:
        assert "--exit-code" in _step(job, "parity verdict")["run"]

    def test_summary_script_exists(self) -> None:
        assert (REPO_ROOT / "scripts" / "parity_run_summary.py").exists()


class TestArtifact:
    @pytest.fixture(scope="class")
    def upload(self, job) -> dict[str, Any]:
        return _step(job, "upload-artifact")

    def test_uploads_on_a_failing_run(self, upload) -> None:
        """Exit 4 is the case worth auditing; the default would skip it."""
        assert upload.get("if") == "always()"

    def test_retention_covers_a_seven_day_streak(self, upload) -> None:
        assert int(upload["with"]["retention-days"]) >= 14

    def test_uploads_the_json_verdict(self, upload) -> None:
        assert ".json" in upload["with"]["path"]

    def test_a_missing_report_does_not_mask_the_real_failure(self, upload) -> None:
        """upload-artifact v4 fails the step when nothing matches. On exit 2
        or 3 there may be no readable report, and that failure would bury
        the producer error that actually needs reading."""
        assert upload["with"].get("if-no-files-found") in {"warn", "ignore"}


class TestDocumentation:
    def test_automation_docs_no_longer_call_the_secrets_unwired(self) -> None:
        """docs/automation.md's 'Unwired secrets (catalog parity)' section
        stops being true the moment this workflow merges (#378 AC)."""
        automation = (REPO_ROOT / "docs" / "automation.md").read_text()
        assert "Unwired secrets" not in automation

    def test_the_parity_secrets_are_documented(self) -> None:
        automation = (REPO_ROOT / "docs" / "automation.md").read_text()
        assert "BACKEND_CATALOG_EMAIL" in automation
        assert "BACKEND_CATALOG_PASSWORD" in automation
