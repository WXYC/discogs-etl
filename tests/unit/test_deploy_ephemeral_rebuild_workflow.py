"""Wiring tests for ``.github/workflows/deploy-ephemeral-rebuild.yml`` (#396).

This workflow's green history is not, on its own, evidence that the deploy
path works. It runs ``sam deploy --no-fail-on-empty-changeset``, so SAM exits 0
both when it applies a changeset and when it finds nothing to do -- and for
months every run was the second while the deploy role lacked
``ec2:DescribeImages`` and could not have applied anything. The condition was
only discovered because #358's ``ReleaseCountAlarm`` happened to be the first
real changeset in that window, and it failed loudly.

The flag has to stay: three of the four triggers legitimately produce no
template diff (``scripts/rebuild-cache-bootstrap.sh`` is fetched by the
instance at runtime and is not in the rendered template, the workflow file is
not either, and a ``workflow_dispatch`` on an already-deployed ``main`` is a
no-op by construction). What changes is that the outcome is now *stated*.

What this file pins is the plumbing that makes the statement possible, all of
which is silently breakable:

1. **SAM's own exit status must survive the pipe.** ``sam deploy | tee`` makes
   ``$?`` ``tee``'s status, which is 0 essentially always. Reading ``$?`` here
   would report every failed deploy as a success -- a strictly worse version of
   the bug being fixed. ``${PIPESTATUS[0]}`` is the only correct read.
2. **A failing deploy must not abort the job at that step.** GitHub skips every
   later step once one fails, so failing in place would discard the summary
   that explains what happened. The report step re-raises the code.
3. **The report step must run when the deploy step did not.** An earlier
   failure (SAM build, credentials) leaves no exit code behind, and "nothing
   was deployed" is still the honest summary for that.
4. **The renderer must be importable without a ``pip install``.** The deploy
   job installs nothing; a renderer that imported a third-party package would
   traceback in the step whose job is to explain other failures.

These are static-structural assertions, which per #397 may pin presence and
ordering but cannot make reachability claims. All five outcomes -- and the
``PIPESTATUS`` read specifically, against a stubbed ``sam`` exiting non-zero
under ``bash -e`` -- were verified by execution while this was written; that
coverage belongs in CI on ``tests/shell_harness.py`` once #397 lands, rather
than in a third private copy of the harness #397 exists to consolidate.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "deploy-ephemeral-rebuild.yml"
RENDERER_PATH = REPO_ROOT / "scripts" / "sam_deploy_summary.py"


def _load() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW_PATH.read_text())


WORKFLOW = _load()


def _steps(job: str) -> list[dict[str, Any]]:
    return WORKFLOW["jobs"][job]["steps"]


def _step(job: str, name: str) -> dict[str, Any]:
    for step in _steps(job):
        if step.get("name") == name:
            return step
    raise AssertionError(f"no step named {name!r} in job {job!r}")


DEPLOY_STEP_NAME = "SAM deploy"
REPORT_STEP_NAME = "Report deploy outcome"


class TestTheDeployStepCapturesItsOutcome:
    def test_the_empty_changeset_flag_is_still_there(self) -> None:
        """Pinned deliberately. Dropping it would turn every benign no-op into
        a red run on ``main``; it is safe only because the outcome is now
        reported, and these two facts belong in the same test file."""
        assert "--no-fail-on-empty-changeset" in _step("deploy", DEPLOY_STEP_NAME)["run"]

    def test_sam_output_is_captured_to_a_file(self) -> None:
        run = _step("deploy", DEPLOY_STEP_NAME)["run"]
        assert "tee" in run
        assert "SAM_DEPLOY_LOG" in run

    def test_the_log_path_is_exported_for_the_report_step(self) -> None:
        run = _step("deploy", DEPLOY_STEP_NAME)["run"]
        assert 'echo "SAM_DEPLOY_LOG=' in run
        assert '>> "$GITHUB_ENV"' in run

    def test_sams_status_is_read_from_pipestatus_not_dollar_question(self) -> None:
        """The load-bearing one. ``$?`` after ``sam deploy | tee`` is ``tee``'s
        status -- 0 unless the disk fills -- so a failed deploy would be
        reported as a success."""
        run = _step("deploy", DEPLOY_STEP_NAME)["run"]
        assert "${PIPESTATUS[0]}" in run

    def test_no_bare_dollar_question_is_captured_after_the_pipeline(self) -> None:
        run = _step("deploy", DEPLOY_STEP_NAME)["run"]
        assert "rc=$?" not in run

    def test_nothing_executable_sits_between_the_pipeline_and_the_read(self) -> None:
        """``PIPESTATUS`` is rewritten by the next pipeline, and every simple
        command is a pipeline of one. Present-and-correct is not enough: the
        read has to be the first thing after ``| tee``, comments aside. This is
        the realistic regression -- somebody inserting a log line."""
        lines = _step("deploy", DEPLOY_STEP_NAME)["run"].splitlines()
        tee = next(i for i, ln in enumerate(lines) if 'tee "$SAM_DEPLOY_LOG"' in ln)
        read = next(i for i, ln in enumerate(lines) if "${PIPESTATUS[0]}" in ln)
        assert read > tee
        between = [ln.strip() for ln in lines[tee + 1 : read]]
        assert all(not ln or ln.startswith("#") for ln in between), (
            f"a command runs between the pipeline and the PIPESTATUS read: {between}"
        )

    def test_the_exit_code_is_exported_rather_than_raised_here(self) -> None:
        """Failing in place would skip the report step, discarding the summary
        that makes a red run legible -- the same reason catalog-parity.yml
        captures its harness's status instead of failing on it."""
        run = _step("deploy", DEPLOY_STEP_NAME)["run"]
        assert 'echo "SAM_DEPLOY_EXIT_CODE=' in run
        assert "set +e" in run and "set -e" in run

    def test_errexit_is_restored_before_the_step_ends(self) -> None:
        run = _step("deploy", DEPLOY_STEP_NAME)["run"]
        assert run.index("set +e") < run.index("${PIPESTATUS[0]}")
        assert run.index("${PIPESTATUS[0]}") < run.rindex("set -e")


class TestTheReportStep:
    def test_it_exists_and_runs_the_renderer(self) -> None:
        run = _step("deploy", REPORT_STEP_NAME)["run"]
        assert "scripts/sam_deploy_summary.py" in run

    def test_the_renderer_it_names_exists(self) -> None:
        assert RENDERER_PATH.is_file()

    def test_it_appends_to_the_step_summary(self) -> None:
        assert '>> "$GITHUB_STEP_SUMMARY"' in _step("deploy", REPORT_STEP_NAME)["run"]

    def test_it_runs_even_when_the_deploy_step_failed(self) -> None:
        """Without this the summary is produced only on the runs that did not
        need it."""
        condition = _step("deploy", REPORT_STEP_NAME)["if"]
        assert "cancelled()" in condition or "always()" in condition

    def test_it_is_not_gated_on_success(self) -> None:
        condition = _step("deploy", REPORT_STEP_NAME)["if"]
        assert "success()" not in condition

    def test_it_handles_the_deploy_step_never_having_run(self) -> None:
        """An earlier failure leaves ``SAM_DEPLOY_EXIT_CODE`` unset. Passing an
        empty string to ``--exit-code`` would traceback in argparse."""
        run = _step("deploy", REPORT_STEP_NAME)["run"]
        assert "--not-run" in run
        assert "SAM_DEPLOY_EXIT_CODE:-" in run

    def test_it_comes_after_the_deploy_step(self) -> None:
        names = [s.get("name") for s in _steps("deploy")]
        assert names.index(REPORT_STEP_NAME) > names.index(DEPLOY_STEP_NAME)

    def test_the_renderer_path_resolves_from_the_steps_working_directory(self) -> None:
        """``SAM deploy`` runs in ``infra/ephemeral-rebuild``. If the report
        step inherited that, the relative script path would not resolve and the
        summary would be a ``can't open file`` traceback."""
        step = _step("deploy", REPORT_STEP_NAME)
        cwd = REPO_ROOT / step.get("working-directory", ".")
        assert (cwd / "scripts" / "sam_deploy_summary.py").is_file()


class TestTheRendererNeedsNoInstall:
    """The ``deploy`` job has no ``pip install`` step, by design -- it deploys,
    it does not test. Everything the report step imports has to be stdlib or
    repo-local."""

    def _top_level_imports(self) -> set[str]:
        tree = ast.parse(RENDERER_PATH.read_text())
        modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules.add(node.module.split(".")[0])
        return modules

    def test_no_third_party_imports(self) -> None:
        third_party = self._top_level_imports() - sys.stdlib_module_names - {"lib"}
        assert not third_party, f"report step would need a pip install for: {sorted(third_party)}"

    def test_the_deploy_job_still_has_no_pip_install(self) -> None:
        """If one is ever added, the import rule above stops being load-bearing
        and this test should be the thing that prompts revisiting it."""
        assert not any("pip install" in (s.get("run") or "") for s in _steps("deploy"))


class TestTheValidateGate:
    """``validate`` is a ``needs:`` gate on ``deploy``, and its pytest
    invocation is an explicit file list rather than a directory, so a new test
    file is silently ungated until it is added here."""

    def _pytest_run(self) -> str:
        return _step("validate", "pytest (stack unit tests)")["run"]

    @pytest.mark.parametrize(
        "test_file",
        [
            "tests/unit/test_sam_deploy_summary.py",
            "tests/unit/test_deploy_ephemeral_rebuild_workflow.py",
        ],
    )
    def test_the_new_tests_gate_the_deploy(self, test_file: str) -> None:
        assert test_file in self._pytest_run()

    def test_every_file_in_the_list_exists(self) -> None:
        """A typo'd path makes pytest exit 4 -- which fails the gate loudly --
        but a *renamed* file that nobody updated here silently stops gating."""
        for token in self._pytest_run().split():
            if token.startswith("tests/"):
                assert (REPO_ROOT / token).is_file(), f"{token} is listed but does not exist"
