"""Unit tests for ``scripts/sam_deploy_summary.py`` (discogs-etl#396).

``deploy-ephemeral-rebuild.yml`` runs ``sam deploy --no-fail-on-empty-changeset``.
That flag is correct and has to stay -- three of the workflow's four triggers
produce no template diff at all (``scripts/rebuild-cache-bootstrap.sh`` is
curled by the instance at runtime and is not part of the rendered template;
the workflow file itself is not either; a ``workflow_dispatch`` re-run of an
already-deployed ``main`` is a no-op by construction) -- but it means SAM exits
0 whether it applied a changeset or found nothing to do. **Those two outcomes
were indistinguishable in the run's output**, and for months the workflow's
green history was the second one while the deploy role silently lacked
``ec2:DescribeImages`` and could not have applied anything:

    run 31970903384   "No changes to deploy. Stack ... is up to date"   exit 0  green
    run 31970987037   AccessDenied ec2:DescribeImages, UPDATE_ROLLBACK  exit 1  red

Two minutes apart. Only the second one carried information, and only because
#358 happened to be the first real changeset in months. This renderer is the
fix for the first line, not the second: a no-op must announce itself.

The classification is deliberately narrow. **The exit code alone decides
failure** -- never prose. For exit 0 there are exactly two accepted top-level
SAM strings, one per outcome, and *anything else is a failure to classify*,
which is red. That direction is the whole point: the defect being fixed is a
silent fall-through to "looks fine", so the ambiguous case must be loud.

Nothing here parses SAM's CloudFormation events table. Its columns are
fixed-width and wrap mid-token -- the real rollback log above contains
``UPDATE_ROLLBACK_COMPLE`` / ``TE`` on two lines, and the successful one
contains ``UPDATE_COMPLETE_CLEANU`` -- so substring matches against resource
statuses are a coin flip on column width. The fixtures below keep that
wrapping verbatim so a future "just also check for UPDATE_COMPLETE" has a test
that shows why not.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "sam_deploy_summary.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("sam_deploy_summary", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # Registered before exec_module: the module defines a dataclass whose field
    # annotations are strings under `from __future__ import annotations`, and
    # dataclasses resolves them through sys.modules[cls.__module__].
    sys.modules["sam_deploy_summary"] = mod
    spec.loader.exec_module(mod)
    return mod


sds = _load_module()


# Verbatim from run 31970903384 (2026-08-16 13:34 PT) -- the no-op that passed
# two minutes before the real changeset failed. This single line is the entire
# output SAM produces for this case.
NO_CHANGES_LOG = """\
\tDeploying with following values
\t===============================
\tStack name                   : wxyc-discogs-rebuild
\tRegion                       : us-east-1
\tDisable rollback             : False

Initiating deployment
=====================


Waiting for changeset to be created..

No changes to deploy. Stack wxyc-discogs-rebuild is up to date
"""

# Verbatim from run 31975090212 (2026-08-16 15:01 PT) -- the first changeset
# this stack applied in months, once the deploy role had ec2:DescribeImages.
# Note `UPDATE_COMPLETE_CLEANU`: SAM wraps the status column at 22 characters.
APPLIED_LOG = """\
Waiting for changeset to be created..

CloudFormation stack changeset
-------------------------------------------------------------------------------------------------
Operation                LogicalResourceId        ResourceType             Replacement
-------------------------------------------------------------------------------------------------
+ Add                    ReleaseCountAlarm        AWS::CloudWatch::Alarm   N/A
-------------------------------------------------------------------------------------------------


Changeset created successfully. arn:aws:cloudformation:us-east-1:203767826763:changeSet/samcli-deploy1786917657/fdc0097a-2be8-43d8-b16f-bcd35081f417


2026-08-16 22:01:08 - Waiting for stack create/update to complete

CloudFormation events from stack operations (refresh every 5.0 seconds)
-------------------------------------------------------------------------------------------------
ResourceStatus           ResourceType             LogicalResourceId        ResourceStatusReason
-------------------------------------------------------------------------------------------------
CREATE_IN_PROGRESS       AWS::CloudWatch::Alarm   ReleaseCountAlarm        -
CREATE_COMPLETE          AWS::CloudWatch::Alarm   ReleaseCountAlarm        -
UPDATE_COMPLETE_CLEANU   AWS::CloudFormation::S   wxyc-discogs-rebuild     -
P_IN_PROGRESS            tack
UPDATE_COMPLETE          AWS::CloudFormation::S   wxyc-discogs-rebuild     -
                         tack
-------------------------------------------------------------------------------------------------

Successfully created/updated stack - wxyc-discogs-rebuild in us-east-1
"""

# Verbatim from run 31970987037 -- #396 itself. The AccessDenied reason and the
# terminal status are both wrapped across lines by SAM's column widths; the
# untruncated `UPDATE_ROLLBACK_COMPLETE` survives only in the final Error line.
ROLLBACK_LOG = """\
Changeset created successfully. arn:aws:cloudformation:us-east-1:203767826763:changeSet/samcli-deploy1786912571/a565f05a-fa28-4847-8eca-b0f415c9d612


2026-08-16 20:36:22 - Waiting for stack create/update to complete

CloudFormation events from stack operations (refresh every 5.0 seconds)
-------------------------------------------------------------------------------------------------
ResourceStatus           ResourceType             LogicalResourceId        ResourceStatusReason
-------------------------------------------------------------------------------------------------
UPDATE_IN_PROGRESS       AWS::CloudFormation::S   wxyc-discogs-rebuild     User Initiated
                         tack
UPDATE_ROLLBACK_IN_PRO   AWS::CloudFormation::S   wxyc-discogs-rebuild     AccessDenied. User
GRESS                    tack                                              doesn't have
                                                                           permission to call
                                                                           ec2:DescribeImages
UPDATE_ROLLBACK_COMPLE   AWS::CloudFormation::S   wxyc-discogs-rebuild     -
TE_CLEANUP_IN_PROGRESS   tack
UPDATE_ROLLBACK_COMPLE   AWS::CloudFormation::S   wxyc-discogs-rebuild     -
TE                       tack
-------------------------------------------------------------------------------------------------

Error: Failed to create/update the stack: wxyc-discogs-rebuild, Waiter StackUpdateComplete failed: Waiter encountered a terminal failure state: For expression "Stacks[].StackStatus" we matched expected path: "UPDATE_ROLLBACK_COMPLETE" at least once
"""


class TestClassify:
    """The exit code decides failure; for exit 0 the two SAM strings decide."""

    def test_the_real_no_op_log_is_no_changes(self) -> None:
        assert sds.classify(0, NO_CHANGES_LOG) == sds.NO_CHANGES

    def test_the_real_applied_log_is_applied(self) -> None:
        assert sds.classify(0, APPLIED_LOG) == sds.APPLIED

    def test_the_real_rollback_log_is_failed(self) -> None:
        assert sds.classify(1, ROLLBACK_LOG) == sds.FAILED

    @pytest.mark.parametrize("code", [1, 2, 137, 255])
    def test_any_non_zero_exit_is_failed_whatever_the_log_says(self, code: int) -> None:
        """Prose never overrides the exit code. A log that got as far as
        'Successfully created/updated stack' and *then* had the step killed is
        not a successful deploy."""
        assert sds.classify(code, APPLIED_LOG) == sds.FAILED
        assert sds.classify(code, NO_CHANGES_LOG) == sds.FAILED

    def test_exit_zero_with_neither_marker_is_unclassifiable(self) -> None:
        """Fail closed. If SAM's wording changes, the run must go red rather
        than quietly inheriting the 'looks fine' reading that #396 was."""
        assert sds.classify(0, "Deploying with following values\nRegion: us-east-1\n")
        assert sds.classify(0, "Deploying with following values\n") == sds.UNCLASSIFIABLE

    def test_exit_zero_with_both_markers_is_unclassifiable(self) -> None:
        assert sds.classify(0, NO_CHANGES_LOG + APPLIED_LOG) == sds.UNCLASSIFIABLE

    def test_an_unreadable_log_at_exit_zero_is_unclassifiable(self) -> None:
        """An exit-0 deploy whose output was lost cannot be called applied."""
        assert sds.classify(0, None) == sds.UNCLASSIFIABLE

    def test_an_unreadable_log_at_a_failing_exit_is_still_failed(self) -> None:
        """The exit code is sufficient on its own for this one."""
        assert sds.classify(1, None) == sds.FAILED

    def test_no_exit_code_at_all_is_not_run(self) -> None:
        """The deploy step never reached SAM -- an earlier step failed."""
        assert sds.classify(None, None) == sds.NOT_RUN

    def test_the_events_table_is_not_a_marker(self) -> None:
        """SAM wraps the status column at 22 characters, so resource statuses
        are not reliably present as whole tokens. A log carrying the events
        table but *not* the terminal SAM line must not read as applied."""
        events_only = APPLIED_LOG.replace(
            "Successfully created/updated stack - wxyc-discogs-rebuild in us-east-1", ""
        )
        assert "UPDATE_COMPLETE" in events_only  # the tempting marker is right there
        assert sds.classify(0, events_only) == sds.UNCLASSIFIABLE

    def test_a_changeset_that_was_only_created_is_not_applied(self) -> None:
        """'Changeset created successfully' precedes the apply; the rollback
        log contains it too. Matching on it would call #396 a success."""
        assert "Changeset created successfully" in ROLLBACK_LOG
        created_only = ROLLBACK_LOG.split("Error:")[0]
        assert sds.classify(0, created_only) == sds.UNCLASSIFIABLE


class TestExitCodes:
    """What the report step returns, and therefore what the job's status is."""

    def _run(self, tmp_path, code, log_text, extra=()):
        argv = []
        if code is not None:
            argv += ["--exit-code", str(code)]
        else:
            argv += ["--not-run"]
        if log_text is not None:
            path = tmp_path / "sam-deploy.log"
            path.write_text(log_text)
            argv += ["--log", str(path)]
        return sds.main([*argv, *extra])

    def test_applied_is_green(self, tmp_path) -> None:
        assert self._run(tmp_path, 0, APPLIED_LOG) == 0

    def test_a_no_op_is_green(self, tmp_path) -> None:
        """A no-op is a legitimate outcome of three of the four triggers. It
        has to be visible, not red -- reddening it would make `main` carry a
        permanent failing check for every bootstrap-script edit."""
        assert self._run(tmp_path, 0, NO_CHANGES_LOG) == 0

    def test_a_failed_deploy_re_raises_sams_own_code(self, tmp_path) -> None:
        assert self._run(tmp_path, 1, ROLLBACK_LOG) == 1
        assert self._run(tmp_path, 137, ROLLBACK_LOG) == 137

    def test_unclassifiable_is_red_with_its_own_code(self, tmp_path) -> None:
        code = self._run(tmp_path, 0, "Deploying with following values\n")
        assert code == sds.UNCLASSIFIABLE_EXIT
        assert code != 0

    def test_the_unclassifiable_code_cannot_be_confused_with_sams(self, tmp_path) -> None:
        """SAM exits 1 (and the runner 137/143 on a kill). 65 is EX_DATAERR and
        is not in either population, so the bare number is diagnostic."""
        assert sds.UNCLASSIFIABLE_EXIT == 65

    def test_not_run_is_red(self, tmp_path) -> None:
        """The deploy step never produced a status. Nothing was deployed and
        nothing explains why -- that is not a green outcome."""
        assert self._run(tmp_path, None, None) != 0


class TestTheNoOpIsUnmistakable:
    """The acceptance criterion, stated as tests.

    Someone scanning a green run must be able to tell, without opening the log,
    that nothing reached AWS.
    """

    def test_the_summary_heading_says_nothing_was_deployed(self) -> None:
        out = sds.render(sds.NO_CHANGES, 0, NO_CHANGES_LOG, None)
        heading = out.splitlines()[0]
        assert heading.startswith("#")
        assert "nothing" in heading.lower() or "no changes" in heading.lower()

    def test_the_no_op_annotation_is_a_warning_not_a_notice(self) -> None:
        """Green-with-a-warning is the shape this outcome needs: it must not
        fail the job, and it must not be as quiet as the success case."""
        assert sds.annotation(sds.NO_CHANGES, 0, NO_CHANGES_LOG).startswith("::warning ")

    def test_the_applied_annotation_is_a_notice(self) -> None:
        assert sds.annotation(sds.APPLIED, 0, APPLIED_LOG).startswith("::notice ")

    @pytest.mark.parametrize("outcome", ["FAILED", "UNCLASSIFIABLE", "NOT_RUN"])
    def test_the_red_outcomes_annotate_as_errors(self, outcome: str) -> None:
        value = getattr(sds, outcome)
        assert sds.annotation(value, 1, ROLLBACK_LOG).startswith("::error ")

    def test_the_two_green_outcomes_do_not_share_a_heading(self) -> None:
        applied = sds.render(sds.APPLIED, 0, APPLIED_LOG, None).splitlines()[0]
        no_changes = sds.render(sds.NO_CHANGES, 0, NO_CHANGES_LOG, None).splitlines()[0]
        assert applied != no_changes

    def test_the_no_op_summary_names_the_benign_causes(self) -> None:
        """Left unexplained, a warning on every bootstrap-script edit becomes
        noise that gets muted -- which is how the signal was lost the first
        time. The summary has to say when a no-op is expected."""
        out = sds.render(sds.NO_CHANGES, 0, NO_CHANGES_LOG, None)
        assert "rebuild-cache-bootstrap.sh" in out
        assert "workflow_dispatch" in out or "dispatch" in out

    def test_the_no_op_summary_says_what_would_make_it_suspicious(self) -> None:
        out = sds.render(sds.NO_CHANGES, 0, NO_CHANGES_LOG, None)
        assert "template.yaml" in out

    @pytest.mark.parametrize(
        "outcome",
        ["APPLIED", "NO_CHANGES", "FAILED", "UNCLASSIFIABLE", "NOT_RUN"],
    )
    def test_every_annotation_is_exactly_one_line(self, outcome: str) -> None:
        """GitHub truncates an annotation at its first newline."""
        line = sds.annotation(getattr(sds, outcome), 1, ROLLBACK_LOG)
        assert "\n" not in line


class TestFailureDiagnosis:
    """#396 took hours to root-cause. The next one should take one look."""

    def test_an_access_denied_failure_names_the_deploy_role_template(self) -> None:
        out = sds.render(sds.FAILED, 1, ROLLBACK_LOG, None)
        assert "infra/bootstrap/deploy-role.yaml" in out

    def test_an_access_denied_failure_points_at_the_simulator(self) -> None:
        out = sds.render(sds.FAILED, 1, ROLLBACK_LOG, None)
        assert "simulate-deploy-role.sh" in out

    def test_an_access_denied_failure_surfaces_the_denied_action(self) -> None:
        out = sds.render(sds.FAILED, 1, ROLLBACK_LOG, None)
        assert "ec2:DescribeImages" in out

    def test_a_rollback_says_the_template_and_the_stack_have_diverged(self) -> None:
        """The state #396 left behind: `main` declared ReleaseCountAlarm and
        the deployed stack did not. Nothing else reports that."""
        out = sds.render(sds.FAILED, 1, ROLLBACK_LOG, None).lower()
        assert "rolled back" in out or "rollback" in out
        assert "diverge" in out

    def test_a_failure_without_access_denied_does_not_blame_iam(self) -> None:
        plain_failure = "Error: Failed to create/update the stack: wxyc-discogs-rebuild\n"
        out = sds.render(sds.FAILED, 1, plain_failure, None)
        assert "deploy-role.yaml" not in out

    def test_the_denied_action_survives_sams_column_wrapping(self) -> None:
        """`AccessDenied. User doesn't have permission to call` is split across
        four lines in the real log; only `ec2:DescribeImages` sits whole on one
        of them. Matching the sentence would have found nothing."""
        assert "permission to call ec2:DescribeImages" not in ROLLBACK_LOG
        assert sds.denied_actions(ROLLBACK_LOG) == ["ec2:DescribeImages"]

    def test_denied_actions_ignores_arns_and_resource_types(self) -> None:
        assert sds.denied_actions(APPLIED_LOG) == []


class TestDegradedInput:
    """This renderer runs in the step whose job is to explain other failures.
    A traceback here turns one legible failure into two."""

    def test_an_absent_log_does_not_traceback(self, tmp_path) -> None:
        assert sds.main(["--exit-code", "0", "--log", str(tmp_path / "gone.log")]) != 0

    def test_an_empty_log_does_not_traceback(self, tmp_path) -> None:
        path = tmp_path / "sam-deploy.log"
        path.write_text("")
        assert sds.main(["--exit-code", "0", "--log", str(path)]) == sds.UNCLASSIFIABLE_EXIT

    def test_an_empty_log_on_a_failing_exit_still_reports_the_failure(self, tmp_path) -> None:
        path = tmp_path / "sam-deploy.log"
        path.write_text("")
        assert sds.main(["--exit-code", "1", "--log", str(path)]) == 1

    def test_undecodable_bytes_do_not_traceback(self, tmp_path) -> None:
        path = tmp_path / "sam-deploy.log"
        path.write_bytes(b"No changes to deploy. Stack Csillagrabl\xc3")
        assert sds.main(["--exit-code", "0", "--log", str(path)]) != 0

    def test_no_log_argument_at_all_does_not_traceback(self) -> None:
        assert sds.main(["--exit-code", "1"]) == 1

    def test_the_problem_is_stated_in_the_summary(self, tmp_path, capsys) -> None:
        sds.main(["--exit-code", "0", "--log", str(tmp_path / "gone.log")])
        out = capsys.readouterr().out
        assert "sam-deploy" in out or "never written" in out

    def test_stdout_is_markdown_and_stderr_is_one_annotation(self, tmp_path, capsys) -> None:
        path = tmp_path / "sam-deploy.log"
        path.write_text(NO_CHANGES_LOG)
        sds.main(["--exit-code", "0", "--log", str(path)])
        captured = capsys.readouterr()
        assert captured.out.startswith("#")
        assert captured.err.strip().startswith("::warning ")
        assert len(captured.err.strip().splitlines()) == 1


class TestSharedLoader:
    """The untrusted-file read is `lib/run_summary.py`'s, not a third copy."""

    def test_it_routes_through_the_shared_loader(self) -> None:
        sys.path.insert(0, str(REPO_ROOT))
        from lib.run_summary import load_text

        assert sds.load_log.func is load_text  # functools.partial over the shared one

    def test_annotations_are_plain_text(self) -> None:
        sys.path.insert(0, str(REPO_ROOT))
        from lib.run_summary import plain

        assert sds._plain is plain
