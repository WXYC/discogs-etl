#!/usr/bin/env python3
"""Say what a ``sam deploy`` run actually did to the stack (#396).

``.github/workflows/deploy-ephemeral-rebuild.yml`` runs ``sam deploy
--no-fail-on-empty-changeset``. SAM then exits 0 in two completely different
situations -- it applied a changeset, or it found nothing to apply -- and until
this renderer existed the run's output could not tell them apart. That is not
a hypothetical: between the #353 account cutover and 2026-08-16 the deploy role
lacked ``ec2:DescribeImages`` and could not have applied *anything*, and the
workflow was green throughout. The condition surfaced only because #358's
``ReleaseCountAlarm`` was the first real changeset in that window:

    run 31970903384   "No changes to deploy. Stack ... is up to date"   exit 0
    run 31970987037   AccessDenied ec2:DescribeImages -> rollback       exit 1

Two minutes apart, the first one green and meaningless.

The flag stays -- three of the workflow's four triggers legitimately produce no
template diff (``scripts/rebuild-cache-bootstrap.sh`` is fetched by the
instance at runtime and is not part of the rendered template; the workflow file
is not either; a ``workflow_dispatch`` on an already-deployed ``main`` is a
no-op by construction), and reddening those would put a permanent failing check
on ``main``. What changes is that a no-op now announces itself.

Outcomes, and the exit code each produces::

    applied         a changeset reached CloudFormation                    0
    no_changes      SAM had nothing to apply -- NOTHING REACHED AWS       0  (::warning)
    failed          SAM exited non-zero                        SAM's own code
    unclassifiable  exit 0, but the output says neither                  65
    not_run         no exit status was recorded                          65

**The exit code alone decides failure; prose never overrides it.** For exit 0
there are exactly two accepted top-level SAM strings, and anything else --
neither present, or both -- is ``unclassifiable`` and red. That direction is
deliberate: the defect being fixed is a silent fall-through to "looks fine", so
an output this cannot read must be loud rather than optimistic.

Nothing here parses SAM's CloudFormation events table. Its columns are
fixed-width and wrap mid-token: the real rollback log contains
``UPDATE_ROLLBACK_COMPLE`` / ``TE`` on two lines and the successful one
contains ``UPDATE_COMPLETE_CLEANU``, so substring matches against resource
statuses are a coin flip on column width.

Markdown on stdout (the workflow appends it to ``$GITHUB_STEP_SUMMARY``) and
exactly one GitHub workflow-command annotation on stderr, then exit with the
code above. Deliberately no ``init_logger`` and no third-party import: the
``deploy`` job installs nothing, and JSON log lines on stderr would corrupt the
annotation.

Usage::

    python scripts/sam_deploy_summary.py --exit-code 0 --log sam-deploy.log \\
        >> "$GITHUB_STEP_SUMMARY"
    python scripts/sam_deploy_summary.py --not-run >> "$GITHUB_STEP_SUMMARY"
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from functools import partial
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.run_summary import annotation_line as _annotation_line  # noqa: E402
from lib.run_summary import load_text as _load_text  # noqa: E402

# The untrusted-output reader, shared with the two --json renderers so a
# hardening fix reaches all three (#384). Bound here with this tool's noun.
load_log = partial(_load_text, noun="deploy log")

APPLIED = "applied"
NO_CHANGES = "no_changes"
FAILED = "failed"
UNCLASSIFIABLE = "unclassifiable"
NOT_RUN = "not_run"

# EX_DATAERR. Chosen so the bare number is diagnostic: SAM exits 1, and a
# killed runner step exits 137 or 143, so 65 cannot be mistaken for either.
UNCLASSIFIABLE_EXIT = 65

# The two top-level lines SAM prints, one per exit-0 outcome. Both are terminal
# statements about the whole deploy, which is why they are the only two
# accepted. In particular "Changeset created successfully" is NOT here: it
# precedes the apply and appears in the #396 rollback log too, so matching on
# it would have called that failure a success.
APPLIED_MARKER = "Successfully created/updated stack"
NO_CHANGES_MARKER = "No changes to deploy"

STACK_NAME = "wxyc-discogs-rebuild"

# `service:Action` as it appears in an AccessDenied reason. Anchored on a
# lowercase service prefix and an upper-camel action so ARNs
# (`arn:aws:cloudformation:...`) and resource types (`AWS::CloudWatch::Alarm`)
# do not match. SAM's column wrapping can split a long action across lines --
# the summary says so rather than pretending the list is exhaustive.
_ACTION_RE = re.compile(r"(?<![:\w])([a-z][a-z0-9-]{1,31}:[A-Z][A-Za-z0-9]+)(?![:\w])")


@dataclass(frozen=True)
class _Outcome:
    """Everything one outcome means, in one place.

    The step summary and the annotation say the same thing in two registers.
    Branching on the outcome separately in each is how they drift, and a
    drifted no-op message is precisely the failure this file exists to prevent.
    """

    heading: str
    lead: str
    annotation_kind: str
    annotation_title: str
    annotation_body: str


_OUTCOMES: dict[str, _Outcome] = {
    APPLIED: _Outcome(
        heading="Ephemeral-rebuild deploy: changeset applied",
        lead=(
            f"**A changeset reached CloudFormation.** `{STACK_NAME}` now matches "
            "`infra/ephemeral-rebuild/template.yaml` at this commit. The resources SAM "
            "created, modified, or deleted are listed in the changeset table in the deploy "
            "step's log."
        ),
        annotation_kind="notice",
        annotation_title="Ephemeral-rebuild stack updated",
        annotation_body=f"A changeset was applied to {STACK_NAME}.",
    ),
    NO_CHANGES: _Outcome(
        heading="Ephemeral-rebuild deploy: nothing was deployed",
        lead=(
            "**SAM found no changes to apply, so nothing reached AWS.** This run did not "
            "modify the stack. It is reported rather than silent because a green run of "
            "this workflow used to mean either this or a real deploy, and between the #353 "
            "account cutover and 2026-08-16 it always meant this -- the deploy role lacked "
            "`ec2:DescribeImages` and could not have applied anything ("
            "[#396](https://github.com/WXYC/discogs-etl/issues/396)).\n\n"
            "**When this is expected.** Only `infra/ephemeral-rebuild/**` feeds the rendered "
            "template. The workflow also fires on `scripts/rebuild-cache-bootstrap.sh` -- "
            "which the instance fetches from the repo at run time and is not part of the "
            "template -- on changes to the workflow file itself, and on any "
            "`workflow_dispatch`. A no-op is the correct outcome for all three.\n\n"
            "**When it is not.** If this push changed "
            "`infra/ephemeral-rebuild/template.yaml` or a Lambda handler under "
            "`infra/ephemeral-rebuild/`, then a template change did not reach the stack and "
            "something is wrong. Compare the deployed stack against the template before "
            "assuming a declared resource exists."
        ),
        annotation_kind="warning",
        annotation_title="Ephemeral-rebuild stack unchanged",
        annotation_body=(
            f"sam deploy found no changes; nothing was applied to {STACK_NAME}. Expected "
            "when the trigger was a bootstrap-script, workflow-file, or dispatch-only "
            "change -- suspicious if template.yaml changed in this push."
        ),
    ),
    FAILED: _Outcome(
        heading="Ephemeral-rebuild deploy: failed",
        lead=(
            "**SAM exited non-zero -- the deploy did not complete.** CloudFormation rolls a "
            "failed update back to the previous template, so the stack is consistent, but "
            "it is now behind `main`."
        ),
        annotation_kind="error",
        annotation_title="Ephemeral-rebuild deploy failed",
        annotation_body=f"sam deploy failed; {STACK_NAME} is unchanged and behind main.",
    ),
    UNCLASSIFIABLE: _Outcome(
        heading="Ephemeral-rebuild deploy: outcome could not be determined",
        lead=(
            "**SAM exited 0, but its output says neither that it applied a changeset nor "
            "that it had nothing to apply.** This run is deliberately red. An exit-0 deploy "
            "whose output cannot be read might have deployed nothing, and treating that as "
            "success is the exact failure mode "
            "[#396](https://github.com/WXYC/discogs-etl/issues/396) was -- so the ambiguous "
            "case fails closed.\n\n"
            "The usual cause is a SAM CLI upgrade rewording one of the two lines this looks "
            f"for: `{APPLIED_MARKER}` and `{NO_CHANGES_MARKER}`. Read the deploy step's log, "
            "confirm what actually happened, then update `scripts/sam_deploy_summary.py` and "
            "the fixtures in `tests/unit/test_sam_deploy_summary.py` -- which are verbatim "
            "captures, so replacing them is how the new wording gets pinned."
        ),
        annotation_kind="error",
        annotation_title="Ephemeral-rebuild deploy outcome unknown",
        annotation_body=(
            "sam deploy exited 0 but its output matched neither the applied nor the "
            "no-changes marker; failing closed rather than assuming success."
        ),
    ),
    NOT_RUN: _Outcome(
        heading="Ephemeral-rebuild deploy: no exit status was recorded",
        lead=(
            "**The deploy step recorded no exit status for `sam deploy`.** Almost always this "
            "means an earlier step in the job failed -- the SAM build, the OIDC credential "
            "assumption, or the checkout -- and SAM was never reached, so nothing was "
            "deployed and the failure is in the step above this one.\n\n"
            'Stated that way on purpose: "no status was recorded" is what was *observed*, '
            'and "nothing was deployed" is an inference from it. The two come apart if the '
            "step is killed (a step timeout, the runner going away) between SAM finishing and "
            "the status being written -- a narrow window, but this file exists because a "
            "confident wrong reading of a deploy cost months. Where the captured log can "
            "settle it, it is quoted below."
        ),
        annotation_kind="error",
        annotation_title="Ephemeral-rebuild deploy status missing",
        annotation_body=(
            "No exit status was recorded for sam deploy -- almost certainly an earlier step "
            "in the deploy job failed and SAM was never reached."
        ),
    ),
}


def classify(exit_code: int | None, log_text: str | None) -> str:
    """Which outcome this run was.

    Args:
        exit_code: ``sam deploy``'s own status, or None when the deploy step
            never ran.
        log_text: The captured console output, or None when it could not be
            read.

    Returns:
        One of :data:`APPLIED`, :data:`NO_CHANGES`, :data:`FAILED`,
        :data:`UNCLASSIFIABLE`, :data:`NOT_RUN`.
    """
    if exit_code is None:
        return NOT_RUN
    if exit_code != 0:
        # Sufficient on its own, and not overridable by the log: a deploy that
        # printed the success line and was then killed did not succeed.
        return FAILED
    if log_text is None:
        return UNCLASSIFIABLE
    applied = APPLIED_MARKER in log_text
    no_changes = NO_CHANGES_MARKER in log_text
    if applied and not no_changes:
        return APPLIED
    if no_changes and not applied:
        return NO_CHANGES
    return UNCLASSIFIABLE


def denied_actions(log_text: str | None) -> list[str]:
    """IAM actions named in an ``AccessDenied`` reason, in order of appearance.

    Empty unless the log actually carries an ``AccessDenied`` -- an unrelated
    failure must not be reported as a permissions problem.
    """
    if not log_text or "AccessDenied" not in log_text:
        return []
    seen: list[str] = []
    for match in _ACTION_RE.finditer(log_text):
        action = match.group(1)
        if action not in seen:
            seen.append(action)
    return seen


def _failure_detail(log_text: str | None) -> list[str]:
    """What a failed deploy can say beyond "it failed"."""
    lines: list[str] = []
    text = log_text or ""

    actions = denied_actions(text)
    if actions:
        rendered = ", ".join(f"`{a}`" for a in actions)
        lines.append(
            f"**CloudFormation was denied an IAM action: {rendered}.** The principal is the "
            "GitHub OIDC deploy role (`vars.AWS_ROLE_TO_ASSUME`), defined in "
            "`infra/bootstrap/deploy-role.yaml` -- *not* the `InstanceRole` the rebuild "
            "assumes at runtime. Add the grant there, add the matching expectation to "
            "`infra/bootstrap/simulate-deploy-role.sh`, then apply the bootstrap template by "
            "hand with admin credentials: CI cannot deploy it, because it is what lets CI "
            "deploy. See `infra/bootstrap/README.md`."
        )
        lines.append(
            "SAM wraps its events table at fixed column widths, so a longer action name can "
            "be split across two lines and missed here. Read the `ResourceStatusReason` "
            "column in the deploy step's log before concluding the list above is complete."
        )

    if "ROLLBACK" in text:
        lines.append(
            f"**The stack rolled back**, so `{STACK_NAME}` still holds the previous template "
            "and no partial state was left behind -- but it has now **diverged from "
            "`infra/ephemeral-rebuild/template.yaml` on `main`**, and will stay diverged "
            "until a deploy succeeds. Anything the failed changeset would have created does "
            "not exist. Do not assume a resource is live because it is declared."
        )

    return lines


def render(outcome: str, exit_code: int | None, log_text: str | None, problem: str | None) -> str:
    """The step-summary Markdown."""
    record = _OUTCOMES[outcome]
    lines = [f"## {record.heading}", "", record.lead, ""]

    if outcome == NOT_RUN:
        # The deploy step exports SAM_DEPLOY_LOG *before* invoking SAM, so a log
        # can exist even when no status was recorded. If it carries a terminal
        # SAM line, "nothing was deployed" is not something this may assert.
        evidence = [m for m in (APPLIED_MARKER, NO_CHANGES_MARKER) if m in (log_text or "")]
        if evidence:
            lines.extend(
                [
                    f"> **The captured log does carry `{evidence[0]}`.** SAM therefore ran and "
                    "the step died before its status could be written, rather than SAM never "
                    "having been reached. **Check the deployed stack against "
                    "`infra/ephemeral-rebuild/template.yaml` before assuming either way** -- "
                    "this run cannot tell you whether the deploy completed.",
                    "",
                ]
            )
        elif log_text:
            lines.append(
                "The captured log carries neither terminal SAM line, which is consistent with "
                "SAM never having been reached."
            )
            lines.append("")

    if outcome == FAILED:
        detail = _failure_detail(log_text)
        if detail:
            lines.extend(f"- {item}" for item in detail)
            lines.append("")
        if exit_code is not None:
            lines.append(f"`sam deploy` exited **{exit_code}**.")
            lines.append("")

    if problem and outcome in (UNCLASSIFIABLE, FAILED):
        lines.append(f"> The captured deploy log could not be read: {problem}.")
        lines.append("")

    # The footer deliberately names only the stack and its template. The deploy
    # role is named in the AccessDenied branch and nowhere else: on a failure
    # with an unrelated cause, a pointer at the IAM template reads as a
    # diagnosis and sends the next reader down #396's path for no reason.
    lines.append(
        f"Stack `{STACK_NAME}` in `us-east-1`, WXYC org account. Template: "
        "`infra/ephemeral-rebuild/template.yaml`. Why this summary exists: "
        "[#396](https://github.com/WXYC/discogs-etl/issues/396)."
    )
    return "\n".join(lines) + "\n"


def annotation(outcome: str, log_text: str | None) -> str:
    """One GitHub workflow command, on one line.

    Formatting -- the one-line and plain-text invariants -- belongs to
    ``lib.run_summary.annotation_line``; this decides only the body. The level
    is the record's, not computed here: an outcome's severity and its exit
    status have to agree, and the two siblings that compute it in an ad-hoc
    ternary outside the record are the shape to move away from.

    No ``exit_code`` parameter: ``outcome`` already carries everything the
    annotation varies on. Taking one would invite call sites that read as if
    NOT_RUN-with-exit-1 were a real combination.
    """
    record = _OUTCOMES[outcome]
    body = record.annotation_body
    if outcome == FAILED:
        actions = denied_actions(log_text)
        if actions:
            body = f"{body} Denied IAM action(s): {', '.join(actions)} -- see the run summary."
    return _annotation_line(record.annotation_kind, title=record.annotation_title, body=body)


def exit_status(outcome: str, exit_code: int | None) -> int:
    """The code the report step returns, and therefore the job's status."""
    if outcome == FAILED:
        # Re-raise SAM's own number so the failing step carries it into the log.
        return exit_code if exit_code else 1
    if outcome in (UNCLASSIFIABLE, NOT_RUN):
        return UNCLASSIFIABLE_EXIT
    return 0


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Render a GitHub Actions step summary for a sam deploy run, distinguishing an "
            "applied changeset from an empty one so a no-op cannot pass for a deploy. "
            "Markdown on stdout, one annotation on stderr."
        ),
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--exit-code",
        type=int,
        help="The exit code sam deploy returned (read from ${PIPESTATUS[0]}, not $?).",
    )
    group.add_argument(
        "--not-run",
        action="store_true",
        help="The deploy step never produced a status because an earlier step failed.",
    )
    p.add_argument(
        "--log",
        default=None,
        metavar="PATH",
        help=(
            "Path to the captured sam deploy output. Optional and untrusted: a missing, "
            "empty, or undecodable file degrades the summary rather than failing it."
        ),
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    exit_code = None if args.not_run else args.exit_code
    log_text, problem = load_log(args.log)
    outcome = classify(exit_code, log_text)
    sys.stdout.write(render(outcome, exit_code, log_text, problem))
    sys.stderr.write(annotation(outcome, log_text) + "\n")
    return exit_status(outcome, exit_code)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
