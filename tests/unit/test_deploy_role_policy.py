"""Pins on the GitHub Actions deploy role's inline policy.

``infra/bootstrap/deploy-role.yaml`` is applied by hand with admin credentials
and is never deployed by CI -- it is what *lets* CI deploy. That makes it the
one template in this repo with no deploy-time feedback loop: a policy that is
too narrow does not fail at merge, it fails silently the next time somebody
tries to change a different stack, possibly months later.

That is not hypothetical. #396: the role was missing ``ec2:DescribeImages``,
so ``sam deploy`` of the ephemeral-rebuild stack could not apply *any*
changeset. Nobody noticed because the deploy workflow runs with
``--no-fail-on-empty-changeset``, so every no-op deploy in between exited 0 and
reported success. The gap only surfaced when #358's ``ReleaseCountAlarm``
became the first real changeset since the org-account migration, and it failed
with ``AccessDenied ... ec2:DescribeImages`` and rolled back.

These tests are therefore a regression pin on the *actions the deploy path is
known to need*, so that a future tightening of this policy fails here rather
than at the next changeset. ``infra/bootstrap/README.md`` already warns that
statements in this role are "easy to overlook when tightening the policy".

They deliberately do not assert the policy is minimal -- that judgment belongs
to a human reading the template. They assert only that specific, experimentally
established requirements have not been dropped.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.cfn_yaml import load_cfn

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY_ROLE_PATH = REPO_ROOT / "infra" / "bootstrap" / "deploy-role.yaml"
EPHEMERAL_TEMPLATE_PATH = REPO_ROOT / "infra" / "ephemeral-rebuild" / "template.yaml"


@pytest.fixture(scope="module")
def deploy_role() -> dict[str, Any]:
    return load_cfn(DEPLOY_ROLE_PATH)


@pytest.fixture(scope="module")
def statements(deploy_role: dict[str, Any]) -> list[dict[str, Any]]:
    """Every statement across every inline policy on the role."""
    roles = [
        resource
        for resource in deploy_role["Resources"].values()
        if resource.get("Type") == "AWS::IAM::Role"
    ]
    assert roles, "deploy-role.yaml declares no AWS::IAM::Role"
    collected: list[dict[str, Any]] = []
    for role in roles:
        for policy in role["Properties"].get("Policies", []):
            collected.extend(policy["PolicyDocument"]["Statement"])
    assert collected, "the deploy role has no inline policy statements"
    return collected


def _allowed_actions(statements: list[dict[str, Any]]) -> set[str]:
    actions: set[str] = set()
    for statement in statements:
        if statement.get("Effect") != "Allow":
            continue
        declared = statement.get("Action", [])
        if isinstance(declared, str):
            declared = [declared]
        actions.update(declared)
    return actions


@pytest.mark.parametrize(
    ("action", "why"),
    [
        (
            "ec2:DescribeImages",
            "AmiId is typed AWS::SSM::Parameter::Value<AWS::EC2::Image::Id>, so "
            "CloudFormation validates the resolved value against EC2 on every "
            "changeset. Without this the stack cannot be updated at all (#396).",
        ),
        (
            "ssm:GetParameter",
            "Resolving the AmiId SSM parameter happens with the deploying "
            "principal's credentials, not the stack's roles.",
        ),
        (
            "cloudformation:CreateChangeSet",
            "sam deploy creates a changeset before applying it.",
        ),
    ],
)
def test_required_deploy_action_is_granted(
    statements: list[dict[str, Any]], action: str, why: str
) -> None:
    assert action in _allowed_actions(statements), f"{action} missing from the deploy role: {why}"


def test_ami_parameter_still_requires_ec2_image_validation() -> None:
    """The reason ec2:DescribeImages is needed, pinned at its source.

    If AmiId ever stops being an AWS::EC2::Image::Id-typed SSM parameter, the
    DescribeImages grant may become unnecessary and this pin should be
    revisited rather than carried forever.
    """
    template = load_cfn(EPHEMERAL_TEMPLATE_PATH)
    ami_param = template["Parameters"]["AmiId"]
    assert ami_param["Type"] == "AWS::SSM::Parameter::Value<AWS::EC2::Image::Id>", (
        "AmiId's parameter type changed; re-derive whether the deploy role still "
        "needs ec2:DescribeImages (see #396) before editing the pin above."
    )


def test_describe_actions_are_unscoped_deliberately(statements: list[dict[str, Any]]) -> None:
    """ec2:Describe* has no resource-level scoping, so Resource must be '*'.

    Pinned because writing a narrower ARN here looks like good hygiene and
    silently denies every call.
    """
    describe_statements = [
        statement
        for statement in statements
        if any(
            action.startswith("ec2:Describe")
            for action in (
                statement["Action"]
                if isinstance(statement.get("Action"), list)
                else [statement.get("Action", "")]
            )
        )
    ]
    assert describe_statements, "no statement grants any ec2:Describe* action"
    for statement in describe_statements:
        assert statement["Resource"] == "*", (
            "ec2:Describe* does not support resource-level permissions; a narrower "
            "Resource silently denies the call"
        )
