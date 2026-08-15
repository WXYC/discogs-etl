"""Unit tests for the ephemeral-rebuild SAM template's schedule wiring.

The monthly rebuild's armed/disarmed state is load-bearing: two armed copies of
``cron(0 6 4 * ? *)`` writing the same cache database is what destroyed 27,163
releases on 2026-08-04 (discogs-etl#352). The mitigation at the time was a manual
``aws events disable-rule``, which is CloudFormation drift — any subsequent
``sam deploy`` silently reverts it. So the state has to live in the template.

There is exactly one correct way to express that, and the obvious way is wrong:

    Enabled: !Ref ScheduleEnabled   ->  State: "ENABLED"   (always, silently)
    Enabled: false                  ->  State: "DISABLED"
    State: !Ref ScheduleState       ->  State: {"Ref": ...} (correct)

``Enabled`` is resolved by the SAM transform in Python (``"ENABLED" if
self.Enabled else "DISABLED"``), so a ``{"Ref": ...}`` dict is *always truthy*
and the rule comes out armed no matter what the parameter says. No error, no
warning, and ``sam validate --lint`` does not catch it — the template is
semantically valid either way.

These tests therefore assert against the **transformed** ``AWS::Events::Rule``,
not the source template. Asserting on the source would pass for both spellings.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml
from samtranslator.translator.transform import transform

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_PATH = REPO_ROOT / "infra" / "ephemeral-rebuild" / "template.yaml"

# The transform resolves pseudo-parameters through a boto3 Session and raises
# NoRegionFound if that session has no region. A developer machine with AWS
# configured supplies one ambiently and a CI runner does not, so the region has
# to be pinned here — in both directions: an ambient AWS_DEFAULT_REGION of
# eu-west-1 must not change what these assertions mean either. No credentials
# are involved; the session is read for its region and nothing else.
_TRANSFORM_REGION = "us-east-1"

# The transform rejects a local CodeUri ("must be an S3 Uri"). In the real deploy
# `sam build` + `sam package` rewrite it before CloudFormation ever runs the
# transform; here the Lambda payload is irrelevant to what we're asserting, so
# substitute a well-formed placeholder.
_PLACEHOLDER_CODE_URI = "s3://placeholder-bucket/placeholder.zip"


class _CfnLoader(yaml.SafeLoader):
    """SafeLoader that understands CloudFormation's short-form intrinsic tags.

    ``yaml.safe_load`` raises ConstructorError on ``!Ref`` / ``!Sub`` / ``!GetAtt``
    / ``!If`` / ``!Not`` / ``!Equals``, all of which this template uses.
    """


def _construct_cfn_tag(loader: yaml.Loader, tag_suffix: str, node: yaml.Node) -> Any:
    key = "Ref" if tag_suffix == "Ref" else f"Fn::{tag_suffix}"
    if isinstance(node, yaml.ScalarNode):
        value: Any = loader.construct_scalar(node)
        if key == "Fn::GetAtt":
            value = value.split(".")
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return {key: value}


_CfnLoader.add_multi_constructor("!", _construct_cfn_tag)


@pytest.fixture(scope="module")
def source_template() -> dict[str, Any]:
    """The template as written, intrinsics preserved as ``{"Ref": ...}`` dicts."""
    with TEMPLATE_PATH.open(encoding="utf-8") as fh:
        return yaml.load(fh, Loader=_CfnLoader)  # noqa: S506 — _CfnLoader subclasses SafeLoader


@pytest.fixture(scope="module")
def transformed_template(source_template: dict[str, Any]) -> dict[str, Any]:
    """The template after the ``AWS::Serverless-2016-10-31`` transform."""
    template = copy.deepcopy(source_template)
    for resource in template["Resources"].values():
        if resource.get("Type") == "AWS::Serverless::Function":
            resource["Properties"]["CodeUri"] = _PLACEHOLDER_CODE_URI

    # The managed-policy loader is only consulted for `Policies:` entries given
    # as AWS managed-policy *names*; every policy in this template is an inline
    # Statement list, so an empty map is never read. It resolves to a clear
    # "unknown managed policy" rather than an AttributeError if that changes.
    policy_loader = MagicMock()
    policy_loader.load.return_value = {}

    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("AWS_DEFAULT_REGION", _TRANSFORM_REGION)
        mp.setenv("AWS_REGION", _TRANSFORM_REGION)
        # A profile that does not exist on this machine would make the session
        # constructor raise ProfileNotFound before it ever looks at the region.
        mp.delenv("AWS_PROFILE", raising=False)
        mp.delenv("AWS_DEFAULT_PROFILE", raising=False)
        return transform(template, {}, policy_loader)


def _events_rule(transformed: dict[str, Any], logical_id: str) -> dict[str, Any]:
    resource = transformed["Resources"][logical_id]
    assert resource["Type"] == "AWS::Events::Rule", (
        f"{logical_id} is {resource['Type']}, expected AWS::Events::Rule — the SAM "
        "transform's generated logical id for a Schedule event changed"
    )
    return resource["Properties"]


def test_schedule_state_parameter_is_constrained(source_template: dict[str, Any]) -> None:
    """The parameter must reject anything EventBridge would not accept.

    ``State`` is passed through to ``AWS::Events::Rule`` verbatim, so a typo such
    as ``Disabled`` or ``false`` would fail at deploy time rather than at review
    time without ``AllowedValues``.
    """
    param = source_template["Parameters"]["ScheduleState"]
    assert param["Type"] == "String"
    assert param["AllowedValues"] == ["ENABLED", "DISABLED"]
    assert param["Default"] == "ENABLED"


def test_monthly_rule_state_is_a_ref_not_a_literal(transformed_template: dict[str, Any]) -> None:
    """The whole point: an unresolved ``Ref`` survives the transform.

    ``Enabled:`` cannot produce this — it resolves to the literal string
    ``"ENABLED"`` or ``"DISABLED"`` during the transform. Seeing a ``Ref`` here is
    proof the schedule is actually parameterized.
    """
    assert _events_rule(transformed_template, "LauncherFunctionMonthly")["State"] == {
        "Ref": "ScheduleState"
    }


def test_monthly_event_does_not_set_enabled(source_template: dict[str, Any]) -> None:
    """``State`` and ``Enabled`` are mutually exclusive, and ``Enabled`` is the trap.

    Pinned separately from the transformed assertion because a future edit that
    adds ``Enabled`` *alongside* ``State`` is the realistic regression — someone
    reading the SAM docs, where ``Enabled`` is the documented spelling.
    """
    monthly = source_template["Resources"]["LauncherFunction"]["Properties"]["Events"]["Monthly"]
    assert "Enabled" not in monthly["Properties"]


def test_sweeper_schedule_is_not_gated(transformed_template: dict[str, Any]) -> None:
    """The hourly sweeper stays armed regardless of ``ScheduleState``.

    It is the failsafe that force-terminates a rebuild instance stuck past its
    wall-clock budget. Disarming the monthly rebuild must not disarm the thing
    that cleans up after a manually-launched one.
    """
    hourly = _events_rule(transformed_template, "SweeperFunctionHourly")
    assert hourly.get("State", "ENABLED") == "ENABLED"


def _cloudwatch_alarm(transformed: dict[str, Any], logical_id: str) -> dict[str, Any]:
    resource = transformed["Resources"][logical_id]
    assert resource["Type"] == "AWS::CloudWatch::Alarm", (
        f"{logical_id} is {resource['Type']}, expected AWS::CloudWatch::Alarm"
    )
    return resource["Properties"]


def test_release_count_alarm_threshold_is_a_revisable_parameter(
    source_template: dict[str, Any],
) -> None:
    """The floor must be a template parameter, never a bare number on the resource.

    #358: the number needs to be revisable via ``--parameter-overrides`` as the
    cache grows and the org-account series (post #353 migration) accrues real
    trailing-90-day history, without touching the alarm resource itself.

    ``MinValue`` is the counterpart to ``ScheduleState``'s ``AllowedValues``:
    ``release_count`` is never negative, so an override of ``0`` or below makes
    ``LessThanThreshold`` unsatisfiable and silently disables the alarm — a
    clean deploy, no drift, no signal, no alarm.
    """
    param = source_template["Parameters"]["ReleaseCountAlarmThreshold"]
    assert param["Type"] == "Number"
    assert float(param["Default"]) > 0
    assert float(param["MinValue"]) >= 1


def test_release_count_alarm_shape(transformed_template: dict[str, Any]) -> None:
    """Simple-form alarm against the undimensioned ``release_count`` series.

    ``scripts/cache_health_metrics.py`` publishes ``release_count`` to
    ``WXYC/DiscogsCache`` with no ``Dimensions`` key, so the plain
    Namespace/MetricName/Statistic form works directly — no dimensionless
    companion metric is needed (see the org CLAUDE.md CloudWatch conventions).
    """
    props = _cloudwatch_alarm(transformed_template, "ReleaseCountAlarm")
    assert props["Namespace"] == "WXYC/DiscogsCache"
    assert props["MetricName"] == "release_count"
    assert "Dimensions" not in props
    assert props["Statistic"] == "Minimum"
    assert props["ComparisonOperator"] == "LessThanThreshold"
    assert props["Threshold"] == {"Ref": "ReleaseCountAlarmThreshold"}
    assert props["AlarmActions"] == [{"Ref": "AlertTopic"}]


def test_release_count_alarm_treats_a_missing_datapoint_as_missing(
    transformed_template: dict[str, Any],
) -> None:
    """An absent datapoint must not score as healthy.

    This is the property that decides whether the alarm detects anything, and
    the one most likely to be "fixed" back to match the sibling alarms. Those
    two count events, where an absent datapoint genuinely means zero; this one
    is a gauge, where absent means *the count could not be taken*. Both real
    failure modes produce that absence rather than a low number:
    ``cache_health_metrics.py`` runs ``COUNT(*) FROM release`` and raises
    ``UndefinedTable`` when a colliding rebuild has dropped the table mid-swap
    (#352), and every step of the publish chain in ``sync-library.yml`` is
    ``if: success()``. Under ``notBreaching`` the alarm rides through the
    incident in OK and — because ``OKActions`` is wired — sends a false
    all-clear the day after a real firing.

    ``Period`` and ``EvaluationPeriods`` are pinned alongside it: the series is
    published once daily, so a shorter period would leave most periods empty
    and make the missing-data policy, not the metric, decide the alarm state.
    """
    props = _cloudwatch_alarm(transformed_template, "ReleaseCountAlarm")
    assert props["TreatMissingData"] == "missing"
    assert props["Period"] == 86400
    assert props["EvaluationPeriods"] == 1
    assert props["OKActions"] == [{"Ref": "AlertTopic"}]


def test_release_count_alarm_is_not_anomaly_detection(
    source_template: dict[str, Any],
) -> None:
    """A static floor only — ``ANOMALY_DETECTION_BAND`` would need the org-account
    series to re-baseline over weeks before it's trustworthy (#358), and the
    account migration (#353) reset that history to nothing.
    """
    alarm = source_template["Resources"]["ReleaseCountAlarm"]["Properties"]
    assert alarm["ComparisonOperator"] == "LessThanThreshold"
    assert "Metrics" not in alarm
    assert "ThresholdMetricId" not in alarm


@pytest.mark.parametrize(
    "logical_id",
    ["LauncherErrorAlarm", "StaleInstanceAlarm", "ReleaseCountAlarm"],
)
def test_every_alarm_fans_into_the_shared_alert_topic(
    transformed_template: dict[str, Any], logical_id: str
) -> None:
    """``AlertTopic`` is the single fan-out point, so every alarm must reach it.

    An alarm wired to no topic — or to one created outside the stack — pages
    nobody, which is the wxyc-canary#13 failure mode restated: a resource that
    looks deployed and alerts into a void.
    """
    props = _cloudwatch_alarm(transformed_template, logical_id)
    assert props["AlarmActions"] == [{"Ref": "AlertTopic"}]
