"""Unit tests for the ephemeral-rebuild sweeper Lambda.

The sweeper is the failsafe for the bootstrap-crashed-before-shutdown
case. EventBridge fires it hourly; it lists running instances tagged
Project=discogs-rebuild, terminates any older than ``MAX_INSTANCE_AGE_HOURS``
(default 3), and emits a ``StaleInstanceTerminated`` CloudWatch metric so
operators get a Slack/email page when the failsafe trips.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HANDLER_PATH = REPO_ROOT / "infra" / "ephemeral-rebuild" / "sweeper" / "handler.py"


def _load_handler():
    spec = importlib.util.spec_from_file_location("ephemeral_rebuild_sweeper", HANDLER_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ephemeral_rebuild_sweeper"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def handler_module():
    return _load_handler()


def _now():
    return datetime.now(timezone.utc)


def _make_ec2(reservations):
    """Build a MagicMock ec2 client whose paginator yields ``reservations``.

    ``reservations`` is a list of dicts shaped like the ``Reservations``
    item in DescribeInstances output.
    """
    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = iter([{"Reservations": reservations}])
    client.get_paginator.return_value = paginator
    return client


def test_no_running_instances_is_a_no_op(handler_module, monkeypatch):
    monkeypatch.setenv("MAX_INSTANCE_AGE_HOURS", "3")
    ec2 = _make_ec2(reservations=[])
    cw = MagicMock()

    result = handler_module.lambda_handler({}, None, ec2_client=ec2, cloudwatch_client=cw)

    assert result == {"terminated": []}
    ec2.terminate_instances.assert_not_called()
    cw.put_metric_data.assert_not_called()


def test_recent_instance_is_left_alone(handler_module, monkeypatch):
    monkeypatch.setenv("MAX_INSTANCE_AGE_HOURS", "3")
    ec2 = _make_ec2(
        reservations=[
            {
                "Instances": [
                    {
                        "InstanceId": "i-young",
                        "LaunchTime": _now() - timedelta(minutes=30),
                    }
                ]
            }
        ]
    )
    cw = MagicMock()

    result = handler_module.lambda_handler({}, None, ec2_client=ec2, cloudwatch_client=cw)

    assert result == {"terminated": []}
    ec2.terminate_instances.assert_not_called()


def test_old_instance_is_terminated_and_metric_emitted(handler_module, monkeypatch):
    monkeypatch.setenv("MAX_INSTANCE_AGE_HOURS", "3")
    ec2 = _make_ec2(
        reservations=[
            {
                "Instances": [
                    {
                        "InstanceId": "i-stale",
                        "LaunchTime": _now() - timedelta(hours=4),
                    }
                ]
            }
        ]
    )
    cw = MagicMock()

    result = handler_module.lambda_handler({}, None, ec2_client=ec2, cloudwatch_client=cw)

    assert result == {"terminated": ["i-stale"]}
    ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-stale"])

    cw.put_metric_data.assert_called_once()
    metric_kwargs = cw.put_metric_data.call_args.kwargs
    assert metric_kwargs["Namespace"] == "WXYC/DiscogsRebuild"
    assert metric_kwargs["MetricData"][0]["MetricName"] == "StaleInstanceTerminated"
    assert metric_kwargs["MetricData"][0]["Value"] == 1.0


def test_describe_filters_to_project_and_running_state(handler_module, monkeypatch):
    monkeypatch.setenv("MAX_INSTANCE_AGE_HOURS", "3")
    ec2 = _make_ec2(reservations=[])

    handler_module.lambda_handler({}, None, ec2_client=ec2, cloudwatch_client=MagicMock())

    paginator_paginate = ec2.get_paginator.return_value.paginate
    paginator_paginate.assert_called_once()
    filters = paginator_paginate.call_args.kwargs["Filters"]
    assert {"Name": "tag:Project", "Values": ["discogs-rebuild"]} in filters
    state_filter = next(f for f in filters if f["Name"] == "instance-state-name")
    assert set(state_filter["Values"]) == {"running", "pending"}


def test_mixed_old_and_young_only_terminates_old(handler_module, monkeypatch):
    monkeypatch.setenv("MAX_INSTANCE_AGE_HOURS", "3")
    ec2 = _make_ec2(
        reservations=[
            {
                "Instances": [
                    {"InstanceId": "i-young", "LaunchTime": _now() - timedelta(minutes=30)},
                    {"InstanceId": "i-stale-1", "LaunchTime": _now() - timedelta(hours=5)},
                    {"InstanceId": "i-stale-2", "LaunchTime": _now() - timedelta(hours=12)},
                ]
            }
        ]
    )

    result = handler_module.lambda_handler({}, None, ec2_client=ec2, cloudwatch_client=MagicMock())

    assert result == {"terminated": ["i-stale-1", "i-stale-2"]}
    ec2.terminate_instances.assert_called_once_with(InstanceIds=["i-stale-1", "i-stale-2"])


def test_max_age_hours_env_override(handler_module, monkeypatch):
    """A 1h override should sweep instances that a 3h default leaves alone."""
    monkeypatch.setenv("MAX_INSTANCE_AGE_HOURS", "1")
    ec2 = _make_ec2(
        reservations=[
            {
                "Instances": [
                    {"InstanceId": "i-1h-old", "LaunchTime": _now() - timedelta(hours=2)},
                ]
            }
        ]
    )

    result = handler_module.lambda_handler({}, None, ec2_client=ec2, cloudwatch_client=MagicMock())

    assert result == {"terminated": ["i-1h-old"]}
