"""Unit tests for the ephemeral-rebuild launcher Lambda.

The launcher is fired by EventBridge once per month. It calls EC2
RunInstances with a tiny user-data stub that clones discogs-etl and
execs ``scripts/rebuild-cache-bootstrap.sh`` on the spawned instance.

These tests pin the contract that:
  * RunInstances is called against the launch-template id from env.
  * The user-data stub clones the configured branch.
  * The instance is tagged Project=discogs-rebuild so the sweeper can
    see it and so the spawned instance's IAM scope (kms:Decrypt /
    ec2:TerminateInstances tag conditions) match.
  * Volumes are tagged too — billing reports group by tag and EBS for
    these instances should be attributable to the rebuild project.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HANDLER_PATH = REPO_ROOT / "infra" / "ephemeral-rebuild" / "launcher" / "handler.py"


def _load_handler():
    spec = importlib.util.spec_from_file_location("ephemeral_rebuild_launcher", HANDLER_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ephemeral_rebuild_launcher"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def handler_module():
    return _load_handler()


@pytest.fixture
def fake_ec2(handler_module):
    client = MagicMock()
    client.run_instances.return_value = {"Instances": [{"InstanceId": "i-0123456789abcdef0"}]}
    return client


def test_user_data_stub_clones_configured_branch(handler_module):
    rendered = handler_module.build_user_data("main")
    assert "git clone --depth 1 --branch main https://github.com/WXYC/discogs-etl.git" in rendered
    assert "exec /opt/discogs-etl/scripts/rebuild-cache-bootstrap.sh" in rendered
    assert rendered.startswith("#!/usr/bin/env bash")
    assert "set -euxo pipefail" in rendered


def test_user_data_stub_respects_repo_branch_override(handler_module):
    rendered = handler_module.build_user_data("feature/x")
    assert "--branch feature/x" in rendered


def test_lambda_handler_calls_run_instances(handler_module, fake_ec2, monkeypatch):
    monkeypatch.setenv("LAUNCH_TEMPLATE_ID", "lt-0fab0123456789ab0")
    monkeypatch.setenv("REPO_BRANCH", "main")

    result = handler_module.lambda_handler({}, None, ec2_client=fake_ec2)

    fake_ec2.run_instances.assert_called_once()
    kwargs = fake_ec2.run_instances.call_args.kwargs

    assert kwargs["LaunchTemplate"] == {
        "LaunchTemplateId": "lt-0fab0123456789ab0",
        "Version": "$Latest",
    }
    assert kwargs["MinCount"] == 1
    assert kwargs["MaxCount"] == 1

    user_data = kwargs["UserData"]
    assert "exec /opt/discogs-etl/scripts/rebuild-cache-bootstrap.sh" in user_data

    assert result["instance_id"] == "i-0123456789abcdef0"
    assert result["name"].startswith("discogs-rebuild-")


def test_lambda_handler_tags_instance_and_volumes(handler_module, fake_ec2, monkeypatch):
    monkeypatch.setenv("LAUNCH_TEMPLATE_ID", "lt-0fab0123456789ab0")

    handler_module.lambda_handler({}, None, ec2_client=fake_ec2)

    tag_specs = fake_ec2.run_instances.call_args.kwargs["TagSpecifications"]
    by_resource = {ts["ResourceType"]: ts for ts in tag_specs}

    # The sweeper Lambda's TerminateInstances grant is conditioned on
    # ec2:ResourceTag/Project=discogs-rebuild. If this tag drifts the
    # sweeper silently no-ops.
    instance_tags = {t["Key"]: t["Value"] for t in by_resource["instance"]["Tags"]}
    assert instance_tags["Project"] == "discogs-rebuild"
    assert instance_tags["Name"].startswith("discogs-rebuild-")
    assert instance_tags["LaunchedBy"] == "ephemeral-rebuild-launcher"

    volume_tags = {t["Key"]: t["Value"] for t in by_resource["volume"]["Tags"]}
    assert volume_tags["Project"] == "discogs-rebuild"


def test_lambda_handler_uses_default_branch_when_unset(handler_module, fake_ec2, monkeypatch):
    monkeypatch.setenv("LAUNCH_TEMPLATE_ID", "lt-0fab0123456789ab0")
    monkeypatch.delenv("REPO_BRANCH", raising=False)

    handler_module.lambda_handler({}, None, ec2_client=fake_ec2)

    user_data = fake_ec2.run_instances.call_args.kwargs["UserData"]
    assert "--branch main" in user_data


def test_lambda_handler_raises_without_launch_template(handler_module, fake_ec2, monkeypatch):
    monkeypatch.delenv("LAUNCH_TEMPLATE_ID", raising=False)

    with pytest.raises(KeyError):
        handler_module.lambda_handler({}, None, ec2_client=fake_ec2)
