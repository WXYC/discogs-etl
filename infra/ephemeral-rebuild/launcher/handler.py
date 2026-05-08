"""Launcher Lambda — fired by EventBridge once per month.

Calls EC2 RunInstances with a tiny user-data stub that clones discogs-etl
and execs scripts/rebuild-cache-bootstrap.sh. The bootstrap then takes
care of the heavy work (deps, secrets, pipeline run, log upload, shutdown).

The instance carries an InstanceProfile that grants it ssm:GetParameters on
${SSM_PREFIX}/* and s3:PutObject on the log bucket. The launch template
sets InstanceInitiatedShutdownBehavior=terminate so the instance releases
itself when bootstrap finishes (or panics into trap EXIT and runs
``shutdown -h now``).

Env vars (all required, set by template.yaml):
    LAUNCH_TEMPLATE_ID    LaunchTemplate id (lt-...)
    REPO_BRANCH           branch of WXYC/discogs-etl to clone (default: main)
    LOG_GROUP_NAME        ignored (CloudWatch Lambda log group is implicit)

Tags applied to the spawned instance:
    Project=discogs-rebuild
    Name=discogs-rebuild-<launch-timestamp>
    LaunchedBy=ephemeral-rebuild-launcher
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

# boto3 is available in the AWS Lambda runtime but is imported lazily so
# unit tests can inject a mock client without having boto3 installed.

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USER_DATA_TEMPLATE = """#!/usr/bin/env bash
set -euxo pipefail
exec > >(tee /var/log/cloud-init-bootstrap.log | logger -t bootstrap) 2>&1

dnf install -y --quiet git
git clone --depth 1 --branch {branch} https://github.com/WXYC/discogs-etl.git /opt/discogs-etl
exec /opt/discogs-etl/scripts/rebuild-cache-bootstrap.sh
"""


def build_user_data(branch: str) -> str:
    """Render the user-data stub. Kept tiny so changes to the heavy
    bootstrap don't require redeploying the Lambda."""
    return USER_DATA_TEMPLATE.format(branch=branch)


def lambda_handler(event, context, ec2_client=None):
    """Entry point. Boto3 client is injectable for unit tests."""
    branch = os.environ.get("REPO_BRANCH", "main")
    launch_template_id = os.environ["LAUNCH_TEMPLATE_ID"]

    user_data = build_user_data(branch)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%MZ")
    name_tag = f"discogs-rebuild-{timestamp}"

    if ec2_client is None:
        import boto3

        ec2_client = boto3.client("ec2")

    logger.info(
        "RunInstances LaunchTemplate=%s branch=%s name=%s", launch_template_id, branch, name_tag
    )

    response = ec2_client.run_instances(
        MinCount=1,
        MaxCount=1,
        LaunchTemplate={"LaunchTemplateId": launch_template_id, "Version": "$Latest"},
        UserData=user_data,
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Project", "Value": "discogs-rebuild"},
                    {"Key": "Name", "Value": name_tag},
                    {"Key": "LaunchedBy", "Value": "ephemeral-rebuild-launcher"},
                ],
            },
            {
                "ResourceType": "volume",
                "Tags": [
                    {"Key": "Project", "Value": "discogs-rebuild"},
                    {"Key": "Name", "Value": name_tag},
                ],
            },
        ],
    )

    instance_id = response["Instances"][0]["InstanceId"]
    logger.info("launched %s", instance_id)
    return {"instance_id": instance_id, "name": name_tag}
