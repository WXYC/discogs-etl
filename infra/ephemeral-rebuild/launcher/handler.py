"""Launcher Lambda — fired by EventBridge once per month.

Calls EC2 RunInstances with a tiny user-data stub that clones discogs-etl
and execs scripts/rebuild-cache-bootstrap.sh. The bootstrap then takes
care of the heavy work (deps, secrets, pipeline run, log upload, shutdown).

Before launching, it prechecks for an already-running rebuild (#304): two
rebuilds against the shared Railway cache DB deadlock on it. If a rebuild
instance is already pending/running the launcher aborts cleanly (emitting
the LaunchCollisionAborted metric) rather than starting a colliding one.

The instance carries an InstanceProfile that grants it ssm:GetParameters on
${SSM_PREFIX}/* and s3:PutObject on the log bucket. The launch template
sets InstanceInitiatedShutdownBehavior=terminate so the instance releases
itself when bootstrap finishes (or panics into trap EXIT and runs
``shutdown -h now``).

Env vars (set by template.yaml):
    LAUNCH_TEMPLATE_ID    LaunchTemplate id (lt-...) — required
    REPO_BRANCH           branch of WXYC/discogs-etl to clone (default: main)
    LOG_BUCKET_NAME       S3 bucket where the bootstrap uploads its log
                          archive + breadcrumb. Injected as
                          REBUILD_LOG_BUCKET in the user-data stub.

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

METRIC_NAMESPACE = "WXYC/DiscogsRebuild"

USER_DATA_TEMPLATE = """#!/usr/bin/env bash
set -euxo pipefail
exec > >(tee /var/log/cloud-init-bootstrap.log | logger -t bootstrap) 2>&1

# Plumbed from the launcher Lambda's env. The bootstrap reads
# REBUILD_LOG_BUCKET to know where to drop its breadcrumb (#174) and the
# trap-EXIT log archive (#173). Empty value falls through to the
# script's "WARN: REBUILD_LOG_BUCKET unset; skipping S3 breadcrumb" path.
export REBUILD_LOG_BUCKET={log_bucket}

dnf install -y --quiet git
git clone --depth 1 --branch {branch} https://github.com/WXYC/discogs-etl.git /opt/discogs-etl
exec /opt/discogs-etl/scripts/rebuild-cache-bootstrap.sh
"""


def build_user_data(branch: str, log_bucket: str = "") -> str:
    """Render the user-data stub. Kept tiny so changes to the heavy
    bootstrap don't require redeploying the Lambda. ``log_bucket`` is
    injected as ``REBUILD_LOG_BUCKET``; empty string is allowed and lets
    the bootstrap fall through to its skip-breadcrumb path."""
    return USER_DATA_TEMPLATE.format(branch=branch, log_bucket=log_bucket)


def list_active_rebuild_instances(ec2_client):
    """Return instance IDs of rebuild EC2s currently ``pending`` or ``running``.

    Mirrors the sweeper's ``list_active_rebuild_instances`` state filter
    (``tag:Project=discogs-rebuild`` + ``pending``/``running``). The two
    Lambdas ship as separate deploy packages, so the query is intentionally
    duplicated here rather than imported; keep the tag/state filter in sync
    with ``sweeper/handler.py``.
    """
    paginator = ec2_client.get_paginator("describe_instances")
    pages = paginator.paginate(
        Filters=[
            {"Name": "tag:Project", "Values": ["discogs-rebuild"]},
            {"Name": "instance-state-name", "Values": ["running", "pending"]},
        ],
    )
    return [
        instance["InstanceId"]
        for page in pages
        for reservation in page.get("Reservations", [])
        for instance in reservation.get("Instances", [])
    ]


def _emit_collision_metric(cloudwatch_client=None):
    """Best-effort ``LaunchCollisionAborted`` emit — never raises.

    The collision is already handled by returning without launching; a
    CloudWatch failure (throttle, or a not-yet-propagated PutMetricData
    grant) must not turn that clean abort into an exception, which would
    trip LauncherErrorAlarm and trigger EventBridge async retries that
    relaunch the instance the guard just suppressed. Losing the metric is
    the acceptable failure here; relaunching is not.
    """
    try:
        if cloudwatch_client is None:
            import boto3

            cloudwatch_client = boto3.client("cloudwatch")
        cloudwatch_client.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[{"MetricName": "LaunchCollisionAborted", "Value": 1.0, "Unit": "Count"}],
        )
    except Exception:
        logger.exception("failed to emit LaunchCollisionAborted metric; aborting launch anyway")


def lambda_handler(event, context, ec2_client=None, cloudwatch_client=None):
    """Entry point. Boto3 clients are injectable for unit tests."""
    branch = os.environ.get("REPO_BRANCH", "main")
    launch_template_id = os.environ["LAUNCH_TEMPLATE_ID"]
    log_bucket = os.environ.get("LOG_BUCKET_NAME", "")

    if ec2_client is None:
        import boto3

        ec2_client = boto3.client("ec2")

    # Collision guard (#304). Two rebuilds against the shared Railway cache DB
    # deadlock on it (2026-07-06 #298 recovery cost several hours). If a
    # rebuild is already pending/running, abort *cleanly* — do NOT raise: a
    # raise trips LauncherErrorAlarm and EventBridge's async retries would
    # relaunch the very instance we're trying to suppress. Instead log, emit
    # the LaunchCollisionAborted metric, and return. Instance state is the
    # lease; the >3h sweeper is its TTL, so a crashed instance can't wedge
    # future rebuilds. Checked before rendering user-data so it's a cheap
    # fail-fast. (TOCTOU note: DescribeInstances isn't atomic, so two launches
    # inside the pending/propagation window can still both proceed — this is a
    # front-door check, not a lock.)
    active = list_active_rebuild_instances(ec2_client)
    if active:
        logger.warning(
            "LaunchCollisionAborted: rebuild already in flight %s; not launching a second instance",
            active,
        )
        _emit_collision_metric(cloudwatch_client)
        return {"aborted": True, "active_instances": active}

    user_data = build_user_data(branch, log_bucket=log_bucket)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%MZ")
    name_tag = f"discogs-rebuild-{timestamp}"

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
