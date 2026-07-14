"""Sweeper Lambda — terminates rebuild EC2s that outlived the budget.

Runs hourly. Lists every running instance tagged ``Project=discogs-rebuild``
and force-terminates any that has been running longer than
``MAX_INSTANCE_AGE_HOURS`` (default 3). A clean rebuild finishes inside
~90 minutes; anything past 3 hours is either a wedged bootstrap or a
forgotten manual launch — either way, billing should stop.

Emits the ``StaleInstanceTerminated`` CloudWatch metric (namespace
``WXYC/DiscogsRebuild``) once per terminated instance so the
``DiscogsRebuildStaleInstance`` alarm can page operators.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

# boto3 is available in the AWS Lambda runtime but is imported lazily so
# unit tests can inject a mock client without having boto3 installed.

logger = logging.getLogger()
logger.setLevel(logging.INFO)

METRIC_NAMESPACE = "WXYC/DiscogsRebuild"


def _stale_cutoff(max_age_hours: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=max_age_hours)


def list_active_rebuild_instances(ec2_client):
    """Return every rebuild EC2 currently ``pending`` or ``running``.

    This is the same state filter the launcher's collision guard (#304)
    applies: the launcher asks "is any rebuild in flight?", the sweeper
    additionally applies an age cutoff (``list_stale_instances``). The
    launcher ships as a separate deploy package and keeps a mirror of this
    query — keep the tag/state filter in sync with ``launcher/handler.py``.
    Each element is ``{"InstanceId": str, "LaunchTime": datetime}``.
    """
    paginator = ec2_client.get_paginator("describe_instances")
    pages = paginator.paginate(
        Filters=[
            {"Name": "tag:Project", "Values": ["discogs-rebuild"]},
            {"Name": "instance-state-name", "Values": ["running", "pending"]},
        ],
    )
    active = []
    for page in pages:
        for reservation in page.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                active.append(
                    {"InstanceId": instance["InstanceId"], "LaunchTime": instance["LaunchTime"]}
                )
    return active


def list_stale_instances(ec2_client, max_age_hours: float):
    """Return running rebuild EC2s older than the cutoff."""
    cutoff = _stale_cutoff(max_age_hours)
    return [i for i in list_active_rebuild_instances(ec2_client) if i["LaunchTime"] < cutoff]


def lambda_handler(event, context, ec2_client=None, cloudwatch_client=None):
    """Entry point. Boto3 clients are injectable for unit tests."""
    max_age_hours = float(os.environ.get("MAX_INSTANCE_AGE_HOURS", "3"))

    if ec2_client is None or cloudwatch_client is None:
        import boto3

        if ec2_client is None:
            ec2_client = boto3.client("ec2")
        if cloudwatch_client is None:
            cloudwatch_client = boto3.client("cloudwatch")

    stale = list_stale_instances(ec2_client, max_age_hours)
    if not stale:
        logger.info("no stale rebuild instances (cutoff %.1f h)", max_age_hours)
        return {"terminated": []}

    instance_ids = [s["InstanceId"] for s in stale]
    logger.warning("terminating stale rebuild instances: %s", instance_ids)
    ec2_client.terminate_instances(InstanceIds=instance_ids)

    cloudwatch_client.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": "StaleInstanceTerminated",
                "Value": float(len(instance_ids)),
                "Unit": "Count",
            }
        ],
    )

    return {"terminated": instance_ids}
