"""Publish discogs-cache artwork-state counts to CloudWatch.

Emits three metrics under the ``WXYC/DiscogsCache`` namespace on every run:

* ``release_count`` — total rows in ``release``.
* ``artwork_never_asked_count`` — ``artwork_url IS NULL AND artwork_checked_at IS NULL``.
  Drainable: covered by LML#221's top-up script. Stagnation here is what we
  alarm on.
* ``artwork_imageless_count`` — ``artwork_url IS NULL AND artwork_checked_at IS NOT NULL``.
  Asked, genuinely no image — unfixable. Tracked so the alarm on
  ``never_asked`` doesn't mistake legitimate imageless rows for a regression.

The headline "% NULL artwork_url" metric from
[#241](https://github.com/WXYC/discogs-etl/issues/241) decomposes as
``(never_asked + imageless) / release_count``. Alarming on the never-asked
share alone — once #239's ``artwork_checked_at`` column is in place — is
strictly better than alarming on the union, because the imageless share is a
permanent floor that grows as LML's runtime path back-patches more rows.

Usage::

    python scripts/cache_health_metrics.py \\
        --database-url "$DATABASE_URL_DISCOGS" \\
        --namespace WXYC/DiscogsCache \\
        [--dry-run]

The unit tests in ``tests/unit/test_cache_health_metrics.py`` cover the
pure logic. The Postgres path is exercised in
``tests/integration/test_cache_health_metrics.py``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "WXYC/DiscogsCache"


@dataclass(frozen=True)
class ArtworkStates:
    """Triple of artwork-state counts read in one SQL round-trip."""

    total: int
    never_asked: int
    imageless: int


_COUNT_QUERY = """
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE artwork_url IS NULL AND artwork_checked_at IS NULL)
            AS never_asked,
        COUNT(*) FILTER (WHERE artwork_url IS NULL AND artwork_checked_at IS NOT NULL)
            AS imageless
    FROM release
"""


def count_artwork_states(database_url: str) -> ArtworkStates:
    """Return the three counts in one SQL round-trip.

    Imported psycopg lazily so the unit tests can patch ``sys.modules``
    before importing the script.
    """
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(_COUNT_QUERY)
            row = cur.fetchone()
    total, never_asked, imageless = row or (0, 0, 0)
    return ArtworkStates(
        total=int(total or 0),
        never_asked=int(never_asked or 0),
        imageless=int(imageless or 0),
    )


def build_metric_data(states: ArtworkStates) -> list[dict]:
    """Map an ``ArtworkStates`` onto the CloudWatch ``MetricData`` payload."""
    return [
        {"MetricName": "release_count", "Value": states.total, "Unit": "Count"},
        {"MetricName": "artwork_never_asked_count", "Value": states.never_asked, "Unit": "Count"},
        {"MetricName": "artwork_imageless_count", "Value": states.imageless, "Unit": "Count"},
    ]


def publish_metrics(
    *,
    client,
    namespace: str,
    states: ArtworkStates,
    dry_run: bool = False,
) -> None:
    """Send one ``put_metric_data`` call carrying all three metrics.

    Returns immediately without calling the client when ``dry_run`` is True.
    """
    if dry_run:
        logger.info(
            "dry-run: would publish to %s: %s",
            namespace,
            states,
            extra={"step": "cache_health"},
        )
        return
    client.put_metric_data(Namespace=namespace, MetricData=build_metric_data(states))


def run(
    *,
    database_url: str,
    cloudwatch_client,
    namespace: str,
    dry_run: bool,
) -> int:
    """Count, log, and publish. Returns a process exit code."""
    states = count_artwork_states(database_url)
    logger.info(
        "artwork states: total=%d never_asked=%d imageless=%d",
        states.total,
        states.never_asked,
        states.imageless,
        extra={"step": "cache_health"},
    )
    publish_metrics(client=cloudwatch_client, namespace=namespace, states=states, dry_run=dry_run)
    return 0


def _build_cloudwatch_client():
    """Lazy boto3 client construction so unit tests can patch this seam."""
    import boto3

    return boto3.client("cloudwatch")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "--database-url",
        default=None,
        help=(
            "PostgreSQL URL for the discogs-cache. Falls back to "
            "DATABASE_URL_DISCOGS, then DATABASE_URL."
        ),
    )
    parser.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help=f"CloudWatch metric namespace. Default: {DEFAULT_NAMESPACE}.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and log counts but skip the CloudWatch publish.",
    )
    args = parser.parse_args(argv)

    init_logger(repo="discogs-etl", tool="discogs-etl cache_health_metrics")

    database_url = (
        args.database_url
        or os.environ.get("DATABASE_URL_DISCOGS")
        or os.environ.get("DATABASE_URL")
    )
    if not database_url:
        print(
            "error: --database-url not provided and DATABASE_URL_DISCOGS/DATABASE_URL not set.",
            file=sys.stderr,
        )
        return 2

    # Skip boto3 client construction on --dry-run so the script stays
    # runnable in environments without boto3 (a [dev]-only dep) or AWS
    # credentials — matches the "skip the CloudWatch publish" semantic.
    cloudwatch_client = None if args.dry_run else _build_cloudwatch_client()
    return run(
        database_url=database_url,
        cloudwatch_client=cloudwatch_client,
        namespace=args.namespace,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
