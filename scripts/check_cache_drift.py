"""Watchdog: alert when the discogs-cache has drifted from the WXYC library.

The discogs-cache (Postgres) is rebuilt from a Discogs XML dump on a monthly
cadence (``rebuild-cache.yml``). Between rebuilds, new artists added to the
WXYC library catalog have no Discogs metadata in the cache. This script
quantifies that drift by comparing two distinct-artist counts:

* ``library`` -- ``SELECT COUNT(DISTINCT artist) FROM library`` against
  the SQLite library.db produced by ``scripts/sync-library.sh``.
* ``cache``   -- ``SELECT COUNT(DISTINCT artist_name) FROM release_artist``
  against the Postgres discogs-cache.

If the ratio ``cache / library`` falls below ``--min-ratio`` (default
``0.7``), the script emits a structured warning, optionally posts a Slack
notification (when ``--slack-webhook`` is provided), and exits non-zero so
the calling CI workflow surfaces the drift as a job failure.

Usage::

    python scripts/check_cache_drift.py \\
        --library-db /tmp/library.db \\
        --database-url $DATABASE_URL_DISCOGS \\
        --min-ratio 0.7 \\
        --slack-webhook "$SLACK_MONITORING_WEBHOOK"

The pure decision logic (``count_library_artists``, ``evaluate_drift``,
``post_slack_alert``, ``run``) is unit-tested in
``tests/unit/test_check_cache_drift.py``. The Postgres path
(``count_cache_artists``) is exercised end-to-end inside the rebuild
workflow itself.

The watchdog covers issue WXYC/discogs-etl#125's third acceptance criterion:
"a watchdog query exposes drift between rebuilds and fires a separate alert
when the ratio drops below a threshold."
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_MIN_RATIO = 0.7


@dataclass(frozen=True)
class DriftResult:
    """Outcome of a drift comparison.

    Attributes:
        ok: True when coverage meets or exceeds ``min_ratio``.
        ratio: ``cache_count / library_count``, or ``None`` when undefined
            (e.g. ``library_count == 0``).
        reason: Short human-readable explanation; populated when ``ok`` is
            False, empty string otherwise.
    """

    ok: bool
    ratio: float | None
    reason: str = ""


def count_library_artists(library_db_path: str) -> int:
    """Return the count of distinct artists in the SQLite library.db."""
    conn = sqlite3.connect(library_db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT artist) FROM library")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        conn.close()


def count_cache_artists(database_url: str) -> int:
    """Return the count of distinct artist_name values in release_artist.

    Imported lazily so unit tests that patch this function never need to
    import psycopg.
    """
    import psycopg  # local import: keeps the test module psycopg-free

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT artist_name) FROM release_artist")
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0


def evaluate_drift(*, library_count: int, cache_count: int, min_ratio: float) -> DriftResult:
    """Decide whether the cache covers enough of the library.

    Args:
        library_count: distinct artists in the WXYC library catalog.
        cache_count: distinct artist_name rows in the discogs-cache.
        min_ratio: the minimum acceptable ``cache_count / library_count``.

    Returns:
        A ``DriftResult``. ``ok`` is True when the ratio is defined and
        at or above ``min_ratio``.
    """
    if library_count <= 0:
        return DriftResult(
            ok=False,
            ratio=None,
            reason="library count is 0; cannot compute drift ratio.",
        )
    ratio = cache_count / library_count
    if ratio < min_ratio:
        return DriftResult(
            ok=False,
            ratio=ratio,
            reason=(
                f"cache/library coverage ratio {ratio:.3f} is below threshold"
                f" {min_ratio:.3f} (cache={cache_count}, library={library_count})."
            ),
        )
    return DriftResult(ok=True, ratio=ratio, reason="")


def post_slack_alert(*, webhook_url: str | None, message: str) -> bool:
    """Best-effort Slack notification.

    Returns True when a request was made and accepted, False on no-op
    (no webhook configured) or on transport error. Never raises.
    """
    if not webhook_url:
        return False
    payload = json.dumps({"text": f":warning: *Discogs cache drift*\n{message}"}).encode("utf-8")
    req = Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=10):
            return True
    except Exception as exc:  # pragma: no cover - logged for operators
        logger.warning("slack notify failed: %s", exc, extra={"step": "drift_check"})
        return False


def run(
    *,
    library_db: str,
    database_url: str,
    min_ratio: float,
    slack_webhook: str | None,
) -> int:
    """Compute drift and return a process exit code.

    Returns 0 when coverage is healthy, 1 when drift is detected (or the
    library count is unusable). Slack is notified only on the unhealthy
    branch.
    """
    library_count = count_library_artists(library_db)
    cache_count = count_cache_artists(database_url)
    logger.info(
        "drift counts: library=%d cache=%d",
        library_count,
        cache_count,
        extra={"step": "drift_check"},
    )
    result = evaluate_drift(
        library_count=library_count, cache_count=cache_count, min_ratio=min_ratio
    )
    if result.ok:
        logger.info(
            "cache coverage healthy (ratio=%.3f >= %.3f)",
            result.ratio if result.ratio is not None else float("nan"),
            min_ratio,
            extra={"step": "drift_check"},
        )
        return 0

    logger.warning(result.reason, extra={"step": "drift_check"})
    post_slack_alert(webhook_url=slack_webhook, message=result.reason)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "--library-db",
        required=True,
        help="Path to the SQLite library.db produced by sync-library.sh.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help=(
            "PostgreSQL URL for the discogs-cache. Falls back to "
            "DATABASE_URL_DISCOGS, then DATABASE_URL."
        ),
    )
    parser.add_argument(
        "--min-ratio",
        type=float,
        default=DEFAULT_MIN_RATIO,
        help=(f"Minimum acceptable cache/library coverage ratio. Default: {DEFAULT_MIN_RATIO}."),
    )
    parser.add_argument(
        "--slack-webhook",
        default=None,
        help=(
            "Slack incoming webhook URL. If unset, falls back to "
            "SLACK_MONITORING_WEBHOOK env. When neither is set, drift is "
            "logged but not posted."
        ),
    )
    args = parser.parse_args(argv)

    init_logger(repo="discogs-etl", tool="discogs-etl check_cache_drift")

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

    slack_webhook = args.slack_webhook or os.environ.get("SLACK_MONITORING_WEBHOOK")

    return run(
        library_db=args.library_db,
        database_url=database_url,
        min_ratio=args.min_ratio,
        slack_webhook=slack_webhook,
    )


if __name__ == "__main__":
    sys.exit(main())
