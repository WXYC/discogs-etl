#!/usr/bin/env python3
"""Record the monthly rebuild's terminal outcome where a human can find it.

On 2026-09-04 the rebuild aborted at 06:48:41 UTC and nobody learned for 14
days. Every alerting path was open-circuit at once: ``SLACK_MONITORING_WEBHOOK``
was unset, so every ``notify_slack`` call in ``rebuild-cache.sh`` was a bare
``return 0``; the drift watchdog never ran because the pipeline died before it;
and the only durable traces were three Sentry issues raised by the *pipeline's*
own Python (DISCOGS-ETL-4/-5/-6), which no alert rule watched, plus a leftover
``_keep_ids_<suffix>`` scratch table that the eventual audit had to reason
backwards from. The shell that owns the rebuild could not reach a person at all,
and a successful run and an aborted one left the same artifacts behind.

``rebuild-cache.sh`` calls this on every terminal path. It does two things:

1. **Writes ``<log-dir>/99-outcome.txt``.** ``rebuild-cache-bootstrap.sh``'s
   ``trap EXIT`` syncs the whole log directory to S3, so the outcome becomes
   visible in a bucket *listing* — no log-reading required — and it sorts after
   the ``00-started.txt`` breadcrumb. **No ``99-outcome.txt`` at all** means the
   script never reached a terminal path (SIGKILL, host loss, or a failure before
   the venv existed), which is itself a distinct and readable state.
2. **Emits the outcome on the JSON logger.** ``lib.observability.init_logger``
   forwards ``logger.error`` to Sentry when ``SENTRY_DSN`` is set, which it is
   on the rebuild host — the one channel proven to carry from that instance.
   A failed or refused run logs at ERROR (so it raises a Sentry issue tagged
   ``step=rebuild_outcome``, targetable by an alert rule); a clean finish logs
   at INFO and raises nothing.

Slack is deliberately *not* posted from here: ``rebuild-cache.sh`` already
posts on each of these paths, and a second post would double every alert.

**The marker write is stdlib-only and never depends on the venv.** The logger
import is deferred into :func:`init_logging_best_effort` and its failure is
swallowed, because the earliest terminal path in ``rebuild-cache.sh`` — the
flock bow-out — fires roughly twenty lines ABOVE
``source "$REPO_DIR/.venv/bin/activate"``, where ``lib.observability`` (and the
``wxyc_etl`` / ``sentry_sdk`` it pulls in) is not importable. A module-level
import would therefore make a same-host double-tick, the single most likely
benign bow-out, the one terminal path that reports nothing — and under this
script's own contract, nothing means "the host was lost". Losing the Sentry leg
there costs nothing real: a bow-out logs at INFO and raises no issue anyway.

A note on reading the S3 listing: ``rebuild-cache-bootstrap.sh`` copies its
``00-started.txt`` breadcrumb under the ``$LAUNCH_ID`` prefix before IMDS is
reachable, then replaces ``INSTANCE_ID`` with the real instance id, so its
``trap EXIT`` recursive sync (which is what carries this marker) lands under
``i-…/``. Both files appear together under whichever prefix the trap used; the
only orphan is that early duplicate breadcrumb under ``bootstrap-…/``, which
has no outcome sibling by construction. Do not read *that* prefix as a lost
host.

Usage::

    python scripts/report_rebuild_outcome.py --status failed \\
        --detail "line 445 (exit 1)" --log-dir /var/log/discogs-rebuild \\
        --run-log /var/log/discogs-rebuild/2026-10-04T0600Z.log
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)

OUTCOME_FILENAME = "99-outcome.txt"

# ``bowed_out`` is a *correct* no-op run (a peer held the flock or the #354
# advisory lock), not a failure — incident #352 is the standing reminder that
# conflating the two is what makes a silent non-rebuild look like a success.
# ``smoke_ok`` is the ``REBUILD_SMOKE=1`` path, which exits 0 before writing
# anything to the cache: not a rebuild that succeeded, not one that failed, and
# not allowed to stay silent either — a silent smoke run would forge the
# "no marker means the host was lost" signal.
STATUSES = ("success", "failed", "bowed_out", "smoke_ok")
FAILURE_STATUSES = ("failed",)


def init_logging_best_effort() -> bool:
    """Wire up the JSON/Sentry logger if it is importable; report whether it was.

    Deferred and swallowed on purpose — see the module docstring. Returns True
    when the real logger is active, False when this process is marker-only.
    """
    try:
        from lib.observability import init_logger
    except Exception as exc:  # pragma: no cover - exercised via subprocess
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        print(
            f"[report_rebuild_outcome] logger unavailable ({exc!r}); writing the "
            "marker only, with no Sentry leg",
            file=sys.stderr,
        )
        return False
    # Sentry is initialized from SENTRY_DSN inside the shim; flushed at exit by
    # wxyc_etl.logger's atexit hook, which matters here because the process
    # exits within milliseconds of the capture.
    init_logger(repo="discogs-etl", tool="discogs-etl report_rebuild_outcome")
    return True


def render_outcome(*, status: str, detail: str, run_log: str, utc_now: str) -> str:
    """Marker body: ``key=value`` per line, grep-able and diff-able."""
    lines = [f"outcome={status}", f"utc={utc_now}"]
    if detail:
        lines.append(f"detail={detail}")
    if run_log:
        lines.append(f"log={run_log}")
    return "\n".join(lines) + "\n"


def write_outcome(log_dir: Path, body: str) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / OUTCOME_FILENAME
    path.write_text(body, encoding="utf-8")
    return path


def run(*, status: str, detail: str, log_dir: Path, run_log: str) -> int:
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = render_outcome(status=status, detail=detail, run_log=run_log, utc_now=utc_now)
    path = write_outcome(log_dir, body)
    message = "discogs-cache rebuild outcome: %s (%s)"
    args = (status, detail or "no detail")
    extra = {"step": "rebuild_outcome", "outcome": status, "outcome_marker": str(path)}
    if status in FAILURE_STATUSES:
        logger.error(message, *args, extra=extra)
    else:
        logger.info(message, *args, extra=extra)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--status", required=True, choices=STATUSES)
    parser.add_argument(
        "--detail", default="", help="Short human-readable cause, e.g. 'line 445 (exit 1)'."
    )
    parser.add_argument(
        "--log-dir", type=Path, required=True, help="Rebuild log directory; the marker lands here."
    )
    parser.add_argument(
        "--run-log", default="", help="Path of this run's log file, for the marker."
    )
    args = parser.parse_args(argv)

    init_logging_best_effort()

    return run(status=args.status, detail=args.detail, log_dir=args.log_dir, run_log=args.run_log)


if __name__ == "__main__":
    sys.exit(main())
