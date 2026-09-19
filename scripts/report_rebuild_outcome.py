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
   the ``00-started.txt`` breadcrumb.

   The canonical marker describes **the run that holds the lock**, and only that
   run. It stamps ``outcome=running`` the moment it acquires the lock, so the
   three readings are total and unambiguous: **no marker at all** means no run
   ever got as far as acquiring the lock; **``outcome=running``** means one did
   and never reached a terminal path (SIGKILL, OOM, host loss); anything else is
   that run's real outcome. A bystander tick that loses the flock files its
   outcome under its own ``--marker-name`` instead, because ``$LOG_DIR`` — and
   the bootstrap's S3 prefix — are shared with the live run, so a bystander
   writing here would let its benign ``bowed_out`` stand in for a killed run's
   fate. That is the #352 false-signal class, pointed the other way.
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
# ``running`` is stamped by the run that OWNS the lock, the moment it acquires
# it, so the canonical marker always describes the lock holder. It makes the
# readings total: no marker at all means no run ever got as far as acquiring the
# lock; ``running`` means one did and never reached a terminal path (SIGKILL,
# OOM, host loss); anything else is that run's real outcome. It also overwrites
# a stale marker from a previous run, which the script's log trim would not have
# removed -- that matches only ``*.log``.
STATUSES = ("success", "failed", "bowed_out", "smoke_ok", "running")
FAILURE_STATUSES = ("failed",)


def init_logging_best_effort() -> bool:
    """Wire up the JSON/Sentry logger if it is importable; report whether it was.

    Deferred and swallowed on purpose — see the module docstring. Returns True
    when the real logger is active, False when this process is marker-only.
    """
    try:
        from lib.observability import init_logger

        # Inside the try, NOT after it. Guarding only the import would leave the
        # larger hazard open: on a host where lib.observability imports
        # perfectly, a malformed or unreachable SENTRY_DSN, a sentry_sdk.init
        # failure, or any error inside wxyc_etl.logger raises from THIS call. It
        # would escape main() before run() ever wrote the marker, while
        # report_outcome's `|| true` in rebuild-cache.sh swallowed the non-zero
        # exit -- so a run that terminated cleanly would read in the S3 listing
        # as "never reached a terminal path". The reporter would forge the exact
        # signal it exists to provide.
        #
        # Sentry is initialized from SENTRY_DSN inside the shim; flushed at exit
        # by wxyc_etl.logger's atexit hook, which matters here because the
        # process exits within milliseconds of the capture.
        init_logger(repo="discogs-etl", tool="discogs-etl report_rebuild_outcome")
    except Exception as exc:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        print(
            f"[report_rebuild_outcome] logger unavailable ({exc!r}); writing the "
            "marker only, with no Sentry leg",
            file=sys.stderr,
        )
        return False
    return True


def render_outcome(*, status: str, detail: str, run_log: str, utc_now: str) -> str:
    """Marker body: ``key=value`` per line, grep-able and diff-able."""
    lines = [f"outcome={status}", f"utc={utc_now}"]
    if detail:
        lines.append(f"detail={detail}")
    if run_log:
        lines.append(f"log={run_log}")
    return "\n".join(lines) + "\n"


def write_outcome(log_dir: Path, body: str, filename: str = OUTCOME_FILENAME) -> Path:
    """Write the marker. ``filename`` is overridable so a bow-out can file its
    own outcome WITHOUT clobbering the canonical marker of the run that holds
    the lock -- see ``--marker-name``."""
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / filename
    path.write_text(body, encoding="utf-8")
    return path


def run(
    *, status: str, detail: str, log_dir: Path, run_log: str, marker_name: str = OUTCOME_FILENAME
) -> int:
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = render_outcome(status=status, detail=detail, run_log=run_log, utc_now=utc_now)
    # The marker and the logger are two independent channels on purpose, so
    # neither one failing may take the other down with it. A full or unwritable
    # $LOG_DIR is exactly the kind of host trouble worth a Sentry issue, and
    # letting the OSError escape would instead lose the ERROR as well -- with
    # report_outcome's `|| true` hiding both.
    path: Path | None = None
    write_error: OSError | None = None
    try:
        path = write_outcome(log_dir, body, marker_name)
    except OSError as exc:
        write_error = exc
    message = "discogs-cache rebuild outcome: %s (%s)"
    args = (status, detail or "no detail")
    extra = {
        "step": "rebuild_outcome",
        "outcome": status,
        "outcome_marker": str(path) if path is not None else "",
    }
    if status in FAILURE_STATUSES:
        logger.error(message, *args, extra=extra)
    else:
        logger.info(message, *args, extra=extra)
    if write_error is not None:
        # Its own ERROR, so this raises a Sentry issue even when the outcome
        # itself was benign: the S3 listing is about to be misleading (no marker,
        # which reads as a lost host) and that is worth knowing about.
        logger.error(
            "could not write the rebuild outcome marker to %s: %r",
            log_dir,
            write_error,
            extra={"step": "rebuild_outcome", "outcome": status, "marker_write_failed": True},
        )
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
    parser.add_argument(
        "--marker-name",
        default=OUTCOME_FILENAME,
        help="Marker filename. Override it so a bow-out files its own outcome "
        "without clobbering the canonical marker of the run holding the lock.",
    )
    args = parser.parse_args(argv)

    init_logging_best_effort()

    return run(
        status=args.status,
        detail=args.detail,
        log_dir=args.log_dir,
        run_log=args.run_log,
        marker_name=args.marker_name,
    )


if __name__ == "__main__":
    sys.exit(main())
