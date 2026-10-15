"""Measure the pinned releases one rebuild lost (discogs-etl#493).

A pinned override (``lml_cache.library_release_override``) with no ``release``
row is either a pin the rebuild lost (the #422 class) or a pin that was never
cached (#446's Draft or deleted pins, or a backlog). Only the first is an
incident. So the rebuild records, before its first destructive statement,
which pinned ids have a ``release`` row (not counting a Discogs-404 tombstone,
``not_found``: losing one costs a 404 re-fetch, not the #422 class), and checks
after the api_fetch restore which of those still-pinned ids have none.

1. :func:`record_pin_start_state` writes that start set to the
   ``release_pin_start`` scratch table. It joins the override table against
   ``release`` and never reads the allowlist file: the file holds every pin,
   including the never-cached ones.
2. :func:`measure_pin_loss` counts the start ids that are still pinned and now
   have no ``release`` row, then drops the table.

The table name is fixed, not per-invocation like ``lib.scratch_namespace``'s.
That is safe for the same reason ``*_api_fetch_snap`` is: only the rebuild
writes it, and the rebuild holds the 354001 advisory lock. A table left
behind by a run that crashed is measured and reported before it is replaced,
and a failed measurement aborts instead; a ``--resume`` run reuses it. This
module holds only the database logic. The threshold, the log level and the
Slack post live in ``scripts/run_pipeline.py::report_pin_loss``.

Read-only against ``lml_cache`` (CLAUDE.md "entity.* schema ownership").
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

import psycopg

logger = logging.getLogger(__name__)

START_TABLE = "release_pin_start"
# Bounds both DROPs: the start-of-run one must not stall a rebuild holding lock
# 354001, and the end-of-run one runs while every pipeline table is UNLOGGED.
LOCK_TIMEOUT = "30s"

_ABSENT = (psycopg.errors.UndefinedTable, psycopg.errors.InvalidSchemaName)


@dataclass(frozen=True)
class PinLoss:
    """The start-set pins with no ``release`` row now, out of those still pinned."""

    lost: list[int]
    at_start: int

    @property
    def share(self) -> float:
        return len(self.lost) / self.at_start if self.at_start else 0.0


def _exists(cur, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (table,))
    return cur.fetchone()[0] is not None


def _measure(cur) -> PinLoss:
    """Measure the start table without dropping it. Unexpected errors propagate.

    With no ``release`` table at all, every recorded id still pinned is lost.
    """
    if _exists(cur, "release"):
        lost = "array_agg(s.release_id ORDER BY s.release_id) FILTER (WHERE r.id IS NULL)"
        rows = f"{START_TABLE} s LEFT JOIN release r ON r.id = s.release_id"
    else:
        lost, rows = "array_agg(s.release_id ORDER BY s.release_id)", f"{START_TABLE} s"
    try:
        with cur.connection.transaction():
            cur.execute(
                f"SELECT count(*), coalesce({lost}, '{{}}') FROM {rows} WHERE s.release_id IN "
                "(SELECT discogs_release_id FROM lml_cache.library_release_override)"
            )
            at_start, lost_ids = cur.fetchone()
    except _ABSENT:
        logger.info("lml_cache.library_release_override not found; no pins to check (#493)")
        return PinLoss(lost=[], at_start=0)
    return PinLoss(lost=list(lost_ids), at_start=at_start)


def record_pin_start_state(
    db_url: str,
    *,
    report_previous: Callable[[PinLoss], None],
    reuse_existing: bool = False,
) -> None:
    """Record the pinned ids that have a ``release`` row, reporting a crashed run's loss first.

    A missing ``release`` table or override table/schema records an empty
    start set. Any other error propagates: nothing has been destroyed yet, so
    aborting here is safe. With *reuse_existing* (a ``--resume`` run) an
    existing table is kept as the start state, and a missing one is logged as
    unknown, so the check is skipped rather than measured against a cache the
    interrupted run already damaged.

    Otherwise a table left behind by an earlier run (one that crashed, or whose
    end-of-run check could not drop it) is measured and passed to
    *report_previous*, outside any transaction, before it is replaced, so a
    failure anywhere keeps it for the next run. A leftover that cannot be
    measured aborts the run. Lock waits are bounded by ``LOCK_TIMEOUT``.
    """
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(f"SET lock_timeout = '{LOCK_TIMEOUT}'")
        left_behind = _exists(cur, START_TABLE)
        if reuse_existing:
            if left_behind:
                logger.info("Reusing the pin start state the interrupted run recorded (#493)")
            else:
                logger.warning(
                    "Resumed run with no recorded pin start state: start state unknown, "
                    "so the pin-loss check is skipped this run (#493)"
                )
            return None
        if left_behind:
            logger.warning(
                "An earlier run left %s behind; measuring its loss first (#493)", START_TABLE
            )
            try:
                previous = _measure(cur)
            except Exception:
                logger.exception(
                    "Could not measure the loss recorded in the left-behind %s (error above); "
                    "aborting before anything is touched. Fix the cause and rerun: dropping "
                    "the table instead discards that loss unreported (#493)",
                    START_TABLE,
                )
                raise
            conn.commit()  # no locks held across the Slack post
            report_previous(previous)
        cur.execute(f"DROP TABLE IF EXISTS {START_TABLE}")
        cur.execute(f"CREATE TABLE {START_TABLE} (release_id integer PRIMARY KEY)")
        if not _exists(cur, "release"):
            logger.info("No release table yet; recording an empty pin start state (#493)")
            return
        try:
            with conn.transaction():
                cur.execute(
                    f"INSERT INTO {START_TABLE} SELECT DISTINCT o.discogs_release_id "
                    "FROM lml_cache.library_release_override o "
                    "JOIN release r ON r.id = o.discogs_release_id AND NOT r.not_found"
                )
        except _ABSENT:
            logger.info("lml_cache.library_release_override not found; no pins to record (#493)")
            return
        logger.info("Recorded %d pinned releases present at start (#493)", cur.rowcount)


def measure_pin_loss(db_url: str) -> PinLoss | None:
    """Return the recorded pins that are still pinned and have lost their row; drop the table.

    Returns ``None`` when no start state was recorded. A missing override
    table or schema reads as no loss; a missing ``release`` table loses every
    recorded pin. Never raises: the rows are already gone, and a failed check
    must not fail the rebuild. On an unexpected error the table is kept, so the
    next run reports it. A DROP that fails (bounded by ``LOCK_TIMEOUT``)
    still returns the loss, and the next run reports it a second time.
    """
    try:
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            if not _exists(cur, START_TABLE):
                logger.info("No pin start state recorded; skipping the pin-loss check (#493)")
                return None
            loss = _measure(cur)
    except Exception:
        logger.exception(
            "The pin-loss check failed; %s is kept for the next run (#493)", START_TABLE
        )
        return None
    try:
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
            cur.execute(f"DROP TABLE {START_TABLE}")
    except Exception:
        logger.exception(
            "Could not drop %s; the next run will report this loss again (#493)", START_TABLE
        )
    return loss
