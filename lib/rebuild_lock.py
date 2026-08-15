"""Session-level PostgreSQL advisory lock guarding the discogs-cache rebuild
against concurrent execution (discogs-etl#354, incident #352).

The EC2-level guard added in #311 (``abort_if_not_winning_rebuild`` in
``scripts/rebuild-cache-bootstrap.sh``) depends on ``ec2:DescribeInstances``,
an IAM permission that can be silently absent in a given AWS account -- which
is exactly what happened on 2026-08-04: a rebuild in an account that predated
the guard's IAM grant ran unchecked against the shared Railway cache at the
same time as a rebuild in the org account, destroying 27,163 releases (#352).
``aws ec2 describe-instances`` is also account-scoped by construction, so it
can never see a peer in a *different* AWS account no matter how the IAM is
fixed.

This module takes the mutual-exclusion question to the one resource every
rebuild instance -- in any account, any region, any launch path -- provably
shares: the destination Postgres database itself. A session-level advisory
lock (``pg_try_advisory_lock`` / ``pg_advisory_unlock``, NOT the transaction-
scoped ``pg_advisory_xact_lock`` family -- the rebuild spans many
transactions, and a transaction-scoped lock would release between pipeline
steps) needs no IAM at all, so it cannot be disarmed by stack drift, and its
scope exactly matches the hazard: two writers touching the same database.

This is *in addition to*, not a replacement for, the #311 EC2 guard --
defence in depth. The EC2 guard runs earlier (before any setup work, before
the converter is even built) and catches most collisions cheaply; this lock
is the backstop that cannot be silently disarmed and that also covers launch
paths the EC2 guard misses entirely (e.g. the manual ``RunInstances`` path
used during the #298 recovery).

Scope note: PostgreSQL session advisory locks are scoped **per-database**,
not cluster-wide -- ``pg_try_advisory_lock(k)`` in one database never
conflicts with the same key held in a different database on the same
Postgres server (verified empirically against this repo's docker-compose
Postgres image while designing this module). That is exactly the property
needed here: the hazard is two rebuilds writing the *same* database, and two
unrelated databases on a shared server (e.g. discogs-cache next to some other
service's database) must not serialize against each other just because they
happen to share a Postgres instance.

Lock key: registered in this repo's ``CLAUDE.md`` under "Advisory lock keys
(shared-PG registry)" -- see ``REBUILD_LOCK_KEY`` below. Register any new key
there before shipping; an accidental collision silently serializes (or
deadlocks) unrelated work across services with no error to debug from.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import NoReturn

import psycopg
from psycopg.conninfo import conninfo_to_dict

logger = logging.getLogger(__name__)

# Session-level advisory lock key for the discogs-cache monthly rebuild.
# Database-global on the shared discogs-cache Postgres -- see the registry
# table in CLAUDE.md ("Advisory lock keys (shared-PG registry)") before
# reusing or changing this value. 354001 follows the documented
# `<issue-number> * 1000 + sequence` convention (discogs-etl#354, first key).
REBUILD_LOCK_KEY = 354001

# Distinct process exit code for "bowed out: another rebuild already holds
# the advisory lock." Deliberately NOT 0 -- a lock-loser is not a successful
# rebuild, and #352 exists precisely because a silent no-op success (a peer
# guard that fails open) was indistinguishable from a real one. Also
# deliberately not 1, the code every other validation failure in
# scripts/run_pipeline.py already uses, so a caller can tell "bowed out"
# apart from "crashed."
#
# 75 is EX_TEMPFAIL from BSD sysexits.h: "a temporary failure, indicating
# something that is not really an error ... the request should be attempted
# again later" -- precisely a lock-loser's situation, since the next monthly
# tick (or a re-dispatch) is expected to succeed once the winning peer
# finishes.
#
# scripts/rebuild-cache.sh checks for this exact numeric value; the two are
# pinned in sync by tests/unit/test_rebuild_cache_lock_bowout.py.
REBUILD_LOCK_BOWED_OUT_EXIT_CODE = 75

# Client-side TCP keepalives for the dedicated lock connection.
#
# This connection is the one thing in the whole pipeline that issues exactly
# two statements hours apart: `pg_try_advisory_lock` at the start and
# `pg_advisory_unlock` at the end. In between it sits perfectly idle for the
# 60-90+ minutes the rebuild takes, and DATABASE_URL_DISCOGS reaches Railway
# through their public TCP proxy (`*.proxy.rlwy.net`) -- precisely the shape
# of connection an intermediary reaps for inactivity. Without keepalives
# nothing on the wire distinguishes "holding the lock" from "dead".
#
# Measured against the live discogs-cache Postgres (17.10) on 2026-08-14:
#
#   idle_session_timeout                = 0     (server never reaps idle sessions)
#   idle_in_transaction_session_timeout = 0     (n/a anyway -- autocommit)
#   statement_timeout                   = 0
#   tcp_keepalives_idle                 = 7200s (server probes only after 2h)
#   tcp_keepalives_interval             = 75s
#   tcp_keepalives_count                = 9
#
# So the *server* will not time the session out -- good -- but its own
# keepalive floor is 2 hours, far longer than a rebuild. That leaves the
# proxy hop unprotected in both directions:
#
#   - if the proxy drops the connection silently, the backend keeps holding
#     the advisory lock until server-side keepalives notice, ~2h+ later,
#     which can wedge a subsequent run;
#   - if the proxy resets it, the backend dies at once and the lock releases
#     *mid-rebuild*, silently disarming the guard exactly the way #352's IAM
#     gap silently disarmed the EC2 one.
#
# 30s idle + 10s interval x 5 probes keeps the proxy's idle timer from ever
# firing and detects a genuinely dead peer within ~80s, while still tolerating
# a transient blip (it takes 50s of wholly unacknowledged probes to give up).
#
# ``connect_timeout`` is in the same dict for the same reason. Acquiring the
# lock is now the pipeline's *first* contact with Postgres -- earlier than
# ``run_pipeline.wait_for_postgres``, whose whole job is to absorb a
# not-yet-accepting server. Without an explicit timeout a blackholed host
# leaves main() parked in libpq's default TCP connect (~130s on Linux)
# emitting nothing at all.
#
# Applied only for parameters the caller's DSN does not already set, so an
# operator can still tune or disable them per-environment from the URL.
REBUILD_LOCK_CONNECT_PARAMS = {
    "connect_timeout": 15,
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

# Bounded retry around the *connection*, not the lock test itself. Moving
# first contact to the top of main() traded away the tolerance
# ``wait_for_postgres`` used to provide, and a monthly job that aborts on one
# transient blip against Railway's public proxy costs a month of cache
# staleness plus an EC2 instance-hour. A refusal is never retried -- that is a
# definitive answer, and queuing behind a multi-hour rebuild is exactly what
# `pg_try_advisory_lock` exists to avoid.
REBUILD_LOCK_CONNECT_ATTEMPTS = 3
REBUILD_LOCK_CONNECT_BACKOFF_SECONDS = 2.0


def _connection_kwargs(database_url: str) -> dict[str, int]:
    """Params from :data:`REBUILD_LOCK_CONNECT_PARAMS` not already pinned by
    ``database_url`` itself.

    A DSN-supplied value always wins, so an operator can override or switch
    any of them off for an environment where they are unwanted. An unparseable
    DSN yields the full default set rather than raising here -- the caller's
    ``psycopg.connect`` is where that malformed URL should surface, with
    psycopg's own error message, not as a confusing failure from this helper.
    """
    try:
        supplied = conninfo_to_dict(database_url)
    except Exception:
        return dict(REBUILD_LOCK_CONNECT_PARAMS)
    return {k: v for k, v in REBUILD_LOCK_CONNECT_PARAMS.items() if k not in supplied}


def _connect_with_retry(database_url: str) -> psycopg.Connection:
    """Open the dedicated lock connection, retrying transient failures.

    Retries only ``psycopg.OperationalError`` (refused/reset/timed-out
    connections). Authentication and DSN-syntax errors are not transient and
    propagate on the first attempt. The final attempt's exception is raised
    unchanged so the caller sees psycopg's own message.
    """
    kwargs = _connection_kwargs(database_url)
    for attempt in range(1, REBUILD_LOCK_CONNECT_ATTEMPTS + 1):
        try:
            return psycopg.connect(database_url, autocommit=True, **kwargs)
        except psycopg.OperationalError as exc:
            if attempt == REBUILD_LOCK_CONNECT_ATTEMPTS:
                raise
            delay = REBUILD_LOCK_CONNECT_BACKOFF_SECONDS * attempt
            # Never log the DSN itself -- it carries the cache credentials and
            # this warning rides the wxyc_etl/Sentry logging pipeline (#361).
            logger.warning(
                "Could not open the rebuild-lock connection (attempt %d/%d): %s; retrying in %.1fs",
                attempt,
                REBUILD_LOCK_CONNECT_ATTEMPTS,
                exc,
                delay,
            )
            time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


def try_acquire_rebuild_lock(database_url: str) -> psycopg.Connection | None:
    """Attempt to acquire the discogs-cache rebuild's advisory lock.

    Opens a **dedicated** ``autocommit=True`` connection distinct from any
    connection the rest of the pipeline uses. This matters because
    ``pg_advisory_lock`` / ``pg_try_advisory_lock`` are session-scoped (tied
    to the backend process, not a transaction) -- the lock is only held for
    as long as this connection stays open, so the caller must keep the
    returned connection alive for the pipeline's entire run and must not
    reuse it for pipeline queries (closing or losing track of it releases
    the lock early). Because it then sits idle for the rebuild's whole
    multi-hour run, client-side TCP keepalives are applied on top of the
    caller's DSN -- see :data:`REBUILD_LOCK_CONNECT_PARAMS` for the
    measured server settings that make them necessary.

    Non-blocking by design: ``pg_try_advisory_lock`` returns immediately
    regardless of contention. A second rebuild instance must bow out at once
    rather than queue up behind a multi-hour rebuild (matching the #311 EC2
    guard's existing "bow out immediately" semantics).

    Returns the open, lock-holding connection on success. On failure to
    acquire (another session already holds the key), closes the connection
    itself and returns ``None`` -- callers must not reuse a failed-
    acquisition connection for anything else.

    Raises whatever psycopg raises on a genuine connection failure (bad DSN,
    unreachable host, authentication failure). That is a real error, not a
    lock-loser, and callers must not fold it into the "bowed out" case --
    doing so would let a broken connection string report itself as a
    peaceful, retry-later bow-out instead of the infra failure it is.
    """
    conn = _connect_with_retry(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (REBUILD_LOCK_KEY,))
            (acquired,) = cur.fetchone()
    except BaseException:
        conn.close()
        raise
    if not acquired:
        conn.close()
        return None
    return conn


def exit_bowed_out() -> NoReturn:
    """Terminate this process with :data:`REBUILD_LOCK_BOWED_OUT_EXIT_CODE`,
    bypassing exception unwinding entirely.

    ``os._exit`` rather than ``sys.exit`` is deliberate, and the reason is
    recorded in this repo's own history: ``scripts/run_pipeline.py``'s
    ``run_step`` carries the comment *"Raise rather than sys.exit so any
    logger plugin that captures SystemExit (Sentry-enabled run #3 swallowed
    it; #180) cannot mask the failure."* Here the consequence of a swallowed
    ``SystemExit`` is worse than a masked failure: ``main()`` would return
    normally, the process would exit 0, and ``scripts/rebuild-cache.sh`` would
    fall through to the drift watchdog and post ":white_check_mark: rebuilt
    successfully" for a run that never touched the cache -- the exact
    false-positive-success class incident #352 exists to eliminate.

    ``run_step``'s "raise instead" remedy is unavailable to us, because a
    bow-out has to surface a *specific* exit code rather than just any
    non-zero one, so this bypasses the interpreter's unwinding instead.
    Logging handlers are flushed first, since ``os._exit`` skips the
    ``atexit`` machinery that would normally do it. Nothing else needs to run
    on this path: the lock was never acquired, no subprocess was ever
    spawned, and no temporary directory exists yet.
    """
    logging.shutdown()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:  # pragma: no cover - a closed stream is not fatal here
            pass
    os._exit(REBUILD_LOCK_BOWED_OUT_EXIT_CODE)


def release_rebuild_lock(conn: psycopg.Connection) -> None:
    """Release the rebuild advisory lock and close its dedicated connection.

    Call this on every clean exit path (success or a handled failure) so the
    lock is released immediately rather than waiting on the connection to be
    garbage-collected. On an unclean exit (SIGKILL, EC2 instance
    termination, OOM kill) this never runs -- that is fine by design.
    PostgreSQL releases every session-level advisory lock held by a backend
    as soon as it detects that backend's connection has dropped, so the next
    monthly tick is never wedged by a crashed prior run.

    Best-effort and never raises: the caller (``run_pipeline.py::main()``)
    invokes this from a ``finally`` block that may already be unwinding a
    real pipeline failure. If the dedicated lock connection has itself gone
    bad in the meantime (network partition, DB restart) and the explicit
    ``pg_advisory_unlock`` fails, raising here would replace that in-flight
    exception with this cleanup-path one -- a classic "exception in
    ``finally`` masks the original" footgun -- hiding the actual reason the
    pipeline crashed. Log and continue instead; ``conn.close()`` still runs
    either way, and closing the connection is itself sufficient to free the
    lock (see the crash-safety note above), so a failed explicit unlock does
    not leave the lock held. Mirrors the established best-effort-cleanup
    pattern in ``run_derive_va_release_step``'s stale-table drop
    (``scripts/run_pipeline.py``).
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (REBUILD_LOCK_KEY,))
    except Exception as exc:
        logger.warning(
            "pg_advisory_unlock(%d) failed (connection likely already broken); "
            "the lock will still free once this connection closes: %s",
            REBUILD_LOCK_KEY,
            exc,
        )
    finally:
        conn.close()
