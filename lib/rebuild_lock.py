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

import psycopg

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


def try_acquire_rebuild_lock(database_url: str) -> psycopg.Connection | None:
    """Attempt to acquire the discogs-cache rebuild's advisory lock.

    Opens a **dedicated** ``autocommit=True`` connection distinct from any
    connection the rest of the pipeline uses. This matters because
    ``pg_advisory_lock`` / ``pg_try_advisory_lock`` are session-scoped (tied
    to the backend process, not a transaction) -- the lock is only held for
    as long as this connection stays open, so the caller must keep the
    returned connection alive for the pipeline's entire run and must not
    reuse it for pipeline queries (closing or losing track of it releases
    the lock early).

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
    conn = psycopg.connect(database_url, autocommit=True)
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
