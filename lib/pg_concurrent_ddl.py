"""Concurrent-safe DDL primitives for prune/dedup copy-swap steps.

The monthly rebuild's prune (``scripts/verify_cache.py``) and dedup
(``scripts/dedup_releases.py``) both add FK constraints, primary keys, and
indexes to live tables that the library-metadata-lookup runtime is writing
to in parallel. Naively-issued ``ALTER TABLE ... ADD CONSTRAINT`` and
``CREATE INDEX`` (non-CONCURRENTLY) deadlock against LML's open
``write_release`` transactions — see WXYC/discogs-etl#286 for the
2026-06-04 06:45 UTC outage that motivated this module.

Two primitives:

* :func:`add_constraint_safely` — wraps a single ``ALTER TABLE ... ADD
  CONSTRAINT`` (or any DDL that takes blocking locks) in a transaction
  that pre-acquires its target tables in **parent-then-child order**
  under a bounded ``SET LOCAL lock_timeout``. Retries on ``55P03``
  (``lock_not_available``, our timeout fired — the expected path under
  LML contention) with exponential backoff. Retries on ``40P01``
  (``deadlock_detected``) with a loud warning that names the offending
  statement — the canary that fires if some LML write path drifts to a
  child-first acquisition order.

* :func:`add_index_concurrently_safely` — runs ``CREATE INDEX CONCURRENTLY``
  with two safety rails: pre-flight cleanup of any leftover INVALID index
  with the same name (which a CTRL-C'd CONCURRENTLY build leaves behind),
  and a refusal to run inside an open ``conn.transaction()`` block (which
  PostgreSQL rejects with a deliberately opaque error).

Both primitives operate on an ``autocommit=True`` ``psycopg.Connection``;
the constraint primitive opens its own ``conn.transaction()`` block per
attempt. Both are idempotent under retry.

The parent-first ordering property is load-bearing: LML's
``cache_service.write_release`` acquires ``release`` (parent) before
``release_artist`` / ``release_label`` / ... / ``cache_metadata``
(children). Matching that order across all writers means the happy path
doesn't even reach PG's deadlock detector — our prune transaction just
waits for LML's commit, then proceeds. The current production failure
deadlocks precisely because PG's ``ALTER TABLE ... ADD CONSTRAINT
FOREIGN KEY ... REFERENCES release(id) NOT VALID`` acquires the child
first (the ``ALTER TABLE`` target), then the parent — opposite of LML.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass, field

import psycopg

logger = logging.getLogger(__name__)

# SQLSTATE constants used by the retry envelope.
SQLSTATE_LOCK_NOT_AVAILABLE = "55P03"
SQLSTATE_DEADLOCK_DETECTED = "40P01"

# Default retry envelope for FK / PK adds (ShareRowExclusive lock surface).
# Sized for sub-second LML write_release p99 — see #286 for the discussion of
# how to validate this from LML's structured logger before sizing upward.
DEFAULT_LOCK_TIMEOUT = "5s"
DEFAULT_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS: tuple[float, ...] = (5.0, 15.0, 45.0)

# Wider retry budget for the DROP CONSTRAINT + RENAME path in
# ``_prune_copy_swap_tables``: those statements take AccessExclusive, which
# conflicts with every live LML lock mode, not just ShareRowExclusive vs
# RowExclusive. The contention window is wider; the retries should be too.
SWAP_PATH_ATTEMPTS = 5
SWAP_PATH_BACKOFF_SECONDS: tuple[float, ...] = (5.0, 15.0, 45.0, 90.0, 180.0)

_INDEX_NAME_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
    re.IGNORECASE,
)

_INDEX_TARGET_TABLE_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"\w+\s+ON\s+(\w+)",
    re.IGNORECASE,
)


class ConcurrentDDLError(RuntimeError):
    """Raised for caller-side misuse of the helper (e.g. running CONCURRENTLY
    inside an open transaction). Distinct from psycopg errors so callers can
    catch them separately."""


@dataclass
class RetryStats:
    """Observable outcome of a :func:`add_constraint_safely` call.

    Exposes the retry path for tests and operational logging without forcing
    callers to scrape log records. ``sqlstates_seen`` is in attempt order so
    the test for #286 can assert that the contention path was actually
    exercised (``"55P03" in stats.sqlstates_seen``) rather than relying on
    the inherently-flaky "no deadlock ever fired" property.
    """

    attempts: int = 0
    sqlstates_seen: list[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0


def _sentry_breadcrumb(category: str, message: str, data: dict | None = None) -> None:
    """Best-effort Sentry breadcrumb. No-op if ``sentry_sdk`` isn't importable.

    Kept tiny on purpose — the helper must not pull a hard dependency on
    Sentry just to surface the 40P01 canary. If Sentry is wired up via
    :mod:`lib.observability`, the breadcrumb attaches to the next event;
    otherwise it silently drops.
    """
    try:
        import sentry_sdk

        sentry_sdk.add_breadcrumb(
            category=category,
            message=message,
            level="warning",
            data=data or {},
        )
    except Exception:
        pass


def _extract_index_name(ddl: str) -> str | None:
    """Pull the index name out of a ``CREATE INDEX CONCURRENTLY`` statement.

    Returns ``None`` if the statement doesn't match the expected shape — the
    caller falls back to skipping the INVALID-index precleanup in that case
    rather than failing on a perfectly valid DDL that this regex happens not
    to handle. The shape we care about (every call site in
    :mod:`scripts.verify_cache` and :mod:`scripts.dedup_releases`) is what
    the test ``test_extract_index_name_handles_canonical_shapes`` pins.
    """
    match = _INDEX_NAME_RE.search(ddl)
    return match.group(1) if match else None


_UNPARSEABLE_GROUP_KEY = "__unparseable__"


def group_concurrent_index_ddls_by_table(ddls: Sequence[str]) -> dict[str, list[str]]:
    """Group ``CREATE INDEX CONCURRENTLY`` DDLs for safe parallel dispatch.

    Two CONCURRENTLY builds on the **same** table deadlock against each other
    inside PG's wait-for-snapshot phase. Callers that parallelize CONCURRENTLY
    must group by target table and run one worker per group (serial within,
    parallel across).

    Returns ``{table_name: [ddl, ...]}``. DDLs whose target table can't be
    parsed by :func:`extract_index_target_table` are all collected under
    a single sentinel key so they run serially — NOT keyed by the full DDL
    string. The old keyed-by-DDL fallback would put two unparseable DDLs
    on the same table into separate groups, re-introducing the same
    CONCURRENTLY-vs-CONCURRENTLY deadlock the grouping is meant to prevent.
    """
    groups: dict[str, list[str]] = {}
    for ddl in ddls:
        table = extract_index_target_table(ddl)
        if table is None:
            logger.warning(
                "group_concurrent_index_ddls_by_table: could not parse target "
                "table from %r; grouping with other unparseable DDLs for "
                "serial execution",
                ddl,
            )
            groups.setdefault(_UNPARSEABLE_GROUP_KEY, []).append(ddl)
        else:
            groups.setdefault(table, []).append(ddl)
    return groups


def extract_index_target_table(ddl: str) -> str | None:
    """Pull the target table out of a ``CREATE INDEX CONCURRENTLY ... ON <t>``.

    Used by callers that need to parallelize CONCURRENTLY index builds: PG
    deadlocks when two CONCURRENTLY builds run on the **same** table
    concurrently (each takes ShareUpdateExclusive while also waiting on the
    other's virtual transaction in the wait-for-snapshot phase), but
    cleanly tolerates parallel builds across **different** tables. Group
    by this function's return value before dispatching to a thread pool.

    Returns ``None`` if the DDL shape isn't recognized — the caller should
    fall back to serial execution in that case rather than risk grouping
    by an unparsed key. The pin in
    :mod:`tests.unit.test_pg_concurrent_ddl` covers every shape used in
    :mod:`scripts.verify_cache` and :mod:`scripts.dedup_releases`.
    """
    match = _INDEX_TARGET_TABLE_RE.search(ddl)
    return match.group(1) if match else None


def _drop_invalid_index_if_present(conn: psycopg.Connection, index_name: str) -> bool:
    """Drop ``index_name`` if it exists and is marked ``indisvalid = false``.

    A ``CREATE INDEX CONCURRENTLY`` that was interrupted (CTRL-C, SIGTERM,
    crash) leaves the index in pg_class but with ``pg_index.indisvalid =
    false``, and the next ``CREATE INDEX CONCURRENTLY IF NOT EXISTS`` will
    silently no-op against that invalid index — leaving the cache without
    the lookup acceleration the rebuild promised. Drop-then-recreate is
    the documented PG recovery path; doing it as part of the helper means
    a retried prune is idempotent against a previous interrupted run.

    Returns ``True`` if a drop happened (for logging), ``False`` otherwise.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relname = %s AND i.indisvalid = false",
            (index_name,),
        )
        if cur.fetchone() is None:
            return False
        # DROP INDEX takes AccessExclusive on the index itself but not on
        # the underlying table; safe to run on a live cache because the
        # index is INVALID and therefore not used for query planning.
        cur.execute(f"DROP INDEX IF EXISTS {index_name}")
    logger.warning(
        "Dropped INVALID leftover index %s before re-running CONCURRENTLY build "
        "(prior CREATE INDEX CONCURRENTLY likely interrupted)",
        index_name,
    )
    return True


def _connection_in_transaction(conn: psycopg.Connection) -> bool:
    """Return True if ``conn`` is currently inside an open transaction block.

    psycopg3's ``conn.info.transaction_status`` returns
    ``TransactionStatus.IDLE`` for an autocommit connection that isn't inside
    a ``conn.transaction()`` block, and ``INTRANS`` / ``INERROR`` when it is.
    We refuse to run CONCURRENTLY in either non-IDLE state because PG will
    reject it with the opaque "CREATE INDEX CONCURRENTLY cannot run inside a
    transaction block" error — clearer to fail fast at the helper boundary
    with a sentence the caller can act on.
    """
    status = conn.info.transaction_status
    # Idle = autocommit, no open block. Anything else means we're inside one.
    return status != psycopg.pq.TransactionStatus.IDLE


def add_index_concurrently_safely(
    conn: psycopg.Connection,
    ddl: str,
) -> None:
    """Run ``CREATE INDEX CONCURRENTLY ...`` with INVALID-index precleanup.

    Refuses to run if ``conn`` is inside an open ``conn.transaction()``
    block: PG rejects CONCURRENTLY inside any transaction, and the error
    it produces (``25001 active_sql_transaction``) is mostly opaque to
    callers debugging a retry envelope. Raise :class:`ConcurrentDDLError`
    with the call site so future-us doesn't have to guess.

    The connection must be in autocommit mode. Single-statement CONCURRENTLY
    calls then run as their own implicit transactions; PG is happy with that.

    If a previous interrupted build left an INVALID index with the same
    name, drops it first so the new CONCURRENTLY build can proceed.
    ``IF NOT EXISTS`` alone does NOT help — PG considers an INVALID index
    to exist for the purposes of the existence check.
    """
    if _connection_in_transaction(conn):
        raise ConcurrentDDLError(
            f"add_index_concurrently_safely called from inside an open "
            f"transaction block; CREATE INDEX CONCURRENTLY cannot run there. "
            f"DDL: {ddl!r}"
        )

    index_name = _extract_index_name(ddl)
    if index_name is not None:
        _drop_invalid_index_if_present(conn, index_name)

    with conn.cursor() as cur:
        cur.execute(ddl)


def add_constraint_safely(
    conn: psycopg.Connection,
    ddl: str | Sequence[str],
    *,
    lock_tables: Sequence[str],
    lock_timeout: str = DEFAULT_LOCK_TIMEOUT,
    attempts: int = DEFAULT_ATTEMPTS,
    backoff_seconds: Sequence[float] = DEFAULT_BACKOFF_SECONDS,
) -> RetryStats:
    """Run a blocking-lock DDL inside a parent-first ``LOCK TABLE`` envelope.

    Wraps ``ddl`` in ``with conn.transaction(): SET LOCAL lock_timeout ...
    LOCK TABLE <parent>, <child> IN ACCESS EXCLUSIVE MODE; <ddl>``. The
    ``LOCK TABLE`` pre-acquires every involved table in a single statement
    so that on the happy path the prune just waits for any open LML
    transaction to commit, then proceeds — no deadlock detector involved.

    ``ddl`` may be a single SQL statement or a sequence of statements that
    must run atomically within the same locked transaction (e.g. the
    three-RENAME swap step in ``_prune_copy_swap_tables``). All statements
    in the sequence execute under the same ``SET LOCAL lock_timeout`` and
    the same ``LOCK TABLE`` envelope; if any one fails with a retriable
    SQLSTATE the entire group retries.

    On ``55P03`` (``lock_not_available``, our ``lock_timeout`` fired):
      retries after backoff. This is the expected path under contention.
      Cap the retry budget so a pathological LML transaction can't keep
      the prune waiting indefinitely; surface a real failure if the cap
      is reached.

    On ``40P01`` (``deadlock_detected``):
      retries after backoff but ALSO emits ``logger.warning`` and a Sentry
      breadcrumb naming the offending statement. With parent-first ordering
      enforced everywhere, ``40P01`` is structurally impossible — if it
      ever fires post-fix it means some LML write path has drifted to
      child-first acquisition, and the canary should be loud so the
      regression surfaces quickly.

    Args:
        conn: psycopg connection in autocommit mode. ``conn.transaction()``
            is supported in this mode and is used internally per attempt.
        ddl: Single SQL statement to run inside the locked transaction.
            Typically an ``ALTER TABLE ... ADD CONSTRAINT ...`` or
            ``ALTER TABLE ... ADD CONSTRAINT ... USING INDEX ...`` form.
        lock_tables: Tables to acquire AccessExclusive on, **in parent-first
            order**. For an FK add, ``(parent, child)``; for a PK on a
            standalone table, ``(table,)``. Order across call sites must
            match LML's ``write_release`` acquisition order.
        lock_timeout: ``SET LOCAL lock_timeout`` value — Postgres
            duration syntax (e.g. ``'5s'``). Default sized for sub-second
            LML write p99; size from measurement, not guess.
        attempts: Total attempts including the first. ``attempts=1`` means
            no retry.
        backoff_seconds: Sleep before each retry. Must have length
            ``attempts - 1``; surplus entries are ignored, shortfall raises.

    Returns:
        :class:`RetryStats` describing the outcome — ``attempts`` count
        actually executed (1 + retries) and ``sqlstates_seen`` in order so
        the test for #286 can assert that the contention path was exercised.

    Raises:
        :class:`psycopg.errors.LockNotAvailable`: ``lock_timeout`` fired on
            every retry within ``attempts``.
        :class:`psycopg.errors.DeadlockDetected`: ``40P01`` fired on every
            retry — should never happen post-fix; investigate LML if it does.
        Any other psycopg error: surfaced immediately, no retry.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    if len(backoff_seconds) < attempts - 1:
        raise ValueError(
            f"backoff_seconds has {len(backoff_seconds)} entries but "
            f"attempts={attempts} requires at least {attempts - 1}"
        )
    if not lock_tables:
        raise ValueError("lock_tables must name at least one table")

    stats = RetryStats()
    start = time.monotonic()
    lock_clause = ", ".join(lock_tables)
    ddls: list[str] = [ddl] if isinstance(ddl, str) else list(ddl)
    # Truncated rendering for log lines / Sentry breadcrumbs. The multi-stmt
    # form (RENAME swap, 3 statements) would otherwise produce a huge
    # repr() in the 40P01 canary — the canary's job is to name the failure
    # site for an operator, not dump every byte of DDL.
    ddl_summary = ddls[0] if len(ddls) == 1 else f"{len(ddls)}-stmt group: {ddls[0]!r}, ..."

    last_exc: Exception | None = None
    for attempt in range(attempts):
        stats.attempts = attempt + 1
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    # SET LOCAL applies for the duration of this transaction
                    # only — no leak into subsequent autocommit statements.
                    cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'")
                    cur.execute(f"LOCK TABLE {lock_clause} IN ACCESS EXCLUSIVE MODE")
                    for stmt in ddls:
                        cur.execute(stmt)
            stats.elapsed_seconds = time.monotonic() - start
            return stats
        except psycopg.errors.LockNotAvailable as exc:
            stats.sqlstates_seen.append(SQLSTATE_LOCK_NOT_AVAILABLE)
            last_exc = exc
            logger.info(
                "add_constraint_safely: 55P03 lock_not_available on attempt %d/%d "
                "(lock_timeout=%s, lock_tables=%s); will retry",
                stats.attempts,
                attempts,
                lock_timeout,
                lock_clause,
            )
        except psycopg.errors.DeadlockDetected as exc:
            stats.sqlstates_seen.append(SQLSTATE_DEADLOCK_DETECTED)
            last_exc = exc
            # LOUD: parent-first ordering should make 40P01 structurally
            # impossible. Firing here means some writer has drifted to a
            # different acquisition order — almost certainly LML. Name
            # the statement in both the log line and the Sentry breadcrumb
            # so the regressed LML code path surfaces quickly.
            logger.warning(
                "add_constraint_safely: 40P01 deadlock_detected on attempt "
                "%d/%d. Parent-first LOCK TABLE ordering should make this "
                "structurally impossible; an LML writer has likely drifted "
                "to a different acquisition order. Investigate "
                "library-metadata-lookup/discogs/cache_service.py::write_release "
                "ordering. lock_tables=%s ddl=%s",
                stats.attempts,
                attempts,
                lock_clause,
                ddl_summary,
            )
            _sentry_breadcrumb(
                category="pg_concurrent_ddl",
                message="40P01 deadlock_detected in add_constraint_safely",
                data={
                    "attempt": stats.attempts,
                    "attempts_total": attempts,
                    "lock_tables": list(lock_tables),
                    "ddl": ddl_summary,
                },
            )

        if attempt < attempts - 1:
            time.sleep(backoff_seconds[attempt])

    stats.elapsed_seconds = time.monotonic() - start
    assert last_exc is not None  # we only get here after at least one except
    raise last_exc
