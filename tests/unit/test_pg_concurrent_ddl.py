"""Pure-Python tests for the lib.pg_concurrent_ddl helper.

These exercise the parts of the helper that don't need a live PG: input
validation, the index-name extractor, the transaction-status guard, and
the retry envelope (using a fake connection that simulates 55P03 / 40P01
responses).

Concurrent-LML behaviour against a real PG lives in
``tests/integration/test_verify_cache_concurrent_lml_writes.py``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import psycopg
import pytest

from lib.pg_concurrent_ddl import (
    SQLSTATE_DEADLOCK_DETECTED,
    SQLSTATE_LOCK_NOT_AVAILABLE,
    ConcurrentDDLError,
    RetryStats,
    _extract_index_name,
    add_constraint_safely,
    add_index_concurrently_safely,
    extract_index_target_table,
)

# ---------------------------------------------------------------------------
# Index-name extractor
# ---------------------------------------------------------------------------


class TestExtractIndexName:
    """Pin the canonical DDL shapes used by verify_cache + dedup."""

    @pytest.mark.parametrize(
        "ddl,expected",
        [
            (
                "CREATE INDEX CONCURRENTLY idx_release_artist_release_id "
                "ON release_artist(release_id)",
                "idx_release_artist_release_id",
            ),
            (
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                "idx_release_label_release_id ON release_label(release_id)",
                "idx_release_label_release_id",
            ),
            (
                "CREATE UNIQUE INDEX CONCURRENTLY release_pkey ON release(id)",
                "release_pkey",
            ),
            (
                "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS cache_metadata_pkey "
                "ON cache_metadata(release_id)",
                "cache_metadata_pkey",
            ),
            (
                "CREATE INDEX CONCURRENTLY idx_release_artist_name_trgm ON "
                "release_artist USING gin (lower(f_unaccent(artist_name)) "
                "gin_trgm_ops)",
                "idx_release_artist_name_trgm",
            ),
        ],
    )
    def test_extract_index_name_handles_canonical_shapes(self, ddl, expected):
        assert _extract_index_name(ddl) == expected

    def test_returns_none_for_non_concurrent_create_index(self):
        # Non-CONCURRENTLY: the helper isn't responsible for this DDL,
        # so the precleanup step is correctly skipped.
        assert _extract_index_name("CREATE INDEX idx_x ON t(id)") is None

    def test_returns_none_for_alter_table_add_constraint(self):
        assert _extract_index_name("ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (id)") is None


class TestExtractIndexTargetTable:
    """Pin the table parser used to group CONCURRENTLY builds per-table.

    Two ``CREATE INDEX CONCURRENTLY`` calls on the same table deadlock
    against each other; parallel CONCURRENTLY across distinct tables is
    fine. Callers in :mod:`scripts.dedup_releases` use this parser to
    group ``ddls`` before dispatching to a thread pool.
    """

    @pytest.mark.parametrize(
        "ddl,expected",
        [
            (
                "CREATE INDEX CONCURRENTLY idx_x ON release_artist(release_id)",
                "release_artist",
            ),
            (
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_x ON release(id)",
                "release",
            ),
            (
                "CREATE UNIQUE INDEX CONCURRENTLY release_pkey ON release(id)",
                "release",
            ),
            (
                "CREATE INDEX CONCURRENTLY idx_release_artist_name_trgm ON "
                "release_artist USING gin (lower(f_unaccent(artist_name)) "
                "gin_trgm_ops)",
                "release_artist",
            ),
        ],
    )
    def test_extracts_canonical_shapes(self, ddl, expected):
        assert extract_index_target_table(ddl) == expected

    def test_returns_none_when_shape_not_recognized(self):
        assert extract_index_target_table("ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY") is None


# ---------------------------------------------------------------------------
# add_constraint_safely input validation
# ---------------------------------------------------------------------------


@dataclass
class FakeCursor:
    """Captures every execute() call so tests can assert on the sequence."""

    statements: list[str] = field(default_factory=list)
    raise_on_match: dict[str, Exception] = field(default_factory=dict)
    raise_once: dict[str, list[Exception]] = field(default_factory=dict)

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, stmt: str, params: Any = None) -> None:
        self.statements.append(stmt)
        for fragment, exc_list in list(self.raise_once.items()):
            if fragment in stmt and exc_list:
                raise exc_list.pop(0)
        for fragment, exc in self.raise_on_match.items():
            if fragment in stmt:
                raise exc

    def fetchone(self) -> tuple | None:
        return None


@dataclass
class FakeTxnCtx:
    """Pretends to be ``conn.transaction()`` — opens/closes are no-ops."""

    conn: FakeConn
    raise_exc: Exception | None = None

    def __enter__(self) -> FakeTxnCtx:
        self.conn.in_transaction = True
        return self

    def __exit__(self, exc_type, exc_value, tb) -> bool:
        self.conn.in_transaction = False
        return False  # never suppress


@dataclass
class FakeInfo:
    transaction_status: Any = psycopg.pq.TransactionStatus.IDLE


@dataclass
class FakeConn:
    """Minimal psycopg connection stand-in for unit-testing the retry envelope."""

    cursor_factory: Any = None
    in_transaction: bool = False
    info: FakeInfo = field(default_factory=FakeInfo)
    cursors_returned: list[FakeCursor] = field(default_factory=list)
    raise_on_match: dict[str, Exception] = field(default_factory=dict)
    # Per-attempt: a list keyed by fragment; pop()ed off as it's hit, so
    # we can simulate "fail once, then succeed."
    raise_once_by_attempt: list[dict[str, list[Exception]]] = field(default_factory=list)

    def cursor(self) -> FakeCursor:
        # Use the next per-attempt raise_once map if present, else the
        # global raise_on_match.
        per_attempt = (
            self.raise_once_by_attempt[len(self.cursors_returned)]
            if len(self.cursors_returned) < len(self.raise_once_by_attempt)
            else {}
        )
        c = FakeCursor(
            raise_on_match=dict(self.raise_on_match),
            raise_once=per_attempt,
        )
        self.cursors_returned.append(c)
        return c

    def transaction(self) -> FakeTxnCtx:
        return FakeTxnCtx(self)


def _make_lock_not_available() -> psycopg.errors.LockNotAvailable:
    """Construct a LockNotAvailable exception suitable for tests."""
    return psycopg.errors.LockNotAvailable("simulated 55P03 lock_not_available")


def _make_deadlock_detected() -> psycopg.errors.DeadlockDetected:
    return psycopg.errors.DeadlockDetected("simulated 40P01 deadlock_detected")


class TestAddConstraintSafelyInputValidation:
    def test_zero_attempts_raises(self):
        conn = FakeConn()
        with pytest.raises(ValueError, match="attempts must be >= 1"):
            add_constraint_safely(
                conn,  # type: ignore[arg-type]
                "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (id)",
                lock_tables=["t"],
                attempts=0,
            )

    def test_short_backoff_list_raises(self):
        conn = FakeConn()
        with pytest.raises(ValueError, match="backoff_seconds has"):
            add_constraint_safely(
                conn,  # type: ignore[arg-type]
                "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (id)",
                lock_tables=["t"],
                attempts=3,
                backoff_seconds=[1.0],
            )

    def test_empty_lock_tables_raises(self):
        conn = FakeConn()
        with pytest.raises(ValueError, match="at least one table"):
            add_constraint_safely(
                conn,  # type: ignore[arg-type]
                "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (id)",
                lock_tables=[],
            )


class TestAddConstraintSafelyHappyPath:
    def test_success_first_attempt(self):
        conn = FakeConn()
        stats = add_constraint_safely(
            conn,  # type: ignore[arg-type]
            "ALTER TABLE release_artist ADD CONSTRAINT fk FOREIGN KEY (release_id) "
            "REFERENCES release(id) NOT VALID",
            lock_tables=["release", "release_artist"],
            lock_timeout="5s",
        )
        assert stats.attempts == 1
        assert stats.sqlstates_seen == []
        # Verify the order of statements inside the transaction.
        cur = conn.cursors_returned[0]
        assert cur.statements[0] == "SET LOCAL lock_timeout = '5s'"
        assert cur.statements[1] == ("LOCK TABLE release, release_artist IN ACCESS EXCLUSIVE MODE")
        assert "ADD CONSTRAINT fk" in cur.statements[2]

    def test_parent_first_lock_order_is_preserved_verbatim(self):
        """parent-first ordering is load-bearing; pin the LOCK TABLE syntax.

        The deadlock-prevention property doesn't come from the comma form
        itself (PG documents ``LOCK TABLE a, b`` as equivalent to two
        statements in order), but the *order* of names in this LOCK TABLE
        call MUST match LML's ``write_release`` acquisition order:
        release first, then children.
        """
        conn = FakeConn()
        add_constraint_safely(
            conn,  # type: ignore[arg-type]
            "ALTER TABLE cache_metadata ADD CONSTRAINT fk_cm FOREIGN KEY "
            "(release_id) REFERENCES release(id) NOT VALID",
            lock_tables=["release", "cache_metadata"],
        )
        lock_stmt = conn.cursors_returned[0].statements[1]
        assert lock_stmt.index("release") < lock_stmt.index("cache_metadata"), (
            "Parent (release) must be named before child (cache_metadata) in the "
            "LOCK TABLE statement. Reversing the order reintroduces the deadlock "
            "this helper exists to prevent."
        )


class TestAddConstraintSafelyRetry:
    def test_retries_on_55P03_then_succeeds(self, monkeypatch, caplog):
        # Fail SET LOCAL ... well, actually we want the LOCK TABLE to fail
        # on attempt 1 with 55P03 and succeed on attempt 2.
        conn = FakeConn(
            raise_once_by_attempt=[
                {"LOCK TABLE": [_make_lock_not_available()]},
                {},
            ],
        )
        # Speed up backoff for the test.
        monkeypatch.setattr("lib.pg_concurrent_ddl.time.sleep", lambda _s: None)
        with caplog.at_level(logging.INFO, logger="lib.pg_concurrent_ddl"):
            stats = add_constraint_safely(
                conn,  # type: ignore[arg-type]
                "ALTER TABLE release_artist ADD CONSTRAINT fk FOREIGN KEY "
                "(release_id) REFERENCES release(id) NOT VALID",
                lock_tables=["release", "release_artist"],
                attempts=3,
                backoff_seconds=[1.0, 1.0],
            )
        assert stats.attempts == 2
        assert stats.sqlstates_seen == [SQLSTATE_LOCK_NOT_AVAILABLE]
        # Info log line emitted on the retry.
        assert any("55P03" in r.message for r in caplog.records)

    def test_raises_after_exhausting_attempts(self, monkeypatch):
        conn = FakeConn(
            raise_on_match={"LOCK TABLE": _make_lock_not_available()},
        )
        monkeypatch.setattr("lib.pg_concurrent_ddl.time.sleep", lambda _s: None)
        with pytest.raises(psycopg.errors.LockNotAvailable):
            add_constraint_safely(
                conn,  # type: ignore[arg-type]
                "ALTER TABLE release_artist ADD CONSTRAINT fk FOREIGN KEY "
                "(release_id) REFERENCES release(id) NOT VALID",
                lock_tables=["release", "release_artist"],
                attempts=2,
                backoff_seconds=[0.0],
            )

    def test_logs_warning_on_40P01_canary(self, monkeypatch, caplog):
        """40P01 must surface loudly — the canary for LML acquisition-order drift.

        Per #286: parent-first ordering should make 40P01 structurally
        impossible. If it fires post-fix, an LML write path has drifted to
        child-first acquisition — log it loudly so the regression surfaces
        instead of being silently swallowed by the retry envelope.
        """
        conn = FakeConn(
            raise_once_by_attempt=[
                {"LOCK TABLE": [_make_deadlock_detected()]},
                {},
            ],
        )
        monkeypatch.setattr("lib.pg_concurrent_ddl.time.sleep", lambda _s: None)
        with caplog.at_level(logging.WARNING, logger="lib.pg_concurrent_ddl"):
            stats = add_constraint_safely(
                conn,  # type: ignore[arg-type]
                "ALTER TABLE release_artist ADD CONSTRAINT fk FOREIGN KEY "
                "(release_id) REFERENCES release(id) NOT VALID",
                lock_tables=["release", "release_artist"],
                attempts=2,
                backoff_seconds=[0.0],
            )
        assert stats.attempts == 2
        assert stats.sqlstates_seen == [SQLSTATE_DEADLOCK_DETECTED]
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("40P01" in r.message for r in warnings), (
            "40P01 deadlock_detected must produce a WARN log line so "
            "library-metadata-lookup acquisition-order drift surfaces."
        )
        assert any("library-metadata-lookup" in r.message for r in warnings), (
            "WARN log must name library-metadata-lookup so the regressed "
            "writer is identified without forensics."
        )


class TestRetryStatsObservability:
    """RetryStats is the public hook tests use to assert the contention path."""

    def test_retry_stats_records_sqlstate_in_attempt_order(self, monkeypatch):
        conn = FakeConn(
            raise_once_by_attempt=[
                {"LOCK TABLE": [_make_lock_not_available()]},
                {"LOCK TABLE": [_make_deadlock_detected()]},
                {},
            ],
        )
        monkeypatch.setattr("lib.pg_concurrent_ddl.time.sleep", lambda _s: None)
        stats = add_constraint_safely(
            conn,  # type: ignore[arg-type]
            "ALTER TABLE release_artist ADD CONSTRAINT fk FOREIGN KEY "
            "(release_id) REFERENCES release(id) NOT VALID",
            lock_tables=["release", "release_artist"],
            attempts=3,
            backoff_seconds=[0.0, 0.0],
        )
        assert stats.attempts == 3
        assert stats.sqlstates_seen == [
            SQLSTATE_LOCK_NOT_AVAILABLE,
            SQLSTATE_DEADLOCK_DETECTED,
        ]
        assert stats.elapsed_seconds >= 0.0


# ---------------------------------------------------------------------------
# add_index_concurrently_safely transaction guard
# ---------------------------------------------------------------------------


class TestAddIndexConcurrentlySafelyTransactionGuard:
    """The helper must refuse to run inside an open transaction block.

    PostgreSQL would otherwise produce the opaque "CREATE INDEX
    CONCURRENTLY cannot run inside a transaction block" error. Raising
    :class:`ConcurrentDDLError` at the helper boundary gives the caller
    a clearer trace.
    """

    def test_raises_when_called_inside_transaction(self):
        conn = FakeConn(
            info=FakeInfo(transaction_status=psycopg.pq.TransactionStatus.INTRANS),
        )
        with pytest.raises(ConcurrentDDLError, match="inside an open transaction"):
            add_index_concurrently_safely(
                conn,  # type: ignore[arg-type]
                "CREATE INDEX CONCURRENTLY idx_x ON t(id)",
            )

    def test_raises_when_called_in_error_state_transaction(self):
        conn = FakeConn(
            info=FakeInfo(transaction_status=psycopg.pq.TransactionStatus.INERROR),
        )
        with pytest.raises(ConcurrentDDLError):
            add_index_concurrently_safely(
                conn,  # type: ignore[arg-type]
                "CREATE INDEX CONCURRENTLY idx_x ON t(id)",
            )


def test_retry_stats_default_initial_state() -> None:
    stats = RetryStats()
    assert stats.attempts == 0
    assert stats.sqlstates_seen == []
    assert stats.elapsed_seconds == 0.0
