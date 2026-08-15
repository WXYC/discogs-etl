"""Unit tests for lib/rebuild_lock.py's release_rebuild_lock and the
lock connection's keepalive configuration (discogs-etl#354).

Pure/mock-based -- no Postgres needed. The live acquire/refuse/release
behavior against a real database is covered by
tests/integration/test_rebuild_lock.py (pg-marked).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import psycopg
import pytest

from lib.rebuild_lock import (
    REBUILD_LOCK_BOWED_OUT_EXIT_CODE,
    REBUILD_LOCK_CONNECT_ATTEMPTS,
    REBUILD_LOCK_CONNECT_PARAMS,
    REBUILD_LOCK_KEY,
    _connect_with_retry,
    _connection_kwargs,
    exit_bowed_out,
    release_rebuild_lock,
    try_acquire_rebuild_lock,
)


def _mock_connection(*, execute_raises: Exception | None = None) -> MagicMock:
    conn = MagicMock(name="conn")
    cursor_cm = conn.cursor.return_value
    cursor = cursor_cm.__enter__.return_value
    if execute_raises is not None:
        cursor.execute.side_effect = execute_raises
    return conn


class TestReleaseRebuildLock:
    def test_issues_pg_advisory_unlock_with_the_registered_key(self) -> None:
        conn = _mock_connection()
        release_rebuild_lock(conn)
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.execute.assert_called_once_with("SELECT pg_advisory_unlock(%s)", (REBUILD_LOCK_KEY,))

    def test_closes_the_connection_on_success(self) -> None:
        conn = _mock_connection()
        release_rebuild_lock(conn)
        conn.close.assert_called_once()

    def test_a_failed_unlock_does_not_raise(self) -> None:
        """The dedicated lock connection can independently die (network
        partition, DB restart) between acquisition and release. That must
        never surface as an exception out of release_rebuild_lock -- a
        cleanup-path failure raising from inside main()'s `finally` would
        replace/mask whatever real pipeline exception is already in flight
        (a classic Python footgun: an exception raised in `finally`
        supersedes the one being propagated). release_rebuild_lock is
        best-effort: the crash-safety guarantee this repo actually relies on
        is that Postgres frees a session-level advisory lock as soon as it
        detects the holding backend's connection has dropped, independent of
        whether an explicit pg_advisory_unlock ever ran. Matches the
        established best-effort-cleanup pattern in
        run_derive_va_release_step's DROP TABLE cleanup (scripts/run_pipeline.py)."""
        conn = _mock_connection(execute_raises=RuntimeError("connection already closed"))
        release_rebuild_lock(conn)  # must not raise

    def test_still_closes_the_connection_even_when_unlock_fails(self) -> None:
        conn = _mock_connection(execute_raises=RuntimeError("connection already closed"))
        release_rebuild_lock(conn)
        conn.close.assert_called_once()


class TestLockConnectionKeepalives:
    """The lock connection issues two statements hours apart and is otherwise
    perfectly idle for the rebuild's whole run, while DATABASE_URL_DISCOGS
    reaches Railway through their public TCP proxy. Without client-side
    keepalives an intermediary can reap it: either the backend keeps holding
    the lock long after the client is gone (wedging a later run), or the lock
    releases mid-rebuild and the guard is silently disarmed -- the same
    failure shape as #352's missing IAM grant. The measured server settings
    that make this necessary are recorded on REBUILD_LOCK_CONNECT_PARAMS.
    """

    def test_defaults_are_applied_when_the_dsn_names_none_of_them(self) -> None:
        assert _connection_kwargs("postgresql://u:p@host:5432/db") == REBUILD_LOCK_CONNECT_PARAMS

    def test_keepalives_are_enabled_and_well_under_the_servers_two_hour_floor(self) -> None:
        """tcp_keepalives_idle on the cache PG is 7200s, far longer than a
        rebuild -- the whole point is to probe on a timescale that keeps a
        proxy's idle timer from ever firing."""
        assert REBUILD_LOCK_CONNECT_PARAMS["keepalives"] == 1
        assert 0 < REBUILD_LOCK_CONNECT_PARAMS["keepalives_idle"] <= 60

    @pytest.mark.parametrize("param", sorted(REBUILD_LOCK_CONNECT_PARAMS))
    def test_a_dsn_supplied_value_wins_over_the_default(self, param: str) -> None:
        """Operators keep an escape hatch: anything pinned in the URL is left
        alone rather than being silently overridden by this module."""
        assert param not in _connection_kwargs(f"postgresql://u:p@host:5432/db?{param}=99")

    def test_an_unparseable_dsn_defers_to_psycopg_rather_than_raising_here(self) -> None:
        """A malformed URL must surface as psycopg's own connect error, not as
        a confusing failure from the keepalive helper."""
        assert _connection_kwargs("::: not a dsn :::") == REBUILD_LOCK_CONNECT_PARAMS

    def test_acquire_passes_the_keepalives_through_to_connect(self) -> None:
        url = "postgresql://u:p@host:5432/db"
        with patch("lib.rebuild_lock.psycopg.connect") as mock_connect:
            cur = mock_connect.return_value.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = (True,)
            try_acquire_rebuild_lock(url)
        _, kwargs = mock_connect.call_args
        assert kwargs["autocommit"] is True
        for param, value in REBUILD_LOCK_CONNECT_PARAMS.items():
            assert kwargs[param] == value, (
                f"{param} must reach psycopg.connect so the idle lock connection "
                "survives the proxy hop for the rebuild's whole run"
            )


class TestExitBowedOut:
    """The bow-out must never be able to surface as exit 0.

    ``run_step`` in scripts/run_pipeline.py carries the repo's record of a
    logger plugin swallowing SystemExit (#180). If that recurred here the
    process would exit 0 and scripts/rebuild-cache.sh would post
    ":white_check_mark: rebuilt successfully" for a run that never touched the
    cache -- incident #352's exact defect class, one level up.
    """

    def test_bypasses_interpreter_unwinding_with_the_bow_out_code(self) -> None:
        with (
            patch("lib.rebuild_lock.os._exit") as mock_exit,
            patch("lib.rebuild_lock.logging.shutdown") as mock_shutdown,
        ):
            exit_bowed_out()
        mock_exit.assert_called_once_with(REBUILD_LOCK_BOWED_OUT_EXIT_CODE)
        mock_shutdown.assert_called_once()

    def test_is_not_catchable_as_systemexit(self) -> None:
        """A regression back to ``sys.exit`` would make this pass through an
        ``except SystemExit`` -- which is precisely what #180 showed can be
        swallowed by a logging plugin."""
        with (
            patch("lib.rebuild_lock.os._exit") as mock_exit,
            patch("lib.rebuild_lock.logging.shutdown"),
        ):
            try:
                exit_bowed_out()
            except SystemExit:  # pragma: no cover - would mean a regression
                pytest.fail("the bow-out must not terminate via SystemExit")
        mock_exit.assert_called_once()


class TestConnectRetry:
    """Taking the lock is now the pipeline's first PG contact, earlier than
    wait_for_postgres's 30s backoff loop. A monthly job must not abort on a
    single transient blip against Railway's public proxy."""

    def test_retries_a_transient_operational_error_then_succeeds(self) -> None:
        good = MagicMock(name="conn")
        with (
            patch(
                "lib.rebuild_lock.psycopg.connect",
                side_effect=[psycopg.OperationalError("connection refused"), good],
            ) as mock_connect,
            patch("lib.rebuild_lock.time.sleep") as mock_sleep,
        ):
            assert _connect_with_retry("postgresql://u:p@host:5432/db") is good
        assert mock_connect.call_count == 2
        mock_sleep.assert_called_once()

    def test_gives_up_after_the_attempt_budget(self) -> None:
        with (
            patch(
                "lib.rebuild_lock.psycopg.connect",
                side_effect=psycopg.OperationalError("host is down"),
            ) as mock_connect,
            patch("lib.rebuild_lock.time.sleep"),
            pytest.raises(psycopg.OperationalError, match="host is down"),
        ):
            _connect_with_retry("postgresql://u:p@host:5432/db")
        assert mock_connect.call_count == REBUILD_LOCK_CONNECT_ATTEMPTS

    def test_a_non_transient_error_is_not_retried(self) -> None:
        """A bad DSN or a rejected password is a definitive answer; burning
        the retry budget on it only delays a failure the operator must fix."""
        with (
            patch(
                "lib.rebuild_lock.psycopg.connect",
                side_effect=psycopg.ProgrammingError("invalid dsn"),
            ) as mock_connect,
            pytest.raises(psycopg.ProgrammingError),
        ):
            _connect_with_retry("postgresql://u:p@host:5432/db")
        assert mock_connect.call_count == 1

    def test_a_refusal_is_never_retried(self) -> None:
        """Refusal is not a connection failure -- it is the guard working.
        Retrying would queue a peer behind a multi-hour rebuild, which is the
        exact behaviour pg_try_advisory_lock was chosen to avoid."""
        with (
            patch("lib.rebuild_lock.psycopg.connect") as mock_connect,
            patch("lib.rebuild_lock.time.sleep") as mock_sleep,
        ):
            cur = mock_connect.return_value.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = (False,)
            assert try_acquire_rebuild_lock("postgresql://u:p@host:5432/db") is None
        assert mock_connect.call_count == 1
        mock_sleep.assert_not_called()
        mock_connect.return_value.close.assert_called_once()
