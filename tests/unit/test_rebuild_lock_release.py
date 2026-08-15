"""Unit tests for lib/rebuild_lock.py's release_rebuild_lock (discogs-etl#354).

Pure/mock-based -- no Postgres needed. The live acquire/refuse/release
behavior against a real database is covered by
tests/integration/test_rebuild_lock.py (pg-marked).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from lib.rebuild_lock import REBUILD_LOCK_KEY, release_rebuild_lock


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
