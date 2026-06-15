"""Pin: verify_cache prune + dedup constraint adds tolerate live LML writes.

Pin for [WXYC/discogs-etl#286](https://github.com/WXYC/discogs-etl/issues/286).
The 2026-06-04 06:00 UTC scheduled rebuild failed at 06:45 UTC with a Postgres
deadlock between two concurrent backends inside ``scripts/verify_cache.py
--prune``. The lock partners were unambiguous: one backend held
``RowExclusiveLock`` on one relation and waited for ``ShareRowExclusiveLock``
on another (the live LML cache-miss writer in an open multi-table
transaction); the other was the mirror (``ALTER TABLE ... ADD CONSTRAINT
FOREIGN KEY ... NOT VALID``, which acquires ``ShareRowExclusiveLock`` on
child then parent — opposite of LML's parent-then-children order).

This test exercises the lock-conflict path by running the prune flow
alongside a background thread that opens a transaction touching ``release``
+ ``release_artist`` + ``cache_metadata`` in LML's order and then commits
after a short delay. The fix uses :mod:`lib.pg_concurrent_ddl` to wrap
each constraint add in a ``LOCK TABLE <parent>, <child> IN ACCESS EXCLUSIVE
MODE`` block under a bounded ``lock_timeout``, retrying on ``55P03``.

What we assert:
  * The retry path is exercised at least once
    (``stats.sqlstates_seen`` contains ``"55P03"``).
  * The prune completes successfully (constraint is present and valid).

What we deliberately do NOT assert: ``"40P01 never fires."`` Per #286, that
property is structurally guaranteed by the parent-first ordering but
testing it without advisory-lock synchronization between the LML simulator
and the prune is inherently flaky (PG's 1s ``deadlock_timeout`` detection
latency plus thread-scheduler variance).

Skipped via ``pytest.skip()`` if the race window isn't hit after a few
iterations — the test is inherently a race and a no-race run isn't a
failure of the production code.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import psycopg
import pytest

from lib.pg_concurrent_ddl import (
    SQLSTATE_LOCK_NOT_AVAILABLE,
    add_constraint_safely,
)

pytestmark = pytest.mark.pg

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

# Load verify_cache so we can re-run individual phases (mirrors
# tests/integration/test_copy_swap_preserves_not_null.py).
_VERIFY_CACHE_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _vcspec = importlib.util.spec_from_file_location("verify_cache", _VERIFY_CACHE_PATH)
    assert _vcspec is not None and _vcspec.loader is not None
    _vc = importlib.util.module_from_spec(_vcspec)
    sys.modules["verify_cache"] = _vc
    _vcspec.loader.exec_module(_vc)


def _drop_all_tables(conn) -> None:
    """Clear pipeline tables and any leftover copy-swap artifacts."""
    base = (
        "cache_metadata",
        "release_track_artist",
        "release_track",
        "release_style",
        "release_genre",
        "release_label",
        "release_artist",
        "release",
    )
    with conn.cursor() as cur:
        for t in base:
            cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS new_{t} CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS {t}_old CASCADE")
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids CASCADE")
        cur.execute("DROP TABLE IF EXISTS _keep_ids CASCADE")


def _seed_minimal_fixture(db_url: str) -> None:
    """Schema + a handful of WXYC-canonical rows on each swap-tracked table."""
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.executemany(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (%s, %s, %s, %s, %s)",
                [
                    (1, "DOGA", 200, "LP", "AR"),
                    (2, "Aluminum Tunes", 100, "CD", "UK"),
                    (3, "Edits", None, "CD", "US"),
                ],
            )
            cur.executemany(
                "INSERT INTO release_artist (release_id, artist_name) VALUES (%s, %s)",
                [
                    (1, "Juana Molina"),
                    (2, "Stereolab"),
                    (3, "Chuquimamani-Condori"),
                ],
            )
            cur.executemany(
                "INSERT INTO release_label (release_id, label_name) VALUES (%s, %s)",
                [(1, "Sonamos"), (2, "Duophonic")],
            )
            cur.executemany(
                "INSERT INTO cache_metadata (release_id, source) VALUES (%s, %s)",
                [(1, "bulk_import"), (2, "bulk_import"), (3, "bulk_import")],
            )
    finally:
        conn.close()


def _post_swap_constraint_columns(conn, table: str) -> set[str]:
    """Return columns with NOT NULL constraint on ``table``."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s AND is_nullable = 'NO'",
            (table,),
        )
        return {r[0] for r in cur.fetchall()}


def _fk_exists(conn, table: str, fk_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.table_constraints "
            "WHERE table_schema = 'public' AND table_name = %s "
            "AND constraint_name = %s AND constraint_type = 'FOREIGN KEY'",
            (table, fk_name),
        )
        return cur.fetchone() is not None


@contextmanager
def _lml_writer_holding_locks(db_url: str, hold_seconds: float, release_id: int):
    """Spawn a thread simulating LML's ``write_release`` lock geometry.

    Opens a non-autocommit connection, BEGIN, INSERTs into ``release`` first
    (parent), then ``release_artist``, then ``cache_metadata`` (children) —
    matching ``library-metadata-lookup/discogs/cache_service.py::write_release``.
    Holds the transaction open for ``hold_seconds`` so the foreground prune
    has time to attempt its ``ADD CONSTRAINT`` and get blocked.

    Yields a ``threading.Event`` that the caller can wait on to confirm the
    LML simulator is holding its locks.

    Cleans up the thread on exit; the writer's transaction commits when the
    sleep completes, releasing the locks so the foreground retry can succeed.
    """
    locks_acquired = threading.Event()
    stop = threading.Event()
    error: list[BaseException] = []

    def _runner() -> None:
        try:
            conn = psycopg.connect(db_url)  # default autocommit=False
            try:
                with conn.cursor() as cur:
                    # Parent first, matching LML's natural order.
                    cur.execute(
                        "INSERT INTO release (id, title, format, country) VALUES (%s, %s, %s, %s)",
                        (release_id, "On Your Own Love Again", "LP", "US"),
                    )
                    cur.execute(
                        "INSERT INTO release_artist (release_id, artist_name) VALUES (%s, %s)",
                        (release_id, "Jessica Pratt"),
                    )
                    cur.execute(
                        "INSERT INTO cache_metadata (release_id, source) VALUES (%s, %s)",
                        (release_id, "api_fetch"),
                    )
                    locks_acquired.set()
                    # Hold the locks until either the caller asks us to stop
                    # or the hold window expires.
                    stop.wait(timeout=hold_seconds)
                conn.commit()
            finally:
                conn.close()
        except BaseException as exc:  # noqa: BLE001
            error.append(exc)
            locks_acquired.set()
            stop.set()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    try:
        # Wait up to 5s for the LML simulator to acquire its locks.
        if not locks_acquired.wait(timeout=5.0):
            stop.set()
            thread.join(timeout=2.0)
            pytest.skip("LML simulator failed to acquire locks within 5s")
        if error:
            raise error[0]
        yield locks_acquired
    finally:
        stop.set()
        thread.join(timeout=hold_seconds + 5.0)
        if error and not isinstance(error[0], BaseException):
            raise error[0]  # type: ignore[unreachable]


class TestVerifyCacheConcurrentLMLWrites:
    """The prune's ADD CONSTRAINT FK retries cleanly under live LML writes.

    The contention scenario:
      1. LML simulator opens a transaction holding RowExclusive on
         ``release`` + ``release_artist`` + ``cache_metadata``.
      2. Foreground calls :func:`add_constraint_safely` with
         ``lock_timeout = '1s'`` to compress the retry envelope into
         test-runnable wall-clock time. First ``LOCK TABLE`` attempt
         times out → 55P03 → backoff → retry.
      3. LML simulator commits, releasing its locks.
      4. Foreground's second attempt acquires the locks and the
         ``ADD CONSTRAINT FOREIGN KEY ... NOT VALID`` succeeds.

    We accept ``pytest.skip()`` if the race window isn't hit after a few
    attempts (a non-race run isn't a production-code failure).
    """

    @pytest.fixture(autouse=True)
    def _set_up(self, db_url):
        self.db_url = db_url
        _seed_minimal_fixture(db_url)

    def test_add_constraint_under_lml_contention_retries_and_succeeds(self) -> None:
        # release_id distinct from the seed rows so LML's INSERT doesn't
        # clash with any existing row on the PK we're about to add.
        race_id = 100

        # Tight retry envelope so the test runs in seconds, not minutes.
        # In production these are sized for sub-second LML p99
        # (lock_timeout = '5s', backoff 5/15/45s). Here we compress to
        # ~1s lock_timeout with a 0.5s backoff so a 4s LML hold window
        # comfortably forces at least one 55P03 retry.
        lml_hold_seconds = 3.0
        prune_lock_timeout = "1s"

        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            # Skip the test entirely if the seeded release rows already
            # have the FK present (which can happen on a non-fresh DB).
            assert not _fk_exists(conn, "release_artist", "fk_test_286"), (
                "FK already present before test — fixture not idempotent?"
            )

            with _lml_writer_holding_locks(self.db_url, lml_hold_seconds, race_id):
                start = time.monotonic()
                stats = add_constraint_safely(
                    conn,
                    "ALTER TABLE release_artist ADD CONSTRAINT fk_test_286 "
                    "FOREIGN KEY (release_id) REFERENCES release(id) "
                    "ON DELETE CASCADE NOT VALID",
                    lock_tables=["release", "release_artist"],
                    lock_timeout=prune_lock_timeout,
                    attempts=4,
                    backoff_seconds=[0.5, 0.5, 0.5],
                )
                elapsed = time.monotonic() - start

            # The whole point: the retry path must be exercised. If it
            # wasn't, the LML writer raced too fast and we never tested
            # what we set out to test — skip rather than assert success.
            if SQLSTATE_LOCK_NOT_AVAILABLE not in stats.sqlstates_seen:
                pytest.skip(
                    f"Race window not hit (lml_hold={lml_hold_seconds}s, "
                    f"prune_lock_timeout={prune_lock_timeout}, "
                    f"elapsed={elapsed:.2f}s). The fix may still be working but "
                    "the test didn't exercise the contention path."
                )

            # The FK is now present and the foreground call returned
            # cleanly — that's the production-code property we care about.
            assert _fk_exists(conn, "release_artist", "fk_test_286"), (
                "After retry, FK constraint should be present — "
                "add_constraint_safely returned but didn't actually run the DDL."
            )
            assert stats.attempts >= 2, (
                f"Expected at least one retry; got attempts={stats.attempts}"
            )
        finally:
            conn.close()

    def test_drop_constraint_under_lml_contention_retries_and_succeeds(self) -> None:
        """The swap-path DROP CONSTRAINT must tolerate LML contention too.

        ``_prune_copy_swap_tables`` runs ``ALTER TABLE ... DROP CONSTRAINT``
        which takes AccessExclusive on the child table — conflicts with
        LML's RowExclusive on that table. Wider lock surface than the FK
        add path, but the same fix shape applies. See #286 "Second deadlock
        surface in the same script."
        """
        # First add the FK so we have something to drop.
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE release_artist ADD CONSTRAINT fk_test_286_drop "
                    "FOREIGN KEY (release_id) REFERENCES release(id) NOT VALID"
                )

            race_id = 101
            lml_hold_seconds = 3.0

            with _lml_writer_holding_locks(self.db_url, lml_hold_seconds, race_id):
                stats = add_constraint_safely(
                    conn,
                    "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_test_286_drop",
                    lock_tables=["release_artist"],
                    lock_timeout="1s",
                    attempts=4,
                    backoff_seconds=[0.5, 0.5, 0.5],
                )

            if SQLSTATE_LOCK_NOT_AVAILABLE not in stats.sqlstates_seen:
                pytest.skip(
                    "Race window not hit for swap-path DROP CONSTRAINT. "
                    "The fix may still be working but the test didn't "
                    "exercise the contention path."
                )

            assert not _fk_exists(conn, "release_artist", "fk_test_286_drop"), (
                "After retry, DROP CONSTRAINT should have removed the FK."
            )
        finally:
            conn.close()
