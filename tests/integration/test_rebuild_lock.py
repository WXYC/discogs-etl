"""Integration tests for lib/rebuild_lock.py against real PostgreSQL (discogs-etl#354).

Follows the two-connection contention pattern established in
``tests/integration/test_seed_cache_from_clone.py``'s
``test_advisory_lock_serializes_concurrent_seed_writes`` -- a holder
connection takes the lock directly via SQL, a second caller goes through the
module under test, and the test asserts the second caller is refused. Unlike
that xact-lock test (which asserts the *loser blocks*, because
``pg_advisory_xact_lock`` is the blocking primitive), this module wraps
``pg_try_advisory_lock`` -- non-blocking by design (#354's whole point: a
second rebuild instance must bow out immediately, not queue behind a
multi-hour rebuild) -- so the assertion here is that the second caller
returns promptly with a refusal, not that it blocks.

Both connections in every test below share **one** ``fresh_db_url`` --
session advisory locks are scoped per-database (verified empirically against
this repo's docker-compose Postgres while designing the module; see
lib/rebuild_lock.py's docstring), so two connections to two *different*
databases would never contend regardless of the implementation.
"""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from lib.rebuild_lock import (
    REBUILD_LOCK_BOWED_OUT_EXIT_CODE,
    REBUILD_LOCK_CONNECT_PARAMS,
    REBUILD_LOCK_HOLDER_DEDUP,
    REBUILD_LOCK_HOLDER_FREE_EXIT_CODE,
    REBUILD_LOCK_HOLDER_REBUILD,
    REBUILD_LOCK_HOLDER_SEED,
    REBUILD_LOCK_KEY,
    describe_rebuild_lock_holder,
    release_rebuild_lock,
    try_acquire_rebuild_lock,
)

pytestmark = [pytest.mark.pg]

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = REPO_ROOT / "scripts"

# The name the contention tests below take the lock under; the tests that care
# about names pass their own.
_HOLDER = REBUILD_LOCK_HOLDER_REBUILD


class TestTryAcquireRebuildLock:
    def test_acquires_when_unlocked(self, fresh_db_url: str) -> None:
        conn = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)
        try:
            assert conn is not None
            assert not conn.closed
        finally:
            if conn is not None:
                release_rebuild_lock(conn)

    def test_the_held_connection_really_has_keepalives_enabled(self, fresh_db_url: str) -> None:
        """The lock connection is idle for the rebuild's entire multi-hour run
        and reaches Railway through a public TCP proxy, so the keepalives are
        load-bearing rather than decorative. Asserted against a live
        connection's negotiated conninfo, not just the kwargs we passed."""
        conn = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)
        try:
            assert conn is not None
            live = conninfo_to_dict(conn.info.dsn)
            for param, value in REBUILD_LOCK_CONNECT_PARAMS.items():
                assert live.get(param) == str(value), (
                    f"{param} did not survive onto the live lock connection: "
                    f"got {live.get(param)!r}, expected {str(value)!r}"
                )
        finally:
            if conn is not None:
                release_rebuild_lock(conn)

    def test_second_caller_refused_while_first_holds_it(self, fresh_db_url: str) -> None:
        """A holder of pg_try_advisory_lock(354001) on the target must cause a
        second try_acquire_rebuild_lock() against the same database to return
        None immediately -- it must NOT block and it must NOT succeed."""
        holder = psycopg.connect(fresh_db_url, autocommit=True)
        with holder.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (REBUILD_LOCK_KEY,))
            (acquired,) = cur.fetchone()
        assert acquired, "test setup: holder must acquire the lock first"

        result_box: dict = {}

        def run_second_caller() -> None:
            result_box["conn"] = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)

        t = threading.Thread(target=run_second_caller)
        start = time.monotonic()
        t.start()
        t.join(timeout=5.0)
        elapsed = time.monotonic() - start

        try:
            assert not t.is_alive(), "try_acquire_rebuild_lock must not block"
            assert elapsed < 2.0, (
                f"try_acquire_rebuild_lock took {elapsed:.2f}s; pg_try_advisory_lock "
                "is non-blocking and should return in well under a second"
            )
            assert result_box["conn"] is None, "a second caller must be refused, not granted"
        finally:
            with holder.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (REBUILD_LOCK_KEY,))
            holder.close()

    def test_second_caller_does_not_leak_a_connection_on_refusal(self, fresh_db_url: str) -> None:
        """A refused acquisition must close its own connection rather than
        leaving an idle backend behind -- otherwise every bowed-out instance
        accumulates a stray connection against the shared cache DB.

        Polled rather than sampled once: ``conn.close()`` returns as soon as
        the client has closed its socket, but the server-side backend exits
        asynchronously, so an immediate second count races that teardown. A
        single sample failed ~1 run in 25 locally; waiting for the count to
        come back to baseline tests the same property (no *leaked* backend)
        without depending on how promptly the postmaster reaps.
        """
        holder = psycopg.connect(fresh_db_url, autocommit=True)
        with holder.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (REBUILD_LOCK_KEY,))

        admin = psycopg.connect(fresh_db_url, autocommit=True)

        def backend_count() -> int:
            with admin.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND pid <> pg_backend_pid()"
                )
                (count,) = cur.fetchone()
            return count

        try:
            before = backend_count()

            refused = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)
            assert refused is None

            deadline = time.monotonic() + 10.0
            after = backend_count()
            while after != before and time.monotonic() < deadline:
                time.sleep(0.05)
                after = backend_count()

            assert after == before, (
                f"a refused acquisition must not leak an open connection "
                f"({before} backends before, still {after} after 10s)"
            )
        finally:
            with holder.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (REBUILD_LOCK_KEY,))
            holder.close()
            admin.close()

    def test_acquires_once_a_prior_holder_releases(self, fresh_db_url: str) -> None:
        holder = psycopg.connect(fresh_db_url, autocommit=True)
        with holder.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (REBUILD_LOCK_KEY,))
            (acquired,) = cur.fetchone()
        assert acquired

        assert try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER) is None

        with holder.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (REBUILD_LOCK_KEY,))
        holder.close()

        conn = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)
        try:
            assert conn is not None, "lock must become acquirable once the holder releases"
        finally:
            if conn is not None:
                release_rebuild_lock(conn)

    def test_different_databases_do_not_contend(self, fresh_db_url: str, db_url: str) -> None:
        """Sanity check on the per-database scoping this module relies on:
        two different databases on the same Postgres server must not
        serialize against each other just because they share a key."""
        if fresh_db_url.rsplit("/", 1)[0] != db_url.rsplit("/", 1)[0]:
            pytest.skip("fixtures resolved to different Postgres servers")

        holder = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)
        assert holder is not None
        try:
            other = try_acquire_rebuild_lock(db_url, holder=_HOLDER)
            try:
                assert other is not None, (
                    "a lock held in one database must not block acquisition "
                    "in a different database on the same server"
                )
            finally:
                if other is not None:
                    release_rebuild_lock(other)
        finally:
            release_rebuild_lock(holder)


def _hold_lock_by_hand(
    url: str, application_name: str, **connect_kwargs: str
) -> psycopg.Connection:
    """Take 354001 the way a holder outside this module does (an operator's
    psql, a caller from an older checkout): a raw connection, its own name."""
    conn = psycopg.connect(
        url, autocommit=True, application_name=application_name, **connect_kwargs
    )
    (acquired,) = conn.execute("SELECT pg_try_advisory_lock(%s)", (REBUILD_LOCK_KEY,)).fetchone()
    assert acquired, "test setup: the hand-held lock must be acquired"
    return conn


class TestHolderApplicationName:
    @pytest.mark.parametrize(
        "holder",
        [REBUILD_LOCK_HOLDER_REBUILD, REBUILD_LOCK_HOLDER_DEDUP, REBUILD_LOCK_HOLDER_SEED],
    )
    def test_lock_connection_names_itself_in_pg_stat_activity(
        self, fresh_db_url: str, holder: str
    ) -> None:
        conn = try_acquire_rebuild_lock(fresh_db_url, holder=holder)
        assert conn is not None
        try:
            with psycopg.connect(fresh_db_url) as observer:
                row = observer.execute(
                    "SELECT application_name FROM pg_stat_activity WHERE pid = %s",
                    (conn.info.backend_pid,),
                ).fetchone()
            assert row == (holder,)
        finally:
            release_rebuild_lock(conn)

    def test_the_three_names_are_distinct_and_namespaced(self) -> None:
        names = {REBUILD_LOCK_HOLDER_REBUILD, REBUILD_LOCK_HOLDER_DEDUP, REBUILD_LOCK_HOLDER_SEED}
        assert len(names) == 3
        assert all(name.startswith("discogs-etl-") for name in names)


class TestDescribeRebuildLockHolder:
    def test_returns_the_named_holders_application_name(self, fresh_db_url: str) -> None:
        conn = try_acquire_rebuild_lock(fresh_db_url, holder=REBUILD_LOCK_HOLDER_SEED)
        assert conn is not None
        try:
            assert describe_rebuild_lock_holder(fresh_db_url) == REBUILD_LOCK_HOLDER_SEED
        finally:
            release_rebuild_lock(conn)

    def test_returns_empty_string_for_an_unnamed_holder(self, fresh_db_url: str) -> None:
        holder = _hold_lock_by_hand(fresh_db_url, "")
        try:
            assert describe_rebuild_lock_holder(fresh_db_url) == ""
        finally:
            holder.close()

    def test_returns_none_when_nobody_holds_the_lock(self, fresh_db_url: str) -> None:
        assert describe_rebuild_lock_holder(fresh_db_url) is None

    def test_returns_none_after_the_holder_releases(self, fresh_db_url: str) -> None:
        conn = try_acquire_rebuild_lock(fresh_db_url, holder=REBUILD_LOCK_HOLDER_DEDUP)
        assert conn is not None
        release_rebuild_lock(conn)
        assert describe_rebuild_lock_holder(fresh_db_url) is None

    def test_a_different_key_is_not_reported_as_the_holder(self, fresh_db_url: str) -> None:
        with psycopg.connect(fresh_db_url, autocommit=True, application_name="other-key") as c:
            c.execute("SELECT pg_advisory_lock(%s)", (REBUILD_LOCK_KEY + 1,))
            assert describe_rebuild_lock_holder(fresh_db_url) is None

    def test_a_holder_in_another_database_is_not_reported(
        self, fresh_db_url: str, db_url: str
    ) -> None:
        if fresh_db_url.rsplit("/", 1)[0] != db_url.rsplit("/", 1)[0]:
            pytest.skip("fixtures resolved to different Postgres servers")
        conn = try_acquire_rebuild_lock(db_url, holder=REBUILD_LOCK_HOLDER_REBUILD)
        assert conn is not None
        try:
            assert describe_rebuild_lock_holder(fresh_db_url) is None
        finally:
            release_rebuild_lock(conn)

    def test_a_holder_under_another_role_still_reads_by_name(self, fresh_db_url: str) -> None:
        """pg_stat_activity hides other roles' query text and state from an
        unprivileged reader, but application_name stays visible. Pinned rather
        than assumed: the monthly rebuild's role is not the role of a dedup, a
        seed or an operator's psql, and a name that read as '' there would make
        a peer rebuild look like a wait-worthy holder."""
        role = f"rebuild_lock_probe_{time.monotonic_ns()}"
        with psycopg.connect(fresh_db_url, autocommit=True) as admin:
            admin.execute(f"CREATE ROLE {role} LOGIN PASSWORD 'probe'")
        try:
            held_by_other_role = _hold_lock_by_hand(
                fresh_db_url, REBUILD_LOCK_HOLDER_REBUILD, user=role, password="probe"
            )
            try:
                assert describe_rebuild_lock_holder(fresh_db_url) == REBUILD_LOCK_HOLDER_REBUILD
            finally:
                held_by_other_role.close()

            named = try_acquire_rebuild_lock(fresh_db_url, holder=REBUILD_LOCK_HOLDER_SEED)
            assert named is not None
            try:
                as_role = make_conninfo(fresh_db_url, user=role, password="probe")
                assert describe_rebuild_lock_holder(as_role) == REBUILD_LOCK_HOLDER_SEED
            finally:
                release_rebuild_lock(named)
        finally:
            with psycopg.connect(fresh_db_url, autocommit=True) as admin:
                admin.execute(f"DROP ROLE {role}")


class TestDescribeHolderCli:
    """The shell's view of the holder: scripts/rebuild-cache.sh reads stdout and
    the exit code (0 = held, name on stdout, possibly empty; 3 = free)."""

    @staticmethod
    def _run(url: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "lib.rebuild_lock", "--describe-holder", "--database-url", url],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=60,
        )

    def test_prints_the_holder_and_exits_zero(self, fresh_db_url: str) -> None:
        conn = try_acquire_rebuild_lock(fresh_db_url, holder=REBUILD_LOCK_HOLDER_DEDUP)
        assert conn is not None
        try:
            result = self._run(fresh_db_url)
        finally:
            release_rebuild_lock(conn)
        assert (result.returncode, result.stdout) == (0, f"{REBUILD_LOCK_HOLDER_DEDUP}\n")

    def test_an_unnamed_holder_is_exit_zero_with_empty_output(self, fresh_db_url: str) -> None:
        holder = _hold_lock_by_hand(fresh_db_url, "")
        try:
            result = self._run(fresh_db_url)
        finally:
            holder.close()
        assert (result.returncode, result.stdout.strip()) == (0, "")

    def test_a_free_lock_has_its_own_exit_code(self, fresh_db_url: str) -> None:
        result = self._run(fresh_db_url)
        assert (result.returncode, result.stdout) == (REBUILD_LOCK_HOLDER_FREE_EXIT_CODE, "")
        assert REBUILD_LOCK_HOLDER_FREE_EXIT_CODE not in (0, 1, 2, REBUILD_LOCK_BOWED_OUT_EXIT_CODE)


class TestReleaseRebuildLock:
    def test_release_closes_the_connection(self, fresh_db_url: str) -> None:
        conn = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)
        assert conn is not None
        release_rebuild_lock(conn)
        assert conn.closed

    def test_close_without_explicit_release_still_frees_the_lock(self, fresh_db_url: str) -> None:
        """release_rebuild_lock is never called on an abnormal exit path. This
        checks the mechanism that makes that safe: closing the connection
        without ever issuing pg_advisory_unlock must still free the lock,
        because Postgres ties a session-level advisory lock to the backend's
        connection lifetime, not to an explicit unlock call."""
        conn = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)
        assert conn is not None
        conn.close()  # no pg_advisory_unlock call at all

        reacquired = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)
        try:
            assert reacquired is not None, (
                "Postgres must release a session-level advisory lock when its "
                "holding backend disconnects, even without an explicit unlock"
            )
        finally:
            if reacquired is not None:
                release_rebuild_lock(reacquired)

    def test_sigkilled_holder_still_frees_the_lock(self, fresh_db_url: str) -> None:
        """The literal crash scenario release_rebuild_lock's docstring
        promises Postgres handles without any application-level cleanup:
        SIGKILL a subprocess mid-hold (no chance to run any Python cleanup
        code, unlike a graceful close()) and confirm the lock frees anyway.
        This is what makes the guard safe against an EC2 instance dying mid-
        rebuild rather than exiting cleanly."""
        holder_script = (
            "import sys, time\n"
            "import psycopg\n"
            f"conn = psycopg.connect(sys.argv[1], autocommit=True)\n"
            f"conn.execute('SELECT pg_try_advisory_lock({REBUILD_LOCK_KEY})')\n"
            "print('locked', flush=True)\n"
            "time.sleep(60)\n"
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", holder_script, fresh_db_url],
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            line = proc.stdout.readline()
            assert line.strip() == "locked", f"holder subprocess failed to acquire: {line!r}"

            # Confirm real contention while the holder is alive.
            assert try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER) is None

            proc.kill()  # SIGKILL -- no chance for the holder to clean up
            proc.wait(timeout=10)

            # Postgres must notice the dead backend and free the lock without
            # any explicit unlock call ever having run in that process.
            deadline = time.monotonic() + 10.0
            reacquired = None
            while time.monotonic() < deadline:
                reacquired = try_acquire_rebuild_lock(fresh_db_url, holder=_HOLDER)
                if reacquired is not None:
                    break
                time.sleep(0.2)
            assert reacquired is not None, (
                "the lock was never released after its holder was SIGKILLed"
            )
            release_rebuild_lock(reacquired)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)


class TestRunPipelineBowsOutEndToEnd:
    """End-to-end: a lock-loser's run_pipeline.py process must exit with the
    distinct bow-out code and must not be mistaken for a successful run --
    the exact defect class incident #352 exists to eliminate (see the ticket's
    warning about scripts/rebuild-cache.sh's exit-0 trap)."""

    def test_lock_loser_exits_with_bow_out_code_and_touches_nothing(
        self, fresh_db_url: str, tmp_path: Path
    ) -> None:
        holder = psycopg.connect(fresh_db_url, autocommit=True)
        with holder.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (REBUILD_LOCK_KEY,))
            (acquired,) = cur.fetchone()
        assert acquired, "test setup: holder must acquire the lock first"

        try:
            csv_dir = tmp_path / "csv"
            csv_dir.mkdir()
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_DIR / "run_pipeline.py"),
                    "--csv-dir",
                    str(csv_dir),
                    "--database-url",
                    fresh_db_url,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            assert result.returncode == REBUILD_LOCK_BOWED_OUT_EXIT_CODE, (
                f"lock-loser must exit {REBUILD_LOCK_BOWED_OUT_EXIT_CODE}, "
                f"not {result.returncode}. This is the exit code "
                f"scripts/rebuild-cache.sh must branch on to avoid reporting "
                f"a bow-out as a successful rebuild. stderr:\n{result.stderr}"
            )
            assert result.returncode != 0, (
                "a bowed-out run must NEVER exit 0 -- an exit-0 bow-out is "
                "indistinguishable from success to 'set -e' callers such as "
                "scripts/rebuild-cache.sh, which is precisely the false-"
                "positive defect class incident #352 exists to eliminate."
            )

            conn = psycopg.connect(fresh_db_url, autocommit=True)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'release'"
                )
                (release_table_exists,) = cur.fetchone()
            conn.close()
            assert release_table_exists == 0, (
                "a bowed-out run must not have touched the cache -- the "
                "'release' table must never be created"
            )
        finally:
            with holder.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (REBUILD_LOCK_KEY,))
            holder.close()

    def test_uncontended_run_does_not_use_the_bow_out_code(
        self, fresh_db_url: str, tmp_path: Path
    ) -> None:
        """Negative control: an uncontended run must reach real pipeline work
        (and thus fail for a *different* reason -- an empty --csv-dir has no
        CSVs to import) rather than exiting with the bow-out sentinel."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "run_pipeline.py"),
                "--csv-dir",
                str(csv_dir),
                "--database-url",
                fresh_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode != REBUILD_LOCK_BOWED_OUT_EXIT_CODE, (
            "an uncontended run must not report a bow-out -- got the bow-out "
            f"exit code for a run that should have acquired the lock cleanly. "
            f"stderr:\n{result.stderr}"
        )
