"""Pin for WXYC/discogs-etl#356: per-invocation scratch-table namespacing.

Two concurrent rebuild-pipeline invocations against the same discogs-cache
database used to share one namespace for their working tables
(``dedup_delete_ids``, ``keep_release_ids``, ``_keep_ids``, ``new_release*``).
On 2026-08-04 that collision destroyed 27,163 releases: both runs computed
``dedup_delete_ids`` identically, then one run's ``DROP TABLE IF EXISTS
dedup_delete_ids`` deleted the other's mid-flight working set, and the
survivor's copy-and-swap then read from a source table the peer had already
renamed out from under it (WXYC/discogs-etl#352). Fix: every scratch table
name is namespaced with a random per-invocation suffix
(``lib.scratch_namespace``), so two overlapping invocations never share a
name to collide on in the first place.

This file has two concerns:

  * Namespacing actually isolates concurrent invocations -- reproduces the
    incident's collision shape (one invocation force-recreating its own
    table while a peer's same-named-but-for-the-suffix table is mid-flight)
    and asserts the peer is untouched.
  * A crashed invocation leaves no scratch residue -- kills the backend
    running ``build_dedup_scratch_tables`` / ``_prune_build_scratch_tables``
    mid-``CREATE TABLE`` (via ``pg_terminate_backend``, mirroring
    ``tests/integration/test_error_resilience.py``'s
    ``TestDedupTerminatedMidOperation``) and asserts every scratch table for
    that suffix is gone: the whole build phase runs inside one
    non-autocommit transaction, so PostgreSQL rolls it back entirely the
    instant it notices the connection is gone.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
from pathlib import Path

import psycopg
import pytest

from lib.scratch_namespace import new_scratch_suffix

pytestmark = pytest.mark.pg

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

_DEDUP_PATH = Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py"
if "dedup_releases" in sys.modules:
    dedup_releases = sys.modules["dedup_releases"]
else:
    _dspec = importlib.util.spec_from_file_location("dedup_releases", _DEDUP_PATH)
    assert _dspec is not None and _dspec.loader is not None
    dedup_releases = importlib.util.module_from_spec(_dspec)
    sys.modules["dedup_releases"] = dedup_releases
    _dspec.loader.exec_module(dedup_releases)

_VERIFY_CACHE_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
if "verify_cache" in sys.modules:
    verify_cache = sys.modules["verify_cache"]
else:
    _vcspec = importlib.util.spec_from_file_location("verify_cache", _VERIFY_CACHE_PATH)
    assert _vcspec is not None and _vcspec.loader is not None
    verify_cache = importlib.util.module_from_spec(_vcspec)
    sys.modules["verify_cache"] = verify_cache
    _vcspec.loader.exec_module(verify_cache)


def _apply_schema(db_url: str) -> None:
    """Clean-slate schema apply. Mirrors test_error_resilience.py::_apply_schema."""
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
        cur.execute(SCHEMA_DIR.joinpath("drop_core_tables.sql").read_text())
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    conn.close()


def _seed_dedup_workload(db_url: str, n_releases: int) -> None:
    """Populate release/release_artist/release_track with duplicate pairs.

    Every pair of consecutive releases shares a master_id + format, so
    dedup finds one duplicate per pair. Mirrors
    test_error_resilience.py::_seed_dedup_workload (kept local per this
    repo's per-file-helper convention).
    """
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        with cur.copy("COPY release (id, title, country, master_id, format) FROM STDIN") as copy:
            for i in range(n_releases):
                rid = 10_000 + i
                master_id = 50_000 + (i // 2)
                copy.write_row((rid, "On Your Own Love Again", "US", master_id, "LP"))
        with cur.copy(
            "COPY release_artist (release_id, artist_id, artist_name, extra) FROM STDIN"
        ) as copy:
            for i in range(n_releases):
                copy.write_row((10_000 + i, 200, "Jessica Pratt", 0))
        with cur.copy(
            "COPY release_track (release_id, sequence, position, title) FROM STDIN"
        ) as copy:
            for i in range(n_releases):
                copy.write_row((10_000 + i, 1, "A1", "Back, Baby"))
    conn.commit()
    conn.close()


def _table_exists(db_url: str, table_name: str) -> bool:
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                (table_name,),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def _tables_matching_suffix(db_url: str, suffix: str) -> list[str]:
    """Every public-schema table whose name ends in ``_<suffix>``.

    Fetches every public table name and filters in Python rather than a SQL
    LIKE pattern, to sidestep LIKE's own escape-character semantics for the
    literal underscore separating the base name from the suffix.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            names = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    return [n for n in names if n.endswith(f"_{suffix}")]


def _terminate_first_match(
    db_url: str,
    pattern_substrings: tuple[str, ...],
    max_polls: int = 1000,
    poll_interval_s: float = 0.005,
) -> tuple[threading.Event, threading.Event]:
    """Poll pg_stat_activity for any backend on this database running a
    query matching ``pattern_substrings`` and pg_terminate_backend it.

    Generalizes test_error_resilience.py::_terminate_when to not require
    knowing the target PID in advance (build_dedup_scratch_tables /
    _prune_build_scratch_tables open their own internal connection, so the
    caller never sees a backend_pid to target directly).

    Returns ``(matched, finished)`` -- see _terminate_when's docstring for
    the semantics; identical here.
    """
    matched = threading.Event()
    finished = threading.Event()

    def _runner() -> None:
        admin = psycopg.connect(db_url, autocommit=True)
        try:
            for _ in range(max_polls):
                with admin.cursor() as cur:
                    cur.execute(
                        "SELECT pid, query FROM pg_stat_activity "
                        "WHERE datname = current_database() AND pid <> pg_backend_pid()"
                    )
                    rows = cur.fetchall()
                for pid, query in rows:
                    if query and any(p.upper() in query.upper() for p in pattern_substrings):
                        with admin.cursor() as cur2:
                            cur2.execute("SELECT pg_terminate_backend(%s)", (pid,))
                        matched.set()
                        return
                time.sleep(poll_interval_s)
        finally:
            admin.close()
            finished.set()

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    return matched, finished


class TestDedupScratchNamespacingConcurrency:
    """Reproduces the 2026-08-04 collision shape with namespacing in place:
    two invocations' dedup_delete_ids tables coexist under distinct
    suffixes, so one invocation force-recreating its own table can never
    touch the other's."""

    @pytest.fixture(autouse=True)
    def _set_up(self, fresh_db_url):
        self.db_url = fresh_db_url
        _apply_schema(fresh_db_url)
        _seed_dedup_workload(fresh_db_url, n_releases=2_000)

    def test_two_concurrent_invocations_use_independent_dedup_delete_ids(self) -> None:
        suffix_a = new_scratch_suffix()
        suffix_b = new_scratch_suffix()
        assert suffix_a != suffix_b

        barrier = threading.Barrier(2)
        results: dict[str, int] = {}
        errors: list[BaseException] = []

        def _run(suffix: str, conn) -> None:
            try:
                barrier.wait(timeout=10)
                count = dedup_releases.ensure_dedup_ids(conn, suffix=suffix)
                results[suffix] = count
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        conn_a = psycopg.connect(self.db_url, autocommit=True)
        conn_b = psycopg.connect(self.db_url, autocommit=True)
        try:
            t_a = threading.Thread(target=_run, args=(suffix_a, conn_a))
            t_b = threading.Thread(target=_run, args=(suffix_b, conn_b))
            t_a.start()
            t_b.start()
            t_a.join(timeout=30)
            t_b.join(timeout=30)
        finally:
            conn_a.close()
            conn_b.close()

        assert not errors, f"concurrent ensure_dedup_ids raised: {errors}"
        assert results[suffix_a] == 1_000  # half of 2,000 seeded pairs
        assert results[suffix_b] == 1_000
        assert _table_exists(self.db_url, f"dedup_delete_ids_{suffix_a}")
        assert _table_exists(self.db_url, f"dedup_delete_ids_{suffix_b}")

    def test_force_recreate_on_one_invocation_does_not_touch_the_others_table(self) -> None:
        """The exact incident mechanism, made survivable: invocation B's
        DROP TABLE IF EXISTS (from the keep_ids_loaded force-recreate path)
        targets ONLY its own suffixed table now -- invocation A's identically-
        shaped table, mid-flight under a different suffix, is untouched."""
        suffix_a = new_scratch_suffix()
        suffix_b = new_scratch_suffix()

        conn_a = psycopg.connect(self.db_url, autocommit=True)
        conn_b = psycopg.connect(self.db_url, autocommit=True)
        try:
            # Invocation A computes its dedup_delete_ids first (as the
            # 2026-08-04 incident's two runs both did, computing an
            # identical result independently).
            count_a_before = dedup_releases.ensure_dedup_ids(conn_a, suffix=suffix_a)
            assert count_a_before == 1_000

            # Invocation B loads a (trivial, non-matching) keep-release-ids
            # override, which forces ensure_dedup_ids to
            # DROP TABLE IF EXISTS its own dedup_delete_ids before
            # recomputing -- this is the DROP that collided with a peer
            # pre-#356.
            with conn_b.cursor() as cur:
                cur.execute(
                    f"DROP TABLE IF EXISTS keep_release_ids_{suffix_b}",
                )
                cur.execute(
                    f"CREATE UNLOGGED TABLE keep_release_ids_{suffix_b} "
                    "(release_id integer PRIMARY KEY)"
                )
            count_b = dedup_releases.ensure_dedup_ids(conn_b, keep_ids_loaded=True, suffix=suffix_b)
            assert count_b == 1_000

            # Invocation A's table must be completely unaffected.
            with conn_a.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM dedup_delete_ids_{suffix_a}")
                count_a_after = cur.fetchone()[0]
            assert count_a_after == count_a_before
        finally:
            conn_a.close()
            conn_b.close()


class TestDedupScratchTableCrashResidue:
    """A killed dedup invocation leaves zero scratch-table residue: the
    whole scratch-build phase (dedup_delete_ids + every new_X copy) runs
    inside one non-autocommit transaction, so PostgreSQL's own rollback-on-
    disconnect undoes everything the instant the backend is terminated."""

    @pytest.fixture(autouse=True)
    def _set_up(self, fresh_db_url):
        self.db_url = fresh_db_url
        _apply_schema(fresh_db_url)
        _seed_dedup_workload(fresh_db_url, n_releases=50_000)

    def test_killed_mid_create_table_leaves_no_scratch_residue(self) -> None:
        suffix = new_scratch_suffix()
        result: dict = {"raised": False}

        def _drive() -> None:
            try:
                dedup_releases.build_dedup_scratch_tables(self.db_url, suffix)
            except Exception:  # noqa: BLE001
                result["raised"] = True

        matched, finished = _terminate_first_match(
            self.db_url, pattern_substrings=("CREATE TABLE",)
        )

        worker = threading.Thread(target=_drive, daemon=True)
        worker.start()
        worker.join(timeout=60)
        assert not worker.is_alive(), "build_dedup_scratch_tables hung after kill"

        finished.wait(timeout=30)
        assert finished.is_set(), "terminator thread did not exit"

        if not matched.is_set():
            pytest.skip("terminator did not catch a CREATE TABLE on this machine")
        if not result["raised"]:
            pytest.skip(
                "build_dedup_scratch_tables completed before the kill landed; "
                "cannot exercise mid-operation kill on this machine"
            )

        # Zero residue: nothing for THIS suffix survives anywhere in public.
        leftovers = _tables_matching_suffix(self.db_url, suffix)
        assert leftovers == [], f"scratch residue survived a killed invocation: {leftovers}"

        # release itself must be untouched (the build phase never renames
        # anything -- that's swap_tables' job, which never ran).
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM release")
                assert cur.fetchone()[0] == 50_000
        finally:
            conn.close()


class TestPruneScratchNamespacingConcurrency:
    """The verify_cache prune side of the same story: _keep_ids and every
    new_X built by _prune_build_scratch_tables are namespaced, so two
    concurrent prune invocations coexist."""

    @pytest.fixture(autouse=True)
    def _set_up(self, fresh_db_url):
        self.db_url = fresh_db_url
        _apply_schema(fresh_db_url)
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (%s, %s, %s, %s, %s)",
                [
                    (1, "DOGA", 200, "LP", "AR"),
                    (2, "Aluminum Tunes", 100, "CD", "UK"),
                    (3, "Edits", None, "CD", "US"),
                ],
            )
        conn.close()

    def test_two_concurrent_invocations_use_independent_keep_ids_and_new_tables(self) -> None:
        suffix_a = new_scratch_suffix()
        suffix_b = new_scratch_suffix()

        verify_cache._prune_build_scratch_tables(
            self.db_url, keep_ids={1, 2}, review_ids=set(), suffix=suffix_a
        )
        verify_cache._prune_build_scratch_tables(
            self.db_url, keep_ids={1}, review_ids=set(), suffix=suffix_b
        )

        # Both invocations' scratch tables must coexist independently.
        assert _table_exists(self.db_url, f"_keep_ids_{suffix_a}")
        assert _table_exists(self.db_url, f"_keep_ids_{suffix_b}")
        assert _table_exists(self.db_url, f"new_release_{suffix_a}")
        assert _table_exists(self.db_url, f"new_release_{suffix_b}")

        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM new_release_{suffix_a}")
                count_a = cur.fetchone()[0]
                cur.execute(f"SELECT count(*) FROM new_release_{suffix_b}")
                count_b = cur.fetchone()[0]
        finally:
            conn.close()
        assert count_a == 2  # keep_ids={1, 2}
        assert count_b == 1  # keep_ids={1}


class TestPruneScratchTableCrashResidue:
    """Mirrors TestDedupScratchTableCrashResidue for the prune side."""

    @pytest.fixture(autouse=True)
    def _set_up(self, fresh_db_url):
        self.db_url = fresh_db_url
        _apply_schema(fresh_db_url)
        _seed_dedup_workload(fresh_db_url, n_releases=50_000)

    def test_killed_mid_create_table_leaves_no_scratch_residue(self) -> None:
        suffix = new_scratch_suffix()
        keep_ids = {10_000 + i for i in range(0, 50_000, 2)}  # keep every other release
        result: dict = {"raised": False}

        def _drive() -> None:
            try:
                verify_cache._prune_build_scratch_tables(
                    self.db_url, keep_ids=keep_ids, review_ids=set(), suffix=suffix
                )
            except Exception:  # noqa: BLE001
                result["raised"] = True

        matched, finished = _terminate_first_match(
            self.db_url, pattern_substrings=("CREATE TABLE",)
        )

        worker = threading.Thread(target=_drive, daemon=True)
        worker.start()
        worker.join(timeout=60)
        assert not worker.is_alive(), "_prune_build_scratch_tables hung after kill"

        finished.wait(timeout=30)
        assert finished.is_set(), "terminator thread did not exit"

        if not matched.is_set():
            pytest.skip("terminator did not catch a CREATE TABLE on this machine")
        if not result["raised"]:
            pytest.skip(
                "_prune_build_scratch_tables completed before the kill landed; "
                "cannot exercise mid-operation kill on this machine"
            )

        leftovers = _tables_matching_suffix(self.db_url, suffix)
        assert leftovers == [], f"scratch residue survived a killed invocation: {leftovers}"

        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM release")
                assert cur.fetchone()[0] == 50_000
        finally:
            conn.close()
