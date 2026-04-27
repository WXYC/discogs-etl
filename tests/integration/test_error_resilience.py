"""Error/resilience tests for the discogs-cache pipeline.

Tests that external dependency failures are handled gracefully:
- UNLOGGED toggle edge cases (non-existent tables, already-toggled)
- Dedup connection loss simulation
- Dedup mid-ANALYZE / mid-copy-swap connection termination
- Import COPY interruption (malformed data, partial failures)

All tests require PostgreSQL and are marked with @pytest.mark.pg.
Uses WXYC example artists for fixture data.
"""

from __future__ import annotations

import importlib.util
import threading
import time
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

# Load run_pipeline module
_rp_spec = importlib.util.spec_from_file_location(
    "run_pipeline",
    Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py",
)
assert _rp_spec is not None and _rp_spec.loader is not None
run_pipeline = importlib.util.module_from_spec(_rp_spec)
_rp_spec.loader.exec_module(run_pipeline)

# Load import_csv module
_ic_spec = importlib.util.spec_from_file_location(
    "import_csv",
    Path(__file__).parent.parent.parent / "scripts" / "import_csv.py",
)
assert _ic_spec is not None and _ic_spec.loader is not None
import_csv = importlib.util.module_from_spec(_ic_spec)
_ic_spec.loader.exec_module(import_csv)

# Load dedup_releases module
_dd_spec = importlib.util.spec_from_file_location(
    "dedup_releases",
    Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py",
)
assert _dd_spec is not None and _dd_spec.loader is not None
dedup_releases = importlib.util.module_from_spec(_dd_spec)
_dd_spec.loader.exec_module(dedup_releases)

pytestmark = pytest.mark.pg

PIPELINE_TABLES = run_pipeline.PIPELINE_TABLES


def _get_table_persistence(db_url: str, table_name: str) -> str | None:
    """Return relpersistence for a table, or None if table doesn't exist."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT relpersistence FROM pg_class WHERE relname = %s",
            (table_name,),
        )
        result = cur.fetchone()
    conn.close()
    if result is None:
        return None
    return result[0]


def _drop_all_tables(db_url: str) -> None:
    """Drop all pipeline tables with CASCADE.

    Includes dedup transient tables (dedup_delete_ids, new_release,
    new_release_artist, wxyc_label_pref, release_track_count,
    release_label_match) so tests that crash mid-dedup do not leak state
    into subsequent tests sharing the module-scoped db_url.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        for table in [
            "cache_metadata",
            "release_track_artist",
            "release_track",
            "release_label",
            "release_artist",
            "release",
            "artist_url",
            "artist_member",
            "artist_name_variation",
            "artist_alias",
            "artist",
            "dedup_delete_ids",
            "new_release",
            "new_release_artist",
            "wxyc_label_pref",
            "release_track_count",
            "release_label_match",
        ]:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    conn.close()


def _apply_schema(db_url: str) -> None:
    """Apply the pipeline schema to a test database.

    create_database.sql references f_unaccent() in the master_title_trgm
    index expression, and create_functions.sql defines f_unaccent() in
    terms of the unaccent extension. psycopg raises on the first error in
    a multi-statement execute, so we must order setup as:
      1. Create the unaccent + pg_trgm extensions.
      2. Define f_unaccent() (depends on unaccent).
      3. Run create_database.sql (depends on f_unaccent).
    psql tolerates out-of-order setup because it continues past per-
    statement errors; psycopg does not, so this ordering is mandatory
    for the integration tests even if production gets away with running
    create_database.sql first.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    conn.close()


def _insert_wxyc_releases(db_url: str) -> None:
    """Insert WXYC example releases for testing."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO release (id, title, country, master_id, format) VALUES
            (5001, 'DOGA', 'AR', 8001, 'LP'),
            (5002, 'Aluminum Tunes', 'UK', 8002, 'CD'),
            (5003, 'Moon Pix', 'US', 8003, 'LP'),
            (5004, 'On Your Own Love Again', 'US', 8004, 'LP'),
            (5005, 'Edits', 'US', NULL, 'CD'),
            (5006, 'Duke Ellington & John Coltrane', 'US', 8005, 'LP')
        """)
        cur.execute("""
            INSERT INTO release_artist (release_id, artist_id, artist_name, extra) VALUES
            (5001, 101, 'Juana Molina', 0),
            (5002, 102, 'Stereolab', 0),
            (5003, 103, 'Cat Power', 0),
            (5004, 104, 'Jessica Pratt', 0),
            (5005, 105, 'Chuquimamani-Condori', 0),
            (5006, 106, 'Duke Ellington', 0),
            (5006, 107, 'John Coltrane', 0)
        """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# UNLOGGED toggle edge cases
# ---------------------------------------------------------------------------


class TestUnloggedEdgeCases:
    """UNLOGGED/LOGGED toggle fails gracefully on non-existent or missing tables."""

    @pytest.fixture(autouse=True)
    def _store_url(self, db_url):
        self.db_url = db_url

    def test_set_unlogged_without_schema_raises(self) -> None:
        """set_tables_unlogged on a fresh DB (no tables) raises an error."""
        with pytest.raises((psycopg.Error, RuntimeError, OSError)):
            run_pipeline.set_tables_unlogged(self.db_url)

    def test_set_logged_without_schema_raises(self) -> None:
        """set_tables_logged on a fresh DB (no tables) raises an error."""
        with pytest.raises((psycopg.Error, RuntimeError, OSError)):
            run_pipeline.set_tables_logged(self.db_url)

    def test_set_unlogged_idempotent(self) -> None:
        """Calling set_tables_unlogged twice doesn't error."""
        _apply_schema(self.db_url)
        run_pipeline.set_tables_unlogged(self.db_url)
        # Second call should not raise
        run_pipeline.set_tables_unlogged(self.db_url)
        for table in PIPELINE_TABLES:
            assert _get_table_persistence(self.db_url, table) == "u"

    def test_set_logged_idempotent(self) -> None:
        """Calling set_tables_logged twice doesn't error."""
        _apply_schema(self.db_url)
        # Tables are LOGGED by default; toggling to logged again should be fine
        run_pipeline.set_tables_logged(self.db_url)
        run_pipeline.set_tables_logged(self.db_url)
        for table in PIPELINE_TABLES:
            assert _get_table_persistence(self.db_url, table) == "p"

    def test_unlogged_to_logged_preserves_data(self) -> None:
        """Data survives the UNLOGGED -> LOGGED transition."""
        _apply_schema(self.db_url)
        _insert_wxyc_releases(self.db_url)

        run_pipeline.set_tables_unlogged(self.db_url)
        run_pipeline.set_tables_logged(self.db_url)

        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 6, "All 6 WXYC releases should survive UNLOGGED/LOGGED toggle"

    def test_unlogged_partial_schema_fails_gracefully(self) -> None:
        """set_tables_unlogged fails if only some tables exist."""
        # Drop everything first since module-scoped db may have leftover tables
        _drop_all_tables(self.db_url)

        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            # Create only the release table, not child tables
            cur.execute("""
                CREATE TABLE release (
                    id integer PRIMARY KEY,
                    title text NOT NULL
                )
            """)
        conn.close()

        # Should fail because child tables referenced in PIPELINE_TABLES don't exist
        with pytest.raises((psycopg.Error, RuntimeError, OSError)):
            run_pipeline.set_tables_unlogged(self.db_url)


# ---------------------------------------------------------------------------
# Dedup connection loss simulation
# ---------------------------------------------------------------------------


class TestDedupConnectionLoss:
    """Dedup operations handle connection issues gracefully."""

    @pytest.fixture(autouse=True)
    def _set_up(self, db_url):
        self.db_url = db_url
        _apply_schema(db_url)
        _insert_wxyc_releases(db_url)

    def test_dedup_on_empty_table_no_crash(self) -> None:
        """Dedup on an empty release table doesn't crash or corrupt state."""
        # Delete all releases first
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM release")
        conn.commit()
        conn.close()

        # ensure_dedup_ids should handle empty tables gracefully
        conn = psycopg.connect(self.db_url)
        dedup_releases.ensure_dedup_ids(conn)
        conn.commit()
        conn.close()

    def test_dedup_with_no_master_ids_no_crash(self) -> None:
        """Dedup when no releases have master_ids should be a no-op."""
        # Set all master_ids to NULL
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("UPDATE release SET master_id = NULL")
        conn.commit()
        conn.close()

        conn = psycopg.connect(self.db_url)
        dedup_releases.ensure_dedup_ids(conn)
        conn.commit()
        conn.close()

        # All releases should still exist (nothing to dedup)
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 6

    def test_swap_tables_nonexistent_source_fails(self) -> None:
        """swap_tables with non-existent source table fails with clear error."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with pytest.raises((psycopg.Error, RuntimeError, OSError)):
            dedup_releases.swap_tables(conn, "_nonexistent_src", "release")
        conn.close()


# ---------------------------------------------------------------------------
# Import COPY interruption
# ---------------------------------------------------------------------------


class TestImportCopyInterruption:
    """COPY operations handle malformed or interrupted data gracefully."""

    @pytest.fixture(autouse=True)
    def _set_up(self, db_url):
        self.db_url = db_url
        _apply_schema(db_url)

    def test_copy_with_wrong_column_count_fails(self) -> None:
        """COPY with mismatched column count fails without leaving partial data."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            # release table expects (id, title, release_year, country, artwork_url,
            # released, format, master_id) -- 8 columns.
            # Send data with too few columns.
            with pytest.raises((psycopg.Error, RuntimeError, OSError)):
                with cur.copy("COPY release (id, title) FROM STDIN") as copy:
                    # Three tab-separated values where two are expected
                    copy.write(b"9001\tBad Data\tExtra\n")
        conn.rollback()

        # Verify no partial data
        conn2 = psycopg.connect(self.db_url)
        with conn2.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            assert cur.fetchone()[0] == 0, "Failed COPY should not leave partial data"
        conn2.close()

    def test_copy_with_fk_violation_fails(self) -> None:
        """COPY into child table with missing parent FK fails cleanly."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            # Try to insert a release_artist for a non-existent release
            with pytest.raises(psycopg.errors.ForeignKeyViolation):
                with cur.copy(
                    "COPY release_artist (release_id, artist_id, artist_name, extra) FROM STDIN"
                ) as copy:
                    copy.write(b"99999\t101\tJuana Molina\t0\n")
        conn.rollback()

        # Verify no partial data
        conn2 = psycopg.connect(self.db_url)
        with conn2.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_artist")
            assert cur.fetchone()[0] == 0
        conn2.close()

    def test_copy_type_mismatch_fails(self) -> None:
        """COPY with type mismatch (text in integer column) fails cleanly."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            with pytest.raises((psycopg.Error, RuntimeError, OSError)):
                with cur.copy("COPY release (id, title) FROM STDIN") as copy:
                    copy.write(b"not_a_number\tStereolab Album\n")
        conn.rollback()

    def test_successful_copy_followed_by_failed_copy_rolls_back(self) -> None:
        """A successful parent COPY followed by a failed child COPY can be rolled back."""
        conn = psycopg.connect(self.db_url)
        try:
            with conn.cursor() as cur:
                # Successfully insert a release
                with cur.copy("COPY release (id, title) FROM STDIN") as copy:
                    copy.write(b"5001\tDOGA\n")

                # Now try to insert release_artist with bad data (should fail)
                with pytest.raises((psycopg.Error, RuntimeError, OSError)):
                    with cur.copy(
                        "COPY release_artist (release_id, artist_id, artist_name, extra) FROM STDIN"
                    ) as copy:
                        # Type mismatch: 'bad' for integer artist_id
                        copy.write(b"5001\tbad\tJuana Molina\t0\n")
        finally:
            conn.rollback()

        # Both tables should be empty after rollback
        conn2 = psycopg.connect(self.db_url)
        with conn2.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            assert cur.fetchone()[0] == 0, "Rollback should undo successful parent COPY"
            cur.execute("SELECT count(*) FROM release_artist")
            assert cur.fetchone()[0] == 0
        conn2.close()

    def test_copy_with_null_in_not_null_column_fails(self) -> None:
        """COPY with \\N in a NOT NULL column (title) fails cleanly."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            with pytest.raises((psycopg.Error, RuntimeError, OSError)):
                with cur.copy("COPY release (id, title) FROM STDIN") as copy:
                    copy.write(b"5001\t\\N\n")
        conn.rollback()

    def test_copy_mixed_valid_and_invalid_rows(self) -> None:
        """COPY with some valid and some invalid rows fails atomically."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            with pytest.raises((psycopg.Error, RuntimeError, OSError)):
                with cur.copy("COPY release (id, title) FROM STDIN") as copy:
                    copy.write(b"5001\tDOGA\n")
                    copy.write(b"5002\tAluminum Tunes\n")
                    copy.write(b"bad_id\tMoon Pix\n")  # this should cause failure
        conn.rollback()

        # No partial data should remain
        conn2 = psycopg.connect(self.db_url)
        with conn2.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            assert cur.fetchone()[0] == 0, "Atomic COPY failure should leave no partial data"
        conn2.close()


# ---------------------------------------------------------------------------
# Vacuum on empty/missing tables
# ---------------------------------------------------------------------------


class TestVacuumEdgeCases:
    """VACUUM operations handle edge cases gracefully."""

    @pytest.fixture(autouse=True)
    def _store_url(self, db_url):
        self.db_url = db_url

    def test_vacuum_empty_tables(self) -> None:
        """VACUUM FULL on empty tables succeeds without error."""
        _apply_schema(self.db_url)
        # Should not raise
        run_pipeline.run_vacuum(self.db_url)

    def test_vacuum_nonexistent_tables_fails(self) -> None:
        """VACUUM FULL on non-existent tables raises an error."""
        # Explicitly drop all tables (module-scoped db may have leftovers)
        _drop_all_tables(self.db_url)
        with pytest.raises((psycopg.Error, RuntimeError, OSError)):
            run_pipeline.run_vacuum(self.db_url)


# ---------------------------------------------------------------------------
# Dedup mid-operation connection termination
# ---------------------------------------------------------------------------


def _seed_dedup_workload(db_url: str, n_releases: int = 50_000) -> None:
    """Populate the release table with enough rows that dedup copy-swap takes
    long enough to race a pg_terminate_backend.

    Uses canonical WXYC artists. Each release has a master_id derived from
    (id // 2), so every pair of consecutive rows form duplicates that dedup
    must collapse.

    Also seeds one release_track row per release: dedup's ROW_NUMBER query
    inner-joins release_track (or release_track_count) to compute track
    counts, so releases with no tracks are silently excluded from
    dedup_delete_ids and dedup becomes a no-op.
    """
    artists = [
        "Juana Molina",
        "Stereolab",
        "Cat Power",
        "Jessica Pratt",
        "Chuquimamani-Condori",
        "Duke Ellington & John Coltrane",
        "Father John Misty",
        "Autechre",
        "Nilüfer Yanya",
        "Hermanos Gutiérrez",
    ]
    titles = [
        "DOGA",
        "Aluminum Tunes",
        "Moon Pix",
        "On Your Own Love Again",
        "Edits",
        "In a Sentimental Mood",
        "I Love You, Honeybear",
        "Confield",
        "Painless",
        "El Bueno y el Malo",
    ]

    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        with cur.copy("COPY release (id, title, country, master_id, format) FROM STDIN") as copy:
            for i in range(n_releases):
                rid = 10_000 + i
                title = titles[i % len(titles)]
                country = "US" if (i % 3 == 0) else "AR"
                master_id = 50_000 + (i // 2)
                # Both rows in a (master_id) pair share the same format so
                # they fall into the same (master_id, format) dedup partition
                # and one of the pair is marked as a duplicate.
                fmt = "LP"
                copy.write_row((rid, title, country, master_id, fmt))

        with cur.copy(
            "COPY release_artist (release_id, artist_id, artist_name, extra) FROM STDIN"
        ) as copy:
            for i in range(n_releases):
                rid = 10_000 + i
                artist_id = 100 + (i % len(artists))
                artist_name = artists[i % len(artists)]
                copy.write_row((rid, artist_id, artist_name, 0))

        with cur.copy(
            "COPY release_track (release_id, sequence, position, title) FROM STDIN"
        ) as copy:
            for i in range(n_releases):
                rid = 10_000 + i
                copy.write_row((rid, 1, "A1", titles[i % len(titles)]))
    conn.commit()
    conn.close()


def _build_dedup_conn(db_url: str) -> psycopg.Connection:
    """Open a non-autocommit connection used to drive dedup copy-swap."""
    return psycopg.connect(db_url)


def _terminate_when(
    db_url: str,
    target_pid: int,
    pattern_substrings: tuple[str, ...],
    max_polls: int = 400,
    poll_interval_s: float = 0.005,
) -> tuple[threading.Event, threading.Event]:
    """Spawn a thread that polls pg_stat_activity for ``target_pid`` and
    pg_terminate_backend()s it as soon as one of ``pattern_substrings`` (case
    insensitive) appears in the running query.

    Returns ``(matched, finished)``:
      * ``matched`` is set ONLY when termination fired against a query that
        matched ``pattern_substrings``. If we time out without seeing the
        pattern we kill the backend anyway (so the worker can return) but
        leave ``matched`` cleared.
      * ``finished`` is set when the runner exits, regardless of outcome.
    """
    matched = threading.Event()
    finished = threading.Event()

    def _runner() -> None:
        admin = psycopg.connect(db_url, autocommit=True)
        try:
            for _ in range(max_polls):
                with admin.cursor() as cur:
                    cur.execute(
                        "SELECT query FROM pg_stat_activity WHERE pid = %s",
                        (target_pid,),
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        q = row[0].upper()
                        if any(p.upper() in q for p in pattern_substrings):
                            cur.execute("SELECT pg_terminate_backend(%s)", (target_pid,))
                            matched.set()
                            return
                time.sleep(poll_interval_s)
            # Timed out waiting for the pattern. Fire anyway so the worker
            # thread doesn't hang waiting for the dedup connection to return.
            # ``matched`` stays cleared so the caller can distinguish this
            # case (and skip rather than assert mid-operation behaviour).
            with admin.cursor() as cur:
                cur.execute("SELECT pg_terminate_backend(%s)", (target_pid,))
        finally:
            admin.close()
            finished.set()

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    return matched, finished


class TestDedupTerminatedMidOperation:
    """Verify dedup copy-swap and ANALYZE handle mid-operation backend kills.

    These tests populate a moderately-large dataset (~50K rows) so that the
    dedup CREATE TABLE AS / ALTER TABLE RENAME phase or an ANALYZE on the
    populated table takes long enough that a sibling thread can win the race
    against pg_terminate_backend(). Both rolling back cleanly AND leaving the
    database in a state that a subsequent dedup rerun can recover from are
    valid outcomes; the cleanup case is already covered in
    tests/integration/test_dedup.py::TestDedupCopySwapAbortCleanup.
    """

    @pytest.fixture(autouse=True)
    def _set_up(self, db_url):
        """Apply schema and seed a workload large enough to win the race."""
        self.db_url = db_url
        _drop_all_tables(db_url)
        _apply_schema(db_url)
        _seed_dedup_workload(db_url, n_releases=50_000)

    def test_terminated_dedup_during_copy_swap_leaves_consistent_state(self) -> None:
        """Killing dedup mid-CREATE-TABLE-AS / mid-RENAME does not corrupt release.

        The dedup connection runs ensure_dedup_ids + copy_table + swap_tables
        in a background thread. Meanwhile the main thread polls pg_stat_activity
        and pg_terminate_backend()s the dedup backend as soon as it sees a
        CREATE TABLE AS or ALTER TABLE ... RENAME running. After the kill we
        verify (a) dedup raised, (b) the original release table still contains
        all 50_000 rows OR has been atomically swapped to a smaller deduped
        copy, and (c) a subsequent dedup rerun completes successfully and
        cleans up any dangling new_release artifacts.
        """
        dedup_conn = _build_dedup_conn(self.db_url)
        dedup_pid = dedup_conn.info.backend_pid

        # Pre-create dedup_delete_ids in the dedup connection so the long
        # operation we race against is the copy-swap phase, not ensure_dedup_ids.
        dedup_releases.ensure_dedup_ids(dedup_conn)
        dedup_conn.commit()

        result: dict = {"raised": False, "exc": None}

        def _drive_copy_swap() -> None:
            try:
                # Run the full copy-swap sequence on the base tables. We use
                # a fresh connection per call (mirroring scripts/dedup_releases
                # main()'s autocommit pattern is unnecessary here -- the kill
                # will happen during one of these statements).
                dedup_releases.copy_table(
                    dedup_conn,
                    "release",
                    "new_release",
                    "id, title, release_year, country, artwork_url, released, format",
                    "id",
                )
                dedup_releases.copy_table(
                    dedup_conn,
                    "release_artist",
                    "new_release_artist",
                    "release_id, artist_id, artist_name, extra, role",
                    "release_id",
                )
                with dedup_conn.cursor() as cur:
                    cur.execute(
                        "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS "
                        "fk_release_artist_release"
                    )
                dedup_releases.swap_tables(dedup_conn, "release", "new_release")
                dedup_releases.swap_tables(dedup_conn, "release_artist", "new_release_artist")
            except Exception as exc:  # noqa: BLE001 -- we want to record any failure
                result["raised"] = True
                result["exc"] = exc

        # Match only the genuinely long copy-swap statements. ALTER TABLE
        # RENAME and the trivial DROP TABLE IF EXISTS new_release at the
        # start of copy_table are too fast to meaningfully interrupt --
        # if we matched them we'd be killing during a no-op and claiming
        # to test mid-CREATE-TABLE-AS behaviour.
        matched, finished = _terminate_when(
            self.db_url,
            dedup_pid,
            pattern_substrings=("CREATE TABLE",),
        )

        worker = threading.Thread(target=_drive_copy_swap, daemon=True)
        worker.start()
        worker.join(timeout=60)
        assert not worker.is_alive(), "Dedup worker thread hung after kill"

        finished.wait(timeout=30)
        assert finished.is_set(), "Terminator thread did not exit"

        try:
            dedup_conn.close()
        except Exception:
            pass

        if not matched.is_set():
            pytest.skip(
                "Terminator did not catch a CREATE TABLE on this machine; "
                "cannot assert mid-copy-swap kill behaviour"
            )
        if not result["raised"]:
            pytest.skip(
                "Dedup completed before pg_terminate_backend fired; "
                "cannot exercise mid-operation kill on this machine"
            )

        # The release table must still exist and be queryable.
        verify = psycopg.connect(self.db_url)
        with verify.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_name = 'release'"
                ")"
            )
            assert cur.fetchone()[0], "release table disappeared after dedup kill"
            cur.execute("SELECT count(*) FROM release")
            row_count = cur.fetchone()[0]
        verify.close()
        # Either the swap happened atomically before the kill (deduped count
        # == 25_000 because every pair shares a master_id) or the swap was
        # aborted and the original 50_000 rows survive.
        assert row_count in (25_000, 50_000), (
            f"Unexpected release row count after kill: {row_count}. "
            "Expected either pre-swap (50_000) or post-swap (25_000)."
        )

        # Subsequent rerun must clean up dangling new_* tables and finish.
        rerun_conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            # ensure_dedup_ids may have been left behind; drop it to force a
            # rerun against the current state of the release table.
            with rerun_conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
            delete_count = dedup_releases.ensure_dedup_ids(rerun_conn)
            if delete_count > 0:
                dedup_releases.copy_table(
                    rerun_conn,
                    "release",
                    "new_release",
                    "id, title, release_year, country, artwork_url, released, format",
                    "id",
                )
                with rerun_conn.cursor() as cur:
                    cur.execute(
                        "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS "
                        "fk_release_artist_release"
                    )
                dedup_releases.swap_tables(rerun_conn, "release", "new_release")
        finally:
            rerun_conn.close()

        # Final state: release table is queryable and dangling new_release
        # is gone (swap renamed it; copy_table drops any leftover before
        # creating).
        final = psycopg.connect(self.db_url)
        with final.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_name = 'new_release'"
                ")"
            )
            assert not cur.fetchone()[0], "new_release should be gone after successful rerun swap"
            cur.execute("SELECT count(*) FROM release")
            final_count = cur.fetchone()[0]
        final.close()
        # After a clean dedup the row count should be 25_000.
        assert final_count == 25_000

    def test_terminated_analyze_returns_clean_error(self) -> None:
        """Killing a backend mid-ANALYZE on a populated table raises
        OperationalError without corrupting the release table."""
        analyze_conn = _build_dedup_conn(self.db_url)
        analyze_pid = analyze_conn.info.backend_pid

        matched, finished = _terminate_when(
            self.db_url,
            analyze_pid,
            pattern_substrings=("ANALYZE",),
        )

        raised = False
        try:
            with analyze_conn.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute("ANALYZE release")
                cur.execute("ANALYZE release_artist")
        except psycopg.OperationalError:
            raised = True
        finally:
            try:
                analyze_conn.close()
            except Exception:
                pass

        finished.wait(timeout=10)
        assert finished.is_set(), "Terminator thread did not exit"
        if not matched.is_set():
            pytest.skip("Terminator did not catch an ANALYZE on this machine")
        if not raised:
            pytest.skip("ANALYZE completed before pg_terminate_backend fired")

        # release table data is intact and queryable.
        verify = psycopg.connect(self.db_url)
        with verify.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            assert cur.fetchone()[0] == 50_000, (
                "release table data should be intact after ANALYZE termination"
            )
            cur.execute("SELECT count(*) FROM release_artist")
            assert cur.fetchone()[0] == 50_000
        verify.close()
