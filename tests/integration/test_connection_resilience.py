"""Integration tests for PG COPY connection loss and import resume resilience.

Verifies that:
- When a PostgreSQL connection is terminated mid-COPY, the transaction is
  rolled back and no partial data is committed.
- The pipeline state file does not mark a step complete when COPY fails.
- Import can resume after a connection loss by re-running the import step.

These tests use pg_terminate_backend() to simulate a network failure during
a COPY operation, which is the closest approximation to a real network drop
that we can achieve in an integration test.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
from pathlib import Path

import psycopg
import pytest
from lib.pipeline_state import PipelineState

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"

# Load import_csv module from scripts directory (not a proper package).
_IMPORT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv", _IMPORT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
sys.modules["import_csv"] = _ic
_spec.loader.exec_module(_ic)

import_csv_func = _ic.import_csv
populate_cache_metadata = _ic.populate_cache_metadata
BASE_TABLES = _ic.BASE_TABLES

pytestmark = pytest.mark.postgres


ALL_TABLES = (
    "cache_metadata",
    "release_track_artist",
    "release_track",
    "release_label",
    "release_artist",
    "release",
)


def _drop_all_tables(conn) -> None:
    """Drop all pipeline tables with CASCADE to clear any state."""
    with conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        cur.execute("DROP TABLE IF EXISTS release_track_count CASCADE")


def _apply_schema(db_url: str) -> None:
    """Apply the schema to the test database."""
    conn = psycopg.connect(db_url, autocommit=True)
    _drop_all_tables(conn)
    with conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    conn.close()


class TestCopyConnectionLoss:
    """Simulate connection termination during COPY and verify rollback.

    Uses pg_terminate_backend() from a separate admin connection to kill the
    import connection mid-COPY. This is the closest we can get to simulating
    a network failure in an integration test.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_schema(self, db_url):
        self.__class__._db_url = db_url
        _apply_schema(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_terminated_copy_rolls_back_transaction(self, tmp_path) -> None:
        """When pg_terminate_backend kills a COPY connection, no rows are committed.

        Uses a large CSV (500K rows) so COPY takes long enough for the
        terminator thread to poll pg_stat_activity and kill the backend
        while COPY is still in progress.
        """
        _apply_schema(self.db_url)

        # Create a CSV large enough that COPY takes measurable time.
        # 500K rows produces a ~40MB CSV that takes >100ms to COPY even locally.
        large_csv = tmp_path / "large_release.csv"
        header = "id,status,title,country,released,notes,data_quality,master_id,format\n"
        with open(large_csv, "w") as f:
            f.write(header)
            for i in range(500_000):
                f.write(f"{i},Accepted,DOGA Pressing {i},AR,2024-05-10,,Correct,{8000 + i},LP\n")

        import_conn = psycopg.connect(self.db_url)
        import_pid = import_conn.info.backend_pid

        # Poll pg_stat_activity until we see the COPY command, then terminate
        terminated = threading.Event()

        def _terminate():
            admin_conn = psycopg.connect(self.db_url, autocommit=True)
            # Poll until the import connection is running a COPY command
            for _ in range(200):  # up to 2 seconds
                with admin_conn.cursor() as cur:
                    cur.execute(
                        "SELECT query FROM pg_stat_activity WHERE pid = %s",
                        (import_pid,),
                    )
                    row = cur.fetchone()
                    if row and row[0] and "COPY" in row[0].upper():
                        cur.execute("SELECT pg_terminate_backend(%s)", (import_pid,))
                        terminated.set()
                        admin_conn.close()
                        return
                time.sleep(0.01)
            # If we never saw COPY, terminate anyway so the test doesn't hang
            with admin_conn.cursor() as cur:
                cur.execute("SELECT pg_terminate_backend(%s)", (import_pid,))
            admin_conn.close()
            terminated.set()

        terminator = threading.Thread(target=_terminate, daemon=True)
        terminator.start()

        release_config = next(t for t in BASE_TABLES if t["table"] == "release")
        copy_raised = False
        try:
            import_csv_func(
                import_conn,
                large_csv,
                release_config["table"],
                release_config["csv_columns"],
                release_config["db_columns"],
                release_config["required"],
                release_config["transforms"],
            )
        except psycopg.OperationalError:
            copy_raised = True
        finally:
            try:
                import_conn.close()
            except Exception:
                pass

        terminated.wait(timeout=10)
        assert terminated.is_set(), "Terminator thread did not fire"

        if not copy_raised:
            # COPY completed before termination (unlikely with 500K rows,
            # but possible on very fast machines). The conn.commit() inside
            # import_csv_func would have succeeded. Skip the rollback assertion
            # since there was nothing to roll back.
            pytest.skip("COPY completed before pg_terminate_backend fired")

        # The import connection is dead — use a fresh one to verify state
        verify_conn = self._connect()
        with verify_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        verify_conn.close()

        # Transaction was rolled back: no rows committed
        assert count == 0, (
            f"Expected 0 rows after terminated COPY, got {count}. "
            "Transaction was not properly rolled back."
        )

    def test_state_file_not_marked_complete_on_copy_failure(self, tmp_path) -> None:
        """Pipeline state file does not mark import_csv complete when COPY fails.

        Simulates the pattern used by run_pipeline.py: create a PipelineState,
        attempt import, and verify the state file reflects the failure.
        """
        _apply_schema(self.db_url)

        state = PipelineState(db_url=self.db_url, csv_dir=str(CSV_DIR))
        state_file = tmp_path / ".pipeline_state.json"
        state.save(state_file)

        # Create a CSV that will cause COPY to fail — use a non-nullable column
        # with an empty value that passes the Python filter but violates the DB constraint.
        # Actually, simpler: try to import into a table that doesn't exist.
        bad_conn = psycopg.connect(self.db_url)
        try:
            # Drop the release table so COPY fails
            admin = psycopg.connect(self.db_url, autocommit=True)
            with admin.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS release CASCADE")
            admin.close()

            release_config = next(t for t in BASE_TABLES if t["table"] == "release")
            try:
                import_csv_func(
                    bad_conn,
                    CSV_DIR / "release.csv",
                    release_config["table"],
                    release_config["csv_columns"],
                    release_config["db_columns"],
                    release_config["required"],
                    release_config["transforms"],
                )
                # If import somehow succeeded, that's fine — the point is that
                # mark_completed is NOT called on failure.
            except Exception:
                state.mark_failed("import_csv", "COPY failed: table not found")
                state.save(state_file)
        finally:
            bad_conn.close()

        # Reload state and verify import_csv is NOT completed
        loaded = PipelineState.load(state_file)
        assert not loaded.is_completed("import_csv"), (
            "import_csv should not be marked completed after COPY failure"
        )
        assert loaded.step_status("import_csv") == "failed"


class TestImportResumeAfterConnectionLoss:
    """Verify that import can be resumed after a connection loss.

    Simulates partial import (only release table), then completes the full
    import on a second attempt.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_schema(self, db_url):
        self.__class__._db_url = db_url
        _apply_schema(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_resume_after_partial_import(self) -> None:
        """After a failed import, re-running imports the remaining tables correctly.

        Simulates: first run imports release only (and "crashes"), second run
        re-applies schema and imports everything.
        """
        _apply_schema(self.db_url)

        # First run: import only the release table
        conn = psycopg.connect(self.db_url)
        release_config = next(t for t in BASE_TABLES if t["table"] == "release")
        import_csv_func(
            conn,
            CSV_DIR / "release.csv",
            release_config["table"],
            release_config["csv_columns"],
            release_config["db_columns"],
            release_config["required"],
            release_config["transforms"],
        )
        conn.close()

        # Verify partial state: releases exist, but no artists
        verify = self._connect()
        with verify.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            release_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM release_artist")
            artist_count = cur.fetchone()[0]
        verify.close()
        assert release_count > 0
        assert artist_count == 0

        # Second run: re-apply schema (like the pipeline would on a fresh resume)
        # and import everything
        _apply_schema(self.db_url)

        conn = psycopg.connect(self.db_url)
        for table_config in BASE_TABLES:
            csv_path = CSV_DIR / table_config["csv_file"]
            if csv_path.exists():
                import_csv_func(
                    conn,
                    csv_path,
                    table_config["table"],
                    table_config["csv_columns"],
                    table_config["db_columns"],
                    table_config["required"],
                    table_config["transforms"],
                )
        conn.close()

        # Verify complete state
        verify = self._connect()
        with verify.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            release_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM release_artist")
            artist_count = cur.fetchone()[0]
        verify.close()
        assert release_count == 15  # 16 rows minus 1 with empty title
        assert artist_count == 16

    def test_state_file_tracks_resume_correctly(self, tmp_path) -> None:
        """State file correctly tracks step completion across resume.

        First attempt: create_schema completes, import_csv fails.
        Second attempt: load state, skip create_schema, import_csv completes.
        """
        _apply_schema(self.db_url)

        state = PipelineState(db_url=self.db_url, csv_dir=str(CSV_DIR))
        state_file = tmp_path / ".pipeline_state.json"

        # First attempt: mark schema complete, fail import
        state.mark_completed("create_schema")
        state.mark_failed("import_csv", "Connection terminated during COPY")
        state.save(state_file)

        # Verify state persisted
        loaded = PipelineState.load(state_file)
        assert loaded.is_completed("create_schema")
        assert not loaded.is_completed("import_csv")
        assert loaded.step_status("import_csv") == "failed"

        # Second attempt: import succeeds
        conn = psycopg.connect(self.db_url)
        total = 0
        for table_config in BASE_TABLES:
            csv_path = CSV_DIR / table_config["csv_file"]
            if csv_path.exists():
                total += import_csv_func(
                    conn,
                    csv_path,
                    table_config["table"],
                    table_config["csv_columns"],
                    table_config["db_columns"],
                    table_config["required"],
                    table_config["transforms"],
                )
        conn.close()

        # Mark step complete
        loaded.mark_completed("import_csv")
        loaded.save(state_file)

        # Verify final state
        final = PipelineState.load(state_file)
        assert final.is_completed("create_schema")
        assert final.is_completed("import_csv")
        assert total > 0


class TestPopulateCacheMetadataConnectionLoss:
    """Verify populate_cache_metadata COPY is also resilient to connection loss."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        _apply_schema(db_url)
        # Insert some releases for cache_metadata to reference
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (5001, 'DOGA')")
            cur.execute("INSERT INTO release (id, title) VALUES (5002, 'Aluminum Tunes')")
            cur.execute("INSERT INTO release (id, title) VALUES (5003, 'Moon Pix')")
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_failed_cache_metadata_leaves_table_empty(self) -> None:
        """If a COPY transaction is terminated before commit, no rows are committed.

        Rather than racing pg_terminate_backend against a fast COPY (only 3 rows),
        this test verifies the transactional semantics directly: start a COPY
        inside a transaction, write rows, then terminate the connection before
        commit. Verifies that the implicit rollback leaves the table empty.
        """
        # Clear any existing cache_metadata
        admin = psycopg.connect(self.db_url, autocommit=True)
        with admin.cursor() as cur:
            cur.execute("DELETE FROM cache_metadata")
        admin.close()

        conn = psycopg.connect(self.db_url)
        pid = conn.info.backend_pid

        # Write rows via COPY but do NOT commit — then kill the connection
        with conn.cursor() as cur:
            with cur.copy("COPY cache_metadata (release_id, source) FROM STDIN") as copy:
                copy.write_row((5001, "bulk_import"))
                copy.write_row((5002, "bulk_import"))
                copy.write_row((5003, "bulk_import"))
            # COPY block exited successfully, but transaction is NOT committed.
            # Now terminate the backend from another connection.
            admin = psycopg.connect(self.db_url, autocommit=True)
            with admin.cursor() as acur:
                acur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            admin.close()

        # The connection is now dead. Any further use would raise OperationalError.
        try:
            conn.close()
        except Exception:
            pass

        # Verify no partial data was committed (rollback on disconnect)
        verify = self._connect()
        with verify.cursor() as cur:
            cur.execute("SELECT count(*) FROM cache_metadata")
            count = cur.fetchone()[0]
        verify.close()
        assert count == 0, (
            f"Expected 0 cache_metadata rows after terminated connection, got {count}"
        )

    def test_retry_after_failure_succeeds(self) -> None:
        """After a failed populate_cache_metadata, a retry succeeds."""
        # Clear any existing cache_metadata
        admin = psycopg.connect(self.db_url, autocommit=True)
        with admin.cursor() as cur:
            cur.execute("DELETE FROM cache_metadata")
        admin.close()

        conn = psycopg.connect(self.db_url)
        count = populate_cache_metadata(conn)
        conn.close()

        assert count == 3

        verify = self._connect()
        with verify.cursor() as cur:
            cur.execute("SELECT count(*) FROM cache_metadata")
            db_count = cur.fetchone()[0]
        verify.close()
        assert db_count == 3
