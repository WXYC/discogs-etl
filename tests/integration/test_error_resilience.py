"""Error/resilience tests for the discogs-cache pipeline.

Tests that external dependency failures are handled gracefully:
- UNLOGGED toggle edge cases (non-existent tables, already-toggled)
- Dedup connection loss simulation
- Import COPY interruption (malformed data, partial failures)

All tests require PostgreSQL and are marked with @pytest.mark.postgres.
Uses WXYC example artists for fixture data.
"""

from __future__ import annotations

import importlib.util
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

pytestmark = pytest.mark.postgres

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


def _apply_schema(db_url: str) -> None:
    """Apply the pipeline schema to a test database."""
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
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

    def test_copy_table_to_nonexistent_target_fails(self) -> None:
        """copy_table to a non-existent target connection fails with clear error."""
        bad_url = "postgresql://bogus_user:bad_pass@localhost:59999/nonexistent"
        with pytest.raises((psycopg.Error, RuntimeError, OSError)):
            dedup_releases.copy_table(self.db_url, bad_url, "release", "SELECT * FROM release")


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
        # Don't apply schema, so tables don't exist
        with pytest.raises((psycopg.Error, RuntimeError, OSError)):
            run_pipeline.run_vacuum(self.db_url)
