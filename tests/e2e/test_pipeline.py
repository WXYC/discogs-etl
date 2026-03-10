"""End-to-end test for the full pipeline orchestration script.

Runs scripts/run_pipeline.py as a subprocess against a test PostgreSQL database
using fixture CSVs and fixture library.db, then verifies the final database state.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"
FIXTURE_LIBRARY_DB = FIXTURES_DIR / "library.db"
RUN_PIPELINE = Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py"

ADMIN_URL = os.environ.get("DATABASE_URL_TEST", "postgresql://localhost:5433/postgres")

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="class")
def e2e_db_url():
    """Create a fresh database for each E2E test class.

    Each test class gets its own database so that one pipeline run
    (which modifies schema via dedup) does not interfere with another.
    """
    db_name = f"discogs_e2e_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)

    with admin_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    if "@" in ADMIN_URL:
        base = ADMIN_URL.rsplit("/", 1)[0]
    else:
        base = ADMIN_URL.rsplit("/", 1)[0]
    test_url = f"{base}/{db_name}"

    yield test_url

    with admin_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = {} AND pid <> pg_backend_pid()"
            ).format(sql.Literal(db_name))
        )
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
    admin_conn.close()


class TestPipeline:
    """Run the full pipeline and verify final database state."""

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline(self, e2e_db_url):
        """Run run_pipeline.py as a subprocess against the test database."""
        self.__class__._db_url = e2e_db_url

        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                e2e_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        self.__class__._stdout = result.stdout
        self.__class__._stderr = result.stderr
        self.__class__._returncode = result.returncode

        if result.returncode != 0:
            # Print output for debugging
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_tables_populated(self) -> None:
        """Core tables have rows after pipeline completion.

        release_track_artist is excluded because it only contains rows for
        compilation releases, which may be pruned depending on matching.
        """
        conn = self._connect()
        for table in (
            "release",
            "release_artist",
            "release_label",
            "release_track",
            "cache_metadata",
        ):
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {table}")
                count = cur.fetchone()[0]
            assert count > 0, f"Table {table} is empty"
        conn.close()

    def test_duplicates_removed(self) -> None:
        """Duplicate releases (same master_id) have been removed.

        In the fixture data, releases 1001, 1002, 1003 share master_id 500.
        After dedup, only release 1002 (US pressing) should remain.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1002], f"Expected only 1002 after dedup, got {ids}"

    def test_prune_releases_gone(self) -> None:
        """Releases not matching the library have been pruned.

        Release 10001 ('Some Random Album' by 'Random Artist X') should be
        pruned as it doesn't match any library entry.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 10001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0, "Release 10001 should have been pruned"

    def test_keep_releases_present(self) -> None:
        """Releases matching the library are still present after pruning."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Kid A (3001) should survive both dedup and prune
            cur.execute("SELECT count(*) FROM release WHERE id = 3001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 1, "Release 3001 (Kid A) should still exist"

    def test_master_id_column_absent(self) -> None:
        """master_id column is dropped by the dedup copy-swap."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'release' AND column_name = 'master_id'"
            )
            result = cur.fetchone()
        conn.close()
        assert result is None, "master_id column should not exist after dedup"

    def test_country_column_present(self) -> None:
        """country column persists through the dedup copy-swap."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'release' AND column_name = 'country'"
            )
            result = cur.fetchone()
        conn.close()
        assert result is not None, "country column should exist after dedup"

    def test_indexes_exist(self) -> None:
        """Trigram indexes exist on the final database."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE '%trgm%'
            """)
            indexes = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {
            "idx_release_track_title_trgm",
            "idx_release_artist_name_trgm",
            "idx_release_track_artist_name_trgm",
            "idx_release_title_trgm",
        }
        assert expected.issubset(indexes), f"Missing indexes: {expected - indexes}"

    def test_fk_constraints_exist(self) -> None:
        """FK constraints exist on all child tables."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tc.table_name
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type = 'FOREIGN KEY'
            """)
            fk_tables = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {
            "release_artist",
            "release_label",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        }
        assert expected.issubset(fk_tables)

    def test_null_title_release_not_imported(self) -> None:
        """Release 7001 (empty title) should not exist."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 7001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_tables_are_logged(self) -> None:
        """All tables are LOGGED after pipeline completion (not UNLOGGED)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT relname, relpersistence
                FROM pg_class
                WHERE relname IN (
                    'release', 'release_artist', 'release_label',
                    'release_track', 'release_track_artist', 'cache_metadata'
                )
            """)
            results = cur.fetchall()
        conn.close()
        for relname, relpersistence in results:
            assert relpersistence == "p", (
                f"Table {relname} should be LOGGED (p) after pipeline, got {relpersistence}"
            )


FIXTURE_LIBRARY_LABELS = CSV_DIR / "library_labels.csv"


class TestPipelineWithLabels:
    """Run pipeline with --library-labels for label-aware dedup.

    Omits --library-db so the prune step is skipped; this test is focused
    on verifying that label matching changes the dedup winner.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline(self, e2e_db_url):
        """Run run_pipeline.py with --library-labels (no prune)."""
        self.__class__._db_url = e2e_db_url

        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-labels",
                str(FIXTURE_LIBRARY_LABELS),
                "--database-url",
                e2e_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        self.__class__._stdout = result.stdout
        self.__class__._stderr = result.stderr
        self.__class__._returncode = result.returncode

        if result.returncode != 0:
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_label_match_overrides_track_count_master_500(self) -> None:
        """Label-aware dedup keeps release 1001 (Parlophone) over 1002 (Capitol, more tracks)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1001], f"Expected only 1001 after label-aware dedup, got {ids}"

    def test_label_match_overrides_track_count_master_600(self) -> None:
        """Label-aware dedup keeps release 2001 (Factory) over 2002 (Qwest, more tracks)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (2001, 2002) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [2001], f"Expected only 2001 after label-aware dedup, got {ids}"

    def test_temp_tables_cleaned_up(self) -> None:
        """wxyc_label_pref and release_label_match are dropped after dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_name IN ('wxyc_label_pref', 'release_label_match')
            """)
            tables = [row[0] for row in cur.fetchall()]
        conn.close()
        assert tables == [], f"Temp tables should be cleaned up, found {tables}"

    def test_non_label_matched_uses_track_count(self) -> None:
        """Releases without label match still use track count ranking.

        Release 3001 (unique master_id 700) and 4001 (no master_id) should
        be unaffected by label matching and survive both dedup and prune.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id IN (3001, 4001)")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 2


class TestPipelineWithoutLibrary:
    """Run pipeline without library.db (skips prune step)."""

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline(self, e2e_db_url):
        """Run run_pipeline.py without library.db."""
        self.__class__._db_url = e2e_db_url

        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                # No --library-db — prune should be skipped
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env={
                **os.environ,
                "DATABASE_URL": e2e_db_url,
            },
        )

        self.__class__._returncode = result.returncode
        self.__class__._stderr = result.stderr

        if result.returncode != 0:
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_tables_populated(self) -> None:
        """Tables should still be populated when prune is skipped."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count > 0

    def test_prune_skipped_message(self) -> None:
        """Log should indicate prune was skipped."""
        assert "Skipping prune step" in self.__class__._stderr


class TestPipelineWithCopyTo:
    """Run pipeline with --target-db-url (copy matched releases to target)."""

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline(self, e2e_db_url):
        """Run run_pipeline.py with --target-db-url."""
        self.__class__._source_url = e2e_db_url

        # Build target DB name and URL
        target_name = f"discogs_e2e_target_{uuid.uuid4().hex[:8]}"
        if "@" in ADMIN_URL:
            base = ADMIN_URL.rsplit("/", 1)[0]
        else:
            base = ADMIN_URL.rsplit("/", 1)[0]
        target_url = f"{base}/{target_name}"
        self.__class__._target_url = target_url
        self.__class__._target_name = target_name

        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                e2e_db_url,
                "--target-db-url",
                target_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        self.__class__._stdout = result.stdout
        self.__class__._stderr = result.stderr
        self.__class__._returncode = result.returncode

        if result.returncode != 0:
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

        yield

        # Teardown: drop target database
        admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)
        with admin_conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = {} AND pid <> pg_backend_pid()"
                ).format(sql.Literal(target_name))
            )
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(target_name)))
        admin_conn.close()

    @pytest.fixture(autouse=True)
    def _store_urls(self):
        self.source_url = self.__class__._source_url
        self.target_url = self.__class__._target_url

    def test_source_not_pruned(self) -> None:
        """Source database should still have all releases (including PRUNE ones)."""
        # Source was not pruned — it should have more releases than the target.
        source_count = self._count_releases(self.source_url)
        target_count = self._count_releases(self.target_url)
        assert source_count > target_count, (
            f"Source ({source_count}) should have more releases than target ({target_count})"
        )

    def test_target_has_matched_releases(self) -> None:
        """Target database has releases matching the library."""
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            # Kid A (3001) should be in target
            cur.execute("SELECT count(*) FROM release WHERE id = 3001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 1, "Release 3001 (Kid A) should be in target"

    def test_target_prune_releases_absent(self) -> None:
        """PRUNE releases should not be in the target."""
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 10001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0, "Release 10001 should not be in target"

    def test_target_has_indexes(self) -> None:
        """Target database has trigram indexes."""
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE '%trgm%'
            """)
            indexes = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {
            "idx_release_track_title_trgm",
            "idx_release_artist_name_trgm",
            "idx_release_track_artist_name_trgm",
            "idx_release_title_trgm",
        }
        assert expected.issubset(indexes), f"Missing indexes: {expected - indexes}"

    def test_target_tables_populated(self) -> None:
        """Core tables in target have rows."""
        conn = psycopg.connect(self.target_url)
        for table in (
            "release",
            "release_artist",
            "release_label",
            "release_track",
            "cache_metadata",
        ):
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {table}")
                count = cur.fetchone()[0]
            assert count > 0, f"Table {table} is empty in target"
        conn.close()

    def _count_releases(self, db_url: str) -> int:
        conn = psycopg.connect(db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        return count


class TestPipelineStateFile:
    """Pipeline creates a state file with all steps completed."""

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline(self, e2e_db_url, tmp_path_factory):
        """Run pipeline and check that state file is created."""
        self.__class__._db_url = e2e_db_url
        self.__class__._state_dir = tmp_path_factory.mktemp("state")
        state_file = self.__class__._state_dir / "state.json"
        self.__class__._state_file = state_file

        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                e2e_db_url,
                "--state-file",
                str(state_file),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        self.__class__._returncode = result.returncode
        self.__class__._stderr = result.stderr

        if result.returncode != 0:
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

    def test_state_file_created(self) -> None:
        """State file exists after pipeline run."""
        assert self.__class__._state_file.exists()

    def test_all_steps_completed(self) -> None:
        """All steps are marked as completed in the state file."""
        data = json.loads(self.__class__._state_file.read_text())
        for step_name, step_data in data["steps"].items():
            assert step_data["status"] == "completed", (
                f"Step {step_name} is {step_data['status']}, expected completed"
            )

    def test_state_file_has_correct_metadata(self) -> None:
        """State file contains correct database URL and version."""
        data = json.loads(self.__class__._state_file.read_text())
        assert data["version"] == 3
        assert data["database_url"] == self.__class__._db_url


class TestPipelineResume:
    """Resume skips completed steps and runs remaining ones."""

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline_then_resume(self, e2e_db_url, tmp_path_factory):
        """Run full pipeline, then resume (should skip all steps)."""
        self.__class__._db_url = e2e_db_url
        state_dir = tmp_path_factory.mktemp("resume_state")
        state_file = state_dir / "state.json"
        self.__class__._state_file = state_file

        # First run: full pipeline
        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                e2e_db_url,
                "--state-file",
                str(state_file),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"First run failed:\n{result.stderr}"
        self.__class__._first_stderr = result.stderr

        # Second run: resume (should skip everything)
        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                e2e_db_url,
                "--state-file",
                str(state_file),
                "--resume",
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.__class__._resume_returncode = result.returncode
        self.__class__._resume_stderr = result.stderr

        if result.returncode != 0:
            print("RESUME STDOUT:", result.stdout)
            print("RESUME STDERR:", result.stderr)

        assert result.returncode == 0, f"Resume run failed:\n{result.stderr}"

    def test_resume_skips_all_steps(self) -> None:
        """All steps should be skipped on resume after completed run."""
        stderr = self.__class__._resume_stderr
        assert "Skipping create_schema" in stderr
        assert "Skipping import_csv" in stderr
        assert "Skipping create_indexes" in stderr
        assert "Skipping dedup" in stderr
        assert "Skipping import_tracks" in stderr
        assert "Skipping create_track_indexes" in stderr
        assert "Skipping prune" in stderr
        assert "Skipping vacuum" in stderr
        assert "Skipping set_logged" in stderr

    def test_resume_completes_successfully(self) -> None:
        """Resume run exits with code 0."""
        assert self.__class__._resume_returncode == 0

    def test_data_intact_after_resume(self) -> None:
        """Database state is unchanged after resume."""
        conn = psycopg.connect(self.__class__._db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count > 0
