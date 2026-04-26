"""E2E test for verify_cache.py fuzzy classification on fixture data.

Runs the import pipeline (without --library-db, so no prune step) as a
subprocess to populate a test PostgreSQL database. Then runs verify_cache.py
in dry-run mode against the populated database and fixture library.db.
Verifies:
  - verify_cache.py completes without error on the un-pruned database
  - KEEP and PRUNE counts are within expected range for the fixture data
  - The multi-process fuzzy classification path is exercised
  - KEEP + PRUNE + REVIEW accounts for all releases in the database
  - Dry-run mode does not modify the database

The fixture data has ~16 releases (15 after null-title filtering) and ~21
library entries, designed so that classification results are deterministic.
"""

from __future__ import annotations

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
VERIFY_CACHE = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"

ADMIN_URL = os.environ.get("DATABASE_URL_TEST", "postgresql://localhost:5433/postgres")

pytestmark = pytest.mark.e2e


def _postgres_available() -> bool:
    """Return True if we can connect to the test Postgres instance."""
    try:
        conn = psycopg.connect(ADMIN_URL, connect_timeout=3, autocommit=True)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="class")
def e2e_db_url():
    """Create a fresh database for the E2E test class."""
    if not _postgres_available():
        pytest.skip("PostgreSQL not available (set DATABASE_URL_TEST)")

    db_name = f"discogs_fuzzy_e2e_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)

    with admin_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

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


class TestVerifyCacheFuzzy:
    """Run verify_cache.py on un-pruned fixture data and verify KEEP/PRUNE classification."""

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline_and_verify(self, e2e_db_url):
        """Import fixture data (no prune), then run verify_cache.py dry-run."""
        self.__class__._db_url = e2e_db_url

        # Step 1: Run pipeline WITHOUT --library-db so prune is skipped.
        # This populates the database with all fixture releases (import + dedup only).
        pipeline_result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                # No --library-db: skip prune step, keep all releases
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env={
                **os.environ,
                "DATABASE_URL": e2e_db_url,
            },
        )

        if pipeline_result.returncode != 0:
            print("PIPELINE STDOUT:", pipeline_result.stdout)
            print("PIPELINE STDERR:", pipeline_result.stderr)

        assert pipeline_result.returncode == 0, (
            f"Pipeline failed (exit {pipeline_result.returncode}):\n{pipeline_result.stderr}"
        )

        # Record release count before verify_cache.py runs (for dry-run check)
        conn = psycopg.connect(e2e_db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            self.__class__._release_count_before = cur.fetchone()[0]
        conn.close()

        # Step 2: Run verify_cache.py in dry-run mode (no --prune, no --copy-to)
        verify_result = subprocess.run(
            [
                sys.executable,
                str(VERIFY_CACHE),
                str(FIXTURE_LIBRARY_DB),
                e2e_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        self.__class__._verify_stdout = verify_result.stdout
        self.__class__._verify_stderr = verify_result.stderr
        self.__class__._verify_returncode = verify_result.returncode

        if verify_result.returncode != 0:
            print("VERIFY STDOUT:", verify_result.stdout)
            print("VERIFY STDERR:", verify_result.stderr)

        assert verify_result.returncode == 0, (
            f"verify_cache.py failed (exit {verify_result.returncode}):\n{verify_result.stderr}"
        )

    @pytest.fixture(autouse=True)
    def _store_class_state(self):
        self.db_url = self.__class__._db_url
        self.verify_stdout = self.__class__._verify_stdout
        self.verify_stderr = self.__class__._verify_stderr

    def _parse_classification_counts(self) -> dict[str, int]:
        """Parse KEEP/PRUNE/REVIEW counts from verify_cache.py stdout."""
        counts = {}
        for line in self.verify_stdout.splitlines():
            line = line.strip()
            if "Releases to keep:" in line or "Releases kept:" in line:
                counts["keep"] = int(line.split(":")[-1].strip().replace(",", ""))
            elif "Releases to prune:" in line or "Releases pruned:" in line:
                counts["prune"] = int(line.split(":")[-1].strip().replace(",", ""))
            elif "Releases to review:" in line:
                counts["review"] = int(line.split(":")[-1].strip().replace(",", ""))
        return counts

    def test_verify_completed_successfully(self) -> None:
        """verify_cache.py exits with code 0."""
        assert self.__class__._verify_returncode == 0

    def test_keep_count_positive(self) -> None:
        """At least some releases are classified as KEEP."""
        counts = self._parse_classification_counts()
        assert "keep" in counts, f"Could not parse KEEP count from stdout:\n{self.verify_stdout}"
        assert counts["keep"] > 0, "Expected some KEEP releases"

    def test_prune_count_positive(self) -> None:
        """At least some releases are classified as PRUNE.

        The fixture has releases by Random Artist X (10001), Obscure Band Y
        (10002), DJ Unknown (5001), and Mystery Band (5002) that should not
        match any library entry.
        """
        counts = self._parse_classification_counts()
        assert "prune" in counts, f"Could not parse PRUNE count from stdout:\n{self.verify_stdout}"
        assert counts["prune"] > 0, "Expected some PRUNE releases"

    def test_keep_prune_total_matches_release_count(self) -> None:
        """KEEP + PRUNE + REVIEW equals total release count in the database."""
        counts = self._parse_classification_counts()
        total_classified = counts.get("keep", 0) + counts.get("prune", 0) + counts.get("review", 0)

        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            db_count = cur.fetchone()[0]
        conn.close()

        assert total_classified == db_count, (
            f"Classified {total_classified} releases but database has {db_count}"
        )

    def test_keep_count_within_expected_range(self) -> None:
        """KEEP count is in the expected range for fixture data.

        The fixture has matching releases for Autechre (Confield CD+Vinyl,
        Amber, Tri Repetae), Father John Misty (I Love You, Honeybear LP+CD), Nilufer Yanya (PAINLESS),
        Field (From Here We Go Sublime), Duke Ellington & John Coltrane (Duke Ellington & John Coltrane),
        and Nordic Roots (VA compilation). After dedup, some duplicates are removed.
        With 15 imported releases and ~8 matching the library, expected: 5-12 KEEP.
        """
        counts = self._parse_classification_counts()
        keep = counts["keep"]
        assert 5 <= keep <= 12, f"Expected 5-12 KEEP releases, got {keep}"

    def test_prune_count_within_expected_range(self) -> None:
        """PRUNE count is in the expected range for fixture data.

        The fixture has releases by Random Artist X (10001), Obscure Band Y
        (10002), DJ Unknown (5001), Mystery Band (5002), and the Cassette
        pressing of Confield (1003, format mismatch with library). Some
        compilation or extra-artist releases may also be pruned.
        With 15 imported releases and ~7 pruned, expected: 3-10 PRUNE.
        """
        counts = self._parse_classification_counts()
        prune = counts["prune"]
        assert 3 <= prune <= 10, f"Expected 3-10 PRUNE releases, got {prune}"

    def test_fuzzy_classification_exercised(self) -> None:
        """The fuzzy classification path ran (not just exact matching).

        verify_cache.py logs phase information during classification.
        Some fixture artists require fuzzy scoring because their names differ
        between Discogs and the library (e.g. 'Field, The' vs 'The Field',
        'Nilufer Yanya' accent normalization).
        """
        combined = self.verify_stderr + self.verify_stdout
        has_classification_log = (
            "Phase" in combined
            or "fuzzy" in combined.lower()
            or "Classification complete" in combined
        )
        assert has_classification_log, (
            f"Expected classification phase logs in output.\nstderr:\n{self.verify_stderr[:500]}"
        )

    def test_database_unchanged_in_dry_run(self) -> None:
        """Dry-run mode does not delete any releases from the database."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count_after = cur.fetchone()[0]
        conn.close()

        assert count_after == self.__class__._release_count_before, (
            f"Database was modified during dry-run: "
            f"before={self.__class__._release_count_before}, after={count_after}"
        )

    def test_rust_path_was_taken(self) -> None:
        """The Rust (wxyc_etl) batch classifier ran during fuzzy classification.

        ``test_fuzzy_classification_exercised`` only proves that some kind of
        classification phase ran -- both the Rust path and the Python fallback
        emit "Phase" / "fuzzy" log lines. This test asserts on the Rust-only
        marker emitted at scripts/verify_cache.py line ~1575:

            logger.info("  Using Rust (wxyc_etl) batch classification")

        If wxyc_etl is not installed in the test environment (e.g. local dev
        without the Rust wheel), the test is skipped rather than failing,
        because the Python fallback is the correct -- and the only available
        -- path in that situation.
        """
        try:
            import wxyc_etl  # noqa: F401
            from wxyc_etl.fuzzy import batch_classify_releases  # noqa: F401
        except ImportError:
            pytest.skip("wxyc_etl Rust batch classifier not installed; Python fallback is expected")

        combined = self.verify_stderr + self.verify_stdout
        assert "Using Rust (wxyc_etl) batch classification" in combined, (
            "Expected Rust-path marker in verify_cache output. The fuzzy "
            "classifier may have silently fallen back to the Python path. "
            f"\nstdout (last 2KB):\n{self.verify_stdout[-2000:]}"
            f"\nstderr (last 2KB):\n{self.verify_stderr[-2000:]}"
        )
        # Defensive: the Python fallback's marker should NOT also appear --
        # the two paths are mutually exclusive within a single run.
        assert "Using Python fallback" not in combined, (
            "Both Rust and Python fallback markers appeared in the same run; "
            "verify_cache.py may be running both paths."
        )


class TestPythonFallbackPathSelection:
    """Verify that WXYC_ETL_NO_RUST=1 forces the Python fallback even when
    the Rust wheel is installed."""

    @pytest.fixture(scope="class")
    def fallback_run(self, e2e_db_url):
        """Bootstrap a fresh database via run_pipeline, then run verify_cache
        with WXYC_ETL_NO_RUST=1 and capture its output."""
        # Reuse the e2e_db_url fixture; populate it identically to the parent
        # test's bootstrap step.
        pipeline_result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env={
                **os.environ,
                "DATABASE_URL": e2e_db_url,
            },
        )
        assert pipeline_result.returncode == 0, (
            f"Pipeline bootstrap failed (exit {pipeline_result.returncode}):\n"
            f"{pipeline_result.stderr}"
        )

        verify_result = subprocess.run(
            [
                sys.executable,
                str(VERIFY_CACHE),
                str(FIXTURE_LIBRARY_DB),
                e2e_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "WXYC_ETL_NO_RUST": "1"},
        )
        assert verify_result.returncode == 0, (
            f"verify_cache.py failed under WXYC_ETL_NO_RUST=1 (exit "
            f"{verify_result.returncode}):\n{verify_result.stderr}"
        )
        return verify_result

    def test_python_path_with_wxyc_etl_no_rust_env(self, fallback_run) -> None:
        """When WXYC_ETL_NO_RUST=1 is set, verify_cache logs the Python
        fallback marker and does NOT log the Rust marker."""
        combined = fallback_run.stderr + fallback_run.stdout
        assert "Using Python fallback" in combined, (
            "Expected Python fallback marker in verify_cache output under "
            "WXYC_ETL_NO_RUST=1.\nstderr:\n"
            f"{fallback_run.stderr[-2000:]}"
        )
        assert "Using Rust (wxyc_etl) batch classification" not in combined, (
            "Rust marker should NOT appear when WXYC_ETL_NO_RUST=1 is set."
        )
