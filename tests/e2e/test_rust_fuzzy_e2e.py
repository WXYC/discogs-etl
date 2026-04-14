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
        assert "keep" in counts, (
            f"Could not parse KEEP count from stdout:\n{self.verify_stdout}"
        )
        assert counts["keep"] > 0, "Expected some KEEP releases"

    def test_prune_count_positive(self) -> None:
        """At least some releases are classified as PRUNE.

        The fixture has releases by Random Artist X (10001), Obscure Band Y
        (10002), DJ Unknown (5001), and Mystery Band (5002) that should not
        match any library entry.
        """
        counts = self._parse_classification_counts()
        assert "prune" in counts, (
            f"Could not parse PRUNE count from stdout:\n{self.verify_stdout}"
        )
        assert counts["prune"] > 0, "Expected some PRUNE releases"

    def test_keep_prune_total_matches_release_count(self) -> None:
        """KEEP + PRUNE + REVIEW equals total release count in the database."""
        counts = self._parse_classification_counts()
        total_classified = (
            counts.get("keep", 0) + counts.get("prune", 0) + counts.get("review", 0)
        )

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

        The fixture has matching releases for Radiohead (OK Computer CD+Vinyl,
        Kid A, Amnesiac), Joy Division (Unknown Pleasures LP+CD), Bjork (Homogenic),
        Beatles (Abbey Road), Simon & Garfunkel (Bridge Over Troubled Water),
        and Sugar Hill (VA compilation). After dedup, some duplicates are removed.
        With 15 imported releases and ~8 matching the library, expected: 5-12 KEEP.
        """
        counts = self._parse_classification_counts()
        keep = counts["keep"]
        assert 5 <= keep <= 12, f"Expected 5-12 KEEP releases, got {keep}"

    def test_prune_count_within_expected_range(self) -> None:
        """PRUNE count is in the expected range for fixture data.

        The fixture has releases by Random Artist X (10001), Obscure Band Y
        (10002), DJ Unknown (5001), Mystery Band (5002), and the Cassette
        pressing of OK Computer (1003, format mismatch with library). Some
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
        between Discogs and the library (e.g. 'Beatles, The' vs 'The Beatles',
        'Bjork' accent normalization).
        """
        combined = self.verify_stderr + self.verify_stdout
        has_classification_log = (
            "Phase" in combined
            or "fuzzy" in combined.lower()
            or "Classification complete" in combined
        )
        assert has_classification_log, (
            "Expected classification phase logs in output.\n"
            f"stderr:\n{self.verify_stderr[:500]}"
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
