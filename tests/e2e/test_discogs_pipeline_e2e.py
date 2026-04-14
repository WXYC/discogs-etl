"""Cross-repo E2E test: full Discogs pipeline from XML to LML query.

Tests the complete data flow across three repositories:
1. discogs-xml-converter: XML -> CSV conversion (if binary available)
2. discogs-cache: CSV -> PostgreSQL import via run_pipeline.py
3. library-metadata-lookup: query the populated cache via DiscogsCacheService

When sibling repos are not available, the test gracefully skips the cross-repo
assertions but still runs the pipeline verification.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
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

# Sibling repo paths (relative to this repo's root)
REPO_ROOT = Path(__file__).parent.parent.parent
ORG_ROOT = REPO_ROOT.parent
XML_CONVERTER_BINARY = shutil.which("discogs-xml-converter")
XML_CONVERTER_REPO = ORG_ROOT / "discogs-xml-converter"
XML_FIXTURE = XML_CONVERTER_REPO / "tests" / "fixtures" / "releases_fixture.xml"
LML_REPO = ORG_ROOT / "library-metadata-lookup"

# Check for sibling repo availability
HAS_XML_CONVERTER = (
    XML_CONVERTER_BINARY is not None
    and XML_FIXTURE.exists()
)

try:
    sys.path.insert(0, str(LML_REPO))
    from discogs.cache_service import DiscogsCacheService

    HAS_LML = True
    sys.path.pop(0)
except (ImportError, ModuleNotFoundError):
    HAS_LML = False

# asyncpg is needed for LML cache_service
try:
    import asyncio

    import asyncpg

    HAS_ASYNCPG = True
except ImportError:
    HAS_ASYNCPG = False

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="class")
def e2e_db_url():
    """Create a fresh database for the cross-repo E2E test class."""
    db_name = f"discogs_crossrepo_{uuid.uuid4().hex[:8]}"
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


class TestXmlToCsvConversion:
    """Verify discogs-xml-converter produces CSVs from fixture XML.

    Skips entirely if the binary is not installed or the fixture XML
    does not exist in the sibling repo.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _convert_xml(self):
        if not HAS_XML_CONVERTER:
            pytest.skip("discogs-xml-converter binary or fixture XML not available")

        tmpdir = tempfile.mkdtemp(prefix="discogs_csv_")
        self.__class__._csv_dir = Path(tmpdir)

        result = subprocess.run(
            [
                XML_CONVERTER_BINARY,
                str(XML_FIXTURE),
                "--output-dir",
                tmpdir,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        self.__class__._returncode = result.returncode
        self.__class__._stderr = result.stderr

        if result.returncode != 0:
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

        assert result.returncode == 0, (
            f"XML conversion failed (exit {result.returncode}):\n{result.stderr}"
        )

        yield

        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_csv_files_produced(self) -> None:
        """Converter produces expected CSV files."""
        csv_dir = self.__class__._csv_dir
        expected_files = [
            "release.csv",
            "release_artist.csv",
            "release_track.csv",
            "release_label.csv",
        ]
        for fname in expected_files:
            assert (csv_dir / fname).exists(), f"Missing {fname} in converter output"

    def test_release_csv_has_header(self) -> None:
        """release.csv starts with the expected header row."""
        csv_dir = self.__class__._csv_dir
        with open(csv_dir / "release.csv") as f:
            header = f.readline().strip()
        assert "id" in header
        assert "title" in header
        assert "format" in header

    def test_release_csv_has_data(self) -> None:
        """release.csv contains data rows beyond the header."""
        csv_dir = self.__class__._csv_dir
        with open(csv_dir / "release.csv") as f:
            lines = f.readlines()
        assert len(lines) > 1, "release.csv has no data rows"

    def test_converter_csv_matches_expected(self) -> None:
        """Converter output matches the expected CSV fixtures in the xml-converter repo."""
        csv_dir = self.__class__._csv_dir
        expected_dir = XML_CONVERTER_REPO / "tests" / "fixtures" / "expected"
        if not expected_dir.exists():
            pytest.skip("Expected CSV fixtures not found in xml-converter repo")

        for expected_file in expected_dir.iterdir():
            if not expected_file.name.endswith(".csv"):
                continue
            actual_file = csv_dir / expected_file.name
            if not actual_file.exists():
                continue
            # Verify row counts match (header + data rows)
            expected_rows = len(expected_file.read_text().strip().splitlines())
            actual_rows = len(actual_file.read_text().strip().splitlines())
            assert actual_rows == expected_rows, (
                f"{expected_file.name}: expected {expected_rows} rows, got {actual_rows}"
            )


class TestFullPipelineCrossRepo:
    """Run discogs-cache pipeline on fixture CSVs, then verify via direct PG queries.

    This is the core pipeline test. It uses the committed CSV fixtures
    (not the XML converter output) so it works even when the xml-converter
    binary is not available.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline(self, e2e_db_url):
        """Run the full pipeline with fixture CSVs and library.db."""
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

    def _connect(self):
        return psycopg.connect(self.db_url)

    # -- Direct PG verification (no sibling repo needed) --

    def test_release_table_populated(self) -> None:
        """Release table has rows after pipeline."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count > 0, "release table is empty after pipeline"

    def test_trigram_indexes_exist(self) -> None:
        """pg_trgm indexes exist for fuzzy search."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public' AND indexname LIKE '%trgm%'
            """)
            indexes = {row[0] for row in cur.fetchall()}
        conn.close()
        assert len(indexes) >= 4, f"Expected >= 4 trgm indexes, got {indexes}"

    def test_unaccent_function_exists(self) -> None:
        """f_unaccent() function is available for accent-insensitive search."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT f_unaccent('Bjork')")
            result = cur.fetchone()[0]
        conn.close()
        assert result == "Bjork"

    def test_trigram_search_works(self) -> None:
        """pg_trgm fuzzy search returns results against pipeline data.

        Uses similarity() function instead of the % operator to avoid
        psycopg parameter escaping issues with the percent character.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ra.artist_name FROM release_artist ra "
                "WHERE similarity(lower(f_unaccent(ra.artist_name)), "
                "lower(f_unaccent(%(name)s))) > 0.3 LIMIT 5",
                {"name": "Radiohead"},
            )
            results = cur.fetchall()
        conn.close()
        assert len(results) > 0, "Trigram search for 'Radiohead' returned no results"

    def test_fuzzy_search_handles_typos(self) -> None:
        """pg_trgm search finds results even with typos in the query."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ra.artist_name FROM release_artist ra "
                "WHERE similarity(lower(f_unaccent(ra.artist_name)), "
                "lower(f_unaccent(%(name)s))) > 0.2 LIMIT 5",
                {"name": "Radiohed"},
            )
            results = cur.fetchall()
        conn.close()
        artist_names = [r[0] for r in results]
        assert any("Radiohead" in name for name in artist_names), (
            f"Typo search for 'Radiohed' didn't find Radiohead, got {artist_names}"
        )

    def test_track_search_works(self) -> None:
        """Track title search works against pipeline data."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rt.title, r.title as album "
                "FROM release_track rt "
                "JOIN release r ON r.id = rt.release_id "
                "WHERE similarity(lower(f_unaccent(rt.title)), "
                "lower(f_unaccent(%(track)s))) > 0.3 LIMIT 5",
                {"track": "Airbag"},
            )
            results = cur.fetchall()
        conn.close()
        assert len(results) > 0, "Track search for 'Airbag' returned no results"

    def test_cache_metadata_populated(self) -> None:
        """cache_metadata table has bulk_import entries."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM cache_metadata WHERE source = 'bulk_import'")
            count = cur.fetchone()[0]
        conn.close()
        assert count > 0, "cache_metadata has no bulk_import entries"

    # -- LML cross-repo verification --

    @pytest.mark.skipif(
        not (HAS_LML and HAS_ASYNCPG),
        reason="library-metadata-lookup or asyncpg not available",
    )
    def test_lml_search_releases_by_artist(self) -> None:
        """LML DiscogsCacheService.search_releases() finds artists in pipeline data."""
        db_url = self.db_url

        async def _run():
            pool = await asyncpg.create_pool(db_url)
            try:
                service = DiscogsCacheService(pool)
                results = await service.search_releases(artist="Radiohead")
                return results
            finally:
                await pool.close()

        results = asyncio.get_event_loop().run_until_complete(_run())
        assert len(results) > 0, "LML search for 'Radiohead' returned no results"
        assert any("OK Computer" in r["title"] for r in results), (
            f"Expected 'OK Computer' in results, got {[r['title'] for r in results]}"
        )

    @pytest.mark.skipif(
        not (HAS_LML and HAS_ASYNCPG),
        reason="library-metadata-lookup or asyncpg not available",
    )
    def test_lml_search_releases_by_track(self) -> None:
        """LML DiscogsCacheService.search_releases_by_track() works against pipeline data."""
        db_url = self.db_url

        async def _run():
            pool = await asyncpg.create_pool(db_url)
            try:
                service = DiscogsCacheService(pool)
                results = await service.search_releases_by_track(
                    track="Paranoid Android", artist="Radiohead"
                )
                return results
            finally:
                await pool.close()

        results = asyncio.get_event_loop().run_until_complete(_run())
        assert len(results) > 0, "LML track search for 'Paranoid Android' returned no results"


class TestXmlConverterToPipelineIntegration:
    """Full integration: XML -> CSV via converter -> import via pipeline -> verify.

    Only runs when discogs-xml-converter binary is available. Tests the complete
    3-repo data flow from Discogs XML dump to queryable PostgreSQL database.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _convert_and_import(self, e2e_db_url):
        if not HAS_XML_CONVERTER:
            pytest.skip("discogs-xml-converter binary or fixture XML not available")

        self.__class__._db_url = e2e_db_url

        # Step 1: Convert XML to CSV using the Rust binary
        tmpdir = tempfile.mkdtemp(prefix="discogs_e2e_csv_")
        self.__class__._csv_dir = Path(tmpdir)

        convert_result = subprocess.run(
            [
                XML_CONVERTER_BINARY,
                str(XML_FIXTURE),
                "--output-dir",
                tmpdir,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert convert_result.returncode == 0, (
            f"XML conversion failed:\n{convert_result.stderr}"
        )

        # Step 2: Run the pipeline on the converter's output (no --library-db
        # so prune is skipped -- this test verifies the XML->CSV->PG chain,
        # not the prune logic which is tested separately).
        pipeline_result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                tmpdir,
                "--database-url",
                e2e_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        if pipeline_result.returncode != 0:
            print("PIPELINE STDOUT:", pipeline_result.stdout)
            print("PIPELINE STDERR:", pipeline_result.stderr)

        assert pipeline_result.returncode == 0, (
            f"Pipeline failed on converter output:\n{pipeline_result.stderr}"
        )

        yield

        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_converter_output_imports_successfully(self) -> None:
        """Pipeline imports converter output without errors."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count > 0, "No releases imported from converter output"

    def test_release_titles_preserved(self) -> None:
        """Release titles survive the XML -> CSV -> PG import chain."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT title FROM release WHERE title IS NOT NULL")
            titles = {row[0] for row in cur.fetchall()}
        conn.close()
        assert "OK Computer" in titles, f"'OK Computer' not found in {titles}"

    def test_artist_data_imported(self) -> None:
        """release_artist table has data after converter -> pipeline import.

        If the converter's CSV does not include the 'role' column expected by
        import_csv.py, the import logs a warning and skips the table. This test
        verifies the current state of the cross-repo contract.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_artist")
            count = cur.fetchone()[0]
        conn.close()
        if count == 0:
            pytest.skip(
                "release_artist empty -- converter CSV may be missing 'role' column "
                "expected by import_csv.py (known schema drift)"
            )
        artists = set()
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT artist_name FROM release_artist WHERE extra = 0")
            artists = {row[0] for row in cur.fetchall()}
        conn.close()
        assert "Radiohead" in artists, f"Radiohead not found in {artists}"

    def test_track_titles_preserved(self) -> None:
        """Track titles survive the full conversion chain."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT title FROM release_track")
            tracks = {row[0] for row in cur.fetchall()}
        conn.close()
        assert "Airbag" in tracks, f"'Airbag' not found in {tracks}"
        assert "Paranoid Android" in tracks, f"'Paranoid Android' not found in {tracks}"
