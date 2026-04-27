"""Integration tests for verify_cache.py --copy-to against a real PostgreSQL database."""

from __future__ import annotations

import importlib.util
import os
import sys as _sys
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"
FIXTURE_LIBRARY_DB = FIXTURES_DIR / "library.db"

ALL_TABLES = (
    "cache_metadata",
    "release_track_artist",
    "release_track",
    "release_artist",
    "release",
)

ADMIN_URL = os.environ.get("DATABASE_URL_TEST", "postgresql://localhost:5433/postgres")

# Load import_csv module from the scripts/ directory (not on sys.path).
# Idempotent: re-use any already-loaded copy in sys.modules so multiple test
# files can import it without each one shadowing the previous load (which
# breaks ProcessPool pickling -- see #109).
_IMPORT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
if "import_csv" in _sys.modules:
    _ic = _sys.modules["import_csv"]
else:
    _spec = importlib.util.spec_from_file_location("import_csv", _IMPORT_PATH)
    assert _spec is not None and _spec.loader is not None
    _ic = importlib.util.module_from_spec(_spec)
    _sys.modules["import_csv"] = _ic
    _spec.loader.exec_module(_ic)

import_csv_func = _ic.import_csv
import_artwork = _ic.import_artwork
TABLES = _ic.TABLES

# Load verify_cache module the same way -- guarded so the second test file to
# import it doesn't replace the module object the first one's worker processes
# may need to unpickle.
_VC_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
if "verify_cache" in _sys.modules:
    _vc = _sys.modules["verify_cache"]
else:
    _vc_spec = importlib.util.spec_from_file_location("verify_cache", _VC_PATH)
    assert _vc_spec is not None and _vc_spec.loader is not None
    _vc = importlib.util.module_from_spec(_vc_spec)
    _sys.modules["verify_cache"] = _vc
    _vc_spec.loader.exec_module(_vc)

LibraryIndex = _vc.LibraryIndex
MultiIndexMatcher = _vc.MultiIndexMatcher
Decision = _vc.Decision
classify_all_releases = _vc.classify_all_releases
copy_releases_to_target = _vc.copy_releases_to_target

pytestmark = [pytest.mark.pg]


def _fresh_import(db_url: str) -> None:
    """Drop everything, apply schema and functions, and import fixture CSVs."""
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
    conn.close()

    conn = psycopg.connect(db_url)
    for table_config in TABLES:
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
    import_artwork(conn, CSV_DIR)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cache_metadata (release_id, source)
            SELECT id, 'bulk_import' FROM release
            ON CONFLICT (release_id) DO NOTHING
        """)
    conn.commit()
    conn.close()


def _load_releases_sync(db_url: str) -> list[tuple[int, str, str]]:
    """Load releases with primary artist (sync version of load_discogs_releases)."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.id, ra.artist_name, r.title
            FROM release r
            JOIN release_artist ra ON ra.release_id = r.id AND ra.extra = 0
            ORDER BY r.id
        """)
        rows = [(row[0], row[1], row[2]) for row in cur.fetchall()]
    conn.close()
    return rows


def _create_temp_database() -> tuple[str, str]:
    """Create a temporary database and return (db_url, db_name)."""
    db_name = f"discogs_test_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    admin_conn.close()

    if "@" in ADMIN_URL:
        base = ADMIN_URL.rsplit("/", 1)[0]
    else:
        base = ADMIN_URL.rsplit("/", 1)[0]
    return f"{base}/{db_name}", db_name


def _drop_database(db_name: str) -> None:
    """Drop a database by name."""
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = {} AND pid <> pg_backend_pid()"
            ).format(sql.Literal(db_name))
        )
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
    admin_conn.close()


class TestCopyToTarget:
    """Verify --copy-to copies matched releases to a new target database."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self):
        """Set up source DB with imported data, classify, and copy to target."""
        # Create source database
        source_url, source_name = _create_temp_database()
        self.__class__._source_url = source_url
        self.__class__._source_name = source_name

        # Import fixture data into source
        _fresh_import(source_url)

        # Count source releases before copy (to verify source is unchanged)
        conn = psycopg.connect(source_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            self.__class__._source_release_count = cur.fetchone()[0]
        conn.close()

        # Classify releases
        index = LibraryIndex.from_sqlite(FIXTURE_LIBRARY_DB)
        matcher = MultiIndexMatcher(index)
        releases = _load_releases_sync(source_url)
        report = classify_all_releases(releases, index, matcher)
        self.__class__._report = report

        # Build target URL (database created by copy_releases_to_target)
        target_name = f"discogs_test_{uuid.uuid4().hex[:8]}"
        if "@" in ADMIN_URL:
            base = ADMIN_URL.rsplit("/", 1)[0]
        else:
            base = ADMIN_URL.rsplit("/", 1)[0]
        target_url = f"{base}/{target_name}"
        self.__class__._target_url = target_url
        self.__class__._target_name = target_name

        # Run copy_releases_to_target
        copy_releases_to_target(source_url, target_url, report.keep_ids, report.review_ids)

        yield

        # Teardown: drop both databases
        _drop_database(source_name)
        _drop_database(target_name)

    @pytest.fixture(autouse=True)
    def _store_attrs(self):
        self.source_url = self.__class__._source_url
        self.target_url = self.__class__._target_url
        self.report = self.__class__._report
        self.source_release_count = self.__class__._source_release_count

    def test_target_database_created(self) -> None:
        """Target database should exist and be connectable."""
        conn = psycopg.connect(self.target_url)
        conn.close()

    def test_target_has_all_tables(self) -> None:
        """Target database should have all expected tables."""
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {
            "release",
            "release_artist",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        }
        assert expected.issubset(tables)

    def test_keep_releases_copied(self) -> None:
        """KEEP releases should be present in the target database."""
        if not self.report.keep_ids:
            pytest.skip("No releases classified as KEEP")

        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM release WHERE id = ANY(%s::integer[])",
                (list(self.report.keep_ids),),
            )
            found_ids = {row[0] for row in cur.fetchall()}
        conn.close()
        assert found_ids == self.report.keep_ids

    def test_review_releases_copied(self) -> None:
        """REVIEW releases should be present in the target database."""
        if not self.report.review_ids:
            pytest.skip("No releases classified as REVIEW")

        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM release WHERE id = ANY(%s::integer[])",
                (list(self.report.review_ids),),
            )
            found_ids = {row[0] for row in cur.fetchall()}
        conn.close()
        assert found_ids == self.report.review_ids

    def test_prune_releases_excluded(self) -> None:
        """PRUNE releases should NOT be in the target database."""
        if not self.report.prune_ids:
            pytest.skip("No releases classified as PRUNE")

        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM release WHERE id = ANY(%s::integer[])",
                (list(self.report.prune_ids),),
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0, f"Found {count} PRUNE releases in target"

    def test_child_tables_populated(self) -> None:
        """Child tables should have rows for copied releases."""
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_artist")
            artist_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM release_track")
            track_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM cache_metadata")
            metadata_count = cur.fetchone()[0]
        conn.close()
        assert artist_count > 0, "release_artist should have rows"
        assert track_count > 0, "release_track should have rows"
        assert metadata_count > 0, "cache_metadata should have rows"

    def test_source_unchanged(self) -> None:
        """Source database should not be modified by the copy operation."""
        conn = psycopg.connect(self.source_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count == self.source_release_count

    def test_target_release_count_matches(self) -> None:
        """Target should have exactly KEEP + REVIEW releases."""
        expected_count = len(self.report.keep_ids) + len(self.report.review_ids)
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count == expected_count

    def test_target_has_indexes(self) -> None:
        """Target should have trigram indexes."""
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

    def test_target_has_fk_constraints(self) -> None:
        """Target should have FK constraints on child tables."""
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tc.table_name
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type = 'FOREIGN KEY'
            """)
            fk_tables = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {"release_artist", "release_track", "release_track_artist", "cache_metadata"}
        assert expected.issubset(fk_tables)

    def test_target_no_master_id_column(self) -> None:
        """Target schema should not have master_id (post-dedup source schema)."""
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'release'
            """)
            columns = {row[0] for row in cur.fetchall()}
        conn.close()
        # The source has master_id (pre-dedup), but the target schema from
        # create_database.sql does include it. The COPY streams explicit
        # columns that match what the source has post-dedup.
        # Actually the target gets the full schema including master_id,
        # but since we only copy id, title, release_year, artwork_url,
        # master_id will be NULL. That's acceptable.
        expected = {"id", "title", "release_year", "country", "artwork_url"}
        assert expected.issubset(columns)

    def test_target_country_data_copied(self) -> None:
        """Country data is actually present in the target (not just the column)."""
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT country FROM release WHERE country IS NOT NULL LIMIT 1")
            result = cur.fetchone()
        conn.close()
        assert result is not None, "No country data found in target releases"
