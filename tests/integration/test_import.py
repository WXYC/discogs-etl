"""Integration tests for scripts/import_csv.py against a real PostgreSQL database."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"

# Load import_csv module
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

import_csv_func = _ic.import_csv
import_artwork = _ic.import_artwork
create_track_count_table = _ic.create_track_count_table
TABLES = _ic.TABLES
BASE_TABLES = _ic.BASE_TABLES
TRACK_TABLES = _ic.TRACK_TABLES

pytestmark = pytest.mark.postgres


class TestImportCsv:
    """Import fixture CSVs into a fresh schema and verify results."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        """Apply schema and import all fixture CSVs (once per test class)."""
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
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

    @pytest.fixture(autouse=True)
    def _store_url(self, db_url):
        self.db_url = db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_release_row_count(self) -> None:
        """Correct number of releases imported (skipping empty-title row)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        # 16 rows in fixture CSV, minus 1 with empty title (release 7001)
        assert count == 15

    def test_release_artist_row_count(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_artist")
            count = cur.fetchone()[0]
        conn.close()
        # 16 rows in fixture CSV (all have required fields)
        assert count == 16

    def test_release_label_row_count(self) -> None:
        """All label rows imported (one per unique release_id+label pair)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_label")
            count = cur.fetchone()[0]
        conn.close()
        # 16 rows in fixture CSV, all unique (release_id, label) pairs
        assert count == 16

    def test_release_label_column_mapping(self) -> None:
        """CSV 'label' column maps to DB 'label_name'."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT label_name FROM release_label WHERE release_id = 1001 ORDER BY label_name"
            )
            labels = [row[0] for row in cur.fetchall()]
        conn.close()
        assert labels == ["Capitol Records", "Parlophone"]

    def test_release_track_row_count(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 30

    def test_extract_year_applied(self) -> None:
        """Dates are transformed to 4-digit years."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Release 1001 has released="1997-06-16", should become 1997
            cur.execute("SELECT release_year FROM release WHERE id = 1001")
            year = cur.fetchone()[0]
        conn.close()
        assert year == 1997

    def test_unknown_date_yields_null(self) -> None:
        """Non-date strings in released field produce NULL release_year."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Release 6001 has released="Unknown"
            cur.execute("SELECT release_year FROM release WHERE id = 6001")
            year = cur.fetchone()[0]
        conn.close()
        assert year is None

    def test_empty_date_yields_null(self) -> None:
        """Empty released field produces NULL release_year."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Release 5002 has released=""
            cur.execute("SELECT release_year FROM release WHERE id = 5002")
            year = cur.fetchone()[0]
        conn.close()
        assert year is None

    def test_null_required_fields_skipped(self) -> None:
        """Rows with null required fields are not imported."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Release 7001 has empty title (required)
            cur.execute("SELECT count(*) FROM release WHERE id = 7001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_master_id_imported(self) -> None:
        """master_id column is populated for releases that have one."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT master_id FROM release WHERE id = 1001")
            master_id = cur.fetchone()[0]
        conn.close()
        assert master_id == 500

    def test_null_master_id(self) -> None:
        """Releases without master_id have NULL."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT master_id FROM release WHERE id = 4001")
            master_id = cur.fetchone()[0]
        conn.close()
        assert master_id is None

    def test_artwork_url_primary(self) -> None:
        """Primary artwork image is preferred."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 1001")
            url = cur.fetchone()[0]
        conn.close()
        assert url is not None
        assert "release-1001" in url

    def test_artwork_url_fallback(self) -> None:
        """Secondary image used as fallback when no primary exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 2001")
            url = cur.fetchone()[0]
        conn.close()
        assert url is not None
        assert "release-2001" in url

    def test_artwork_url_missing(self) -> None:
        """Releases without images have NULL artwork_url."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 5001")
            url = cur.fetchone()[0]
        conn.close()
        assert url is None

    def test_cache_metadata_populated(self) -> None:
        """All imported releases have cache_metadata entries."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM cache_metadata")
            meta_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM release")
            release_count = cur.fetchone()[0]
        conn.close()
        assert meta_count == release_count

    def test_cache_metadata_source(self) -> None:
        """Cache metadata source is 'bulk_import'."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT source FROM cache_metadata")
            sources = {row[0] for row in cur.fetchall()}
        conn.close()
        assert sources == {"bulk_import"}


ALL_TABLES = (
    "cache_metadata",
    "release_track_artist",
    "release_track",
    "release_label",
    "release_artist",
    "release",
)


def _clean_db(db_url: str) -> None:
    """Drop all pipeline tables and artifacts."""
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        cur.execute("DROP TABLE IF EXISTS release_track_count CASCADE")
    conn.close()


class TestTrackCountTable:
    """Verify create_track_count_table() creates the right data."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        """Apply schema, import base tables, then create track count table."""
        self.__class__._db_url = db_url
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

        conn = psycopg.connect(db_url)
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
        create_track_count_table(conn, CSV_DIR)
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_table_exists(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables"
                "  WHERE table_name = 'release_track_count'"
                ")"
            )
            exists = cur.fetchone()[0]
        conn.close()
        assert exists

    def test_row_count(self) -> None:
        """One row per release_id that has tracks in the CSV."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track_count")
            count = cur.fetchone()[0]
        conn.close()
        # 15 distinct release_ids in release_track.csv
        assert count == 15

    def test_correct_counts(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT track_count FROM release_track_count WHERE release_id = 1002")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 3

    def test_track_tables_empty(self) -> None:
        """Base-only import should not populate track tables."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0


class TestFilteredTrackImport:
    """Import tracks filtered to a subset of release IDs."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        """Import base tables, then import tracks filtered to a subset."""
        self.__class__._db_url = db_url
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

        conn = psycopg.connect(db_url)
        # Import base tables
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

        # Import tracks filtered to only a subset of releases
        filter_ids = {1002, 3001, 4001}
        for table_config in TRACK_TABLES:
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
                    unique_key=table_config.get("unique_key"),
                    release_id_filter=filter_ids,
                )
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_only_filtered_tracks_imported(self) -> None:
        """Only tracks for the filtered release IDs should be present."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT release_id FROM release_track ORDER BY release_id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1002, 3001, 4001]

    def test_excluded_release_has_no_tracks(self) -> None:
        """Releases not in the filter set should have no tracks."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track WHERE release_id = 1001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_included_release_has_correct_track_count(self) -> None:
        """Release 1002 should have all 3 tracks."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track WHERE release_id = 1002")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 3

    def test_total_track_count(self) -> None:
        """Total tracks should be the sum for the filtered releases."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track")
            count = cur.fetchone()[0]
        conn.close()
        # 1002: 3, 3001: 2, 4001: 2 = 7
        assert count == 7


class TestDuplicateReleaseIds:
    """Import a CSV with duplicate release IDs — first occurrence wins."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        self.__class__._db_url = db_url
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_duplicate_release_ids_keep_first(self, tmp_path) -> None:
        """When a CSV has duplicate release IDs, only the first row is imported."""
        csv_path = tmp_path / "release.csv"
        csv_path.write_text(
            "id,status,title,country,released,notes,data_quality,master_id,format\n"
            "5001,Accepted,DOGA,AR,2024-05-10,,Correct,8001,LP\n"
            "5001,Accepted,Different Title,US,2025,,Correct,8002,CD\n"
            "5002,Accepted,Aluminum Tunes,UK,1998-09-01,,Correct,8002,CD\n"
        )

        release_config = next(t for t in BASE_TABLES if t["table"] == "release")
        conn = psycopg.connect(self.db_url)
        count = import_csv_func(
            conn,
            csv_path,
            release_config["table"],
            release_config["csv_columns"],
            release_config["db_columns"],
            release_config["required"],
            release_config["transforms"],
            unique_key=release_config["unique_key"],
        )
        conn.close()

        assert count == 2  # 2 unique IDs, not 3 rows

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT title FROM release WHERE id = 5001")
            title = cur.fetchone()[0]
        conn.close()
        # First occurrence wins
        assert title == "DOGA"
