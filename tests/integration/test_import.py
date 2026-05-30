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
import_release_via_upsert = _ic.import_release_via_upsert
create_track_count_table = _ic.create_track_count_table
populate_cache_metadata = _ic.populate_cache_metadata
populate_release_year = _ic.populate_release_year
_import_tables = _ic._import_tables
TABLES = _ic.TABLES
BASE_TABLES = _ic.BASE_TABLES
TRACK_TABLES = _ic.TRACK_TABLES
VIDEO_TABLES = _ic.VIDEO_TABLES

pytestmark = pytest.mark.pg


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
        populate_release_year(conn)

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
        # 17 rows in fixture CSV (all have required fields)
        assert count == 17

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
        assert labels == ["Arcola", "Warp Records"]

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
            # Release 1001 has released="2001-04-23", should become 2001
            cur.execute("SELECT release_year FROM release WHERE id = 1001")
            year = cur.fetchone()[0]
        conn.close()
        assert year == 2001

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
    "release_video",
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


class TestPopulateCacheMetadata:
    """Verify populate_cache_metadata() inserts metadata for all releases via COPY."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        self.__class__._db_url = db_url
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute("INSERT INTO release (id, title) VALUES (5001, 'DOGA')")
            cur.execute("INSERT INTO release (id, title) VALUES (5002, 'Aluminum Tunes')")
            cur.execute("INSERT INTO release (id, title) VALUES (5003, 'Moon Pix')")
        conn.close()

        conn = psycopg.connect(db_url)
        populate_cache_metadata(conn)
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_metadata_row_count(self) -> None:
        """One cache_metadata row per release."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM cache_metadata")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 3

    def test_metadata_source(self) -> None:
        """All rows have source='bulk_import'."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT source FROM cache_metadata")
            sources = {row[0] for row in cur.fetchall()}
        conn.close()
        assert sources == {"bulk_import"}

    def test_metadata_release_ids(self) -> None:
        """Metadata release_ids match the inserted releases."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT release_id FROM cache_metadata ORDER BY release_id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [5001, 5002, 5003]

    def test_metadata_cached_at_not_null(self) -> None:
        """cached_at defaults to current timestamp (not null)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM cache_metadata WHERE cached_at IS NOT NULL")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 3


class TestImportArtwork:
    """Verify ``import_artwork()`` populates ``artwork_url`` from
    ``release_image.csv``. Parity coverage for WXYC/discogs-etl#240 —
    the four cases the issue mandates are pinned across this class
    (primary preferred, fallback when no primary, empty URI skipped) +
    ``TestImportArtworkMissing`` (CSV file absent) + the gap case
    ``test_no_row_for_release_leaves_artwork_null`` below (CSV present
    but no row for the target release). A deliberate breakage of
    ``import_artwork``'s primary-preference branch fails
    ``test_primary_image_preferred``."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        self.__class__._db_url = db_url
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute("INSERT INTO release (id, title) VALUES (101, 'Album A')")
            cur.execute("INSERT INTO release (id, title) VALUES (102, 'Album B')")
            cur.execute("INSERT INTO release (id, title) VALUES (103, 'Album C')")
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_primary_image_preferred(self, tmp_path) -> None:
        """Primary image type is used over secondary."""
        csv_path = tmp_path / "release_image.csv"
        csv_path.write_text(
            "release_id,type,width,height,uri\n"
            "101,secondary,300,300,https://img.discogs.com/secondary-101.jpg\n"
            "101,primary,600,600,https://img.discogs.com/primary-101.jpg\n"
        )
        conn = psycopg.connect(self.db_url)
        import_artwork(conn, tmp_path)
        conn.close()

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 101")
            url = cur.fetchone()[0]
        conn.close()
        assert url == "https://img.discogs.com/primary-101.jpg"

    def test_fallback_when_no_primary(self, tmp_path) -> None:
        """Secondary image used as fallback when no primary exists."""
        csv_path = tmp_path / "release_image.csv"
        csv_path.write_text(
            "release_id,type,width,height,uri\n"
            "102,secondary,600,600,https://img.discogs.com/secondary-102.jpg\n"
        )
        # Reset artwork_url for release 102
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("UPDATE release SET artwork_url = NULL WHERE id = 102")
        conn.commit()
        import_artwork(conn, tmp_path)
        conn.close()

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 102")
            url = cur.fetchone()[0]
        conn.close()
        assert url == "https://img.discogs.com/secondary-102.jpg"

    def test_invalid_release_id_skipped(self, tmp_path) -> None:
        """Rows with non-integer release_id are silently skipped."""
        csv_path = tmp_path / "release_image.csv"
        csv_path.write_text(
            "release_id,type,width,height,uri\n"
            "abc,primary,600,600,https://img.discogs.com/bad.jpg\n"
            "103,primary,600,600,https://img.discogs.com/good-103.jpg\n"
        )
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("UPDATE release SET artwork_url = NULL WHERE id = 103")
        conn.commit()
        count = import_artwork(conn, tmp_path)
        conn.close()

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 103")
            url = cur.fetchone()[0]
        conn.close()
        assert url == "https://img.discogs.com/good-103.jpg"
        assert count >= 1

    def test_empty_uri_skipped(self, tmp_path) -> None:
        """Rows with empty URI are skipped."""
        csv_path = tmp_path / "release_image.csv"
        csv_path.write_text("release_id,type,width,height,uri\n103,primary,600,600,\n")
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("UPDATE release SET artwork_url = NULL WHERE id = 103")
        conn.commit()
        count = import_artwork(conn, tmp_path)
        conn.close()

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 103")
            url = cur.fetchone()[0]
        conn.close()
        assert url is None
        assert count == 0

    def test_no_row_for_release_leaves_artwork_null(self, tmp_path) -> None:
        """A release with no row in ``release_image.csv`` keeps
        ``artwork_url IS NULL`` after the import — the "never asked" path
        downstream. Distinct from ``TestImportArtworkMissing`` (whole CSV
        absent) and ``test_empty_uri_skipped`` (row exists, URI empty);
        this is the silent-not-mentioned case the bulk loader has to
        leave untouched. Closing the WXYC/discogs-etl#240 acceptance
        matrix."""
        csv_path = tmp_path / "release_image.csv"
        # Image row exists for 101, NOT for 102 — even though release 102 is
        # present in the table from the class fixture.
        csv_path.write_text(
            "release_id,type,width,height,uri\n"
            "101,primary,600,600,https://img.discogs.com/no-row-101.jpg\n"
        )
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("UPDATE release SET artwork_url = NULL WHERE id = 102")
        conn.commit()
        import_artwork(conn, tmp_path)
        conn.close()

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 102")
            url = cur.fetchone()[0]
        conn.close()
        assert url is None, (
            "release 102 has no row in release_image.csv — its artwork_url "
            "must stay NULL after import_artwork. If this regresses, the "
            "loader is writing speculative URIs to unrelated releases."
        )


class TestImportArtworkPreservation:
    """Acceptance grid for WXYC/discogs-etl#242 — the rebuild preserves
    LML-back-patched ``(artwork_url, artwork_checked_at)`` across runs."""

    @pytest.fixture(autouse=True)
    def _fresh_schema(self, fresh_db_url):
        """Each test gets its own DB so seeded back-patches + CSV state are
        isolated. Cheaper schema-apply than the full ETL setup other classes
        use, and the per-test isolation is necessary because tests 1, 2, 5
        mutate the same release_id."""
        self.db_url = fresh_db_url
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

    def _write_release_csv(self, tmp_path: Path, rows: list[tuple]) -> None:
        """Write a minimal release.csv covering the columns BASE_TABLES['release']
        expects. Each row: (id, title, country, released, format, master_id)."""
        lines = ["id,title,country,released,format,master_id"]
        for row in rows:
            lines.append(",".join("" if v is None else str(v) for v in row))
        (tmp_path / "release.csv").write_text("\n".join(lines) + "\n")

    def _write_release_image_csv(self, tmp_path: Path, rows: list[tuple]) -> None:
        """Write release_image.csv. Each row: (release_id, type, uri)."""
        lines = ["release_id,type,width,height,uri"]
        for release_id, img_type, uri in rows:
            lines.append(f"{release_id},{img_type},600,600,{uri}")
        (tmp_path / "release_image.csv").write_text("\n".join(lines) + "\n")

    def _rebuild(self, tmp_path: Path) -> None:
        """Run the post-Option-B base import flow against tmp_path's CSVs:
        upsert from release_staging, then back-patch artwork."""
        conn = psycopg.connect(self.db_url)
        import_release_via_upsert(conn, tmp_path)
        import_artwork(conn, tmp_path)
        conn.close()

    def _read_artwork(self, release_id: int) -> tuple:
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT artwork_url, artwork_checked_at FROM release WHERE id = %s",
                (release_id,),
            )
            row = cur.fetchone()
        conn.close()
        return row

    def test_rebuild_preserves_artwork_when_dump_missing_image(self, tmp_path) -> None:
        """The issue's verbatim acceptance criterion: a release whose
        ``release_image.csv`` row is absent retains the prior LML
        back-patched ``artwork_url`` + ``artwork_checked_at``."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO release (id, title, artwork_url, artwork_checked_at) "
                "VALUES (501, 'Seed', 'lml-backpatched', '2026-04-01 00:00:00+00')"
            )
        conn.commit()
        conn.close()

        self._write_release_csv(tmp_path, [(501, "Seed", "US", "2024", "LP", None)])
        # No release_image.csv row for 501.
        self._write_release_image_csv(tmp_path, [])
        self._rebuild(tmp_path)

        url, checked_at = self._read_artwork(501)
        assert url == "lml-backpatched", (
            "rebuild wiped the prior LML back-patch — Option B's UPSERT must "
            "exclude artwork_url from the SET list so prior back-patches survive."
        )
        assert checked_at is not None
        assert checked_at.year == 2026 and checked_at.month == 4

    def test_rebuild_overwrites_artwork_when_dump_has_image(self, tmp_path) -> None:
        """When the bulk dump has a real artwork URL for a previously
        back-patched release, the dump wins (EXCLUDED-first precedence)
        and ``artwork_checked_at`` is restamped at rebuild time."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO release (id, title, artwork_url, artwork_checked_at) "
                "VALUES (502, 'Seed', 'lml-backpatched', '2026-04-01 00:00:00+00')"
            )
        conn.commit()
        conn.close()

        self._write_release_csv(tmp_path, [(502, "Seed", "US", "2024", "LP", None)])
        self._write_release_image_csv(tmp_path, [(502, "primary", "dump-uri")])
        # Capture a lower bound for the post-rebuild artwork_checked_at.
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT now()")
            rebuild_lower_bound = cur.fetchone()[0]
        conn.close()
        self._rebuild(tmp_path)

        url, checked_at = self._read_artwork(502)
        assert url == "dump-uri"
        assert checked_at is not None and checked_at >= rebuild_lower_bound

    def test_rebuild_stamps_checked_at_for_freshly_imported_release(self, tmp_path) -> None:
        """A first-time-imported release whose dump carries an image gets
        ``artwork_checked_at`` stamped at import time. Matches the semantics
        LML's runtime ``write_release`` already applies (LML#423)."""
        self._write_release_csv(tmp_path, [(503, "Fresh", "US", "2024", "LP", None)])
        self._write_release_image_csv(tmp_path, [(503, "primary", "dump-uri")])
        self._rebuild(tmp_path)

        url, checked_at = self._read_artwork(503)
        assert url == "dump-uri"
        assert checked_at is not None, (
            "import_artwork must stamp artwork_checked_at = now() when it "
            "sets artwork_url from the dump — otherwise LML's predicate "
            "(LML#423) treats the row as 'never asked' and burns API quota."
        )

    def test_rebuild_leaves_checked_at_null_when_no_dump_image_on_fresh_release(
        self, tmp_path
    ) -> None:
        """A first-time-imported release with no dump image stays in the
        'never asked' state (both columns NULL). LML#221's partial index
        ``release_artwork_null_idx`` is what covers this case for the
        drain."""
        self._write_release_csv(tmp_path, [(504, "Imageless", "US", "2024", "LP", None)])
        self._write_release_image_csv(tmp_path, [])
        self._rebuild(tmp_path)

        url, checked_at = self._read_artwork(504)
        assert url is None
        assert checked_at is None

    def test_rebuild_purges_releases_not_in_dump(self, tmp_path) -> None:
        """When a release falls out of the new dump (artist removed from
        the library, etc.), the rebuild path removes it and its child
        rows. With Option B, child cleanup is via the §3a TRUNCATE step
        (TRUNCATE release_artist + siblings before re-COPY); the parent
        is removed by ``DELETE FROM release WHERE id NOT IN staging``.
        End state: 506 absent from release; absent from release_artist
        (was wiped by TRUNCATE and never re-COPYed because its row isn't
        in the new dump's release_artist.csv); 505 present in both."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (505, 'Stays'), (506, 'Goes')")
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name, extra) "
                "VALUES (505, 'Old Artist A', 0), (506, 'Old Artist B', 0)"
            )
        conn.commit()
        conn.close()

        # Only 505 in the new dump.
        self._write_release_csv(tmp_path, [(505, "Stays", "US", "2024", "LP", None)])
        self._write_release_image_csv(tmp_path, [])
        (tmp_path / "release_artist.csv").write_text(
            "release_id,artist_id,artist_name,extra\n505,,Artist A,0\n"
        )
        self._rebuild(tmp_path)
        # Re-COPY the children the way the full base step would. The
        # rebuild's TRUNCATE has already cleared them.
        conn = psycopg.connect(self.db_url)
        artist_cfg = next(t for t in BASE_TABLES if t["table"] == "release_artist")
        import_csv_func(
            conn,
            tmp_path / artist_cfg["csv_file"],
            artist_cfg["table"],
            artist_cfg["csv_columns"],
            artist_cfg["db_columns"],
            artist_cfg["required"],
            artist_cfg["transforms"],
            unique_key=artist_cfg.get("unique_key"),
        )
        conn.commit()
        conn.close()

        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release ORDER BY id")
            release_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT release_id FROM release_artist ORDER BY release_id")
            artist_release_ids = [r[0] for r in cur.fetchall()]
        conn.close()
        assert release_ids == [505]
        assert artist_release_ids == [505]

    def test_rebuild_refuses_empty_staging(self, tmp_path) -> None:
        """A truncated / mid-write ``release.csv`` (0 rows after the COPY
        skip-on-required-null path) would otherwise let the DELETE step
        wipe every release. Safety floor in
        ``import_release_via_upsert`` raises instead."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (507, 'Stays')")
        conn.commit()
        conn.close()

        # Header-only release.csv (no rows). import_csv will COPY 0 rows
        # into release_staging, tripping the safety floor.
        (tmp_path / "release.csv").write_text(
            "id,title,country,released,format,master_id\n"
        )
        (tmp_path / "release_image.csv").write_text("release_id,type,width,height,uri\n")

        upsert_conn = psycopg.connect(self.db_url)
        with pytest.raises(RuntimeError, match="release_staging is empty"):
            import_release_via_upsert(upsert_conn, tmp_path)
        upsert_conn.close()

        # Pre-existing release survived; nothing was DELETEd.
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release ORDER BY id")
            release_ids = [r[0] for r in cur.fetchall()]
        conn.close()
        assert release_ids == [507]


class TestImportArtworkMissing:
    """Verify import_artwork() returns 0 when release_image.csv is missing."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        self.__class__._db_url = db_url
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Test')")
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_returns_zero(self, tmp_path) -> None:
        """import_artwork returns 0 when release_image.csv does not exist."""
        conn = psycopg.connect(self.db_url)
        result = import_artwork(conn, tmp_path)
        conn.close()
        assert result == 0


class TestCreateTrackCountTableMissing:
    """Verify create_track_count_table() returns 0 when release_track.csv is missing."""

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

    def test_returns_zero(self, tmp_path) -> None:
        """create_track_count_table returns 0 when release_track.csv does not exist."""
        conn = psycopg.connect(self.db_url)
        result = create_track_count_table(conn, tmp_path)
        conn.close()
        assert result == 0


class TestImportTables:
    """Verify _import_tables() sequential import of table configs."""

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

    def test_imports_all_tables(self) -> None:
        """_import_tables imports all CSVs in the table list and returns total count."""
        conn = psycopg.connect(self.db_url)
        total = _import_tables(conn, CSV_DIR, BASE_TABLES)
        conn.close()

        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            release_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM release_artist")
            artist_count = cur.fetchone()[0]
        conn.close()
        assert release_count > 0
        assert artist_count > 0
        assert total == release_count + artist_count + 16  # + release_label count

    def test_skips_missing_csv(self, tmp_path) -> None:
        """_import_tables skips table configs whose CSV file does not exist."""
        conn = psycopg.connect(self.db_url)
        total = _import_tables(conn, tmp_path, TRACK_TABLES)
        conn.close()
        assert total == 0


class TestImportReleaseVideo:
    """Import release_video.csv into a real database and verify results."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        """Apply schema, import release data, then import release_video."""
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
        for table_config in VIDEO_TABLES:
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
                )
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_row_count(self) -> None:
        """All 5 fixture rows are imported (no required fields missing)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_video")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 5

    def test_multiple_videos_per_release(self) -> None:
        """Release 1001 has two video rows imported in sequence order."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sequence, title FROM release_video WHERE release_id = 1001 ORDER BY sequence"
            )
            rows = cur.fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0] == (1, "VI Scose Poise")
        assert rows[1] == (2, "Cfern")

    def test_embed_false_stored_correctly(self) -> None:
        """embed=false in CSV is stored as FALSE boolean in the database."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT embed FROM release_video WHERE release_id = 2001")
            embed = cur.fetchone()[0]
        conn.close()
        assert embed is False

    def test_embed_true_stored_correctly(self) -> None:
        """embed=true in CSV is stored as TRUE boolean in the database."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT embed FROM release_video WHERE release_id = 1001 AND sequence = 1")
            embed = cur.fetchone()[0]
        conn.close()
        assert embed is True

    def test_duration_null_when_empty(self) -> None:
        """Empty duration in CSV becomes NULL in the database."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT duration FROM release_video WHERE release_id = 5001")
            duration = cur.fetchone()[0]
        conn.close()
        assert duration is None

    def test_duration_integer_when_present(self) -> None:
        """Non-empty duration is stored as an integer (seconds)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT duration FROM release_video WHERE release_id = 1001 AND sequence = 1"
            )
            duration = cur.fetchone()[0]
        conn.close()
        assert duration == 291

    def test_src_stored(self) -> None:
        """src URL is stored as-is."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT src FROM release_video WHERE release_id = 3001 AND sequence = 1")
            src = cur.fetchone()[0]
        conn.close()
        assert src == "https://www.youtube.com/watch?v=ghijkl01"

    def test_empty_src_skipped(self, tmp_path) -> None:
        """Rows with empty src (required field) are skipped during import."""
        csv_path = tmp_path / "release_video.csv"
        csv_path.write_text(
            "release_id,sequence,src,title,duration,embed\n"
            "1001,99,,Empty src title,100,true\n"
            "1001,98,https://www.youtube.com/watch?v=valid,Valid,100,true\n"
        )
        conn = self._connect()
        config = VIDEO_TABLES[0]
        count = import_csv_func(
            conn,
            csv_path,
            config["table"],
            config["csv_columns"],
            config["db_columns"],
            config["required"],
            config["transforms"],
            unique_key=config.get("unique_key"),
            release_id_filter={1001},
        )
        conn.close()
        assert count == 1  # only the row with valid src is imported

    def test_index_exists(self) -> None:
        """idx_release_video_release_id index exists on release_video."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'release_video'
                  AND indexname = 'idx_release_video_release_id'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None, "idx_release_video_release_id index should exist"

    def test_on_delete_cascade(self) -> None:
        """Deleting a release cascades to delete its release_video rows."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Verify 5001 has a video before deletion
            cur.execute("SELECT count(*) FROM release_video WHERE release_id = 5001")
            before = cur.fetchone()[0]
        conn.close()
        assert before == 1

        # Delete release 5001 — should cascade to release_video
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM release WHERE id = 5001")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_video WHERE release_id = 5001")
            after = cur.fetchone()[0]
        conn.close()
        assert after == 0


class TestFilteredVideoImport:
    """Import videos filtered to a subset of release IDs."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        """Import base tables, then import videos filtered to a subset."""
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

        # Import videos filtered to only a subset of releases
        filter_ids = {1001, 3001}
        for table_config in VIDEO_TABLES:
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

    def test_only_filtered_videos_imported(self) -> None:
        """Only videos for the filtered release IDs should be present."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT release_id FROM release_video ORDER BY release_id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1001, 3001]

    def test_excluded_release_has_no_videos(self) -> None:
        """Releases not in the filter set should have no videos."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_video WHERE release_id = 2001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_included_release_has_correct_video_count(self) -> None:
        """Release 1001 should have all 2 videos."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_video WHERE release_id = 1001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 2

    def test_total_video_count(self) -> None:
        """Total videos should be the sum for the filtered releases."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_video")
            count = cur.fetchone()[0]
        conn.close()
        # 1001: 2, 3001: 1 = 3
        assert count == 3
