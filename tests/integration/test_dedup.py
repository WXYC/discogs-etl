"""Integration tests for scripts/dedup_releases.py against a real PostgreSQL database."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"

# Load modules
_IMPORT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv", _IMPORT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

_DEDUP_PATH = Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py"
_dspec = importlib.util.spec_from_file_location("dedup_releases", _DEDUP_PATH)
assert _dspec is not None and _dspec.loader is not None
_dd = importlib.util.module_from_spec(_dspec)
_dspec.loader.exec_module(_dd)

import_csv_func = _ic.import_csv
import_artwork = _ic.import_artwork
create_track_count_table = _ic.create_track_count_table
BASE_TABLES = _ic.BASE_TABLES
TRACK_TABLES = _ic.TRACK_TABLES
ensure_dedup_ids = _dd.ensure_dedup_ids
copy_table = _dd.copy_table
swap_tables = _dd.swap_tables
add_base_constraints_and_indexes = _dd.add_base_constraints_and_indexes
add_track_constraints_and_indexes = _dd.add_track_constraints_and_indexes
add_constraints_and_indexes = _dd.add_constraints_and_indexes
load_library_labels = _dd.load_library_labels
load_label_hierarchy = _dd.load_label_hierarchy
create_label_match_table = _dd.create_label_match_table

pytestmark = pytest.mark.pg

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
        # Also drop dedup artifacts
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids CASCADE")
        cur.execute("DROP TABLE IF EXISTS release_track_count CASCADE")
        for prefix in ("new_", ""):
            for table in ALL_TABLES:
                cur.execute(f"DROP TABLE IF EXISTS {prefix}{table}_old CASCADE")


def _fresh_import(db_url: str) -> None:
    """Drop everything, apply schema and functions, and import base fixture CSVs.

    Imports only BASE_TABLES (release, release_artist) plus artwork, cache_metadata,
    and the release_track_count table. Track tables are NOT imported (deferred).
    """
    conn = psycopg.connect(db_url, autocommit=True)
    _drop_all_tables(conn)
    with conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
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
    import_artwork(conn, CSV_DIR)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cache_metadata (release_id, source)
            SELECT id, 'bulk_import' FROM release
            ON CONFLICT (release_id) DO NOTHING
        """)
    conn.commit()
    create_track_count_table(conn, CSV_DIR)
    conn.close()


DEDUP_TABLES = [
    (
        "release",
        "new_release",
        "id, title, release_year, country, artwork_url, released, format",
        "id",
    ),
    (
        "release_artist",
        "new_release_artist",
        "release_id, artist_id, artist_name, extra, role",
        "release_id",
    ),
    (
        "release_label",
        "new_release_label",
        "release_id, label_id, label_name, catno",
        "release_id",
    ),
    # release_genre and release_style must be in this list so swap_tables drops the
    # original tables (and their indexes from create_database.sql) before
    # add_base_constraints_and_indexes recreates them. Otherwise the index creation
    # in add_base_constraints_and_indexes fails with DuplicateTable.
    (
        "release_genre",
        "new_release_genre",
        "release_id, genre",
        "release_id",
    ),
    (
        "release_style",
        "new_release_style",
        "release_id, style",
        "release_id",
    ),
    (
        "cache_metadata",
        "new_cache_metadata",
        "release_id, cached_at, source, last_validated",
        "release_id",
    ),
]


def _run_dedup(db_url: str) -> None:
    """Run the dedup pipeline (base tables only) against the database."""
    conn = psycopg.connect(db_url, autocommit=True)
    delete_count = ensure_dedup_ids(conn)
    if delete_count > 0:
        for old, new, cols, id_col in DEDUP_TABLES:
            copy_table(conn, old, new, cols, id_col)

        # Drop FK constraints before swap
        with conn.cursor() as cur:
            for stmt in [
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE release_genre DROP CONSTRAINT IF EXISTS fk_release_genre_release",
                "ALTER TABLE release_style DROP CONSTRAINT IF EXISTS fk_release_style_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ]:
                cur.execute(stmt)

        for old, new, _, _ in DEDUP_TABLES:
            swap_tables(conn, old, new)
        add_base_constraints_and_indexes(conn, db_url=db_url)

        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
            cur.execute("DROP TABLE IF EXISTS release_track_count")
    conn.close()


def _import_tracks_after_dedup(db_url: str) -> None:
    """Import tracks filtered to surviving release IDs after dedup."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM release")
        release_ids = {row[0] for row in cur.fetchall()}

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
                release_id_filter=release_ids,
            )
    conn.close()


class TestDedup:
    """Deduplicate releases by master_id using the copy-swap strategy."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_dedup(self, db_url):
        """Import base fixtures, run dedup, then import tracks."""
        self.__class__._db_url = db_url
        _fresh_import(db_url)
        _run_dedup(db_url)
        _import_tracks_after_dedup(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_all_formats_survive_for_master_500(self) -> None:
        """All three formats (CD, Vinyl, Cassette) survive — dedup is per (master_id, format)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1001, 1002, 1003]

    def test_all_formats_survive_for_master_600(self) -> None:
        """Both formats (LP/Vinyl, CD) survive — dedup is per (master_id, format)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (2001, 2002) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [2001, 2002]

    def test_unique_master_id_release_untouched(self) -> None:
        """Release 3001 (unique master_id 700) is not removed."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 3001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 1

    def test_null_master_id_release_untouched(self) -> None:
        """Release 4001 (no master_id) is not removed."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 4001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 1

    def test_all_releases_have_child_rows(self) -> None:
        """All format-unique releases keep their child table rows."""
        conn = self._connect()
        with conn.cursor() as cur:
            # All three releases in master_id 500 survive (different formats)
            for rid in (1001, 1002, 1003):
                cur.execute("SELECT count(*) FROM release_artist WHERE release_id = %s", (rid,))
                assert cur.fetchone()[0] > 0, f"release_artist missing for {rid}"
        conn.close()

    def test_all_releases_have_labels(self) -> None:
        """All format-unique releases keep their labels after dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT label_name FROM release_label WHERE release_id = 1001 ORDER BY label_name"
            )
            labels = [row[0] for row in cur.fetchall()]
        conn.close()
        assert "Warp Records" in labels

    def test_all_releases_have_tracks(self) -> None:
        """All format-unique releases have their tracks (imported after dedup)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track WHERE release_id = 1001")
            assert cur.fetchone()[0] == 5  # UK CD has 5 tracks
            cur.execute("SELECT count(*) FROM release_track WHERE release_id = 1002")
            assert cur.fetchone()[0] == 3  # US Vinyl has 3 tracks
        conn.close()

    def test_country_column_preserved(self) -> None:
        """country column exists after dedup copy-swap and has the expected value."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT country FROM release WHERE id = 1002")
            country = cur.fetchone()[0]
        conn.close()
        assert country == "US"

    def test_different_formats_all_survive(self) -> None:
        """All releases with different formats survive dedup, regardless of country/track count.

        With format-aware dedup, (master_id, format) groups each have one member,
        so no releases are deleted from the fixture data.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            # All three should survive (different formats: CD, Vinyl, Cassette)
            cur.execute("SELECT count(*) FROM release WHERE id IN (1001, 1002, 1003)")
            assert cur.fetchone()[0] == 3
        conn.close()

    def test_master_id_column_persists_when_no_dedup(self) -> None:
        """master_id persists when no duplicates found (copy-swap not triggered).

        With format-aware dedup, each (master_id, format) group in fixtures has one member,
        so no copy-swap occurs and master_id stays in the schema.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'release' AND column_name = 'master_id'"
            )
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_primary_key_recreated(self) -> None:
        """Primary key on release(id) exists after dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT constraint_name FROM information_schema.table_constraints
                WHERE table_name = 'release' AND constraint_type = 'PRIMARY KEY'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_base_fk_constraints_recreated(self) -> None:
        """FK constraints on base child tables exist after dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tc.table_name
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type = 'FOREIGN KEY'
            """)
            fk_tables = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {"release_artist", "release_label", "cache_metadata"}
        assert expected.issubset(fk_tables)

    def test_format_column_persists_after_dedup(self) -> None:
        """format column exists after dedup copy-swap (unlike master_id which is dropped)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'release' AND column_name = 'format'"
            )
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_format_values_preserved(self) -> None:
        """format column has the normalized values after dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT format FROM release WHERE id = 1001")
            assert cur.fetchone()[0] == "CD"
            cur.execute("SELECT format FROM release WHERE id = 1002")
            assert cur.fetchone()[0] == "Vinyl"
        conn.close()

    def test_total_release_count_after_dedup(self) -> None:
        """Total releases: 15 imported (7001 skipped), 0 deduped (all unique formats) = 15."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        # All 15 imported releases survive — each (master_id, format) group has one member
        assert count == 15

    def test_release_track_count_persists_when_no_dedup(self) -> None:
        """release_track_count persists when no duplicates found (cleanup in dedup path)."""
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
        # release_track_count cleanup is inside the if delete_count > 0 block
        assert exists


class TestDedupNoop:
    """Verify dedup is a no-op when there are no duplicates."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute("INSERT INTO release (id, title, master_id) VALUES (1, 'A', 100)")
            cur.execute("INSERT INTO release (id, title, master_id) VALUES (2, 'B', 200)")
            # Use release_track_count instead of release_track for ranking
            cur.execute("""
                CREATE UNLOGGED TABLE release_track_count (
                    release_id integer PRIMARY KEY,
                    track_count integer NOT NULL
                )
            """)
            cur.execute("INSERT INTO release_track_count (release_id, track_count) VALUES (1, 3)")
            cur.execute("INSERT INTO release_track_count (release_id, track_count) VALUES (2, 5)")
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_no_duplicates_found(self) -> None:
        """ensure_dedup_ids returns 0 when no duplicates exist."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        count = ensure_dedup_ids(conn)
        # Clean up dedup_delete_ids if created
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
        conn.close()
        assert count == 0


def _run_dedup_with_labels(db_url: str, library_labels_csv: Path) -> None:
    """Run the dedup pipeline with label-matching enabled."""
    conn = psycopg.connect(db_url, autocommit=True)
    load_library_labels(conn, library_labels_csv)
    create_label_match_table(conn)
    delete_count = ensure_dedup_ids(conn)
    if delete_count > 0:
        for old, new, cols, id_col in DEDUP_TABLES:
            copy_table(conn, old, new, cols, id_col)

        with conn.cursor() as cur:
            for stmt in [
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ]:
                cur.execute(stmt)

        for old, new, _, _ in DEDUP_TABLES:
            swap_tables(conn, old, new)
        add_base_constraints_and_indexes(conn, db_url=db_url)

    # Cleanup temp tables regardless of whether dedup ran
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
        cur.execute("DROP TABLE IF EXISTS release_track_count")
        cur.execute("DROP TABLE IF EXISTS wxyc_label_pref")
        cur.execute("DROP TABLE IF EXISTS release_label_match")
    conn.close()


class TestDedupWithLabels:
    """Dedup ranking prefers releases matching WXYC label preferences."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_dedup(self, db_url):
        """Import base fixtures, run label-aware dedup, then import tracks."""
        self.__class__._db_url = db_url
        _fresh_import(db_url)
        library_labels_csv = CSV_DIR / "library_labels.csv"
        _run_dedup_with_labels(db_url, library_labels_csv)
        _import_tracks_after_dedup(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_all_formats_survive_with_labels_master_500(self) -> None:
        """All formats survive — dedup is per (master_id, format), labels only rank within format."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1001, 1002, 1003]

    def test_all_formats_survive_with_labels_master_600(self) -> None:
        """Both formats survive — LP and CD are different format groups."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (2001, 2002) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [2001, 2002]

    def test_unique_and_null_master_id_untouched(self) -> None:
        """Releases with unique or NULL master_id survive label-aware dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id IN (3001, 4001)")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 2

    def test_label_match_temp_tables_cleaned_up(self) -> None:
        """Temp tables wxyc_label_pref and release_label_match are dropped."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_name IN ('wxyc_label_pref', 'release_label_match')"
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_total_release_count_after_dedup(self) -> None:
        """All 15 survive — each (master_id, format) group has one member."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 15


class TestDedupFallback:
    """Verify dedup falls back to release_track when release_track_count doesn't exist."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            # Two releases with same master_id
            cur.execute("INSERT INTO release (id, title, master_id) VALUES (1, 'A', 100)")
            cur.execute("INSERT INTO release (id, title, master_id) VALUES (2, 'B', 100)")
            # Release 2 has more tracks
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, title) VALUES (1, 1, 'T1')"
            )
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, title) VALUES (2, 1, 'T1')"
            )
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, title) VALUES (2, 2, 'T2')"
            )
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_fallback_to_release_track(self) -> None:
        """Without release_track_count, ensure_dedup_ids uses release_track."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        count = ensure_dedup_ids(conn)
        conn.close()
        # Release 1 should be marked for deletion (fewer tracks)
        assert count == 1

    def test_correct_release_deleted(self) -> None:
        """Release 1 (1 track) is deleted, release 2 (2 tracks) is kept."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT release_id FROM dedup_delete_ids")
            ids = [row[0] for row in cur.fetchall()]
            cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
        conn.close()
        assert ids == [1]


def _run_dedup_with_labels_and_hierarchy(
    db_url: str, library_labels_csv: Path, label_hierarchy_csv: Path
) -> None:
    """Run the dedup pipeline with label-matching and sublabel resolution enabled."""
    conn = psycopg.connect(db_url, autocommit=True)
    load_library_labels(conn, library_labels_csv)
    load_label_hierarchy(conn, label_hierarchy_csv)
    create_label_match_table(conn)
    delete_count = ensure_dedup_ids(conn)
    if delete_count > 0:
        for old, new, cols, id_col in DEDUP_TABLES:
            copy_table(conn, old, new, cols, id_col)

        with conn.cursor() as cur:
            for stmt in [
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ]:
                cur.execute(stmt)

        for old, new, _, _ in DEDUP_TABLES:
            swap_tables(conn, old, new)
        add_base_constraints_and_indexes(conn, db_url=db_url)

    # Cleanup temp tables regardless of whether dedup ran
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
        cur.execute("DROP TABLE IF EXISTS release_track_count")
        cur.execute("DROP TABLE IF EXISTS wxyc_label_pref")
        cur.execute("DROP TABLE IF EXISTS release_label_match")
        cur.execute("DROP TABLE IF EXISTS label_hierarchy")
    conn.close()


class TestDedupWithLabelHierarchy:
    """Dedup ranking resolves sublabels via label_hierarchy."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_dedup(self, db_url):
        """Import base fixtures, run label+hierarchy-aware dedup, then import tracks."""
        self.__class__._db_url = db_url
        _fresh_import(db_url)
        library_labels_csv = CSV_DIR / "library_labels.csv"
        label_hierarchy_csv = CSV_DIR / "label_hierarchy.csv"
        _run_dedup_with_labels_and_hierarchy(db_url, library_labels_csv, label_hierarchy_csv)
        _import_tracks_after_dedup(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_all_formats_survive_with_hierarchy_master_500(self) -> None:
        """All formats survive — dedup is per (master_id, format)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1001, 1002, 1003]

    def test_label_hierarchy_temp_table_cleaned_up(self) -> None:
        """label_hierarchy table is dropped after dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_name = 'label_hierarchy'"
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_total_release_count_same(self) -> None:
        """All 15 survive — each (master_id, format) group has one member."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 15


class TestEnsureDedupIdsAlreadyExists:
    """Verify ensure_dedup_ids returns existing count when table already populated."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            # Insert releases with duplicate master_ids (would normally be deduped)
            cur.execute("INSERT INTO release (id, title, master_id) VALUES (1, 'A', 100)")
            cur.execute("INSERT INTO release (id, title, master_id) VALUES (2, 'B', 100)")
            # Pre-create the dedup_delete_ids table with some IDs
            cur.execute("""
                CREATE UNLOGGED TABLE dedup_delete_ids (
                    release_id integer PRIMARY KEY
                )
            """)
            cur.execute("INSERT INTO dedup_delete_ids (release_id) VALUES (1)")
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_returns_existing_count(self) -> None:
        """When dedup_delete_ids already exists, returns its count without recreating."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        count = ensure_dedup_ids(conn)
        conn.close()
        assert count == 1

    def test_table_not_recreated(self) -> None:
        """The pre-existing dedup_delete_ids table is not dropped and recreated."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        # Add a second ID to the existing table
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dedup_delete_ids (release_id) VALUES (2) ON CONFLICT DO NOTHING"
            )
        count = ensure_dedup_ids(conn)
        conn.close()
        # Should reflect the updated count (not recreate from ROW_NUMBER query)
        assert count == 2


class TestAddTrackConstraintsAndIndexes:
    """Verify add_track_constraints_and_indexes creates FK constraints and indexes."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            # Drop existing constraints (schema creates them, we want to test adding them)
            cur.execute(
                "ALTER TABLE release_track DROP CONSTRAINT IF EXISTS release_track_release_id_fkey"
            )
            cur.execute(
                "ALTER TABLE release_track_artist "
                "DROP CONSTRAINT IF EXISTS release_track_artist_release_id_fkey"
            )
            cur.execute("DROP INDEX IF EXISTS idx_release_track_release_id")
            cur.execute("DROP INDEX IF EXISTS idx_release_track_artist_release_id")
            cur.execute("DROP INDEX IF EXISTS idx_release_track_title_trgm")
            cur.execute("DROP INDEX IF EXISTS idx_release_track_artist_name_trgm")
            # Insert test data
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Test Album')")
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, title) VALUES (1, 1, 'Track 1')"
            )
            cur.execute(
                "INSERT INTO release_track_artist (release_id, track_sequence, artist_name) "
                "VALUES (1, 1, 'Test Artist')"
            )
        add_track_constraints_and_indexes(conn, db_url=db_url)
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_release_track_fk_exists(self) -> None:
        """FK constraint on release_track referencing release exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT constraint_name FROM information_schema.table_constraints
                WHERE table_name = 'release_track' AND constraint_type = 'FOREIGN KEY'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_release_track_artist_fk_exists(self) -> None:
        """FK constraint on release_track_artist referencing release exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT constraint_name FROM information_schema.table_constraints
                WHERE table_name = 'release_track_artist' AND constraint_type = 'FOREIGN KEY'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_release_track_release_id_index_exists(self) -> None:
        """Index on release_track(release_id) exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'release_track' AND indexname = 'idx_release_track_release_id'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_release_track_artist_release_id_index_exists(self) -> None:
        """Index on release_track_artist(release_id) exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'release_track_artist'
                AND indexname = 'idx_release_track_artist_release_id'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_release_track_title_trgm_index_exists(self) -> None:
        """GIN trigram index on release_track(title) exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'release_track'
                AND indexname = 'idx_release_track_title_trgm'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_release_track_artist_name_trgm_index_exists(self) -> None:
        """GIN trigram index on release_track_artist(artist_name) exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'release_track_artist'
                AND indexname = 'idx_release_track_artist_name_trgm'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None


class TestAddConstraintsAndIndexes:
    """Verify add_constraints_and_indexes creates both base and track constraints."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            # Drop all FK constraints and indexes (schema creates them).
            # release_video must be included so the subsequent
            # ``DROP CONSTRAINT release_pkey`` doesn't fail with
            # ``cannot drop constraint release_pkey on table release because
            # other objects depend on it`` (release_video's FK depends on
            # the release primary key index; see #105).
            for constraint, table in [
                ("release_artist_release_id_fkey", "release_artist"),
                ("release_label_release_id_fkey", "release_label"),
                ("release_genre_release_id_fkey", "release_genre"),
                ("release_style_release_id_fkey", "release_style"),
                ("release_track_release_id_fkey", "release_track"),
                ("release_track_artist_release_id_fkey", "release_track_artist"),
                ("release_video_release_id_fkey", "release_video"),
                ("cache_metadata_release_id_fkey", "cache_metadata"),
            ]:
                cur.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}")
            # Drop the PK on release so add_constraints_and_indexes can recreate it
            cur.execute("ALTER TABLE release DROP CONSTRAINT IF EXISTS release_pkey")
            cur.execute("ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS cache_metadata_pkey")
            # Drop all indexes (FK, GIN trigram, cache metadata)
            for idx in [
                "idx_release_artist_release_id",
                "idx_release_label_release_id",
                "idx_release_genre_release_id",
                "idx_release_style_release_id",
                "idx_release_track_release_id",
                "idx_release_track_artist_release_id",
                "idx_release_artist_name_trgm",
                "idx_release_title_trgm",
                "idx_release_track_title_trgm",
                "idx_release_track_artist_name_trgm",
                "idx_cache_metadata_cached_at",
                "idx_cache_metadata_source",
                "idx_release_master_id",
            ]:
                cur.execute(f"DROP INDEX IF EXISTS {idx}")
            # Insert test data
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Test Album')")
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name) VALUES (1, 'Test Artist')"
            )
            cur.execute("INSERT INTO release_label (release_id, label_name) VALUES (1, 'Test Lbl')")
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, title) VALUES (1, 1, 'Track 1')"
            )
            cur.execute(
                "INSERT INTO release_track_artist (release_id, track_sequence, artist_name) "
                "VALUES (1, 1, 'Track Artist')"
            )
            cur.execute("INSERT INTO cache_metadata (release_id, source) VALUES (1, 'bulk_import')")
        add_constraints_and_indexes(conn, db_url=db_url)
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_release_pk_exists(self) -> None:
        """Primary key on release(id) exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT constraint_name FROM information_schema.table_constraints
                WHERE table_name = 'release' AND constraint_type = 'PRIMARY KEY'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_all_fk_constraints_exist(self) -> None:
        """FK constraints on all child tables exist."""
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

    def test_base_and_track_indexes_exist(self) -> None:
        """Both base and track FK indexes exist."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
            indexes = {row[0] for row in cur.fetchall()}
        conn.close()
        expected_indexes = {
            "idx_release_artist_release_id",
            "idx_release_label_release_id",
            "idx_release_track_release_id",
            "idx_release_track_artist_release_id",
        }
        assert expected_indexes.issubset(indexes)


class TestFormatAwareDedup:
    """Dedup partitions by (master_id, format): same-format duplicates are deduped,
    different-format releases survive."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            # Two CD releases with same master_id — only one should survive
            cur.execute(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (1, 'Album', 100, 'CD', 'UK')"
            )
            cur.execute(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (2, 'Album', 100, 'CD', 'US')"
            )
            # One Vinyl release with same master_id — should survive alongside the CD winner
            cur.execute(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (3, 'Album', 100, 'Vinyl', 'UK')"
            )
            # Two NULL-format releases with same master_id — NULLs group together, one survives
            cur.execute(
                "INSERT INTO release (id, title, master_id, country) "
                "VALUES (4, 'Album B', 200, 'US')"
            )
            cur.execute(
                "INSERT INTO release (id, title, master_id, country) "
                "VALUES (5, 'Album B', 200, 'UK')"
            )
            # Track counts for ranking
            cur.execute("""
                CREATE UNLOGGED TABLE release_track_count (
                    release_id integer PRIMARY KEY,
                    track_count integer NOT NULL
                )
            """)
            cur.execute(
                "INSERT INTO release_track_count VALUES (1, 5), (2, 3), (3, 4), (4, 2), (5, 1)"
            )
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_same_format_deduped_normally(self) -> None:
        """Two CD releases sharing master_id: only the US one survives (country preference)."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        count = ensure_dedup_ids(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT release_id FROM dedup_delete_ids ORDER BY release_id")
            deleted = [row[0] for row in cur.fetchall()]
            cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
            cur.execute("DROP TABLE IF EXISTS release_track_count")
        conn.close()
        # Release 1 (UK CD) deleted in favor of release 2 (US CD)
        # Release 5 (UK, NULL format) deleted in favor of release 4 (US, NULL format)
        assert 1 in deleted
        assert 5 in deleted
        assert count == 2

    def test_different_format_survives(self) -> None:
        """Vinyl release (3) is not in dedup_delete_ids — different format group."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        # Recreate track counts and run dedup
        with conn.cursor() as cur:
            cur.execute("""
                CREATE UNLOGGED TABLE IF NOT EXISTS release_track_count (
                    release_id integer PRIMARY KEY,
                    track_count integer NOT NULL
                )
            """)
            cur.execute(
                "INSERT INTO release_track_count VALUES (1, 5), (2, 3), (3, 4), (4, 2), (5, 1) "
                "ON CONFLICT DO NOTHING"
            )
        ensure_dedup_ids(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT release_id FROM dedup_delete_ids")
            deleted = {row[0] for row in cur.fetchall()}
            cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
            cur.execute("DROP TABLE IF EXISTS release_track_count")
        conn.close()
        assert 3 not in deleted

    def test_null_format_groups_together(self) -> None:
        """Releases with NULL format group together by master_id only."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                CREATE UNLOGGED TABLE IF NOT EXISTS release_track_count (
                    release_id integer PRIMARY KEY,
                    track_count integer NOT NULL
                )
            """)
            cur.execute(
                "INSERT INTO release_track_count VALUES (1, 5), (2, 3), (3, 4), (4, 2), (5, 1) "
                "ON CONFLICT DO NOTHING"
            )
        ensure_dedup_ids(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT release_id FROM dedup_delete_ids")
            deleted = {row[0] for row in cur.fetchall()}
            cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
            cur.execute("DROP TABLE IF EXISTS release_track_count")
        conn.close()
        # Release 5 (UK, NULL format) should be deleted — groups with release 4 (US, NULL format)
        assert 5 in deleted
        assert 4 not in deleted


class TestDedupCopySwapAbortCleanup:
    """Verify that abandoned temp tables from a failed copy-swap are cleaned up.

    Simulates a scenario where a previous dedup run created new_* tables
    (the copy phase) but crashed before completing the swap. On the next run,
    copy_table() drops any pre-existing new_* table before recreating it.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            # Two releases with same master_id — triggers dedup
            cur.execute(
                "INSERT INTO release (id, title, master_id, country) VALUES (1, 'DOGA', 100, 'AR')"
            )
            cur.execute(
                "INSERT INTO release (id, title, master_id, country) VALUES (2, 'DOGA', 100, 'US')"
            )
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name) VALUES (1, 'Juana Molina')"
            )
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name) VALUES (2, 'Juana Molina')"
            )
            cur.execute("INSERT INTO cache_metadata (release_id, source) VALUES (1, 'bulk_import')")
            cur.execute("INSERT INTO cache_metadata (release_id, source) VALUES (2, 'bulk_import')")
            # Track counts for ranking
            cur.execute("""
                CREATE UNLOGGED TABLE release_track_count (
                    release_id integer PRIMARY KEY,
                    track_count integer NOT NULL
                )
            """)
            cur.execute("INSERT INTO release_track_count VALUES (1, 3), (2, 5)")
            # Simulate a previous failed run: leave a dangling new_release table
            cur.execute("CREATE TABLE new_release AS SELECT * FROM release WHERE false")
            cur.execute(
                "INSERT INTO new_release (id, title, master_id, country) "
                "VALUES (999, 'Stale Ghost Row', 999, 'XX')"
            )
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_dangling_new_table_exists_before_dedup(self) -> None:
        """Precondition: new_release from a failed previous run exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_name = 'new_release'"
                ")"
            )
            exists = cur.fetchone()[0]
        conn.close()
        assert exists, "new_release should exist as a leftover from a failed run"

    def test_copy_table_replaces_dangling_table(self) -> None:
        """copy_table() drops the stale new_release and creates a fresh one.

        This verifies that copy_table's `DROP TABLE IF EXISTS new_table`
        handles cleanup of abandoned temp tables from prior failed runs.
        """
        conn = psycopg.connect(self.db_url, autocommit=True)
        ensure_dedup_ids(conn)

        # copy_table drops the stale new_release and creates a fresh one
        count = copy_table(
            conn,
            "release",
            "new_release",
            "id, title, release_year, country, artwork_url, released, format",
            "id",
        )

        # The stale ghost row (id=999) should be gone — new_release is rebuilt
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM new_release WHERE id = 999")
            ghost_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM new_release")
            total = cur.fetchone()[0]
        conn.close()

        assert ghost_count == 0, "Stale ghost row should not survive copy_table"
        assert total == count
        # Only the US release (id=2) survives dedup (US preference)
        assert count == 1

    def test_full_dedup_succeeds_despite_dangling_tables(self) -> None:
        """A complete dedup cycle succeeds even with leftover new_* tables.

        Re-runs the full dedup pipeline from scratch, verifying it handles
        cleanup of any leftover artifacts from the prior test methods.
        """
        # Re-create a clean state with dedup-worthy data
        conn = psycopg.connect(self.db_url, autocommit=True)
        _drop_all_tables(conn)
        # Also drop any leftover new_* tables from the prior test method
        with conn.cursor() as cur:
            for table in ALL_TABLES:
                cur.execute(f"DROP TABLE IF EXISTS new_{table} CASCADE")
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.execute(
                "INSERT INTO release (id, title, master_id, country) "
                "VALUES (1, 'Aluminum Tunes', 100, 'UK')"
            )
            cur.execute(
                "INSERT INTO release (id, title, master_id, country) "
                "VALUES (2, 'Aluminum Tunes', 100, 'US')"
            )
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name) VALUES (1, 'Stereolab')"
            )
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name) VALUES (2, 'Stereolab')"
            )
            cur.execute(
                "INSERT INTO release_label (release_id, label_name) VALUES (1, 'Duophonic')"
            )
            cur.execute(
                "INSERT INTO release_label (release_id, label_name) VALUES (2, 'Duophonic')"
            )
            cur.execute("INSERT INTO cache_metadata (release_id, source) VALUES (1, 'bulk_import')")
            cur.execute("INSERT INTO cache_metadata (release_id, source) VALUES (2, 'bulk_import')")
            cur.execute("""
                CREATE UNLOGGED TABLE release_track_count (
                    release_id integer PRIMARY KEY,
                    track_count integer NOT NULL
                )
            """)
            cur.execute("INSERT INTO release_track_count VALUES (1, 5), (2, 3)")
            # Leave a dangling new_release from a "crashed" previous run
            cur.execute("CREATE TABLE new_release (id integer, title text)")
            cur.execute("INSERT INTO new_release VALUES (999, 'Ghost')")
        conn.close()

        # Run the full dedup pipeline
        _run_dedup(self.db_url)

        # Verify the final state
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            # Only US release survives
            cur.execute("SELECT id FROM release ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
            # No dangling new_* or *_old tables
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE 'new_%' OR table_name LIKE '%_old'"
            )
            dangling = [row[0] for row in cur.fetchall()]
            # dedup_delete_ids should be cleaned up
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_name = 'dedup_delete_ids'"
                ")"
            )
            dedup_exists = cur.fetchone()[0]
        conn.close()

        assert ids == [2], f"Only US release should survive dedup, got {ids}"
        assert dangling == [], f"No dangling temp tables should remain: {dangling}"
        assert not dedup_exists, "dedup_delete_ids should be cleaned up"
