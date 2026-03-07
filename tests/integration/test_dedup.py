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
add_constraints_and_indexes = _dd.add_constraints_and_indexes
load_library_labels = _dd.load_library_labels
load_label_hierarchy = _dd.load_label_hierarchy
create_label_match_table = _dd.create_label_match_table

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


def _run_dedup(db_url: str) -> None:
    """Run the dedup pipeline (base tables only) against the database."""
    conn = psycopg.connect(db_url, autocommit=True)
    delete_count = ensure_dedup_ids(conn)
    if delete_count > 0:
        # Only base tables + cache_metadata (no track tables)
        tables = [
            ("release", "new_release", "id, title, release_year, country, artwork_url", "id"),
            (
                "release_artist",
                "new_release_artist",
                "release_id, artist_id, artist_name, extra",
                "release_id",
            ),
            (
                "release_label",
                "new_release_label",
                "release_id, label_name",
                "release_id",
            ),
            (
                "cache_metadata",
                "new_cache_metadata",
                "release_id, cached_at, source, last_validated",
                "release_id",
            ),
        ]

        for old, new, cols, id_col in tables:
            copy_table(conn, old, new, cols, id_col)

        # Drop FK constraints before swap
        with conn.cursor() as cur:
            for stmt in [
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ]:
                cur.execute(stmt)

        for old, new, _, _ in tables:
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

    def test_correct_release_kept_for_master_500(self) -> None:
        """Release 1002 (US, 3 tracks) kept over 1001 (UK, 5 tracks) by country preference."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1002]

    def test_correct_release_kept_for_master_600(self) -> None:
        """Release 2002 (DE, 4 tracks) kept over 2001 (UK, 2 tracks) by track count fallback."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (2001, 2002) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [2002]

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

    def test_child_table_rows_cleaned(self) -> None:
        """Deduped releases have their child table rows removed (not imported)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_artist WHERE release_id = 1001")
            artist_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM release_label WHERE release_id = 1001")
            label_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM release_track WHERE release_id = 1001")
            track_count = cur.fetchone()[0]
        conn.close()
        assert artist_count == 0
        assert label_count == 0
        assert track_count == 0

    def test_kept_release_labels_preserved(self) -> None:
        """The kept release still has its labels after dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT label_name FROM release_label WHERE release_id = 1002 ORDER BY label_name"
            )
            labels = [row[0] for row in cur.fetchall()]
        conn.close()
        assert labels == ["Capitol Records"]

    def test_deduped_release_has_no_labels(self) -> None:
        """Releases removed by dedup have no labels."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_label WHERE release_id = 1001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_kept_release_tracks_preserved(self) -> None:
        """The kept release still has its tracks (imported after dedup)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track WHERE release_id = 1002")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 3

    def test_country_column_preserved(self) -> None:
        """country column exists after dedup copy-swap and has the expected value."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT country FROM release WHERE id = 1002")
            country = cur.fetchone()[0]
        conn.close()
        assert country == "US"

    def test_us_preferred_over_track_count(self) -> None:
        """US release (1002, 3 tracks) kept over UK release (1001, 5 tracks).

        Proves country preference is the deciding factor: the kept release has
        fewer tracks than the removed one.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            # 1002 should be kept (US, 3 tracks)
            cur.execute("SELECT count(*) FROM release WHERE id = 1002")
            assert cur.fetchone()[0] == 1
            # 1001 should be removed (UK, 5 tracks — more tracks but not US)
            cur.execute("SELECT count(*) FROM release WHERE id = 1001")
            assert cur.fetchone()[0] == 0
            # Verify the kept release has fewer tracks (proving country was decisive)
            cur.execute("SELECT count(*) FROM release_track WHERE release_id = 1002")
            kept_tracks = cur.fetchone()[0]
        conn.close()
        assert kept_tracks == 3, "Kept US release should have 3 tracks (fewer than removed UK's 5)"

    def test_master_id_column_dropped(self) -> None:
        """master_id column no longer exists after copy-swap (not in SELECT list)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'release' AND column_name = 'master_id'"
            )
            result = cur.fetchone()
        conn.close()
        assert result is None

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

    def test_deduped_release_has_no_tracks(self) -> None:
        """Releases removed by dedup have no tracks (not imported for them)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track WHERE release_id = 1001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_total_release_count_after_dedup(self) -> None:
        """Total releases: 15 imported - 3 duplicates = 12."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        # 15 imported (7001 skipped), 1001+1003 removed (master 500), 2001 removed (master 600)
        assert count == 12

    def test_release_track_count_dropped(self) -> None:
        """release_track_count table is cleaned up after dedup."""
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
        assert not exists


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
        tables = [
            ("release", "new_release", "id, title, release_year, country, artwork_url", "id"),
            (
                "release_artist",
                "new_release_artist",
                "release_id, artist_id, artist_name, extra",
                "release_id",
            ),
            (
                "release_label",
                "new_release_label",
                "release_id, label_name",
                "release_id",
            ),
            (
                "cache_metadata",
                "new_cache_metadata",
                "release_id, cached_at, source, last_validated",
                "release_id",
            ),
        ]

        for old, new, cols, id_col in tables:
            copy_table(conn, old, new, cols, id_col)

        with conn.cursor() as cur:
            for stmt in [
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ]:
                cur.execute(stmt)

        for old, new, _, _ in tables:
            swap_tables(conn, old, new)
        add_base_constraints_and_indexes(conn, db_url=db_url)

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

    def test_label_match_overrides_track_count_master_500(self) -> None:
        """Release 1001 (Parlophone, 3 tracks) wins over 1002 (Capitol, 5 tracks)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1001]

    def test_label_match_overrides_track_count_master_600(self) -> None:
        """Release 2001 (Factory, 2 tracks) wins over 2002 (Qwest, 4 tracks)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (2001, 2002) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [2001]

    def test_unmatched_releases_use_track_count(self) -> None:
        """Releases with unique master_ids are not affected by label matching."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id IN (5001, 5002)")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 2

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
        """Same total: 15 imported - 3 duplicates = 12 (just different winners)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 12


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
        tables = [
            ("release", "new_release", "id, title, release_year, country, artwork_url", "id"),
            (
                "release_artist",
                "new_release_artist",
                "release_id, artist_id, artist_name, extra",
                "release_id",
            ),
            (
                "release_label",
                "new_release_label",
                "release_id, label_name",
                "release_id",
            ),
            (
                "cache_metadata",
                "new_cache_metadata",
                "release_id, cached_at, source, last_validated",
                "release_id",
            ),
        ]

        for old, new, cols, id_col in tables:
            copy_table(conn, old, new, cols, id_col)

        with conn.cursor() as cur:
            for stmt in [
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ]:
                cur.execute(stmt)

        for old, new, _, _ in tables:
            swap_tables(conn, old, new)
        add_base_constraints_and_indexes(conn, db_url=db_url)

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

    def test_sublabel_match_for_master_500(self) -> None:
        """Release 1001 (Parlophone) wins — library says Parlophone, direct match."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id")
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1001]

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
        """Same total: 15 imported - 3 duplicates = 12."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 12
