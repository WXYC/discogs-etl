"""Integration tests for verify_cache.py pruning against a real PostgreSQL database."""

from __future__ import annotations

import asyncio
import importlib.util
import sys as _sys
from pathlib import Path

import asyncpg
import psycopg
import pytest

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

# Load import_csv module from scripts/ (not on sys.path). Guard against re-load
# so multiple integration test files share the same module object -- otherwise
# the second-loaded copy shadows the first and breaks ProcessPool pickling for
# any worker that holds a reference to symbols from the original load (see #109).
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

# verify_cache uses asyncpg, so we load it but only use the non-async pieces.
# Same idempotent-load guard as above.
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
get_table_sizes = _vc.get_table_sizes
count_rows_to_delete = _vc.count_rows_to_delete
prune_releases = _vc.prune_releases

pytestmark = [pytest.mark.pg]


def _fresh_import(db_url: str) -> None:
    """Drop everything, apply schema, and import fixture CSVs."""
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
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


class TestPruneClassification:
    """Verify KEEP/PRUNE classifications against fixture library.db."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        _fresh_import(db_url)
        index = LibraryIndex.from_sqlite(FIXTURE_LIBRARY_DB)
        matcher = MultiIndexMatcher(index)
        releases = _load_releases_sync(db_url)
        self.__class__._report = classify_all_releases(releases, index, matcher)

    @pytest.fixture(autouse=True)
    def _store_attrs(self):
        self.db_url = self.__class__._db_url
        self.report = self.__class__._report

    def test_radiohead_ok_computer_kept(self) -> None:
        """Autechre 'Confield' should be classified as KEEP."""
        assert self.report.keep_ids & {1001, 1002, 1003}

    def test_joy_division_unknown_pleasures_kept(self) -> None:
        """Father John Misty 'I Love You, Honeybear' should be classified as KEEP."""
        assert self.report.keep_ids & {2001, 2002}

    def test_unknown_album_pruned(self) -> None:
        """Release 5001 'Unknown Album' by 'DJ Unknown' should be PRUNE."""
        assert 5001 in self.report.prune_ids

    def test_non_library_artist_pruned(self) -> None:
        """Release 10001 by 'Random Artist X' should be PRUNE."""
        assert 10001 in self.report.prune_ids

    def test_abbey_road_kept(self) -> None:
        """Field 'From Here We Go Sublime' should be KEEP (tests comma convention)."""
        assert 9001 in self.report.keep_ids

    def test_kid_a_kept(self) -> None:
        """Autechre 'Amber' should be KEEP."""
        assert 3001 in self.report.keep_ids

    def test_amnesiac_kept(self) -> None:
        """Autechre 'Tri Repetae' should be KEEP."""
        assert 4001 in self.report.keep_ids


class TestPruneExecution:
    """Verify --prune actually deletes PRUNE releases."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_classify(self, db_url):
        self.__class__._db_url = db_url
        _fresh_import(db_url)
        index = LibraryIndex.from_sqlite(FIXTURE_LIBRARY_DB)
        matcher = MultiIndexMatcher(index)
        releases = _load_releases_sync(db_url)
        self.__class__._report = classify_all_releases(releases, index, matcher)

    @pytest.fixture(autouse=True)
    def _store_attrs(self):
        self.db_url = self.__class__._db_url
        self.report = self.__class__._report

    def test_prune_deletes_releases(self) -> None:
        """Pruned release IDs are actually deleted from the release table."""
        if not self.report.prune_ids:
            pytest.skip("No releases classified as PRUNE")

        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            id_list = list(self.report.prune_ids)
            cur.execute("DELETE FROM release WHERE id = ANY(%s::integer[])", (id_list,))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM release WHERE id = ANY(%s::integer[])",
                (id_list,),
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_fk_cascade_cleans_child_tables(self) -> None:
        """Deleting releases cascades to child tables (verified after prune above)."""
        if not self.report.prune_ids:
            pytest.skip("No releases classified as PRUNE")

        prune_id = next(iter(self.report.prune_ids))
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            # After prune_deletes_releases ran, child rows should be gone too
            cur.execute(
                "SELECT count(*) FROM release_artist WHERE release_id = %s",
                (prune_id,),
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_keep_releases_survive_prune(self) -> None:
        """KEEP releases are not affected by the prune operation."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            keep_list = list(self.report.keep_ids)
            cur.execute(
                "SELECT count(*) FROM release WHERE id = ANY(%s::integer[])",
                (keep_list,),
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == len(self.report.keep_ids)


def _set_up_tables_for_async(db_url: str) -> None:
    """Set up schema and insert test data for async function tests."""
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        cur.execute("DROP TABLE IF EXISTS release_label CASCADE")
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        cur.execute("INSERT INTO release (id, title) VALUES (101, 'Confield')")
        cur.execute("INSERT INTO release (id, title) VALUES (102, 'DOGA')")
        cur.execute("INSERT INTO release (id, title) VALUES (103, 'Moon Pix')")
        cur.execute(
            "INSERT INTO release_artist (release_id, artist_name, extra) "
            "VALUES (101, 'Autechre', 0)"
        )
        cur.execute(
            "INSERT INTO release_artist (release_id, artist_name, extra) "
            "VALUES (102, 'Juana Molina', 0)"
        )
        cur.execute(
            "INSERT INTO release_artist (release_id, artist_name, extra) "
            "VALUES (103, 'Cat Power', 0)"
        )
        cur.execute("INSERT INTO release_label (release_id, label_name) VALUES (101, 'Warp')")
        cur.execute("INSERT INTO release_label (release_id, label_name) VALUES (102, 'Sonamos')")
        cur.execute(
            "INSERT INTO release_track (release_id, sequence, title) "
            "VALUES (101, 1, 'VI Scose Poise')"
        )
        cur.execute(
            "INSERT INTO release_track (release_id, sequence, title) VALUES (102, 1, 'Cosoco')"
        )
        cur.execute(
            "INSERT INTO release_track_artist (release_id, track_sequence, artist_name) "
            "VALUES (101, 1, 'Autechre')"
        )
        cur.execute("INSERT INTO cache_metadata (release_id, source) VALUES (101, 'bulk_import')")
        cur.execute("INSERT INTO cache_metadata (release_id, source) VALUES (102, 'bulk_import')")
        cur.execute("INSERT INTO cache_metadata (release_id, source) VALUES (103, 'bulk_import')")
    conn.close()


class TestGetTableSizes:
    """Verify get_table_sizes returns row counts and sizes for each release table."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        _set_up_tables_for_async(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_returns_all_tables(self) -> None:
        """get_table_sizes returns entries for all RELEASE_TABLES."""

        async def _run():
            conn = await asyncpg.connect(self.db_url)
            try:
                return await get_table_sizes(conn)
            finally:
                await conn.close()

        sizes = asyncio.run(_run())
        expected_tables = {
            "release",
            "release_artist",
            "release_label",
            "release_genre",
            "release_style",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        }
        assert set(sizes.keys()) == expected_tables

    def test_release_row_count(self) -> None:
        """release table has 3 rows."""

        async def _run():
            conn = await asyncpg.connect(self.db_url)
            try:
                return await get_table_sizes(conn)
            finally:
                await conn.close()

        sizes = asyncio.run(_run())
        row_count, size_bytes = sizes["release"]
        assert row_count == 3
        assert size_bytes > 0

    def test_track_artist_row_count(self) -> None:
        """release_track_artist table has 1 row from test data."""

        async def _run():
            conn = await asyncpg.connect(self.db_url)
            try:
                return await get_table_sizes(conn)
            finally:
                await conn.close()

        sizes = asyncio.run(_run())
        row_count, _ = sizes["release_track_artist"]
        assert row_count == 1


class TestCountRowsToDelete:
    """Verify count_rows_to_delete counts rows matching given release IDs."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        _set_up_tables_for_async(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_counts_for_single_release(self) -> None:
        """Counts rows to delete for a single release ID."""

        async def _run():
            conn = await asyncpg.connect(self.db_url)
            try:
                return await count_rows_to_delete(conn, {101})
            finally:
                await conn.close()

        counts = asyncio.run(_run())
        assert counts["release"] == 1
        assert counts["release_artist"] == 1
        assert counts["release_label"] == 1
        assert counts["release_track"] == 1
        assert counts["release_track_artist"] == 1
        assert counts["cache_metadata"] == 1

    def test_counts_for_multiple_releases(self) -> None:
        """Counts rows to delete for multiple release IDs."""

        async def _run():
            conn = await asyncpg.connect(self.db_url)
            try:
                return await count_rows_to_delete(conn, {101, 102})
            finally:
                await conn.close()

        counts = asyncio.run(_run())
        assert counts["release"] == 2
        assert counts["release_artist"] == 2

    def test_empty_release_set(self) -> None:
        """Empty release set returns zero counts."""

        async def _run():
            conn = await asyncpg.connect(self.db_url)
            try:
                return await count_rows_to_delete(conn, set())
            finally:
                await conn.close()

        counts = asyncio.run(_run())
        for table_count in counts.values():
            assert table_count == 0

    def test_nonexistent_release_id(self) -> None:
        """Non-existent release ID returns zero counts."""

        async def _run():
            conn = await asyncpg.connect(self.db_url)
            try:
                return await count_rows_to_delete(conn, {99999})
            finally:
                await conn.close()

        counts = asyncio.run(_run())
        assert counts["release"] == 0


class TestPruneReleases:
    """Verify prune_releases deletes releases and child rows via CASCADE."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        _set_up_tables_for_async(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_deletes_specified_releases_and_cascades(self) -> None:
        """prune_releases deletes the specified release IDs and cascades to children."""

        async def _run():
            conn = await asyncpg.connect(self.db_url)
            try:
                return await prune_releases(conn, {101})
            finally:
                await conn.close()

        result = asyncio.run(_run())
        assert result["release"] == 1

        # Verify release 101 is gone, and cascades cleaned children
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 101")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM release_artist WHERE release_id = 101")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM release_track WHERE release_id = 101")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM cache_metadata WHERE release_id = 101")
            assert cur.fetchone()[0] == 0
        conn.close()

    def test_undeleted_releases_survive(self) -> None:
        """Releases not in the prune set are not affected."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id IN (102, 103)")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 2

    def test_empty_set_deletes_nothing(self) -> None:
        """Empty release set returns zero count and deletes nothing."""

        async def _run():
            conn = await asyncpg.connect(self.db_url)
            try:
                return await prune_releases(conn, set())
            finally:
                await conn.close()

        result = asyncio.run(_run())
        assert result["release"] == 0
