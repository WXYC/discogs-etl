"""Unit tests for scripts/export_to_sqlite.py."""

import importlib.util
import sqlite3
from pathlib import Path

import pytest

# Load export_to_sqlite module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "export_to_sqlite.py"
_spec = importlib.util.spec_from_file_location("export_to_sqlite", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
_do_export = _mod._do_export
format_size = _mod.format_size
_OUTPUT_PATH_ATTR = "OUTPUT_PATH"


class TestDoExport:
    """Tests for the _do_export function."""

    @pytest.fixture(autouse=True)
    def use_tmp_output(self, tmp_path, monkeypatch):
        """Redirect OUTPUT_PATH to a temp directory."""
        self.output_path = tmp_path / "library.db"
        monkeypatch.setattr(_mod, _OUTPUT_PATH_ATTR, self.output_path)

    def test_exports_alternate_artist_name(self):
        """Rows with alternate_artist_name should be exported to SQLite."""
        rows = [
            {
                "id": "1",
                "title": "Drum n Bass for Papa",
                "artist": "Luke Vibert",
                "call_letters": "V",
                "artist_call_number": "15",
                "release_call_number": "1",
                "genre": "Electronic",
                "format": "CD",
                "alternate_artist_name": "Plug",
            },
        ]
        _do_export(rows)

        conn = sqlite3.connect(self.output_path)
        cursor = conn.execute("SELECT alternate_artist_name FROM library WHERE id = 1")
        row = cursor.fetchone()
        conn.close()

        assert row is not None
        assert row[0] == "Plug"

    def test_exports_null_alternate_artist_name(self):
        """Rows with NULL alternate_artist_name should store NULL in SQLite."""
        rows = [
            {
                "id": "1",
                "title": "Big Soup",
                "artist": "Luke Vibert",
                "call_letters": "V",
                "artist_call_number": "15",
                "release_call_number": "2",
                "genre": "Electronic",
                "format": "CD",
                "alternate_artist_name": None,
            },
        ]
        _do_export(rows)

        conn = sqlite3.connect(self.output_path)
        cursor = conn.execute("SELECT alternate_artist_name FROM library WHERE id = 1")
        row = cursor.fetchone()
        conn.close()

        assert row is not None
        assert row[0] is None

    def test_fts_indexes_alternate_artist_name(self):
        """The FTS5 index should include alternate_artist_name for full-text search."""
        rows = [
            {
                "id": "1",
                "title": "Drum n Bass for Papa",
                "artist": "Luke Vibert",
                "call_letters": "V",
                "artist_call_number": "15",
                "release_call_number": "1",
                "genre": "Electronic",
                "format": "CD",
                "alternate_artist_name": "Plug",
            },
        ]
        _do_export(rows)

        conn = sqlite3.connect(self.output_path)
        # Search FTS for "Plug" - should match via alternate_artist_name
        cursor = conn.execute("""
            SELECT l.id, l.artist, l.alternate_artist_name
            FROM library l
            JOIN library_fts fts ON l.id = fts.rowid
            WHERE library_fts MATCH 'Plug'
        """)
        results = cursor.fetchall()
        conn.close()

        assert len(results) == 1
        assert results[0][2] == "Plug"

    def test_alternate_artist_index_created(self):
        """An index should be created on the alternate_artist_name column."""
        rows = [
            {
                "id": "1",
                "title": "Album",
                "artist": "Artist",
                "call_letters": "A",
                "artist_call_number": "1",
                "release_call_number": "1",
                "genre": "Rock",
                "format": "CD",
                "alternate_artist_name": None,
            },
        ]
        _do_export(rows)

        conn = sqlite3.connect(self.output_path)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_alternate_artist'"
        )
        index = cursor.fetchone()
        conn.close()

        assert index is not None

    def test_mixed_rows_with_and_without_alternate(self):
        """Mix of rows with and without alternate_artist_name should export correctly."""
        rows: list[dict] = [
            {
                "id": "1",
                "title": "Drum n Bass for Papa",
                "artist": "Luke Vibert",
                "call_letters": "V",
                "artist_call_number": "15",
                "release_call_number": "1",
                "genre": "Electronic",
                "format": "CD",
                "alternate_artist_name": "Plug",
            },
            {
                "id": "2",
                "title": "Big Soup",
                "artist": "Luke Vibert",
                "call_letters": "V",
                "artist_call_number": "15",
                "release_call_number": "2",
                "genre": "Electronic",
                "format": "CD",
                "alternate_artist_name": None,
            },
        ]
        _do_export(rows)

        conn = sqlite3.connect(self.output_path)
        cursor = conn.execute("SELECT id, alternate_artist_name FROM library ORDER BY id")
        results = cursor.fetchall()
        conn.close()

        assert len(results) == 2
        assert results[0] == (1, "Plug")
        assert results[1] == (2, None)

    def test_creates_correct_tables_and_row_count(self):
        """Verify the exported database has the correct tables, FTS index, and row count."""
        rows = [
            {
                "id": "1",
                "title": "DOGA",
                "artist": "Juana Molina",
                "call_letters": "M",
                "artist_call_number": "42",
                "release_call_number": "1",
                "genre": "Rock",
                "format": "LP",
                "alternate_artist_name": None,
            },
            {
                "id": "2",
                "title": "Aluminum Tunes",
                "artist": "Stereolab",
                "call_letters": "S",
                "artist_call_number": "88",
                "release_call_number": "1",
                "genre": "Rock",
                "format": "CD",
                "alternate_artist_name": None,
            },
            {
                "id": "3",
                "title": "Moon Pix",
                "artist": "Cat Power",
                "call_letters": "C",
                "artist_call_number": "7",
                "release_call_number": "1",
                "genre": "Rock",
                "format": "LP",
                "alternate_artist_name": None,
            },
        ]
        _do_export(rows)

        conn = sqlite3.connect(self.output_path)

        # Verify table exists
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='library'"
        )
        assert cursor.fetchone() is not None

        # Verify FTS table exists
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='library_fts'"
        )
        assert cursor.fetchone() is not None

        # Verify row count
        cursor = conn.execute("SELECT COUNT(*) FROM library")
        assert cursor.fetchone()[0] == 3

        # Verify FTS works
        cursor = conn.execute("""
            SELECT l.id FROM library l
            JOIN library_fts fts ON l.id = fts.rowid
            WHERE library_fts MATCH 'Stereolab'
        """)
        results = cursor.fetchall()
        assert len(results) == 1
        assert results[0][0] == 2

        conn.close()


class TestDoExportCompilationTrackArtists:
    """Tests for compilation_track_artist table in _do_export."""

    @pytest.fixture(autouse=True)
    def use_tmp_output(self, tmp_path, monkeypatch):
        """Redirect OUTPUT_PATH to a temp directory."""
        self.output_path = tmp_path / "library.db"
        monkeypatch.setattr(_mod, _OUTPUT_PATH_ATTR, self.output_path)

    def _make_rows(self):
        return [
            {
                "id": "1",
                "title": "Vintage Palmwine",
                "artist": "Various Artists",
                "call_letters": "Z-X",
                "artist_call_number": "1",
                "release_call_number": "1",
                "genre": "World",
                "format": "CD",
                "alternate_artist_name": None,
            },
        ]

    def _make_track_artists(self):
        return [
            {"library_release_id": 1, "artist_name": "Koo Nimo", "track_title": "odo akosomo"},
            {"library_release_id": 1, "artist_name": "T.O. Jazz", "track_title": "Yaa Amponsah"},
        ]

    def test_creates_compilation_track_artist_table(self):
        """The compilation_track_artist table should be created when track artists are provided."""
        _do_export(self._make_rows(), self._make_track_artists())

        conn = sqlite3.connect(self.output_path)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='compilation_track_artist'"
        )
        assert cursor.fetchone() is not None
        conn.close()

    def test_inserts_track_artist_rows(self):
        """Track artist rows should be inserted into the compilation_track_artist table."""
        _do_export(self._make_rows(), self._make_track_artists())

        conn = sqlite3.connect(self.output_path)
        cursor = conn.execute("SELECT library_release_id, artist_name, track_title FROM compilation_track_artist ORDER BY artist_name")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 2
        assert rows[0] == (1, "Koo Nimo", "odo akosomo")
        assert rows[1] == (1, "T.O. Jazz", "Yaa Amponsah")

    def test_creates_indexes_on_compilation_track_artist(self):
        """Indexes should be created on library_release_id and artist_name columns."""
        _do_export(self._make_rows(), self._make_track_artists())

        conn = sqlite3.connect(self.output_path)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_cta_%'")
        indexes = {row[0] for row in cursor.fetchall()}
        conn.close()

        assert "idx_cta_release" in indexes
        assert "idx_cta_artist" in indexes

    def test_no_compilation_table_when_empty(self):
        """When no track artists are provided, the table should not be created."""
        _do_export(self._make_rows())

        conn = sqlite3.connect(self.output_path)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='compilation_track_artist'"
        )
        assert cursor.fetchone() is None
        conn.close()

    def test_backward_compatible_without_track_artists(self):
        """Calling _do_export without track artists should work as before."""
        _do_export(self._make_rows())

        conn = sqlite3.connect(self.output_path)
        cursor = conn.execute("SELECT COUNT(*) FROM library")
        assert cursor.fetchone()[0] == 1
        conn.close()


class TestFormatSize:
    """Test human-readable size formatting."""

    @pytest.mark.parametrize(
        "size_bytes, expected",
        [
            (0, "0.0 B"),
            (1023, "1023.0 B"),
            (1024, "1.0 KB"),
            (1048576, "1.0 MB"),
            (1073741824, "1.0 GB"),
            (1099511627776, "1.0 TB"),
        ],
        ids=["zero", "bytes", "kilobytes", "megabytes", "gigabytes", "terabytes"],
    )
    def test_format_size(self, size_bytes: int, expected: str) -> None:
        assert format_size(size_bytes) == expected
