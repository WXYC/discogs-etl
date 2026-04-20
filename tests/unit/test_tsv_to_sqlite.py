"""Unit tests for scripts/tsv_to_sqlite.py.

Each TSV row has 10 tab-separated fields matching the MySQL SELECT output:
id, title, artist, call_letters, artist_call_number, release_call_number,
genre, format, alternate_artist_name, album_artist.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

# Load tsv_to_sqlite module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "tsv_to_sqlite.py"
_spec = importlib.util.spec_from_file_location("tsv_to_sqlite", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

tsv_to_sqlite = _mod.tsv_to_sqlite


def _make_tsv(rows: list[list[str]]) -> str:
    """Build a TSV string from a list of field lists."""
    return "\n".join("\t".join(r) for r in rows) + "\n"


class TestTsvToSqlite:
    """Tests for the tsv_to_sqlite function."""

    def test_basic_export(self, tmp_path: Path) -> None:
        """3-row TSV produces a library table with 3 rows and correct data."""
        tsv = _make_tsv(
            [
                ["1", "Aluminum Tunes", "Stereolab", "ST", "100", "1", "Rock", "CD", "\\N", "\\N"],
                ["2", "DOGA", "Juana Molina", "MO", "200", "2", "Rock", "LP", "\\N", "\\N"],
                ["3", "Confield", "Autechre", "AU", "300", "3", "Electronic", "CD", "\\N", "\\N"],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, title, artist, genre FROM library ORDER BY id").fetchall()
        conn.close()

        assert len(rows) == 3
        assert rows[0] == (1, "Aluminum Tunes", "Stereolab", "Rock")
        assert rows[1] == (2, "DOGA", "Juana Molina", "Rock")
        assert rows[2] == (3, "Confield", "Autechre", "Electronic")

    def test_null_handling(self, tmp_path: Path) -> None:
        """TSV with \\N values are stored as Python None (SQL NULL) in SQLite."""
        tsv = _make_tsv(
            [
                [
                    "1",
                    "Aluminum Tunes",
                    "Stereolab",
                    "ST",
                    "100",
                    "\\N",
                    "Rock",
                    "CD",
                    "\\N",
                    "\\N",
                ],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT release_call_number, alternate_artist_name FROM library WHERE id = 1"
        ).fetchone()
        conn.close()

        assert row[0] is None
        assert row[1] is None

    def test_ten_column_validation(self, tmp_path: Path) -> None:
        """Rows with != 10 fields are skipped; valid rows are still imported."""
        tsv = (
            "1\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\n"
            "bad\trow\twith\ttoo\tfew\n"
            "2\tDOGA\tJuana Molina\tMO\t200\t2\tRock\tLP\t\\N\t\\N\n"
            "3\textra\tfields\there\t1\t2\t3\t4\t5\t6\t7\t8\n"
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        count = tsv_to_sqlite(str(tsv_file), str(db_path))

        assert count == 2

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id FROM library ORDER BY id").fetchall()
        conn.close()
        assert [r[0] for r in rows] == [1, 2]

    def test_fts5_index_created(self, tmp_path: Path) -> None:
        """After import, FTS MATCH queries work against artist and title."""
        tsv = _make_tsv(
            [
                ["1", "Aluminum Tunes", "Stereolab", "ST", "100", "1", "Rock", "CD", "\\N", "\\N"],
                ["2", "DOGA", "Juana Molina", "MO", "200", "2", "Rock", "LP", "\\N", "\\N"],
                ["3", "Confield", "Autechre", "AU", "300", "3", "Electronic", "CD", "\\N", "\\N"],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        # FTS search by artist
        hits = conn.execute(
            "SELECT rowid FROM library_fts WHERE library_fts MATCH 'Autechre'"
        ).fetchall()
        assert len(hits) == 1
        assert hits[0][0] == 3

        # FTS search by title
        hits = conn.execute(
            "SELECT rowid FROM library_fts WHERE library_fts MATCH 'Aluminum'"
        ).fetchall()
        assert len(hits) == 1
        assert hits[0][0] == 1
        conn.close()

    def test_indexes_created(self, tmp_path: Path) -> None:
        """idx_artist, idx_title, and idx_alternate_artist indexes exist."""
        tsv = _make_tsv(
            [
                ["1", "Aluminum Tunes", "Stereolab", "ST", "100", "1", "Rock", "CD", "\\N", "\\N"],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'library'"
            ).fetchall()
        }
        conn.close()

        assert "idx_artist" in indexes
        assert "idx_title" in indexes
        assert "idx_alternate_artist" in indexes
        assert "idx_album_artist" in indexes

    def test_empty_tsv_creates_schema(self, tmp_path: Path) -> None:
        """An empty TSV creates the schema but contains 0 rows."""
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text("", encoding="utf-8")
        db_path = tmp_path / "library.db"

        count = tsv_to_sqlite(str(tsv_file), str(db_path))

        assert count == 0

        conn = sqlite3.connect(str(db_path))
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'shadow')"
            ).fetchall()
        }
        row_count = conn.execute("SELECT COUNT(*) FROM library").fetchone()[0]
        conn.close()

        assert "library" in tables
        assert row_count == 0

    def test_unicode_data(self, tmp_path: Path) -> None:
        """Unicode characters (accents, non-Latin scripts) round-trip correctly."""
        tsv = _make_tsv(
            [
                ["1", "DOGA", "Juana Molina", "MO", "200", "2", "Rock", "LP", "\\N", "\\N"],
                [
                    "2",
                    "Pequena Vertigem de Amor",
                    "Sessa",
                    "SE",
                    "300",
                    "1",
                    "Latin",
                    "LP",
                    "\\N",
                    "\\N",
                ],
                [
                    "3",
                    "( )",
                    "Sigur R\u00f3s",
                    "SI",
                    "400",
                    "1",
                    "Rock",
                    "CD",
                    "Sigur R\u00f3s",
                    "\\N",
                ],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT artist, alternate_artist_name FROM library ORDER BY id"
        ).fetchall()
        conn.close()

        assert rows[0] == ("Juana Molina", None)
        assert rows[1] == ("Sessa", None)
        assert rows[2] == ("Sigur R\u00f3s", "Sigur R\u00f3s")

    def test_returns_row_count(self, tmp_path: Path) -> None:
        """Return value matches the number of rows inserted."""
        tsv = _make_tsv(
            [
                ["1", "Aluminum Tunes", "Stereolab", "ST", "100", "1", "Rock", "CD", "\\N", "\\N"],
                ["2", "DOGA", "Juana Molina", "MO", "200", "2", "Rock", "LP", "\\N", "\\N"],
                ["3", "Confield", "Autechre", "AU", "300", "3", "Electronic", "CD", "\\N", "\\N"],
                [
                    "4",
                    "Pequena Vertigem de Amor",
                    "Sessa",
                    "SE",
                    "400",
                    "1",
                    "Latin",
                    "LP",
                    "\\N",
                    "\\N",
                ],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        count = tsv_to_sqlite(str(tsv_file), str(db_path))

        assert count == 4

    def test_malformed_row_logged(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        """Wrong column count produces a WARNING on stderr."""
        tsv = "1\tAluminum Tunes\tStereolab\tST\t100\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        captured = capsys.readouterr()
        assert "WARNING" in captured.err
        assert "5 fields" in captured.err

    def test_cli_invocation(self, tmp_path: Path) -> None:
        """Running as a subprocess produces a valid SQLite database."""
        tsv = _make_tsv(
            [
                ["1", "Aluminum Tunes", "Stereolab", "ST", "100", "1", "Rock", "CD", "\\N", "\\N"],
                ["2", "DOGA", "Juana Molina", "MO", "200", "2", "Rock", "LP", "\\N", "\\N"],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        result = subprocess.run(
            [sys.executable, str(_SCRIPT_PATH), str(tsv_file), str(db_path)],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "Exported 2 rows" in result.stdout

        conn = sqlite3.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM library").fetchone()[0]
        conn.close()
        assert count == 2

    def test_tab_in_field_value(self, tmp_path: Path) -> None:
        r"""MySQL escapes literal tabs in fields as \t; they should be unescaped."""
        # MySQL -B -N escapes real tabs inside data as the two-char sequence \t.
        # Our code splits on real tabs (\t), so literal \t in the data arrives as
        # the two characters backslash-t, which are preserved as-is in the field.
        tsv = "1\tAluminum\\tTunes\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        title = conn.execute("SELECT title FROM library WHERE id = 1").fetchone()[0]
        conn.close()

        # The escaped sequence \t is stored literally (two chars: backslash + t)
        assert title == "Aluminum\\tTunes"

    def test_newline_in_field_value(self, tmp_path: Path) -> None:
        r"""MySQL escapes literal newlines in fields as \n; they should be preserved."""
        # Similar to tabs: MySQL -B outputs literal \n (two chars) for embedded newlines.
        # Since we split on real newlines, the two-char sequence stays intact.
        tsv = "1\tNotes\\nMore notes\tAutechre\tAU\t300\t3\tElectronic\tCD\t\\N\t\\N\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        title = conn.execute("SELECT title FROM library WHERE id = 1").fetchone()[0]
        conn.close()

        # The escaped sequence \n is stored literally (two chars: backslash + n)
        assert title == "Notes\\nMore notes"
