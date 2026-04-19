"""Integration tests for scripts/tsv_to_sqlite.py."""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest

# Load tsv_to_sqlite module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "tsv_to_sqlite.py"
_spec = importlib.util.spec_from_file_location("tsv_to_sqlite", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

tsv_to_sqlite = _mod.tsv_to_sqlite


@pytest.mark.integration
class TestTsvToSqliteIntegration:
    """Integration tests using realistic MySQL output."""

    def test_realistic_mysql_output(self, tmp_path: Path) -> None:
        """TSV mimicking real MySQL -B -N output imports correctly and FTS search works."""
        # Realistic MySQL batch-mode output with mix of NULLs, Unicode, and varied genres
        lines = [
            "10001\tAluminum Tunes\tStereolab\tST\t1234\t1\tRock\tCD\t\\N",
            "10002\tDOGA\tJuana Molina\tMO\t5678\t2\tRock\tLP\t\\N",
            "10003\tConfield\tAutechre\tAU\t9012\t3\tElectronic\tCD\t\\N",
            "10004\tMoon Pix\tCat Power\tCA\t3456\t1\tRock\tLP\tCharlyn Marie Marshall",
            "10005\tPequena Vertigem de Amor\tSessa\tSE\t7890\t1\tLatin\tLP\t\\N",
            "10006\tDuke Ellington & John Coltrane\tDuke Ellington\tEL\t2345\t1\tJazz\tLP\tEdward Kennedy Ellington",
            "10007\tOn Your Own Love Again\tJessica Pratt\tPR\t6789\t1\tRock\tLP\t\\N",
            "10008\tEdits\tChuquimamani-Condori\tCH\t\\N\t\\N\tElectronic\tCD\t\\N",
        ]
        tsv_file = tmp_path / "mysql_output.tsv"
        tsv_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        db_path = tmp_path / "library.db"

        count = tsv_to_sqlite(str(tsv_file), str(db_path))
        assert count == 8

        conn = sqlite3.connect(str(db_path))

        # FTS search for artist
        hits = conn.execute(
            "SELECT l.id, l.title FROM library l "
            "JOIN library_fts ON library_fts.rowid = l.id "
            "WHERE library_fts MATCH 'Stereolab'"
        ).fetchall()
        assert len(hits) == 1
        assert hits[0] == (10001, "Aluminum Tunes")

        # FTS search for title
        hits = conn.execute(
            "SELECT l.id, l.artist FROM library l "
            "JOIN library_fts ON library_fts.rowid = l.id "
            "WHERE library_fts MATCH 'Confield'"
        ).fetchall()
        assert len(hits) == 1
        assert hits[0] == (10002, "Juana Molina") or hits[0] == (10003, "Autechre")
        assert hits[0] == (10003, "Autechre")

        # FTS search for alternate artist name
        hits = conn.execute(
            "SELECT l.id, l.artist FROM library l "
            "JOIN library_fts ON library_fts.rowid = l.id "
            "WHERE library_fts MATCH 'Marshall'"
        ).fetchall()
        assert len(hits) == 1
        assert hits[0] == (10004, "Cat Power")

        # Verify NULLs handled correctly
        row = conn.execute(
            "SELECT artist_call_number, release_call_number FROM library WHERE id = 10008"
        ).fetchone()
        assert row[0] is None
        assert row[1] is None

        conn.close()

    def test_schema_matches_wxyc_catalog(self, tmp_path: Path) -> None:
        """PRAGMA table_info columns match the expected WXYC library schema."""
        tsv_file = tmp_path / "empty.tsv"
        tsv_file.write_text("", encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        columns = conn.execute("PRAGMA table_info(library)").fetchall()
        conn.close()

        expected_columns = [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "title", "TEXT", 0, None, 0),
            (2, "artist", "TEXT", 0, None, 0),
            (3, "call_letters", "TEXT", 0, None, 0),
            (4, "artist_call_number", "INTEGER", 0, None, 0),
            (5, "release_call_number", "INTEGER", 0, None, 0),
            (6, "genre", "TEXT", 0, None, 0),
            (7, "format", "TEXT", 0, None, 0),
            (8, "alternate_artist_name", "TEXT", 0, None, 0),
        ]

        assert columns == expected_columns
