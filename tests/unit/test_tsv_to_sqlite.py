"""Unit tests for scripts/tsv_to_sqlite.py.

Each TSV row has 11 tab-separated fields matching the MySQL SELECT output:
id, title, artist, call_letters, artist_call_number, release_call_number,
genre, format, alternate_artist_name, album_artist, cross_reference_names.
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
                [
                    "1",
                    "Aluminum Tunes",
                    "Stereolab",
                    "ST",
                    "100",
                    "1",
                    "Rock",
                    "CD",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
                [
                    "2",
                    "DOGA",
                    "Juana Molina",
                    "MO",
                    "200",
                    "2",
                    "Rock",
                    "LP",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
                [
                    "3",
                    "Confield",
                    "Autechre",
                    "AU",
                    "300",
                    "3",
                    "Electronic",
                    "CD",
                    "\\N",
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

    def test_empty_fields_are_stored_as_empty_string_not_null_or_literal_null(
        self, tmp_path: Path
    ) -> None:
        """Empty TSV fields -- the shape sync-library.sh now emits for a real
        SQL NULL after the IFNULL(<col>, '') wrap -- land as '' in SQLite, not
        None and not the literal string 'NULL'.

        This pins the actual post-fix production contract end to end: the
        wrap turns a genuine NULL into an empty field on the wire (proven at
        the SQL layer in tests/e2e/test_sync_library_e2e.py), and this parser
        must carry that empty field through as ''. It complements
        test_null_handling above (which still exercises the \\N sentinel that
        tsv_to_sqlite.py keeps mapping to SQL NULL for other callers, but
        which the live -B -N sync no longer emits).

        album_artist and cross_reference_names are the two columns that held
        the literal string 'NULL' on ~64k prod rows; the assertion that
        MATCH 'null' returns nothing is the exact regression the fix closes.
        """
        tsv = _make_tsv(
            [
                # album_artist (col 10) and cross_reference_names (col 11) are
                # empty -- the post-IFNULL shape for a real NULL. The empty
                # trailing field must not break the 11-column count.
                ["1", "Aluminum Tunes", "Stereolab", "ST", "100", "1", "Rock", "CD", "", "", ""],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        count = tsv_to_sqlite(str(tsv_file), str(db_path))

        # An empty trailing field is preserved by rstrip("\n").split("\t"),
        # so the row is a valid 11-field row and is imported (not skipped).
        assert count == 1

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT alternate_artist_name, album_artist, cross_reference_names"
            " FROM library WHERE id = 1"
        ).fetchone()
        # The empty fields are the empty string, never None or 'NULL'.
        assert row == ("", "", "")
        # The literal-'NULL'-string bug: an empty album_artist must not put a
        # 'null' token into the FTS index.
        null_hits = conn.execute(
            "SELECT rowid FROM library_fts WHERE library_fts MATCH 'null'"
        ).fetchall()
        conn.close()
        assert null_hits == []

    def test_eleven_column_validation(self, tmp_path: Path) -> None:
        """Rows with != 11 fields are skipped; valid rows are still imported."""
        tsv = (
            "1\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\t\\N\n"
            "bad\trow\twith\ttoo\tfew\n"
            "2\tDOGA\tJuana Molina\tMO\t200\t2\tRock\tLP\t\\N\t\\N\t\\N\n"
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
                [
                    "1",
                    "Aluminum Tunes",
                    "Stereolab",
                    "ST",
                    "100",
                    "1",
                    "Rock",
                    "CD",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
                [
                    "2",
                    "DOGA",
                    "Juana Molina",
                    "MO",
                    "200",
                    "2",
                    "Rock",
                    "LP",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
                [
                    "3",
                    "Confield",
                    "Autechre",
                    "AU",
                    "300",
                    "3",
                    "Electronic",
                    "CD",
                    "\\N",
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
                [
                    "1",
                    "Aluminum Tunes",
                    "Stereolab",
                    "ST",
                    "100",
                    "1",
                    "Rock",
                    "CD",
                    "\\N",
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
                [
                    "1",
                    "DOGA",
                    "Juana Molina",
                    "MO",
                    "200",
                    "2",
                    "Rock",
                    "LP",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
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
                [
                    "1",
                    "Aluminum Tunes",
                    "Stereolab",
                    "ST",
                    "100",
                    "1",
                    "Rock",
                    "CD",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
                [
                    "2",
                    "DOGA",
                    "Juana Molina",
                    "MO",
                    "200",
                    "2",
                    "Rock",
                    "LP",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
                [
                    "3",
                    "Confield",
                    "Autechre",
                    "AU",
                    "300",
                    "3",
                    "Electronic",
                    "CD",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
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
                [
                    "1",
                    "Aluminum Tunes",
                    "Stereolab",
                    "ST",
                    "100",
                    "1",
                    "Rock",
                    "CD",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
                [
                    "2",
                    "DOGA",
                    "Juana Molina",
                    "MO",
                    "200",
                    "2",
                    "Rock",
                    "LP",
                    "\\N",
                    "\\N",
                    "\\N",
                ],
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

    def test_fts5_tokenizer_indexes_emoji(self, tmp_path: Path) -> None:
        """FTS5 tokenizer must index supplementary-plane emoji rows.

        The default unicode61 tokenizer drops 4-byte emoji (Symbol category)
        and U+200D ZWJ; the production schema overrides it with
        ``categories 'L* N* Co S*' tokenchars '\\u200d'`` so bare-emoji and
        ZWJ-grapheme artist rows can be located by exact-string FTS MATCH.
        See WXYC/discogs-etl#161 and WXYC/library-metadata-lookup#251.
        """
        # Bare emoji (U+1F44B WAVING HAND SIGN) -- fails the default unicode61.
        # ZWJ family (man + ZWJ + woman + ZWJ + girl + ZWJ + boy) -- exercises
        # the explicit U+200D tokenchars opt-in. Each ZWJ is written as the
        # \\u200d escape so the source stays unambiguous in editors and diffs
        # that render zero-width characters invisibly.
        zwj = "\u200d"
        zwj_family = f"\U0001f468{zwj}\U0001f469{zwj}\U0001f467{zwj}\U0001f466"
        tsv = _make_tsv(
            [
                ["1", "Hello", "\U0001f44b", "EM", "100", "1", "Rock", "CD", "\\N", "\\N", "\\N"],
                ["2", "Family", zwj_family, "FA", "200", "2", "Rock", "CD", "\\N", "\\N", "\\N"],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        try:
            hits = conn.execute(
                "SELECT rowid FROM library_fts WHERE library_fts MATCH '\"\U0001f44b\"'"
            ).fetchall()
            assert hits == [(1,)], f"bare emoji row not findable via FTS MATCH: {hits!r}"

            hits = conn.execute(
                "SELECT rowid FROM library_fts WHERE library_fts MATCH ?",
                (f'"{zwj_family}"',),
            ).fetchall()
            assert hits == [(2,)], f"ZWJ-grapheme row not findable via FTS MATCH: {hits!r}"
        finally:
            conn.close()

    def test_cross_reference_names_column(self, tmp_path: Path) -> None:
        """The 11th TSV field populates cross_reference_names (WXYC/discogs-etl#334).

        Mirrors library.db row 57833: filed under the band name "Burning Star
        Core" with alternate_artist_name "C.S. Yeh". The WXYC catalog's
        LIBRARY_CODE_CROSS_REFERENCE table also links it to the personal name
        "C. Spencer Yeh", which the daily sync now carries through as a
        pipe-joined cross_reference_names value so LML can match a typed
        "C. Spencer Yeh" lookup against this row.
        """
        tsv = _make_tsv(
            [
                [
                    "57833",
                    '"In The Blink of an Eye" 7-inch',
                    "Burning Star Core",
                    "BU",
                    "110",
                    "6",
                    "Rock",
                    'vinyl - 7"',
                    "C.S. Yeh",
                    "\\N",
                    "C. Spencer Yeh",
                ],
            ]
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT artist, alternate_artist_name, cross_reference_names "
            "FROM library WHERE id = 57833"
        ).fetchone()
        conn.close()

        assert row == ("Burning Star Core", "C.S. Yeh", "C. Spencer Yeh")

    def test_cross_reference_names_null_when_absent(self, tmp_path: Path) -> None:
        """A row with no cross-reference (11th field is \\N) stores SQL NULL."""
        tsv = _make_tsv(
            [
                [
                    "1",
                    "Aluminum Tunes",
                    "Stereolab",
                    "ST",
                    "100",
                    "1",
                    "Rock",
                    "CD",
                    "\\N",
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
        row = conn.execute("SELECT cross_reference_names FROM library WHERE id = 1").fetchone()
        conn.close()

        assert row[0] is None

    def test_tab_in_field_value(self, tmp_path: Path) -> None:
        r"""MySQL escapes literal tabs in fields as \t; the parser unescapes it
        back to a real TAB byte (WXYC/discogs-etl#370)."""
        # MySQL -B -N (no --raw) escapes a real tab inside data as the two-char
        # sequence \t. We split on real tabs, so the two chars backslash-t stay
        # intact within the field -- then must be unescaped to a single TAB.
        tsv = "1\tAluminum\\tTunes\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\t\\N\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        title = conn.execute("SELECT title FROM library WHERE id = 1").fetchone()[0]
        conn.close()

        # Unescaped: a single real TAB byte, not the two-char sequence.
        assert title == "Aluminum\tTunes"

    def test_newline_in_field_value(self, tmp_path: Path) -> None:
        r"""MySQL escapes literal newlines in fields as \n; the parser unescapes
        it back to a real newline byte (WXYC/discogs-etl#370)."""
        # Similar to tabs: MySQL -B -N (no --raw) outputs literal \n (two chars)
        # for an embedded newline. We split on real newlines, so the two-char
        # sequence stays intact within the field -- then must be unescaped.
        tsv = "1\tNotes\\nMore notes\tAutechre\tAU\t300\t3\tElectronic\tCD\t\\N\t\\N\t\\N\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        title = conn.execute("SELECT title FROM library WHERE id = 1").fetchone()[0]
        conn.close()

        # Unescaped: a single real newline byte, not the two-char sequence.
        assert title == "Notes\nMore notes"

    def test_backslash_in_field_value(self, tmp_path: Path) -> None:
        r"""MySQL escapes a literal backslash in a field as \\; the parser
        unescapes it back to a single backslash byte."""
        # Source data holds one backslash character; mysql -B -N (no --raw)
        # escapes it to the two-char wire sequence \\.
        tsv = "1\tFoo\\\\Bar\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\t\\N\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        title = conn.execute("SELECT title FROM library WHERE id = 1").fetchone()[0]
        conn.close()

        assert title == "Foo\\Bar"

    def test_nul_in_field_value(self, tmp_path: Path) -> None:
        r"""MySQL escapes an embedded NUL byte as \0; the parser writes a real
        NUL byte. Measured at 0 rows on the 2026-07-19 prod snapshot -- this
        pins the tie-break decided in plans/346-parity-clean-definition.md
        rather than leaving \0 unhandled."""
        tsv = "1\tFoo\\0Bar\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\t\\N\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        title = conn.execute("SELECT title FROM library WHERE id = 1").fetchone()[0]
        conn.close()

        assert title == "Foo\x00Bar"

    def test_backslash_then_literal_t_is_not_corrupted_by_naive_replace(
        self, tmp_path: Path
    ) -> None:
        r"""Source data holding a literal backslash immediately followed by the
        letter 't' proves the unescape is a single left-to-right pass, not a
        sequence of str.replace calls.

        On the wire this is three characters: \\t -- an escaped backslash (the
        real backslash byte in the source), then an unescaped literal 't'. A
        naive `.replace("\\t", " ")` would match at offset 1 (the second
        backslash plus the 't') and corrupt the result to backslash+space; a
        single left-to-right scan consumes the \\ pair first and correctly
        yields backslash+t.
        """
        tsv = "1\t\\\\t\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\t\\N\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        title = conn.execute("SELECT title FROM library WHERE id = 1").fetchone()[0]
        conn.close()

        assert title == "\\t"
        assert title != "\t"  # not a real TAB -- the naive-replace failure mode

    def test_literal_backslash_n_survives_unescape_and_is_not_null(self, tmp_path: Path) -> None:
        r"""Source data literally spelled backslash-N must survive as the
        two-char string \N and must NOT collapse to SQL NULL.

        On the wire this is three characters: \\N -- an escaped backslash
        (the real backslash byte in the source), then an unescaped literal
        'N'. Unescaping before testing the \N NULL sentinel would turn this
        into the same two-char \N spelling as the sentinel and silently drop
        the value -- so the sentinel test must run against the raw
        (pre-unescape) field, per WXYC/discogs-etl#370.
        """
        tsv = "1\t\\\\N\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\t\\N\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        title = conn.execute("SELECT title FROM library WHERE id = 1").fetchone()[0]
        conn.close()

        assert title == "\\N"
        assert title is not None

    def test_cr_in_final_column_survives_intact(self, tmp_path: Path) -> None:
        r"""A bare CR in the final column (cross_reference_names) must survive
        intact rather than being read as a line terminator and silently
        truncating the value (WXYC/discogs-etl#373).

        Red before the ``newline="\n"`` fix: the default universal-newline
        ``open()`` treats the bare CR as a line terminator, so the first
        fragment still carries all 11 fields and is accepted with the value
        truncated at ``Cross``, while the orphaned tail (``Ref``) is dropped
        as malformed. The dropped tail does emit a WARNING, but it identifies
        neither the truncated row nor the truncation -- the corrupted value
        lands with no diagnostic attached to it, which is what makes this the
        dangerous half of the defect.
        """
        tsv = (
            "1\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\tCross\rRef\n"
            "2\tDOGA\tJuana Molina\tMO\t200\t2\tRock\tCD\t\\N\t\\N\t\\N\n"
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, cross_reference_names FROM library ORDER BY id").fetchall()
        conn.close()

        # Both rows land intact -- the CR-bearing final column is not
        # truncated, and the row following the bare CR is not mistaken for a
        # continuation of the first.
        assert rows == [(1, "Cross\rRef"), (2, None)]

    def test_cr_mid_row_no_longer_splits_row(self, tmp_path: Path) -> None:
        r"""A bare CR in a non-final column no longer terminates the line.

        Pre-fix this was the loud, already-handled case: both fragments fail
        the field-count check and the row is dropped with a WARNING. Post-fix
        the row is accepted whole, with the CR preserved inside the field
        (WXYC/discogs-etl#373) -- this is the intended behavior change noted
        in the ticket, not a regression.
        """
        tsv = "1\tAluminum Tunes\tStereo\rlab\tST\t100\t1\tRock\tCD\t\\N\t\\N\t\\N\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        row = conn.execute("SELECT id, artist FROM library WHERE id = 1").fetchone()
        conn.close()

        assert row == (1, "Stereo\rlab")

    def test_crlf_in_field_value_round_trips(self, tmp_path: Path) -> None:
        r"""A Windows-entered line break survives as a real CRLF *inside* the
        field, pinning the interaction between the ``newline="\n"`` fix and
        ``_unescape_mysql_field`` (WXYC/discogs-etl#373).

        This is the likeliest real origin of a CR in this data, and on the
        wire it is a two-part shape: ``mysql -B -N`` escapes the LF to the two
        characters backslash-``n`` but leaves the CR as a bare byte, so the
        field arrives as ``Cross`` + CR + ``\n`` + ``Ref``. Both halves have
        to be handled by different mechanisms for the value to reassemble --
        ``newline="\n"`` keeps the bare CR from ending the line, and the
        unescape turns the two-char sequence back into a real LF. Covered
        once here rather than twice: the unescape half lives in
        ``_parse_nullable_field``, which both parsers share.
        """
        tsv = "1\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\tCross\r\\nRef\n"
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        value = conn.execute("SELECT cross_reference_names FROM library WHERE id = 1").fetchone()[0]
        conn.close()

        # A real CRLF pair, not the escaped spelling and not a truncation.
        assert value == "Cross\r\nRef"

    def test_multiple_rows_still_parse_after_newline_fix(self, tmp_path: Path) -> None:
        r"""A normal, CR-free multi-row TSV parses unchanged under
        ``newline="\n"`` -- regression guard for WXYC/discogs-etl#373."""
        tsv = (
            "1\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\\N\t\\N\t\\N\n"
            "2\tDOGA\tJuana Molina\tMO\t200\t2\tRock\tCD\t\\N\t\\N\t\\N\n"
        )
        tsv_file = tmp_path / "input.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(tsv_file), str(db_path))

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, title, artist FROM library ORDER BY id").fetchall()
        conn.close()

        assert rows == [(1, "Aluminum Tunes", "Stereolab"), (2, "DOGA", "Juana Molina")]


class TestCtaEscaping:
    """Escaping tests for parse_compilation_track_tsv (WXYC/discogs-etl#370).

    The only route into that parser from this file is ``tsv_to_sqlite``'s
    optional ``cta_tsv_path`` argument (``scripts/tsv_to_sqlite.py:60``);
    every test above passes only the two positional (library) arguments and
    never reaches it. Re-runs the same four escapes plus both ordering
    hazards from ``TestTsvToSqlite`` above, but against ``artist_name`` --
    the CTA field with the ordering hazard that actually matters, since
    ``artist_name`` is NOT NULL (the NULL-guard in
    ``lib/library_db.py::parse_compilation_track_tsv``): a `\\N` collision
    there drops the whole row, not just one column.
    """

    def _library_tsv(self, tmp_path: Path, release_id: str = "1") -> Path:
        """A single valid library row -- tsv_to_sqlite always needs one,
        independent of what's under test in the CTA TSV."""
        tsv = _make_tsv(
            [
                [
                    release_id,
                    "Aluminum Tunes",
                    "Stereolab",
                    "ST",
                    "100",
                    "1",
                    "Rock",
                    "CD",
                    "\\N",
                    "\\N",
                    "\\N",
                ]
            ]
        )
        tsv_file = tmp_path / "library.tsv"
        tsv_file.write_text(tsv, encoding="utf-8")
        return tsv_file

    def _cta_tsv(self, tmp_path: Path, rows: list[list[str]]) -> Path:
        cta_file = tmp_path / "cta.tsv"
        cta_file.write_text(_make_tsv(rows), encoding="utf-8")
        return cta_file

    def test_cta_tab_in_artist_name(self, tmp_path: Path) -> None:
        r"""MySQL escapes a literal tab in artist_name as \t; the CTA parser
        unescapes it back to a real TAB byte."""
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(tmp_path, [["1", "Burning\\tStar", "Some Track"]])
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT library_release_id, artist_name FROM compilation_track_artist"
        ).fetchone()
        conn.close()

        assert row == (1, "Burning\tStar")

    def test_cta_newline_in_artist_name(self, tmp_path: Path) -> None:
        r"""MySQL escapes a literal newline in artist_name as \n; the CTA
        parser unescapes it back to a real newline byte."""
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(tmp_path, [["1", "Side A\\nSide B", "Some Track"]])
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        artist_name = conn.execute("SELECT artist_name FROM compilation_track_artist").fetchone()[0]
        conn.close()

        assert artist_name == "Side A\nSide B"

    def test_cta_backslash_in_artist_name(self, tmp_path: Path) -> None:
        r"""MySQL escapes a literal backslash in artist_name as \\; the CTA
        parser unescapes it back to a single backslash byte."""
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(tmp_path, [["1", "Stereo\\\\lab", "Some Track"]])
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        artist_name = conn.execute("SELECT artist_name FROM compilation_track_artist").fetchone()[0]
        conn.close()

        assert artist_name == "Stereo\\lab"

    def test_cta_nul_in_artist_name(self, tmp_path: Path) -> None:
        r"""MySQL escapes an embedded NUL byte in artist_name as \0; the CTA
        parser writes a real NUL byte."""
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(tmp_path, [["1", "Foo\\0Bar", "Some Track"]])
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        artist_name = conn.execute("SELECT artist_name FROM compilation_track_artist").fetchone()[0]
        conn.close()

        assert artist_name == "Foo\x00Bar"

    def test_cta_backslash_then_literal_t_is_not_corrupted_by_naive_replace(
        self, tmp_path: Path
    ) -> None:
        r"""Same single-left-to-right-pass proof as the library parser test of
        the same name, against artist_name: on the wire \\t is three chars
        (an escaped backslash, then a literal 't'); a naive
        `.replace("\\t", " ")` would corrupt it to backslash+space."""
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(tmp_path, [["1", "\\\\t", "Some Track"]])
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        artist_name = conn.execute("SELECT artist_name FROM compilation_track_artist").fetchone()[0]
        conn.close()

        assert artist_name == "\\t"
        assert artist_name != "\t"

    def test_cta_literal_backslash_n_artist_survives_row_not_dropped(self, tmp_path: Path) -> None:
        r"""artist_name is NOT NULL (the NULL-guard in
        lib/library_db.py::parse_compilation_track_tsv): a raw \N
        there drops the whole row with a WARNING. Source data literally
        spelled backslash-N must survive as the two-char string \N and must
        NOT be misread as the NULL sentinel and dropped -- the data-loss
        variant this ordering exists to prevent, per
        WXYC/discogs-etl#370. On the wire this is three chars: \\N (an
        escaped backslash, then a literal 'N').

        Also pins that the ordering fix leaves library_release_id's
        int()-cast -- which runs after the sentinel test in
        parse_compilation_track_tsv -- undisturbed.
        """
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(tmp_path, [["1", "\\\\N", "Some Track"]])
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT library_release_id, artist_name FROM compilation_track_artist"
        ).fetchall()
        conn.close()

        # The row survives -- it is not dropped as a false NULL-artist_name.
        assert rows == [(1, "\\N")]

    def test_cta_track_title_null_sentinel_still_works(self, tmp_path: Path) -> None:
        r"""The real \N sentinel (two chars, exactly) in the nullable
        track_title column still maps to SQL NULL -- the ordering fix must
        not disturb this existing, still-correct case."""
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(tmp_path, [["1", "Stereolab", "\\N"]])
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        track_title = conn.execute("SELECT track_title FROM compilation_track_artist").fetchone()[0]
        conn.close()

        assert track_title is None

    def test_cta_cr_in_final_column_survives_intact(self, tmp_path: Path) -> None:
        r"""A bare CR in the final column (track_title) must survive intact
        rather than being read as a line terminator and silently truncating
        the value (WXYC/discogs-etl#373).

        Red before the ``newline="\n"`` fix: the default universal-newline
        ``open()`` treats the bare CR as a line terminator, so the first
        fragment is accepted with ``track_title`` truncated at ``Track`` and
        the trailing fragment (``Title``) is dropped as malformed.
        """
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(
            tmp_path,
            [
                ["1", "Stereolab", "Track\rTitle"],
                ["1", "Juana Molina", "Other Track"],
            ],
        )
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT artist_name, track_title FROM compilation_track_artist ORDER BY rowid"
        ).fetchall()
        conn.close()

        # Both rows land intact -- the CR-bearing final column is not
        # truncated, and the row following the bare CR is not mistaken for a
        # continuation of the first.
        assert rows == [("Stereolab", "Track\rTitle"), ("Juana Molina", "Other Track")]

    def test_cta_cr_mid_row_no_longer_splits_row(self, tmp_path: Path) -> None:
        r"""A bare CR in a non-final column (artist_name) no longer
        terminates the line.

        Pre-fix this was the loud, already-handled case: both fragments fail
        the field-count check and the row is dropped with a WARNING. Post-fix
        the row is accepted whole, with the CR preserved inside the field
        (WXYC/discogs-etl#373) -- an intended behavior change, not a
        regression.
        """
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(tmp_path, [["1", "Stereo\rlab", "Some Track"]])
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT library_release_id, artist_name FROM compilation_track_artist"
        ).fetchone()
        conn.close()

        assert row == (1, "Stereo\rlab")

    def test_cta_multiple_rows_still_parse_after_newline_fix(self, tmp_path: Path) -> None:
        r"""A normal, CR-free multi-row CTA TSV parses unchanged under
        ``newline="\n"`` -- regression guard for WXYC/discogs-etl#373."""
        library_tsv = self._library_tsv(tmp_path)
        cta_tsv = self._cta_tsv(
            tmp_path,
            [
                ["1", "Stereolab", "Some Track"],
                ["1", "Juana Molina", "Other Track"],
            ],
        )
        db_path = tmp_path / "library.db"

        tsv_to_sqlite(str(library_tsv), str(db_path), str(cta_tsv))

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT artist_name, track_title FROM compilation_track_artist ORDER BY rowid"
        ).fetchall()
        conn.close()

        assert rows == [("Stereolab", "Some Track"), ("Juana Molina", "Other Track")]
