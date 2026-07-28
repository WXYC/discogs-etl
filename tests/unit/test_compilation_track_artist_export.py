"""Unit tests for the compilation_track_artist export in scripts/tsv_to_sqlite.py.

Restores CTA-export test coverage that existed in the deleted
scripts/export_to_sqlite.py (WXYC/discogs-etl#61, commit f7bfdcd) after the #65
slim-down dropped the export without carrying the logic into its wxyc-catalog
CLI replacement (WXYC/discogs-etl#332). The source is now a TSV file (as
produced by ``mysql -B -N`` against tubafrenzy's COMPILATION_TRACK_ARTIST
table) rather than a list of dicts, so the fixtures below build TSV strings
instead of Python dicts.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

# Load tsv_to_sqlite module from scripts directory (mirrors test_tsv_to_sqlite.py).
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "tsv_to_sqlite.py"
_spec = importlib.util.spec_from_file_location("tsv_to_sqlite", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

tsv_to_sqlite = _mod.tsv_to_sqlite


def _make_library_tsv(tmp_path: Path) -> Path:
    """A minimal one-row library TSV -- CTA tests don't care about its content.

    11 fields (WXYC/discogs-etl#334 appended cross_reference_names as the
    11th column); the trailing \\N keeps this row valid under the current
    schema so it isn't silently skipped by the field-count guard.
    """
    tsv_file = tmp_path / "library.tsv"
    tsv_file.write_text(
        "1\tVintage Palmwine\tVarious Artists\tZX\t1\t1\tWorld\tCD\t\\N\t\\N\t\\N\n",
        encoding="utf-8",
    )
    return tsv_file


def _make_cta_tsv(tmp_path: Path, rows: list[list[str]], name: str = "cta.tsv") -> Path:
    cta_file = tmp_path / name
    cta_file.write_text("\n".join("\t".join(r) for r in rows) + "\n", encoding="utf-8")
    return cta_file


class TestCompilationTrackArtistExport:
    """compilation_track_artist table creation via the optional cta_tsv_path arg."""

    def _cta_rows(self, tmp_path: Path) -> Path:
        return _make_cta_tsv(
            tmp_path,
            [
                ["1", "Koo Nimo", "odo akosomo"],
                ["1", "T.O. Jazz", "Yaa Amponsah"],
            ],
        )

    def test_creates_compilation_track_artist_table(self, tmp_path: Path) -> None:
        """The table is created when a non-empty CTA TSV is supplied."""
        db_path = tmp_path / "library.db"
        tsv_to_sqlite(str(_make_library_tsv(tmp_path)), str(db_path), str(self._cta_rows(tmp_path)))

        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='compilation_track_artist'"
        )
        assert cursor.fetchone() is not None
        conn.close()

    def test_inserts_track_artist_rows(self, tmp_path: Path) -> None:
        """Track artist rows are inserted with correct values."""
        db_path = tmp_path / "library.db"
        tsv_to_sqlite(str(_make_library_tsv(tmp_path)), str(db_path), str(self._cta_rows(tmp_path)))

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT library_release_id, artist_name, track_title "
            "FROM compilation_track_artist ORDER BY artist_name"
        ).fetchall()
        conn.close()

        assert rows == [(1, "Koo Nimo", "odo akosomo"), (1, "T.O. Jazz", "Yaa Amponsah")]

    def test_creates_indexes_on_compilation_track_artist(self, tmp_path: Path) -> None:
        """idx_cta_release and idx_cta_artist indexes exist, matching f7bfdcd's DDL."""
        db_path = tmp_path / "library.db"
        tsv_to_sqlite(str(_make_library_tsv(tmp_path)), str(db_path), str(self._cta_rows(tmp_path)))

        conn = sqlite3.connect(str(db_path))
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_cta_%'"
            ).fetchall()
        }
        conn.close()

        assert "idx_cta_release" in indexes
        assert "idx_cta_artist" in indexes

    def test_nullable_track_title(self, tmp_path: Path) -> None:
        """MySQL \\N in the track_title column becomes SQL NULL (column is nullable)."""
        cta_file = _make_cta_tsv(tmp_path, [["1", "Various", "\\N"]])
        db_path = tmp_path / "library.db"
        tsv_to_sqlite(str(_make_library_tsv(tmp_path)), str(db_path), str(cta_file))

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT track_title FROM compilation_track_artist WHERE artist_name = 'Various'"
        ).fetchone()
        conn.close()
        assert row[0] is None

    def test_no_compilation_table_when_cta_tsv_omitted(self, tmp_path: Path) -> None:
        """Degrades gracefully: without cta_tsv_path, no table is created (e.g. source
        COMPILATION_TRACK_ARTIST table absent in pre-V008 fixtures / BS source)."""
        db_path = tmp_path / "library.db"
        tsv_to_sqlite(str(_make_library_tsv(tmp_path)), str(db_path))

        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='compilation_track_artist'"
        )
        assert cursor.fetchone() is None
        conn.close()

    def test_no_compilation_table_when_cta_tsv_empty(self, tmp_path: Path) -> None:
        """An empty (0-row) CTA TSV also results in no table -- same graceful-degrade path."""
        cta_file = tmp_path / "cta.tsv"
        cta_file.write_text("", encoding="utf-8")
        db_path = tmp_path / "library.db"
        tsv_to_sqlite(str(_make_library_tsv(tmp_path)), str(db_path), str(cta_file))

        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='compilation_track_artist'"
        )
        assert cursor.fetchone() is None
        conn.close()

    def test_malformed_cta_row_skipped_with_warning(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        """A row with the wrong field count is skipped and logged (WARNING on stderr),
        not silently dropped; well-formed rows in the same file still import."""
        cta_file = tmp_path / "cta.tsv"
        cta_file.write_text(
            "1\tKoo Nimo\todo akosomo\nbad\trow\n1\tT.O. Jazz\tYaa Amponsah\n",
            encoding="utf-8",
        )
        db_path = tmp_path / "library.db"
        tsv_to_sqlite(str(_make_library_tsv(tmp_path)), str(db_path), str(cta_file))

        captured = capsys.readouterr()
        assert "WARNING" in captured.err

        conn = sqlite3.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM compilation_track_artist").fetchone()[0]
        conn.close()
        assert count == 2

    def test_library_row_count_unaffected_by_cta(self, tmp_path: Path) -> None:
        """The function's return value stays the library row count -- CTA is supplementary
        and does not change tsv_to_sqlite's existing return contract."""
        db_path = tmp_path / "library.db"
        count = tsv_to_sqlite(
            str(_make_library_tsv(tmp_path)), str(db_path), str(self._cta_rows(tmp_path))
        )
        assert count == 1

    def test_cli_accepts_cta_tsv_flag(self, tmp_path: Path) -> None:
        """Running as a subprocess with --cta-tsv creates the compilation_track_artist table."""
        db_path = tmp_path / "library.db"
        result = subprocess.run(
            [
                sys.executable,
                str(_SCRIPT_PATH),
                str(_make_library_tsv(tmp_path)),
                str(db_path),
                "--cta-tsv",
                str(self._cta_rows(tmp_path)),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

        conn = sqlite3.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM compilation_track_artist").fetchone()[0]
        conn.close()
        assert count == 2

    def test_cli_without_cta_flag_still_works(self, tmp_path: Path) -> None:
        """Backward compatible: the CLI works with just the two positional args, as before."""
        db_path = tmp_path / "library.db"
        result = subprocess.run(
            [sys.executable, str(_SCRIPT_PATH), str(_make_library_tsv(tmp_path)), str(db_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

        conn = sqlite3.connect(str(db_path))
        tables = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        conn.close()
        assert "compilation_track_artist" not in tables
