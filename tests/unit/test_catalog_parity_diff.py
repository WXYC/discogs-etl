"""Unit tests for ``scripts/catalog_parity_diff.py``.

The diff-two-files core of the discogs-etl#346 catalog-parity harness:
given two already-built ``library.db`` SQLite files (one from the daily
MySQL-sourced build, one from a future Backend-sourced build), report where
they diverge -- per-column field mismatches, row-set membership (ids present
in only one side), and ``compilation_track_artist`` (CTA) drift.

The "producer" half of #346 (building a fresh library.db from a live source)
is out of scope here -- see the ``TestBuildFromSourceStubs`` class, which
pins the NotImplementedError contract for the reserved ``--mysql-source`` /
``--backend-source`` flags.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "catalog_parity_diff.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("catalog_parity_diff", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["catalog_parity_diff"] = mod
    spec.loader.exec_module(mod)
    return mod


# The exact daily-sync `library` table shape, derived from
# scripts/tsv_to_sqlite.py's CREATE TABLE statement.
_LIBRARY_COLUMNS = (
    "id",
    "title",
    "artist",
    "call_letters",
    "artist_call_number",
    "release_call_number",
    "genre",
    "format",
    "alternate_artist_name",
    "album_artist",
    "label",
    "cross_reference_names",
)

_DEFAULT_ROW = {
    "title": "Aluminum Tunes",
    "artist": "Stereolab",
    "call_letters": "ST",
    "artist_call_number": 100,
    "release_call_number": 1,
    "genre": "Rock",
    "format": "CD",
    "alternate_artist_name": None,
    "album_artist": None,
    "label": None,
    "cross_reference_names": None,
}


def _make_library_db(
    path: Path,
    rows: list[dict],
    cta_rows: list[tuple[int, str, str | None]] | None = None,
) -> None:
    """Build a real library.db (matching the daily-sync schema) at ``path``.

    Each entry in ``rows`` is a dict with at least an "id" key; any column
    left unset falls back to ``_DEFAULT_ROW``. Pass ``cta_rows`` (id,
    artist_name, track_title tuples) to also create the optional
    compilation_track_artist table.
    """
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE library (
        id INTEGER PRIMARY KEY, title TEXT, artist TEXT, call_letters TEXT,
        artist_call_number INTEGER, release_call_number INTEGER,
        genre TEXT, format TEXT, alternate_artist_name TEXT,
        album_artist TEXT, label TEXT, cross_reference_names TEXT
    )"""
    )
    for row in rows:
        merged = {**_DEFAULT_ROW, **row}
        values = [merged["id"]] + [merged[c] for c in _LIBRARY_COLUMNS if c != "id"]
        cur.execute(
            f"INSERT INTO library ({', '.join(_LIBRARY_COLUMNS)}) "
            f"VALUES ({', '.join('?' for _ in _LIBRARY_COLUMNS)})",
            values,
        )
    if cta_rows:
        cur.execute(
            """CREATE TABLE compilation_track_artist (
            library_release_id INTEGER NOT NULL,
            artist_name TEXT NOT NULL,
            track_title TEXT
        )"""
        )
        cur.executemany(
            "INSERT INTO compilation_track_artist"
            " (library_release_id, artist_name, track_title) VALUES (?,?,?)",
            cta_rows,
        )
    conn.commit()
    conn.close()


class TestNormalize:
    """Pure normalization rule: NULL / '' / 'NULL' / whitespace-padding collapse to equal."""

    def test_none_normalizes_to_none(self) -> None:
        mod = _load_module()
        assert mod._normalize(None) is None

    def test_empty_string_normalizes_to_none(self) -> None:
        mod = _load_module()
        assert mod._normalize("") is None

    def test_literal_null_string_normalizes_to_none(self) -> None:
        mod = _load_module()
        assert mod._normalize("NULL") is None

    def test_whitespace_padded_value_is_stripped(self) -> None:
        mod = _load_module()
        assert mod._normalize("  Stereolab  ") == "Stereolab"

    def test_whitespace_only_normalizes_to_none(self) -> None:
        mod = _load_module()
        assert mod._normalize("   ") is None

    def test_non_string_passthrough(self) -> None:
        mod = _load_module()
        assert mod._normalize(100) == 100
        assert mod._normalize(None) is None

    def test_case_is_not_folded(self) -> None:
        """Only the exact literal 'NULL' is treated as NULL -- no case folding."""
        mod = _load_module()
        assert mod._normalize("null") == "null"
        assert mod._normalize("Null") == "Null"

    def test_null_foo_is_not_normalized_away(self) -> None:
        """'null foo' is genuinely different data, not the transient NULL artifact."""
        mod = _load_module()
        assert mod._normalize("null foo") == "null foo"
        assert mod._normalize("null foo") != mod._normalize("")


class TestDiffLibraryDbs:
    """Row-level and field-level diff semantics via diff_library_dbs()."""

    def test_perfect_match_has_zero_diffs(self, tmp_path: Path) -> None:
        mod = _load_module()
        rows = [
            {"id": 1, "title": "Aluminum Tunes", "artist": "Stereolab"},
            {"id": 2, "title": "DOGA", "artist": "Juana Molina"},
        ]
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, rows)
        _make_library_db(backend_db, rows)

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.matched == 2
        assert result.missing_in_backend == 0
        assert result.extra_in_backend == 0
        assert result.missing_in_backend_ids == []
        assert result.extra_in_backend_ids == []
        assert all(count == 0 for count in result.field_mismatches.values())
        assert result.cta_missing == 0
        assert result.cta_extra == 0

    def test_field_mismatch_in_specific_column(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "Rock"}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Electronic"}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.matched == 1
        assert result.field_mismatches["genre"] == 1
        assert all(v == 0 for col, v in result.field_mismatches.items() if col != "genre")

    def test_id_missing_in_backend(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}, {"id": 2}])
        _make_library_db(backend_db, [{"id": 1}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.matched == 1
        assert result.missing_in_backend == 1
        assert result.missing_in_backend_ids == [2]
        assert result.extra_in_backend == 0
        assert result.extra_in_backend_ids == []

    def test_id_extra_in_backend(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}, {"id": 99}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.matched == 1
        assert result.extra_in_backend == 1
        assert result.extra_in_backend_ids == [99]
        assert result.missing_in_backend == 0
        assert result.missing_in_backend_ids == []

    def test_label_column_always_null_is_a_no_op(self, tmp_path: Path) -> None:
        """label is always NULL in prod; excluded from the diffed column set."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "label": None}])
        _make_library_db(backend_db, [{"id": 1, "label": None}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert "label" not in result.field_mismatches
        assert result.matched == 1

    def test_normalization_equivalence_null_empty_string_and_whitespace(
        self, tmp_path: Path
    ) -> None:
        """NULL, '', 'NULL', and whitespace-padded values all count as equal."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(
            mysql_db,
            [
                {"id": 1, "alternate_artist_name": None},
                {"id": 2, "alternate_artist_name": ""},
                {"id": 3, "alternate_artist_name": "NULL"},
                {"id": 4, "alternate_artist_name": "  Sessa  "},
            ],
        )
        _make_library_db(
            backend_db,
            [
                {"id": 1, "alternate_artist_name": ""},
                {"id": 2, "alternate_artist_name": "NULL"},
                {"id": 3, "alternate_artist_name": None},
                {"id": 4, "alternate_artist_name": "Sessa"},
            ],
        )

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.field_mismatches["alternate_artist_name"] == 0

    def test_genuine_difference_is_not_normalized_away(self, tmp_path: Path) -> None:
        """'null foo' vs '' must NOT be normalized to equal."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "alternate_artist_name": "null foo"}])
        _make_library_db(backend_db, [{"id": 1, "alternate_artist_name": ""}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.field_mismatches["alternate_artist_name"] == 1

    def test_cta_row_missing_in_backend(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(
            mysql_db,
            [{"id": 1}],
            cta_rows=[(1, "Duke Ellington", "In a Sentimental Mood")],
        )
        _make_library_db(backend_db, [{"id": 1}], cta_rows=[])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.cta_missing == 1
        assert result.cta_extra == 0

    def test_cta_row_extra_in_backend(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}], cta_rows=[])
        _make_library_db(
            backend_db,
            [{"id": 1}],
            cta_rows=[(1, "John Coltrane", "In a Sentimental Mood")],
        )

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.cta_missing == 0
        assert result.cta_extra == 1

    def test_cta_table_absent_on_both_sides_is_a_noop(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.cta_missing == 0
        assert result.cta_extra == 0


class TestRunDiff:
    """run_diff() opens both files read-only and raises SourceError on bad input."""

    def test_missing_mysql_file_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        backend_db = tmp_path / "backend.db"
        _make_library_db(backend_db, [{"id": 1}])

        with pytest.raises(mod.SourceError):
            mod.run_diff(str(tmp_path / "does-not-exist.db"), str(backend_db))

    def test_missing_backend_file_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        _make_library_db(mysql_db, [{"id": 1}])

        with pytest.raises(mod.SourceError):
            mod.run_diff(str(mysql_db), str(tmp_path / "does-not-exist.db"))

    def test_missing_library_table_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        # backend.db exists but has no `library` table at all.
        conn = sqlite3.connect(backend_db)
        conn.execute("CREATE TABLE unrelated (id INTEGER)")
        conn.commit()
        conn.close()

        with pytest.raises(mod.SourceError):
            mod.run_diff(str(mysql_db), str(backend_db))

    def test_unreadable_file_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        backend_db = tmp_path / "backend.db"
        _make_library_db(backend_db, [{"id": 1}])
        not_a_db = tmp_path / "not-a-db.db"
        not_a_db.write_text("this is plainly not a sqlite database", encoding="utf-8")

        with pytest.raises(mod.SourceError):
            mod.run_diff(str(not_a_db), str(backend_db))

    def test_run_diff_never_writes_to_inputs(self, tmp_path: Path) -> None:
        """Read-only connections: mtime of both input files is unchanged after run_diff()."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        mysql_mtime_before = mysql_db.stat().st_mtime_ns
        backend_mtime_before = backend_db.stat().st_mtime_ns

        mod.run_diff(str(mysql_db), str(backend_db))

        assert mysql_db.stat().st_mtime_ns == mysql_mtime_before
        assert backend_db.stat().st_mtime_ns == backend_mtime_before

    def test_run_diff_happy_path_returns_parity_diff(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "Rock"}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Rock"}])

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.matched == 1


class TestMainCli:
    """CLI wiring: argument validation, JSON contract, exit codes."""

    def test_missing_required_args_exits_2(self) -> None:
        mod = _load_module()
        assert mod.main([]) == 2

    def test_only_mysql_db_given_exits_2(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        _make_library_db(mysql_db, [{"id": 1}])
        assert mod.main(["--mysql-db", str(mysql_db)]) == 2

    def test_missing_file_exits_3(self, tmp_path: Path) -> None:
        mod = _load_module()
        backend_db = tmp_path / "backend.db"
        _make_library_db(backend_db, [{"id": 1}])
        exit_code = mod.main(
            [
                "--mysql-db",
                str(tmp_path / "does-not-exist.db"),
                "--backend-db",
                str(backend_db),
            ]
        )
        assert exit_code == 3

    def test_missing_table_exits_3(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        conn = sqlite3.connect(backend_db)
        conn.execute("CREATE TABLE unrelated (id INTEGER)")
        conn.commit()
        conn.close()

        exit_code = mod.main(["--mysql-db", str(mysql_db), "--backend-db", str(backend_db)])
        assert exit_code == 3

    def test_success_exits_0_even_with_diffs(self, tmp_path: Path) -> None:
        """Exit 0 means 'ran successfully' -- a nonzero diff count is still exit 0."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}, {"id": 2}])
        _make_library_db(backend_db, [{"id": 1}])

        exit_code = mod.main(["--mysql-db", str(mysql_db), "--backend-db", str(backend_db)])
        assert exit_code == 0

    def test_json_output_has_exact_contract_shape(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "Rock"}, {"id": 2}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Electronic"}, {"id": 99}])

        exit_code = mod.main(
            ["--mysql-db", str(mysql_db), "--backend-db", str(backend_db), "--json"]
        )
        assert exit_code == 0

        captured = capsys.readouterr()
        payload = json.loads(captured.out)

        assert payload["matched"] == 1
        assert payload["missing_in_backend"] == 1
        assert payload["extra_in_backend"] == 1
        assert payload["field_mismatches"]["genre"] == 1
        assert payload["cta_missing"] == 0
        assert payload["cta_extra"] == 0
        assert payload["missing_in_backend_ids"] == [2]
        assert payload["extra_in_backend_ids"] == [99]

    def test_human_output_is_not_json(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        exit_code = mod.main(["--mysql-db", str(mysql_db), "--backend-db", str(backend_db)])
        assert exit_code == 0

        captured = capsys.readouterr()
        with pytest.raises(json.JSONDecodeError):
            json.loads(captured.out)
        assert "matched" in captured.out

    def test_cli_subprocess_invocation(self, tmp_path: Path) -> None:
        """Running as a subprocess exercises the real __main__ entrypoint."""
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--mysql-db",
                str(mysql_db),
                "--backend-db",
                str(backend_db),
                "--json",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload["matched"] == 1


class TestBuildFromSourceStubs:
    """The producer half of #346 is out of scope: reserved flags raise/exit cleanly."""

    def test_mysql_source_flag_is_not_implemented(self, tmp_path: Path) -> None:
        mod = _load_module()
        exit_code = mod.main(["--mysql-source", "mysql://example", "--backend-db", "x"])
        assert exit_code == 2

    def test_backend_source_flag_is_not_implemented(self, tmp_path: Path) -> None:
        mod = _load_module()
        exit_code = mod.main(["--mysql-db", "x", "--backend-source", "https://example"])
        assert exit_code == 2

    def test_build_stub_functions_raise_not_implemented_error(self) -> None:
        mod = _load_module()
        with pytest.raises(NotImplementedError):
            mod._build_library_db_from_mysql("mysql://example", "/tmp/out.db")
        with pytest.raises(NotImplementedError):
            mod._build_library_db_from_backend("https://example", "/tmp/out.db")
