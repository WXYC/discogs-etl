"""Unit tests for scripts/extract_library_labels.py."""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Load module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "extract_library_labels.py"
_spec = importlib.util.spec_from_file_location("extract_library_labels", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
sys.modules["extract_library_labels"] = _mod
_spec.loader.exec_module(_mod)

extract_library_labels = _mod.extract_library_labels
write_library_labels_csv = _mod.write_library_labels_csv
parse_args = _mod.parse_args


class TestExtractLibraryLabels:
    """Extracting (artist, title, label) triples from a MySQL cursor."""

    def test_returns_set_of_triples(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [
            ("Radiohead", "OK Computer", "Parlophone"),
            ("Joy Division", "Unknown Pleasures", "Factory Records"),
        ]
        result = extract_library_labels(mock_conn)
        assert result == {
            ("Radiohead", "OK Computer", "Parlophone"),
            ("Joy Division", "Unknown Pleasures", "Factory Records"),
        }

    def test_strips_whitespace(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [
            ("  Radiohead  ", " OK Computer ", "  Parlophone "),
        ]
        result = extract_library_labels(mock_conn)
        assert result == {("Radiohead", "OK Computer", "Parlophone")}

    def test_skips_empty_fields(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [
            ("Radiohead", "OK Computer", "Parlophone"),
            ("", "OK Computer", "Parlophone"),  # empty artist
            ("Radiohead", "", "Parlophone"),  # empty title
            ("Radiohead", "OK Computer", ""),  # empty label
            (None, "OK Computer", "Parlophone"),  # null artist
        ]
        result = extract_library_labels(mock_conn)
        assert result == {("Radiohead", "OK Computer", "Parlophone")}

    def test_empty_result_set(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        result = extract_library_labels(mock_conn)
        assert result == set()


class TestWriteLibraryLabelsCsv:
    """Writing label triples to CSV."""

    def test_writes_correct_headers(self, tmp_path: Path) -> None:
        output = tmp_path / "labels.csv"
        write_library_labels_csv(
            {("Radiohead", "OK Computer", "Parlophone")},
            output,
        )
        with open(output, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
        assert headers == ["artist_name", "release_title", "label_name"]

    def test_writes_sorted_rows(self, tmp_path: Path) -> None:
        output = tmp_path / "labels.csv"
        write_library_labels_csv(
            {
                ("Radiohead", "OK Computer", "Parlophone"),
                ("Joy Division", "Unknown Pleasures", "Factory Records"),
            },
            output,
        )
        with open(output, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            rows = list(reader)
        assert rows[0] == ["Joy Division", "Unknown Pleasures", "Factory Records"]
        assert rows[1] == ["Radiohead", "OK Computer", "Parlophone"]

    def test_empty_set_writes_header_only(self, tmp_path: Path) -> None:
        output = tmp_path / "labels.csv"
        write_library_labels_csv(set(), output)
        with open(output, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
            rows = list(reader)
        assert headers == ["artist_name", "release_title", "label_name"]
        assert rows == []


class TestParseArgs:
    """CLI argument parsing."""

    def test_required_args(self) -> None:
        args = parse_args(["--wxyc-db-url", "mysql://u:p@h/db", "--output", "out.csv"])
        assert args.wxyc_db_url == "mysql://u:p@h/db"
        assert args.output == Path("out.csv")

    def test_catalog_source_args(self) -> None:
        args = parse_args(
            [
                "--catalog-source",
                "backend-service",
                "--catalog-db-url",
                "postgresql://u:p@h/db",
                "--output",
                "out.csv",
            ]
        )
        assert args.catalog_source == "backend-service"
        assert args.catalog_db_url == "postgresql://u:p@h/db"
        assert args.wxyc_db_url is None

    def test_missing_output_exits(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["--wxyc-db-url", "mysql://u:p@h/db"])
