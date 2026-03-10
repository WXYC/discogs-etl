"""Unit tests for scripts/fix_csv_newlines.py."""

from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import pytest

# Load fix_csv_newlines module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "fix_csv_newlines.py"
_spec = importlib.util.spec_from_file_location("fix_csv_newlines", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_fn = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fn)

fix_csv = _fn.fix_csv
fix_csv_dir = _fn.fix_csv_dir
main = _fn.main


class TestFixCsv:
    """Fixing embedded newlines in CSV fields."""

    def _write_csv(self, path: Path, headers: list[str], rows: list[list[str]]) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def _read_csv(self, path: Path) -> list[list[str]]:
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            return list(reader)

    def test_embedded_newlines_replaced_with_spaces(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"

        self._write_csv(input_path, ["text"], [["line one\nline two"]])
        fix_csv(input_path, output_path)

        rows = self._read_csv(output_path)
        assert rows[0][0] == "line one line two"

    def test_carriage_returns_removed(self, tmp_path: Path) -> None:
        """Bare \\r is translated to \\n by Python's universal newlines, then replaced with space."""
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"

        self._write_csv(input_path, ["text"], [["hello\rworld"]])
        fix_csv(input_path, output_path)

        rows = self._read_csv(output_path)
        # \r → \n (universal newline) → space (newline replacement)
        assert rows[0][0] == "hello world"

    def test_combined_crlf(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"

        self._write_csv(input_path, ["text"], [["first\r\nsecond"]])
        fix_csv(input_path, output_path)

        rows = self._read_csv(output_path)
        assert rows[0][0] == "first second"

    def test_preserves_normal_fields(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"

        self._write_csv(input_path, ["a", "b"], [["hello", "world"]])
        fix_csv(input_path, output_path)

        rows = self._read_csv(output_path)
        assert rows[0] == ["hello", "world"]

    def test_row_count_returned(self, tmp_path: Path) -> None:
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"

        self._write_csv(input_path, ["a"], [["x"], ["y"], ["z"]])
        count = fix_csv(input_path, output_path)
        assert count == 3

    @pytest.mark.parametrize(
        "field, expected",
        [
            ("no special chars", "no special chars"),
            ("tabs\there", "tabs\there"),  # tabs are not modified
            ("multi\n\nblank", "multi  blank"),
        ],
        ids=["no-change", "tabs-preserved", "double-newline"],
    )
    def test_edge_cases(self, tmp_path: Path, field: str, expected: str) -> None:
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"

        self._write_csv(input_path, ["text"], [[field]])
        fix_csv(input_path, output_path)

        rows = self._read_csv(output_path)
        assert rows[0][0] == expected


class TestFixCsvDir:
    """Batch-processing a directory of CSV files."""

    def _write_csv(self, path: Path, headers: list[str], rows: list[list[str]]) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def test_processes_all_csv_files(self, tmp_path: Path) -> None:
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()

        self._write_csv(input_dir / "a.csv", ["text"], [["line\none"]])
        self._write_csv(input_dir / "b.csv", ["text"], [["hello\nworld"]])

        fix_csv_dir(input_dir, output_dir)

        assert (output_dir / "a.csv").exists()
        assert (output_dir / "b.csv").exists()

        with open(output_dir / "a.csv") as f:
            reader = csv.reader(f)
            next(reader)
            assert next(reader)[0] == "line one"

        with open(output_dir / "b.csv") as f:
            reader = csv.reader(f)
            next(reader)
            assert next(reader)[0] == "hello world"

    def test_empty_dir_logs_warning(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        input_dir = tmp_path / "empty"
        output_dir = tmp_path / "output"
        input_dir.mkdir()

        with caplog.at_level(logging.WARNING):
            fix_csv_dir(input_dir, output_dir)

        assert any("No .csv files found" in r.message for r in caplog.records)


class TestMain:
    """Tests for the main() entry point."""

    def test_wrong_arg_count_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.argv", ["fix_csv_newlines.py"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_too_many_args_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.argv", ["fix_csv_newlines.py", "a", "b", "c"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_happy_path(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"

        with open(input_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["text"])
            writer.writerow(["line\none"])

        monkeypatch.setattr("sys.argv", ["fix_csv_newlines.py", str(input_path), str(output_path)])
        main()

        with open(output_path) as f:
            reader = csv.reader(f)
            next(reader)
            assert next(reader)[0] == "line one"
