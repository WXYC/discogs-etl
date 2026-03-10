"""Unit tests for scripts/csv_to_tsv.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Load csv_to_tsv module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "csv_to_tsv.py"
_spec = importlib.util.spec_from_file_location("csv_to_tsv", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_ct = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ct)

convert = _ct.convert
main = _ct.main


class TestConvert:
    """TSV conversion with PostgreSQL-compatible escaping."""

    def test_basic_conversion(self, tmp_path: Path) -> None:
        """Simple CSV converts to tab-separated output."""
        csv_file = tmp_path / "input.csv"
        tsv_file = tmp_path / "output.tsv"
        csv_file.write_text("name,value\nalpha,1\nbeta,2\n")

        count = convert(csv_file, tsv_file)
        assert count == 2

        lines = tsv_file.read_text().splitlines()
        assert lines[0] == "name\tvalue"
        assert lines[1] == "alpha\t1"
        assert lines[2] == "beta\t2"

    def test_empty_fields_become_null(self, tmp_path: Path) -> None:
        """Empty CSV fields are converted to \\N (PostgreSQL NULL)."""
        csv_file = tmp_path / "input.csv"
        tsv_file = tmp_path / "output.tsv"
        csv_file.write_text("a,b,c\n1,,3\n")

        convert(csv_file, tsv_file)
        lines = tsv_file.read_text().splitlines()
        assert lines[1] == "1\t\\N\t3"

    @pytest.mark.parametrize(
        "input_val, expected_val",
        [
            ("back\\slash", "back\\\\slash"),
            ("tab\there", "tab\\there"),
            ("new\nline", "new\\nline"),
            ("carriage\rreturn", "carriage\\nreturn"),  # \r → \n via universal newlines
        ],
        ids=["backslash", "tab", "newline", "carriage-return"],
    )
    def test_special_char_escaping(self, tmp_path: Path, input_val: str, expected_val: str) -> None:
        """Special characters are escaped for PostgreSQL COPY."""
        csv_file = tmp_path / "input.csv"
        tsv_file = tmp_path / "output.tsv"

        # Write CSV with the special character in a quoted field
        import csv

        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["val"])
            writer.writerow([input_val])

        convert(csv_file, tsv_file)
        lines = tsv_file.read_text().splitlines()
        assert lines[1] == expected_val

    def test_row_count_returned(self, tmp_path: Path) -> None:
        """Returns the number of data rows (excluding header)."""
        csv_file = tmp_path / "input.csv"
        tsv_file = tmp_path / "output.tsv"
        csv_file.write_text("h\na\nb\nc\n")

        count = convert(csv_file, tsv_file)
        assert count == 3

    def test_empty_csv(self, tmp_path: Path) -> None:
        """A CSV with only a header produces zero data rows."""
        csv_file = tmp_path / "input.csv"
        tsv_file = tmp_path / "output.tsv"
        csv_file.write_text("h\n")

        count = convert(csv_file, tsv_file)
        assert count == 0
        lines = tsv_file.read_text().splitlines()
        assert lines == ["h"]


class TestMain:
    """Tests for the main() entry point."""

    def test_wrong_arg_count_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.argv", ["csv_to_tsv.py"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_too_many_args_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.argv", ["csv_to_tsv.py", "a", "b", "c"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_happy_path(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        csv_file = tmp_path / "input.csv"
        tsv_file = tmp_path / "output.tsv"
        csv_file.write_text("name,value\nalpha,1\nbeta,2\n")

        monkeypatch.setattr("sys.argv", ["csv_to_tsv.py", str(csv_file), str(tsv_file)])
        main()

        lines = tsv_file.read_text().splitlines()
        assert lines[0] == "name\tvalue"
        assert lines[1] == "alpha\t1"
        assert lines[2] == "beta\t2"
