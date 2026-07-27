"""Unit tests for lib/keep_release_ids.py's allowlist file parser."""

from __future__ import annotations

from pathlib import Path

from lib.keep_release_ids import parse_keep_release_ids


class TestParseKeepReleaseIds:
    def test_missing_file_returns_empty_set(self, tmp_path: Path) -> None:
        assert parse_keep_release_ids(tmp_path / "nope.txt") == set()

    def test_empty_file_returns_empty_set(self, tmp_path: Path) -> None:
        path = tmp_path / "keep_ids.txt"
        path.write_text("")
        assert parse_keep_release_ids(path) == set()

    def test_parses_one_id_per_line(self, tmp_path: Path) -> None:
        path = tmp_path / "keep_ids.txt"
        path.write_text("101\n202\n303\n")
        assert parse_keep_release_ids(path) == {101, 202, 303}

    def test_blank_lines_ignored(self, tmp_path: Path) -> None:
        path = tmp_path / "keep_ids.txt"
        path.write_text("101\n\n\n202\n")
        assert parse_keep_release_ids(path) == {101, 202}

    def test_comment_lines_ignored(self, tmp_path: Path) -> None:
        path = tmp_path / "keep_ids.txt"
        path.write_text("# WXYC library overrides\n101\n# another comment\n202\n")
        assert parse_keep_release_ids(path) == {101, 202}

    def test_duplicate_ids_deduplicated(self, tmp_path: Path) -> None:
        path = tmp_path / "keep_ids.txt"
        path.write_text("101\n101\n202\n")
        assert parse_keep_release_ids(path) == {101, 202}

    def test_surrounding_whitespace_stripped(self, tmp_path: Path) -> None:
        path = tmp_path / "keep_ids.txt"
        path.write_text("  101  \n\t202\t\n")
        assert parse_keep_release_ids(path) == {101, 202}
