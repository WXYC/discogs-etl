"""Unit tests for scripts/enrich_library_artists.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Load enrich_library_artists module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "enrich_library_artists.py"
_spec = importlib.util.spec_from_file_location("enrich_library_artists", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
sys.modules["enrich_library_artists"] = _mod
_spec.loader.exec_module(_mod)

extract_base_artists = _mod.extract_base_artists
extract_alternate_names = _mod.extract_alternate_names
extract_cross_referenced_artists = _mod.extract_cross_referenced_artists
extract_release_cross_ref_artists = _mod.extract_release_cross_ref_artists
merge_and_write = _mod.merge_and_write
parse_args = _mod.parse_args

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# ---------------------------------------------------------------------------
# extract_base_artists
# ---------------------------------------------------------------------------


class TestExtractBaseArtists:
    """Extracting unique artist names from library.db."""

    def test_returns_nonempty_set(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        assert isinstance(artists, set)
        assert len(artists) > 0

    def test_contains_known_artist(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        # library.db has "Radiohead" as an artist
        assert "Radiohead" in artists

    def test_excludes_compilation_artists(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        for name in artists:
            name_lower = name.lower()
            assert "various" not in name_lower, f"Compilation artist not excluded: {name}"

    def test_no_empty_strings(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        assert "" not in artists
        assert all(name.strip() for name in artists)

    def test_preserves_original_case(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        # Should have mixed case, not all lowercase
        assert "Radiohead" in artists
        assert "radiohead" not in artists


# ---------------------------------------------------------------------------
# merge_and_write
# ---------------------------------------------------------------------------


class TestMergeAndWrite:
    """Merging artist sets and writing output file."""

    def test_merges_all_sources(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha", "Beta"},
            alternates={"Gamma"},
            cross_refs={"Delta"},
            release_cross_refs={"Epsilon"},
            output=output,
        )
        lines = output.read_text().splitlines()
        assert set(lines) == {"Alpha", "Beta", "Gamma", "Delta", "Epsilon"}

    def test_no_duplicates(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha", "Beta"},
            alternates={"Beta", "Gamma"},
            cross_refs={"Gamma", "Delta"},
            release_cross_refs={"Alpha"},
            output=output,
        )
        lines = output.read_text().splitlines()
        assert len(lines) == len(set(lines))
        assert set(lines) == {"Alpha", "Beta", "Gamma", "Delta"}

    def test_sorted_output(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Zebra", "Apple", "Mango"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = output.read_text().splitlines()
        assert lines == sorted(lines)

    def test_excludes_empty_strings(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha", ""},
            alternates={"  ", "Beta"},
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = output.read_text().splitlines()
        assert "" not in lines
        assert "  " not in lines
        assert set(lines) == {"Alpha", "Beta"}

    def test_excludes_compilation_artists(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha"},
            alternates={"Various Artists", "Soundtrack Orchestra"},
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = output.read_text().splitlines()
        assert "Various Artists" not in lines
        assert "Soundtrack Orchestra" not in lines
        assert "Alpha" in lines

    def test_preserves_original_case(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"The Beatles", "RZA", "dj shadow"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = output.read_text().splitlines()
        assert "The Beatles" in lines
        assert "RZA" in lines
        assert "dj shadow" in lines

    def test_trailing_newline(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        content = output.read_text()
        assert content.endswith("\n")

    def test_empty_sets_produce_empty_file(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base=set(),
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        content = output.read_text()
        assert content == ""


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


class TestParseArgs:
    """CLI argument parsing."""

    def test_required_args(self) -> None:
        args = parse_args(
            [
                "--library-db",
                "library.db",
                "--output",
                "artists.txt",
            ]
        )
        assert args.library_db == Path("library.db")
        assert args.output == Path("artists.txt")
        assert args.wxyc_db_url is None

    def test_with_wxyc_db_url(self) -> None:
        args = parse_args(
            [
                "--library-db",
                "library.db",
                "--output",
                "artists.txt",
                "--wxyc-db-url",
                "mysql://wxyc:wxyc@localhost:3307/wxycmusic",
            ]
        )
        assert args.wxyc_db_url == "mysql://wxyc:wxyc@localhost:3307/wxycmusic"

    def test_missing_required_args_exits(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["--library-db", "library.db"])  # missing --output


# ---------------------------------------------------------------------------
# Multi-artist splitting in merge_and_write
# ---------------------------------------------------------------------------


class TestMultiArtistSplitting:
    """merge_and_write should expand multi-artist entries into components."""

    def test_comma_split_adds_components(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Mike Vainio, Ryoji, Alva Noto"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = set(output.read_text().splitlines())
        # Original + components
        assert "Mike Vainio, Ryoji, Alva Noto" in lines
        assert "Mike Vainio" in lines
        assert "Ryoji" in lines
        assert "Alva Noto" in lines

    def test_slash_split_adds_components(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"J Dilla / Jay Dee"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = set(output.read_text().splitlines())
        assert "J Dilla / Jay Dee" in lines
        assert "J Dilla" in lines
        assert "Jay Dee" in lines

    def test_ampersand_split_with_known_standalone(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Duke Ellington & John Coltrane", "Duke Ellington"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = set(output.read_text().splitlines())
        assert "Duke Ellington & John Coltrane" in lines
        assert "Duke Ellington" in lines
        assert "John Coltrane" in lines

    def test_ampersand_no_split_without_standalone(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Simon & Garfunkel"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = set(output.read_text().splitlines())
        assert "Simon & Garfunkel" in lines
        # Neither component should appear
        assert "Simon" not in lines
        assert "Garfunkel" not in lines

    def test_no_duplicate_lines(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Duke Ellington & John Coltrane", "Duke Ellington", "John Coltrane"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = output.read_text().splitlines()
        assert len(lines) == len(set(lines))

    def test_compilation_components_excluded(self, tmp_path: Path) -> None:
        """If a split component is a compilation artist, it should be excluded."""
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Juana Molina / Various Artists"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = set(output.read_text().splitlines())
        assert "Juana Molina" in lines
        assert "Various Artists" not in lines


# ---------------------------------------------------------------------------
# extract_alternate_names (mocked MySQL)
# ---------------------------------------------------------------------------


class TestExtractAlternateNames:
    """extract_alternate_names() queries LIBRARY_RELEASE for alternate artist names."""

    def test_returns_alternate_names(self) -> None:
        """Mock cursor returns sample alternate artist names."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.__iter__ = MagicMock(return_value=iter([("Body Count",), ("Ice Cube",)]))
        mock_conn.cursor.return_value = mock_cursor

        result = extract_alternate_names(mock_conn)
        assert result == {"Body Count", "Ice Cube"}

    def test_empty_result(self) -> None:
        """Empty cursor returns empty set."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        mock_conn.cursor.return_value = mock_cursor

        result = extract_alternate_names(mock_conn)
        assert result == set()

    def test_strips_whitespace_and_skips_empty(self) -> None:
        """Whitespace-only and None values are excluded."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.__iter__ = MagicMock(return_value=iter([("  Stereolab  ",), ("",), (None,)]))
        mock_conn.cursor.return_value = mock_cursor

        result = extract_alternate_names(mock_conn)
        assert result == {"Stereolab"}


# ---------------------------------------------------------------------------
# extract_cross_referenced_artists (mocked MySQL)
# ---------------------------------------------------------------------------


class TestExtractCrossReferencedArtists:
    """extract_cross_referenced_artists() queries LIBRARY_CODE_CROSS_REFERENCE."""

    def test_returns_cross_referenced_names(self) -> None:
        """Mock cursor returns UNION query results."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.__iter__ = MagicMock(
            return_value=iter([("Cat Power",), ("Chan Marshall",), ("Cat Power",)])
        )
        mock_conn.cursor.return_value = mock_cursor

        result = extract_cross_referenced_artists(mock_conn)
        # Duplicates from UNION are already handled by the set
        assert "Cat Power" in result
        assert "Chan Marshall" in result

    def test_deduplication(self) -> None:
        """Duplicate names across UNION branches produce unique results."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.__iter__ = MagicMock(
            return_value=iter([("Jessica Pratt",), ("Jessica Pratt",), ("Chuquimamani-Condori",)])
        )
        mock_conn.cursor.return_value = mock_cursor

        result = extract_cross_referenced_artists(mock_conn)
        assert len(result) == 2
        assert result == {"Jessica Pratt", "Chuquimamani-Condori"}


# ---------------------------------------------------------------------------
# extract_release_cross_ref_artists (mocked MySQL)
# ---------------------------------------------------------------------------


class TestExtractReleaseCrossRefArtists:
    """extract_release_cross_ref_artists() queries RELEASE_CROSS_REFERENCE."""

    def test_returns_release_cross_ref_names(self) -> None:
        """Mock cursor returns cross-reference artist names."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.__iter__ = MagicMock(return_value=iter([("Juana Molina",), ("Sessa",)]))
        mock_conn.cursor.return_value = mock_cursor

        result = extract_release_cross_ref_artists(mock_conn)
        assert result == {"Juana Molina", "Sessa"}

    def test_empty_result(self) -> None:
        """Empty cursor returns empty set."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        mock_conn.cursor.return_value = mock_cursor

        result = extract_release_cross_ref_artists(mock_conn)
        assert result == set()


# ---------------------------------------------------------------------------
# main (mocked)
# ---------------------------------------------------------------------------


class TestMain:
    """main() orchestrates extraction and merge."""

    def test_with_library_db_only(self, tmp_path) -> None:
        """With --library-db only (no MySQL), base artists are extracted and written."""
        library_db = tmp_path / "library.db"
        # Create a minimal SQLite library.db
        import sqlite3

        conn = sqlite3.connect(library_db)
        conn.execute("CREATE TABLE library (artist TEXT, title TEXT)")
        conn.execute("INSERT INTO library VALUES ('Stereolab', 'Aluminum Tunes')")
        conn.execute("INSERT INTO library VALUES ('Cat Power', 'Moon Pix')")
        conn.commit()
        conn.close()

        output = tmp_path / "artists.txt"

        with patch.object(
            _mod,
            "parse_args",
            return_value=parse_args(["--library-db", str(library_db), "--output", str(output)]),
        ):
            _mod.main()

        lines = set(output.read_text().splitlines())
        assert "Stereolab" in lines
        assert "Cat Power" in lines

    def test_with_library_db_and_wxyc_db_url(self, tmp_path) -> None:
        """With --library-db and --wxyc-db-url, MySQL enrichment is performed."""
        library_db = tmp_path / "library.db"
        import sqlite3

        conn = sqlite3.connect(library_db)
        conn.execute("CREATE TABLE library (artist TEXT, title TEXT)")
        conn.execute("INSERT INTO library VALUES ('Stereolab', 'Aluminum Tunes')")
        conn.commit()
        conn.close()

        output = tmp_path / "artists.txt"
        mock_mysql_conn = MagicMock()

        with (
            patch.object(
                _mod,
                "parse_args",
                return_value=parse_args(
                    [
                        "--library-db",
                        str(library_db),
                        "--output",
                        str(output),
                        "--wxyc-db-url",
                        "mysql://user:pass@host/db",
                    ]
                ),
            ),
            patch.object(_mod, "connect_mysql", return_value=mock_mysql_conn),
            patch.object(_mod, "extract_alternate_names", return_value={"Nourished by Time"}),
            patch.object(_mod, "extract_cross_referenced_artists", return_value={"Buck Meek"}),
            patch.object(_mod, "extract_release_cross_ref_artists", return_value={"Sessa"}),
        ):
            _mod.main()

        lines = set(output.read_text().splitlines())
        assert "Stereolab" in lines
        assert "Nourished by Time" in lines
        assert "Buck Meek" in lines
        assert "Sessa" in lines
        mock_mysql_conn.close.assert_called_once()

    def test_missing_library_db_exits(self, tmp_path) -> None:
        """Non-existent library.db triggers sys.exit(1)."""
        output = tmp_path / "artists.txt"
        with (
            patch.object(
                _mod,
                "parse_args",
                return_value=parse_args(
                    [
                        "--library-db",
                        str(tmp_path / "missing.db"),
                        "--output",
                        str(output),
                    ]
                ),
            ),
            pytest.raises(SystemExit, match="1"),
        ):
            _mod.main()
