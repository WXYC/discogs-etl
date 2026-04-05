"""Unit tests for scripts/import_csv.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Load import_csv module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

extract_year = _ic.extract_year
count_tracks_from_csv = _ic.count_tracks_from_csv
import_csv = _ic.import_csv
TABLES = _ic.TABLES
BASE_TABLES = _ic.BASE_TABLES
TRACK_TABLES = _ic.TRACK_TABLES
ARTIST_TABLES = _ic.ARTIST_TABLES
TableConfig = _ic.TableConfig
_import_tables_parallel = _ic._import_tables_parallel
import_artist_details = _ic.import_artist_details

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"


# ---------------------------------------------------------------------------
# extract_year
# ---------------------------------------------------------------------------


class TestExtractYear:
    """Extracting a 4-digit year from Discogs 'released' field."""

    @pytest.mark.parametrize(
        "input_val, expected",
        [
            ("2023-01-15", "2023"),
            ("1997-06-16", "1997"),
            ("1969-09-26", "1969"),
            ("2000", "2000"),
            ("1979", "1979"),
            ("", None),
            (None, None),
            ("Unknown", None),
            ("TBD", None),
            ("0000", "0000"),
            ("2023-00-00", "2023"),
            ("202１", None),  # fullwidth digit U+FF11
            ("２０２３", None),  # all fullwidth digits
        ],
        ids=[
            "full-date",
            "full-date-1997",
            "full-date-1969",
            "year-only",
            "year-only-1979",
            "empty",
            "none",
            "unknown-text",
            "tbd-text",
            "zeros",
            "partial-date",
            "fullwidth-digit",
            "all-fullwidth-digits",
        ],
    )
    def test_extract_year(self, input_val: str | None, expected: str | None) -> None:
        assert extract_year(input_val) == expected


# ---------------------------------------------------------------------------
# TABLES config validation
# ---------------------------------------------------------------------------


class TestTablesConfig:
    """Validate the TABLES configuration for CSV import."""

    def test_all_tables_have_matching_column_lengths(self) -> None:
        """csv_columns and db_columns must be the same length."""
        for table_config in TABLES:
            assert len(table_config["csv_columns"]) == len(table_config["db_columns"]), (
                f"Column length mismatch in {table_config['table']}: "
                f"csv_columns={len(table_config['csv_columns'])}, "
                f"db_columns={len(table_config['db_columns'])}"
            )

    def test_required_columns_are_subset_of_csv_columns(self) -> None:
        """Required columns must exist in csv_columns."""
        for table_config in TABLES:
            csv_set = set(table_config["csv_columns"])
            required_set = set(table_config["required"])
            assert required_set.issubset(csv_set), (
                f"Required columns not in csv_columns for {table_config['table']}: "
                f"{required_set - csv_set}"
            )

    def test_release_table_includes_master_id(self) -> None:
        """The release table must import master_id for dedup."""
        release_config = next(t for t in TABLES if t["table"] == "release")
        assert "master_id" in release_config["csv_columns"]
        assert "master_id" in release_config["db_columns"]

    def test_release_table_includes_country(self) -> None:
        """The release table must import country for US-preferred dedup ranking."""
        release_config = next(t for t in TABLES if t["table"] == "release")
        assert "country" in release_config["csv_columns"]
        assert "country" in release_config["db_columns"]

    def test_release_table_includes_format(self) -> None:
        """The release table must import format for format-aware dedup."""
        release_config = next(t for t in TABLES if t["table"] == "release")
        assert "format" in release_config["csv_columns"]
        assert "format" in release_config["db_columns"]

    def test_release_table_transforms_format(self) -> None:
        """The format field should be transformed via normalize_format."""
        release_config = next(t for t in TABLES if t["table"] == "release")
        assert "format" in release_config["transforms"]
        # Verify it normalizes correctly
        transform = release_config["transforms"]["format"]
        assert transform("2xLP") == "Vinyl"
        assert transform("CD") == "CD"
        assert transform(None) is None

    def test_release_artist_table_includes_artist_id(self) -> None:
        """The release_artist table must import artist_id for alias-enhanced filtering."""
        ra_config = next(t for t in TABLES if t["table"] == "release_artist")
        assert "artist_id" in ra_config["csv_columns"]
        assert "artist_id" in ra_config["db_columns"]

    def test_release_table_imports_released_as_raw_text(self) -> None:
        """The released field should be imported as raw text (no transform)."""
        release_config = next(t for t in TABLES if t["table"] == "release")
        assert "released" in release_config["csv_columns"]
        assert "released" in release_config["db_columns"]
        assert "released" not in release_config["transforms"]

    @pytest.mark.parametrize(
        "table_name",
        ["release", "release_artist", "release_label", "release_track", "release_track_artist"],
    )
    def test_table_has_csv_file(self, table_name: str) -> None:
        """Each table config specifies a CSV file."""
        table_config = next(t for t in TABLES if t["table"] == table_name)
        assert table_config["csv_file"].endswith(".csv")

    def test_all_tables_have_required_keys(self) -> None:
        """Each table config must have all required TypedDict keys."""
        required_keys = {"csv_file", "table", "csv_columns", "db_columns", "required", "transforms"}
        for table_config in TABLES:
            assert required_keys.issubset(table_config.keys()), (
                f"Missing keys in {table_config.get('table', '?')}: "
                f"{required_keys - table_config.keys()}"
            )

    def test_release_table_has_unique_key_on_id(self) -> None:
        """The release table must dedup on id to handle duplicate releases in CSVs."""
        release_config = next(t for t in TABLES if t["table"] == "release")
        assert "unique_key" in release_config, "release table needs unique_key for dedup"
        assert release_config["unique_key"] == ["id"]

    def test_tables_with_unique_constraints_have_unique_key(self) -> None:
        """Tables with unique constraints must specify unique_key for dedup during import."""
        tables_needing_dedup = {
            "release",
            "release_artist",
            "release_label",
            "release_genre",
            "release_style",
            "release_track_artist",
        }
        for table_config in TABLES:
            if table_config["table"] in tables_needing_dedup:
                assert "unique_key" in table_config, (
                    f"{table_config['table']} needs unique_key for dedup"
                )
                # unique_key columns must be a subset of csv_columns
                key_set = set(table_config["unique_key"])
                csv_set = set(table_config["csv_columns"])
                assert key_set.issubset(csv_set), (
                    f"unique_key {key_set} not subset of csv_columns {csv_set} "
                    f"in {table_config['table']}"
                )


# ---------------------------------------------------------------------------
# Column header detection
# ---------------------------------------------------------------------------


class TestColumnHeaderDetection:
    """Verify CSV column expectations match fixture data."""

    def test_release_csv_has_expected_columns(self) -> None:
        import csv as csv_mod

        csv_path = Path(__file__).parent.parent / "fixtures" / "csv" / "release.csv"
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            headers = reader.fieldnames
        assert headers is not None
        release_config = next(t for t in TABLES if t["table"] == "release")
        for col in release_config["csv_columns"]:
            assert col in headers, f"Expected column {col!r} not in release.csv headers: {headers}"

    def test_release_artist_csv_has_expected_columns(self) -> None:
        import csv as csv_mod

        csv_path = Path(__file__).parent.parent / "fixtures" / "csv" / "release_artist.csv"
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            headers = reader.fieldnames
        assert headers is not None
        ra_config = next(t for t in TABLES if t["table"] == "release_artist")
        for col in ra_config["csv_columns"]:
            assert col in headers, (
                f"Expected column {col!r} not in release_artist.csv headers: {headers}"
            )

    def test_release_label_csv_has_expected_columns(self) -> None:
        import csv as csv_mod

        csv_path = Path(__file__).parent.parent / "fixtures" / "csv" / "release_label.csv"
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            headers = reader.fieldnames
        assert headers is not None
        rl_config = next(t for t in TABLES if t["table"] == "release_label")
        for col in rl_config["csv_columns"]:
            assert col in headers, (
                f"Expected column {col!r} not in release_label.csv headers: {headers}"
            )


# ---------------------------------------------------------------------------
# count_tracks_from_csv
# ---------------------------------------------------------------------------


class TestCountTracksFromCsv:
    """Count tracks per release_id from a release_track CSV file."""

    def test_counts_tracks_per_release(self) -> None:
        """Returns a dict mapping release_id -> track count."""
        csv_path = CSV_DIR / "release_track.csv"
        counts = count_tracks_from_csv(csv_path)
        # Release 1002 (US) has 3 tracks in the fixture data
        assert counts[1002] == 3

    def test_all_releases_counted(self) -> None:
        """Every release_id in the CSV has an entry."""
        csv_path = CSV_DIR / "release_track.csv"
        counts = count_tracks_from_csv(csv_path)
        assert len(counts) == 15

    def test_returns_empty_for_nonexistent_file(self, tmp_path) -> None:
        """Returns empty dict when file is empty (only header)."""
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("release_id,sequence,position,title,duration\n")
        counts = count_tracks_from_csv(csv_path)
        assert counts == {}

    def test_skips_invalid_release_ids(self, tmp_path) -> None:
        """Rows with non-integer release_id are skipped."""
        csv_path = tmp_path / "bad.csv"
        csv_path.write_text(
            "release_id,sequence,position,title,duration\n"
            "abc,1,A1,Track 1,3:00\n"
            "1,1,A1,Track 1,3:00\n"
        )
        counts = count_tracks_from_csv(csv_path)
        assert counts == {1: 1}


# ---------------------------------------------------------------------------
# BASE_TABLES / TRACK_TABLES split
# ---------------------------------------------------------------------------


class TestParallelImport:
    """Test parallel CSV import."""

    def test_parent_imports_before_children(self, tmp_path) -> None:
        """Parent tables (release) must be imported before child tables."""
        import threading
        from unittest.mock import MagicMock, patch

        # Track import order via thread-safe list
        import_order: list[str] = []
        lock = threading.Lock()

        def mock_import_csv(conn, csv_path, table, *args, **kwargs):
            with lock:
                import_order.append(table)
            return 1

        # Create dummy CSV files
        for name in ["release.csv", "release_artist.csv", "release_label.csv"]:
            (tmp_path / name).write_text("id\n1\n")

        with (
            patch.object(_ic, "import_csv", side_effect=mock_import_csv),
            patch.object(_ic.psycopg, "connect", return_value=MagicMock()),
        ):
            _import_tables_parallel(
                "postgresql:///test", tmp_path, BASE_TABLES[:1], BASE_TABLES[1:]
            )

        # release must come before release_artist and release_label
        assert import_order[0] == "release"

    def test_child_tables_imported(self, tmp_path) -> None:
        """All child tables are imported."""
        from unittest.mock import MagicMock, patch

        imported_tables: list[str] = []

        def mock_import_csv(conn, csv_path, table, *args, **kwargs):
            imported_tables.append(table)
            return 1

        for name in ["release.csv", "release_artist.csv", "release_label.csv"]:
            (tmp_path / name).write_text("id\n1\n")

        with (
            patch.object(_ic, "import_csv", side_effect=mock_import_csv),
            patch.object(_ic.psycopg, "connect", return_value=MagicMock()),
        ):
            _import_tables_parallel(
                "postgresql:///test", tmp_path, BASE_TABLES[:1], BASE_TABLES[1:]
            )

        assert "release_artist" in imported_tables
        assert "release_label" in imported_tables


class TestTableSplit:
    """TABLES is the union of BASE_TABLES and TRACK_TABLES."""

    def test_tables_is_union(self) -> None:
        assert TABLES == BASE_TABLES + TRACK_TABLES

    def test_base_tables_names(self) -> None:
        names = [t["table"] for t in BASE_TABLES]
        assert names == ["release", "release_artist", "release_label", "release_genre", "release_style"]

    def test_track_tables_are_release_track_and_release_track_artist(self) -> None:
        names = [t["table"] for t in TRACK_TABLES]
        assert names == ["release_track", "release_track_artist"]

    def test_no_overlap(self) -> None:
        base_names = {t["table"] for t in BASE_TABLES}
        track_names = {t["table"] for t in TRACK_TABLES}
        assert base_names.isdisjoint(track_names)


# ---------------------------------------------------------------------------
# import_csv missing columns
# ---------------------------------------------------------------------------


class TestImportCsvMissingColumns:
    """import_csv reports clear errors when CSV header is missing expected columns."""

    def test_missing_columns_returns_zero_and_logs_error(self, tmp_path, caplog) -> None:
        """When the CSV header lacks required columns, import_csv returns 0 and logs them."""
        csv_path = tmp_path / "release.csv"
        # Write a CSV missing the 'title' and 'master_id' columns
        csv_path.write_text("id,country,released\n1001,US,2024\n")

        from unittest.mock import MagicMock

        conn = MagicMock()
        count = import_csv(
            conn,
            csv_path,
            table="release",
            csv_columns=["id", "title", "country", "released", "master_id"],
            db_columns=["id", "title", "country", "release_year", "master_id"],
            required_columns=["id", "title"],
            transforms={},
        )

        assert count == 0
        assert "Missing columns" in caplog.text
        assert "master_id" in caplog.text
        assert "title" in caplog.text

    def test_no_header_returns_zero(self, tmp_path, caplog) -> None:
        """An empty CSV file returns 0 with a warning."""
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("")

        from unittest.mock import MagicMock

        conn = MagicMock()
        count = import_csv(
            conn,
            csv_path,
            table="release",
            csv_columns=["id", "title"],
            db_columns=["id", "title"],
            required_columns=["id"],
            transforms={},
        )

        assert count == 0
        assert "No header" in caplog.text


# ---------------------------------------------------------------------------
# main() argument parsing and dispatch
# ---------------------------------------------------------------------------


class TestMainArgParsing:
    """import_csv.py main() validates args and dispatches to correct import mode."""

    def test_missing_csv_dir_exits(self, tmp_path) -> None:
        """Non-existent CSV directory triggers sys.exit(1)."""
        from unittest.mock import patch

        with (
            patch("sys.argv", ["import_csv.py", str(tmp_path / "missing_csv")]),
            pytest.raises(SystemExit, match="1"),
        ):
            _ic.main()

    def test_default_mode_calls_import_tables(self, tmp_path) -> None:
        """Default mode (no flags) calls _import_tables for all tables."""
        from unittest.mock import MagicMock, patch

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        mock_conn = MagicMock()

        with (
            patch("sys.argv", ["import_csv.py", str(csv_dir), "postgresql:///test"]),
            patch.object(_ic.psycopg, "connect", return_value=mock_conn),
            patch.object(_ic, "_import_tables", return_value=100) as mock_import,
            patch.object(_ic, "import_artwork", return_value=10),
            patch.object(_ic, "populate_release_year", return_value=50),
            patch.object(_ic, "populate_cache_metadata", return_value=50),
            patch.object(_ic, "import_artist_details", return_value=20),
        ):
            _ic.main()

        mock_import.assert_called_once()
        call_args = mock_import.call_args
        assert call_args[0][0] is mock_conn
        assert call_args[0][1] == csv_dir
        assert call_args[0][2] == TABLES

    def test_base_only_mode_calls_parallel(self, tmp_path) -> None:
        """--base-only mode calls _import_tables_parallel with base tables."""
        from unittest.mock import MagicMock, patch

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        mock_conn = MagicMock()

        with (
            patch(
                "sys.argv",
                ["import_csv.py", "--base-only", str(csv_dir), "postgresql:///test"],
            ),
            patch.object(_ic.psycopg, "connect", return_value=mock_conn),
            patch.object(_ic, "_import_tables_parallel", return_value=100) as mock_parallel,
            patch.object(_ic, "import_artwork", return_value=10),
            patch.object(_ic, "populate_release_year", return_value=50),
            patch.object(_ic, "populate_cache_metadata", return_value=50),
            patch.object(_ic, "create_track_count_table", return_value=20),
            patch.object(_ic, "import_artist_details", return_value=20),
        ):
            _ic.main()

        mock_parallel.assert_called_once()
        call_args = mock_parallel.call_args
        assert call_args[0][0] == "postgresql:///test"
        assert call_args[0][1] == csv_dir
        # Parent tables are BASE_TABLES[:1], child tables are BASE_TABLES[1:]
        assert call_args[1]["parent_tables"] == BASE_TABLES[:1]
        assert call_args[1]["child_tables"] == BASE_TABLES[1:]

    def test_tracks_only_mode_uses_release_id_filter(self, tmp_path) -> None:
        """--tracks-only mode queries release IDs and filters track import."""
        from unittest.mock import MagicMock, patch

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(5001,), (5002,), (5003,)]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "sys.argv",
                ["import_csv.py", "--tracks-only", str(csv_dir), "postgresql:///test"],
            ),
            patch.object(_ic.psycopg, "connect", return_value=mock_conn),
            patch.object(_ic, "_import_tables_parallel", return_value=200) as mock_parallel,
        ):
            _ic.main()

        mock_parallel.assert_called_once()
        call_args = mock_parallel.call_args
        # --tracks-only passes empty parent_tables and TRACK_TABLES as children
        assert call_args[1]["parent_tables"] == []
        assert call_args[1]["child_tables"] == TRACK_TABLES
        assert call_args[1]["release_id_filter"] == {5001, 5002, 5003}


# ---------------------------------------------------------------------------
# Artist table dedup and filtering
# ---------------------------------------------------------------------------


class TestArtistTablesConfig:
    """ARTIST_TABLES must have unique_key for dedup and be filtered by artist ID."""

    def test_artist_alias_has_unique_key(self) -> None:
        """artist_alias must dedup on (artist_id, alias_name)."""
        config = next(t for t in ARTIST_TABLES if t["table"] == "artist_alias")
        assert "unique_key" in config
        assert config["unique_key"] == ["artist_id", "alias_name"]

    def test_artist_member_has_unique_key(self) -> None:
        """artist_member must dedup on (group_artist_id, member_artist_id)."""
        config = next(t for t in ARTIST_TABLES if t["table"] == "artist_member")
        assert "unique_key" in config
        assert config["unique_key"] == ["group_artist_id", "member_artist_id"]


class TestReleaseTrackUniqueKey:
    """release_track must have unique_key for dedup."""

    def test_release_track_has_unique_key(self) -> None:
        """release_track must dedup on (release_id, sequence)."""
        config = next(t for t in TRACK_TABLES if t["table"] == "release_track")
        assert "unique_key" in config
        assert config["unique_key"] == ["release_id", "sequence"]


class TestImportArtistDetailsFiltersById:
    """import_artist_details must filter artist tables to known artist IDs."""

    def test_filters_artist_tables_by_artist_id(self, tmp_path) -> None:
        """Only rows with artist_id in the artist table should be imported."""
        from unittest.mock import MagicMock, patch

        # Create dummy CSVs
        alias_csv = tmp_path / "artist_alias.csv"
        alias_csv.write_text("artist_id,alias_name\n1,Known Alias\n999,Unknown Alias\n")
        member_csv = tmp_path / "artist_member.csv"
        member_csv.write_text(
            "group_artist_id,member_artist_id,member_name\n1,2,Member A\n999,3,Member B\n"
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Simulate artist table with only artist_id=1
        mock_cursor.rowcount = 1
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(_ic, "_import_tables") as mock_import:
            mock_import.return_value = 1
            import_artist_details(mock_conn, tmp_path)

        # _import_tables should be called with an artist_id_filter
        mock_import.assert_called_once()
        call_kwargs = mock_import.call_args
        assert "artist_id_filter" in call_kwargs[1] or (
            len(call_kwargs[0]) > 3 and call_kwargs[0][3] is not None
        )
