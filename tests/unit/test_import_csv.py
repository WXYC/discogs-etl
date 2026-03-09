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
TableConfig = _ic.TableConfig
_import_tables_parallel = _ic._import_tables_parallel

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

    def test_release_artist_table_includes_artist_id(self) -> None:
        """The release_artist table must import artist_id for alias-enhanced filtering."""
        ra_config = next(t for t in TABLES if t["table"] == "release_artist")
        assert "artist_id" in ra_config["csv_columns"]
        assert "artist_id" in ra_config["db_columns"]

    def test_release_table_transforms_released_to_year(self) -> None:
        """The released field should be transformed via extract_year."""
        release_config = next(t for t in TABLES if t["table"] == "release")
        assert "released" in release_config["transforms"]
        assert release_config["transforms"]["released"] is extract_year

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

    def test_tables_with_unique_constraints_have_unique_key(self) -> None:
        """Tables with unique constraints must specify unique_key for dedup during import."""
        tables_needing_dedup = {"release_artist", "release_label", "release_track_artist"}
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
        assert names == ["release", "release_artist", "release_label"]

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
