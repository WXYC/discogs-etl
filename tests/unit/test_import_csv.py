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
VIDEO_TABLES = _ic.VIDEO_TABLES
ARTIST_TABLES = _ic.ARTIST_TABLES
TableConfig = _ic.TableConfig
_import_tables_parallel = _ic._import_tables_parallel
import_artist_details = _ic.import_artist_details

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"


# ---------------------------------------------------------------------------
# populate_cache_metadata race-tolerance
# ---------------------------------------------------------------------------


class TestReleaseArtistRoleColumn:
    """``release_artist.role`` is read via ``optional_csv_columns``: present
    in the converter's CSV header → loaded; absent (older CSV) → PG default
    NULL. It must stay OUT of the required ``csv_columns`` / ``db_columns``
    so a pre-role CSV still imports instead of bailing with "Missing columns"
    and writing zero rows (the #204 failure mode).

    The converter began emitting release-level ``<role>`` so release-level
    writer/composer credits reach ``release_artist.role`` (the release-level
    composer fallback, WXYC/library-metadata-lookup#699), mirroring the
    ``release_track_artist`` handling from WXYC/discogs-etl#218.
    """

    def test_role_declared_as_optional_csv_column(self) -> None:
        ra_config = next(t for t in BASE_TABLES if t["table"] == "release_artist")
        assert ra_config.get("optional_csv_columns") == ["role"]

    def test_role_stays_out_of_required_columns(self) -> None:
        """Role is optional, not required: keeping it out of csv_columns /
        db_columns is what lets a pre-role CSV still import (#204)."""
        ra_config = next(t for t in BASE_TABLES if t["table"] == "release_artist")
        assert "role" not in ra_config["csv_columns"]
        assert "role" not in ra_config["db_columns"]


class TestPopulateCacheMetadataRaceTolerance:
    """``cache_metadata`` is concurrently written by the live LML service:
    on every Discogs API miss, LML's ``discogs/cache_service.py`` inserts an
    ``'api_fetch'`` row. During a rebuild, those concurrent writes race the
    bulk populate and cause duplicate-key violations on COPY.

    The fix uses INSERT ... ON CONFLICT DO NOTHING so the bulk populate
    skips rows LML has already inserted. Surfaced in the 2026-05-13
    21:32 UTC rebuild run (#188), where 52 ``'api_fetch'`` rows appeared
    in the 34-second window between TRUNCATE and ``populate_cache_metadata``.
    """

    def test_uses_insert_on_conflict_not_copy(self) -> None:
        """The function emits an INSERT ... ON CONFLICT DO NOTHING statement,
        not a COPY. The race with LML's runtime cache writes requires the
        ON CONFLICT semantics that COPY can't provide."""
        from unittest.mock import MagicMock

        mock_cursor = MagicMock()
        mock_cursor.rowcount = 50
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        _ic.populate_cache_metadata(mock_conn)

        # cur.copy should NEVER be called — that's the failure mode we're fixing.
        mock_cursor.copy.assert_not_called()
        # cur.execute should be called with an INSERT ... ON CONFLICT statement.
        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO cache_metadata" in sql
        assert "ON CONFLICT" in sql
        assert "DO NOTHING" in sql

    def test_insert_uses_bulk_import_source(self) -> None:
        """The bulk populate's source column must be 'bulk_import' so
        post-rebuild analytics can distinguish bulk-loaded rows from
        LML's runtime ``'api_fetch'`` writes."""
        from unittest.mock import MagicMock

        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        _ic.populate_cache_metadata(mock_conn)

        sql = mock_cursor.execute.call_args[0][0]
        assert "'bulk_import'" in sql

    def test_targets_release_id_conflict(self) -> None:
        """ON CONFLICT must reference (release_id), the cache_metadata pkey."""
        from unittest.mock import MagicMock

        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        _ic.populate_cache_metadata(mock_conn)

        sql = mock_cursor.execute.call_args[0][0]
        # The ON CONFLICT target is the primary key (release_id).
        assert "ON CONFLICT (release_id)" in sql

    def test_commits_on_success(self) -> None:
        """The function commits the transaction so the inserted rows are
        durable before the next pipeline step starts."""
        from unittest.mock import MagicMock

        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        _ic.populate_cache_metadata(mock_conn)

        mock_conn.commit.assert_called_once()

    def test_returns_rowcount(self) -> None:
        """The return value is the number of rows actually inserted
        (cur.rowcount after INSERT). ON CONFLICT skips don't count, which is
        the right denominator for the log line that reports the populate's
        effect."""
        from unittest.mock import MagicMock

        mock_cursor = MagicMock()
        mock_cursor.rowcount = 677_934
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        assert _ic.populate_cache_metadata(mock_conn) == 677_934


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
    """TABLES is the union of BASE_TABLES, TRACK_TABLES, and VIDEO_TABLES."""

    def test_tables_is_union(self) -> None:
        assert TABLES == BASE_TABLES + TRACK_TABLES + VIDEO_TABLES

    def test_base_tables_names(self) -> None:
        names = [t["table"] for t in BASE_TABLES]
        assert names == [
            "release",
            "release_artist",
            "release_label",
            "release_genre",
            "release_style",
        ]

    def test_track_tables_are_release_track_and_release_track_artist(self) -> None:
        names = [t["table"] for t in TRACK_TABLES]
        assert names == ["release_track", "release_track_artist"]

    def test_video_tables_are_release_video(self) -> None:
        names = [t["table"] for t in VIDEO_TABLES]
        assert names == ["release_video"]

    def test_no_overlap(self) -> None:
        base_names = {t["table"] for t in BASE_TABLES}
        track_names = {t["table"] for t in TRACK_TABLES}
        video_names = {t["table"] for t in VIDEO_TABLES}
        assert base_names.isdisjoint(track_names)
        assert base_names.isdisjoint(video_names)
        assert track_names.isdisjoint(video_names)


# ---------------------------------------------------------------------------
# import_csv missing columns
# ---------------------------------------------------------------------------


class TestImportCsvOptionalColumns:
    """``optional_csv_columns`` lets the loader tolerate both old and new
    converter output for ``release_track_artist`` (WXYC/discogs-etl#218).

    When the converter ships the new ``extra`` / ``role`` columns, they
    appear in the CSV header and the loader appends them to the COPY
    column list. When the converter omits them (pre-#55 output), the
    loader drops them silently and lets the DB-side defaults populate.
    """

    def test_new_csv_with_optional_columns_appends_them_to_copy(self, tmp_path) -> None:
        """When the CSV header carries the optional columns, they are
        appended to the COPY column list and the values are written."""
        csv_path = tmp_path / "release_track_artist.csv"
        csv_path.write_text(
            "release_id,track_sequence,artist_name,extra,role\n"
            "674529,5,The Orb,0,\n"
            "674529,5,Alex Paterson,1,Producer\n"
        )

        conn, captured = _recording_conn()
        count = import_csv(
            conn,
            csv_path,
            table="release_track_artist",
            csv_columns=["release_id", "track_sequence", "artist_name"],
            db_columns=["release_id", "track_sequence", "artist_name"],
            required_columns=["release_id", "track_sequence"],
            transforms={},
            optional_csv_columns=["extra", "role"],
        )

        assert count == 2
        # COPY SQL lists the optional columns
        assert "release_id, track_sequence, artist_name, extra, role" in captured["sql"]
        # Row values include the extra/role data
        assert captured["rows"][0] == ("674529", "5", "The Orb", "0", None)
        assert captured["rows"][1] == ("674529", "5", "Alex Paterson", "1", "Producer")

    def test_legacy_csv_without_optional_columns_still_imports(self, tmp_path) -> None:
        """A 3-column CSV from a pre-#55 converter must still load. The
        loader drops the optional columns from the COPY so the PG defaults
        (``extra=0``, ``role=NULL``) take over for absent values."""
        csv_path = tmp_path / "release_track_artist.csv"
        csv_path.write_text("release_id,track_sequence,artist_name\n674529,5,Alex Paterson\n")

        conn, captured = _recording_conn()
        count = import_csv(
            conn,
            csv_path,
            table="release_track_artist",
            csv_columns=["release_id", "track_sequence", "artist_name"],
            db_columns=["release_id", "track_sequence", "artist_name"],
            required_columns=["release_id", "track_sequence"],
            transforms={},
            optional_csv_columns=["extra", "role"],
        )

        assert count == 1
        # COPY SQL omits the optional columns; PG defaults take over.
        assert "extra" not in captured["sql"]
        assert "role" not in captured["sql"]
        # Row has only the 3 base columns.
        assert captured["rows"][0] == ("674529", "5", "Alex Paterson")

    def test_release_track_artist_table_config_declares_extra_and_role(self) -> None:
        """Pin that the table config opts into the optional columns. If a
        future change drops ``optional_csv_columns`` here, the loader will
        silently stop populating ``extra`` / ``role`` from new converter
        output — a regression of WXYC/discogs-etl#218."""
        rta_config = next(t for t in TRACK_TABLES if t["table"] == "release_track_artist")
        assert rta_config.get("optional_csv_columns") == ["extra", "role"]

    def test_new_release_artist_csv_with_role_appends_it_to_copy(self, tmp_path) -> None:
        """A ``release_artist`` CSV carrying release-level ``role`` loads it;
        an empty role coerces to NULL, and main artists (extra=0) carry none.
        This is the release-level analogue of the per-track case above
        (WXYC/library-metadata-lookup#699)."""
        csv_path = tmp_path / "release_artist.csv"
        csv_path.write_text(
            "release_id,artist_id,artist_name,extra,role\n"
            "9100,10,Stereolab,0,\n"
            "9100,20,Tim Gane,1,Written-By\n"
        )

        conn, captured = _recording_conn()
        count = import_csv(
            conn,
            csv_path,
            table="release_artist",
            csv_columns=["release_id", "artist_id", "artist_name", "extra"],
            db_columns=["release_id", "artist_id", "artist_name", "extra"],
            required_columns=["release_id", "artist_name"],
            transforms={},
            optional_csv_columns=["role"],
        )

        assert count == 2
        # COPY SQL lists the optional role column.
        assert "release_id, artist_id, artist_name, extra, role" in captured["sql"]
        # Main artist: empty role → NULL. Writer credit: source role preserved.
        assert captured["rows"][0] == ("9100", "10", "Stereolab", "0", None)
        assert captured["rows"][1] == ("9100", "20", "Tim Gane", "1", "Written-By")

    def test_legacy_release_artist_csv_without_role_still_imports(self, tmp_path) -> None:
        """A pre-role ``release_artist`` CSV (no ``role`` column) must still
        import: the loader drops ``role`` from the COPY so the schema default
        (NULL) takes over, instead of bailing with "Missing columns" and
        writing zero rows. This is the exact #204 failure mode that left
        ``release_artist`` empty in the 2026-05-13 rebuild, pinned at the
        behavioral level (the config-pin tests above never run the loader).
        Release-level analogue of ``test_legacy_csv_without_optional_columns_still_imports``."""
        csv_path = tmp_path / "release_artist.csv"
        csv_path.write_text("release_id,artist_id,artist_name,extra\n9100,10,Stereolab,0\n")

        conn, captured = _recording_conn()
        count = import_csv(
            conn,
            csv_path,
            table="release_artist",
            csv_columns=["release_id", "artist_id", "artist_name", "extra"],
            db_columns=["release_id", "artist_id", "artist_name", "extra"],
            required_columns=["release_id", "artist_name"],
            transforms={},
            optional_csv_columns=["role"],
        )

        assert count == 1
        # COPY SQL omits the absent optional column; the schema NULL default takes over.
        assert "role" not in captured["sql"]
        # Row has only the 4 base columns.
        assert captured["rows"][0] == ("9100", "10", "Stereolab", "0")


def _recording_conn():
    """Build a MagicMock conn whose ``cur.copy(...)`` records the COPY SQL and
    every written row.

    Returns ``(conn, captured)`` where ``captured["sql"]`` is the COPY
    statement and ``captured["rows"]`` is the list of written row tuples,
    both populated as ``import_csv`` runs. Mirrors the inline
    ``_RecordingCopy`` spy used by ``TestImportCsvOptionalColumns``.
    """
    from unittest.mock import MagicMock

    captured: dict = {"sql": None, "rows": []}

    class _RecordingCopy:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def write_row(self, row):
            captured["rows"].append(tuple(row))

    def _cur_copy(sql, *_):
        captured["sql"] = sql
        return _RecordingCopy()

    mock_cursor = MagicMock()
    mock_cursor.copy.side_effect = _cur_copy
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, captured


class TestImportCsvExtraDedupKey:
    """WXYC/discogs-etl#293: the loader must keep a role-bearing ``extra=1``
    row that collides on name with a same-release (or same-track) ``extra=0``
    main-artist row.

    The converter writes the main-artist row (``extra=0``) before the
    extra-credit row (``extra=1``); the loader keeps the *first* occurrence of
    each ``unique_key``. With ``extra`` absent from the key, a singer-songwriter
    who is also ``Written-By`` lost the ``extra=1`` row — and its ``role``.
    Widening the dedup key on ``extra`` converges the CSV->loader path (prod)
    with the converter's direct-PG dedup (discogs-xml-converter#74's
    ``WideArtistDedup`` / ``WideTrackArtistDedup``).

    The same-name collision is the load-bearing fixture: the existing
    optional-column tests use *two different* names (``The Orb`` / ``Alex
    Paterson``), which never collide and so cannot reproduce the loss. These
    tests use a self-credit collision (``Jessica Pratt`` at both ``extra=0``
    and ``extra=1``), and pass a real ``unique_key`` so the dedup branch
    actually runs.
    """

    def test_release_artist_keeps_same_name_extra_row(self, tmp_path) -> None:
        """A ``release_artist`` CSV with the same name at ``extra=0`` and
        ``extra=1`` (role) keeps **both** rows; the writer credit's ``role``
        survives. Driven from the real config so the static ``unique_key``
        widening is what makes this pass. RED on current ``main`` (1 row, role
        lost), GREEN after widening the key with ``extra``."""
        ra_config = next(t for t in BASE_TABLES if t["table"] == "release_artist")

        csv_path = tmp_path / "release_artist.csv"
        csv_path.write_text(
            "release_id,artist_id,artist_name,extra,role\n"
            "5512,1,Jessica Pratt,0,\n"
            "5512,1,Jessica Pratt,1,Written-By\n"
        )

        conn, captured = _recording_conn()
        count = import_csv(
            conn,
            csv_path,
            table="release_artist",
            csv_columns=ra_config["csv_columns"],
            db_columns=ra_config["db_columns"],
            required_columns=ra_config["required"],
            transforms=ra_config["transforms"],
            unique_key=ra_config["unique_key"],
            optional_csv_columns=ra_config.get("optional_csv_columns"),
        )

        assert count == 2
        assert captured["rows"][0] == ("5512", "1", "Jessica Pratt", "0", None)
        assert captured["rows"][1] == ("5512", "1", "Jessica Pratt", "1", "Written-By")

    def test_release_track_artist_keeps_same_name_extra_row(self, tmp_path) -> None:
        """A ``release_track_artist`` CSV with the same name at one
        ``(release_id, track_sequence)`` and both ``extra`` values keeps both
        rows with the role preserved. Driven from the real config: the static
        key stays ``(release_id, track_sequence, artist_name)`` and is widened
        with ``extra`` only because the header carries it and the config opts
        in via ``optional_unique_key``.

        Note the RED differs from the ``release_artist`` case: on current
        ``main`` this raises ``TypeError`` (the ``optional_unique_key`` param
        does not exist yet), so it is the *missing param*, not the data loss,
        that reds here. What it pins going forward is the conditional widening
        itself — drop that logic but keep the param (a partial revert) and the
        key falls back to ``(release_id, track_sequence, artist_name)``, the
        two rows collapse, and ``assert count == 2`` fails."""
        rta_config = next(t for t in TRACK_TABLES if t["table"] == "release_track_artist")

        csv_path = tmp_path / "release_track_artist.csv"
        csv_path.write_text(
            "release_id,track_sequence,artist_name,extra,role\n"
            "5512,3,Jessica Pratt,0,\n"
            "5512,3,Jessica Pratt,1,Written-By\n"
        )

        conn, captured = _recording_conn()
        count = import_csv(
            conn,
            csv_path,
            table="release_track_artist",
            csv_columns=rta_config["csv_columns"],
            db_columns=rta_config["db_columns"],
            required_columns=rta_config["required"],
            transforms=rta_config["transforms"],
            unique_key=rta_config["unique_key"],
            optional_csv_columns=rta_config.get("optional_csv_columns"),
            optional_unique_key=rta_config.get("optional_unique_key"),
        )

        assert count == 2
        assert captured["rows"][0] == ("5512", "3", "Jessica Pratt", "0", None)
        assert captured["rows"][1] == ("5512", "3", "Jessica Pratt", "1", "Written-By")

    def test_release_track_artist_legacy_csv_without_extra_still_imports(self, tmp_path) -> None:
        """The asymmetry guard: a legacy 3-column ``release_track_artist`` CSV
        (no ``extra`` column) must still load even though the config now sets
        ``optional_unique_key=["extra"]``. Because ``extra`` is absent from the
        header it never joins the dedup key, so ``csv_columns.index("extra")``
        is never reached and no ``ValueError`` is raised. The key falls back to
        ``(release_id, track_sequence, artist_name)``, so a genuine duplicate
        still collapses, and the COPY omits ``extra`` / ``role`` (PG defaults
        apply). This is RED if anyone reverts the conditional path to a static
        ``extra`` key."""
        rta_config = next(t for t in TRACK_TABLES if t["table"] == "release_track_artist")

        csv_path = tmp_path / "release_track_artist.csv"
        csv_path.write_text(
            "release_id,track_sequence,artist_name\n5512,3,Jessica Pratt\n5512,3,Jessica Pratt\n"
        )

        conn, captured = _recording_conn()
        count = import_csv(
            conn,
            csv_path,
            table="release_track_artist",
            csv_columns=rta_config["csv_columns"],
            db_columns=rta_config["db_columns"],
            required_columns=rta_config["required"],
            transforms=rta_config["transforms"],
            unique_key=rta_config["unique_key"],
            optional_csv_columns=rta_config.get("optional_csv_columns"),
            optional_unique_key=rta_config.get("optional_unique_key"),
        )

        # Fallback dedup on (release_id, track_sequence, artist_name) collapses
        # the two identical rows to one — no crash on the missing column.
        assert count == 1
        assert "extra" not in captured["sql"]
        assert "role" not in captured["sql"]
        assert captured["rows"][0] == ("5512", "3", "Jessica Pratt")

    def test_release_artist_genuine_duplicate_still_collapses(self, tmp_path) -> None:
        """Over-widening guard: two rows identical on the key ``(release_id,
        artist_name, extra)`` collapse to one, keeping the first. The two rows
        deliberately *differ* on the non-key columns ``artist_id`` (1 vs 2) and
        ``role`` (``Vocals`` vs ``Guitar``) so that widening the key onto
        either of those columns by mistake would split them into two rows and
        fail this test — i.e. this catches over-widening on a column whose
        value varies, not just on ``extra`` (which is constant here and already
        in the key)."""
        ra_config = next(t for t in BASE_TABLES if t["table"] == "release_artist")

        csv_path = tmp_path / "release_artist.csv"
        csv_path.write_text(
            "release_id,artist_id,artist_name,extra,role\n"
            "5512,1,Jessica Pratt,0,Vocals\n"
            "5512,2,Jessica Pratt,0,Guitar\n"
        )

        conn, captured = _recording_conn()
        count = import_csv(
            conn,
            csv_path,
            table="release_artist",
            csv_columns=ra_config["csv_columns"],
            db_columns=ra_config["db_columns"],
            required_columns=ra_config["required"],
            transforms=ra_config["transforms"],
            unique_key=ra_config["unique_key"],
            optional_csv_columns=ra_config.get("optional_csv_columns"),
        )

        assert count == 1
        # First occurrence wins: the ``artist_id=1`` / ``role=Vocals`` row.
        assert captured["rows"][0] == ("5512", "1", "Jessica Pratt", "0", "Vocals")

    def test_release_artist_different_names_kept_distinct(self, tmp_path) -> None:
        """Sanity: two genuinely different artists at the same ``extra`` are
        not duplicates and both survive — confirms the key was widened, not
        broken."""
        ra_config = next(t for t in BASE_TABLES if t["table"] == "release_artist")

        csv_path = tmp_path / "release_artist.csv"
        csv_path.write_text(
            "release_id,artist_id,artist_name,extra,role\n"
            "5512,1,Stereolab,0,\n"
            "5512,2,Cat Power,0,\n"
        )

        conn, captured = _recording_conn()
        count = import_csv(
            conn,
            csv_path,
            table="release_artist",
            csv_columns=ra_config["csv_columns"],
            db_columns=ra_config["db_columns"],
            required_columns=ra_config["required"],
            transforms=ra_config["transforms"],
            unique_key=ra_config["unique_key"],
            optional_csv_columns=ra_config.get("optional_csv_columns"),
        )

        # Both distinct artists survive intact — assert the rows, not just the
        # count, so a bug that wrote one row twice or mangled a name is caught.
        assert count == 2
        assert captured["rows"][0] == ("5512", "1", "Stereolab", "0", None)
        assert captured["rows"][1] == ("5512", "2", "Cat Power", "0", None)

    def test_release_artist_config_unique_key_includes_extra(self) -> None:
        """Config pin: ``release_artist`` dedups on ``extra`` so a future edit
        that drops it from the static key trips this test rather than silently
        reintroducing the loss. ``extra`` is a hard ``csv_columns`` entry, so
        the static key is safe."""
        ra_config = next(t for t in BASE_TABLES if t["table"] == "release_artist")
        assert ra_config["unique_key"] == ["release_id", "artist_name", "extra"]

    def test_release_track_artist_config_declares_optional_unique_key(self) -> None:
        """Config pin: ``release_track_artist`` opts into widening the dedup key
        with ``extra`` only when the column is present (the conditional path
        that keeps legacy 3-column CSVs loading). Mirrors
        ``WideTrackArtistDedup`` in discogs-xml-converter#74."""
        rta_config = next(t for t in TRACK_TABLES if t["table"] == "release_track_artist")
        assert rta_config.get("optional_unique_key") == ["extra"]


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
            patch.object(_ic, "import_masters", return_value=10),
        ):
            _ic.main()

        mock_import.assert_called_once()
        call_args = mock_import.call_args
        assert call_args[0][0] is mock_conn
        assert call_args[0][1] == csv_dir
        assert call_args[0][2] == TABLES

    def test_base_only_mode_calls_parallel(self, tmp_path) -> None:
        """``--base-only`` (default, no --truncate-existing): the release
        parent is loaded via ``import_release_via_upsert`` to preserve
        LML-back-patched artwork columns (WXYC/discogs-etl#242), then
        children are COPYed via ``_import_tables_parallel`` with empty
        ``parent_tables`` (release is already in place from the upsert).
        """
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
            patch.object(_ic, "import_release_via_upsert", return_value=50) as mock_upsert,
            patch.object(_ic, "import_artwork", return_value=10),
            patch.object(_ic, "populate_release_year", return_value=50),
            patch.object(_ic, "populate_cache_metadata", return_value=50),
            patch.object(_ic, "create_track_count_table", return_value=20),
            patch.object(_ic, "import_artist_details", return_value=20),
        ):
            _ic.main()

        mock_upsert.assert_called_once()
        upsert_args = mock_upsert.call_args
        assert upsert_args[0][1] == csv_dir

        mock_parallel.assert_called_once()
        call_args = mock_parallel.call_args
        assert call_args[0][0] == "postgresql:///test"
        assert call_args[0][1] == csv_dir
        # Default incremental path: release is loaded by import_release_via_upsert
        # (so parent_tables is empty); children COPY in parallel as before.
        assert call_args[1]["parent_tables"] == []
        assert call_args[1]["child_tables"] == BASE_TABLES[1:]

    def test_base_only_with_truncate_existing_uses_legacy_parallel(self, tmp_path) -> None:
        """``--base-only --truncate-existing`` (operator-visible escape
        hatch): TRUNCATE wiped release, so the legacy straight-COPY path
        runs — release goes through ``_import_tables_parallel`` as the
        sole parent, and ``import_release_via_upsert`` is NOT called.
        Pins the contract that --truncate-existing's wipe-and-reload
        semantics survive Option B."""
        from unittest.mock import MagicMock, patch

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        mock_conn = MagicMock()

        with (
            patch(
                "sys.argv",
                [
                    "import_csv.py",
                    "--base-only",
                    "--truncate-existing",
                    str(csv_dir),
                    "postgresql:///test",
                ],
            ),
            patch.object(_ic.psycopg, "connect", return_value=mock_conn),
            patch.object(_ic, "_import_tables_parallel", return_value=100) as mock_parallel,
            patch.object(_ic, "import_release_via_upsert") as mock_upsert,
            patch.object(_ic, "_truncate_tables"),
            patch.object(_ic, "import_artwork", return_value=10),
            patch.object(_ic, "populate_release_year", return_value=50),
            patch.object(_ic, "populate_cache_metadata", return_value=50),
            patch.object(_ic, "create_track_count_table", return_value=20),
            patch.object(_ic, "import_artist_details", return_value=20),
        ):
            _ic.main()

        mock_upsert.assert_not_called()
        mock_parallel.assert_called_once()
        call_args = mock_parallel.call_args
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
        # --tracks-only passes empty parent_tables and TRACK_TABLES + VIDEO_TABLES as children
        assert call_args[1]["parent_tables"] == []
        assert call_args[1]["child_tables"] == TRACK_TABLES + VIDEO_TABLES
        assert call_args[1]["release_id_filter"] == {5001, 5002, 5003}

    def test_masters_only_mode_calls_import_masters(self, tmp_path) -> None:
        """``--masters-only`` imports ONLY masters — none of the
        release/artist/label helpers run. This is the mode the one-time prod
        import + any ad-hoc masters refresh use (WXYC/discogs-etl#317)."""
        from unittest.mock import MagicMock, patch

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        mock_conn = MagicMock()

        with (
            patch(
                "sys.argv",
                ["import_csv.py", "--masters-only", str(csv_dir), "postgresql:///test"],
            ),
            patch.object(_ic.psycopg, "connect", return_value=mock_conn),
            patch.object(_ic, "import_masters", return_value=42) as mock_masters,
            patch.object(_ic, "_import_tables") as mock_import_tables,
            patch.object(_ic, "_import_tables_parallel") as mock_parallel,
            patch.object(_ic, "import_release_via_upsert") as mock_upsert,
            patch.object(_ic, "import_artist_details") as mock_artist,
        ):
            _ic.main()

        mock_masters.assert_called_once()
        mc_args = mock_masters.call_args
        assert mc_args[0][0] is mock_conn
        assert mc_args[0][1] == csv_dir
        # Nothing that writes release/artist/label runs in masters-only mode.
        mock_import_tables.assert_not_called()
        mock_parallel.assert_not_called()
        mock_upsert.assert_not_called()
        mock_artist.assert_not_called()

    def test_masters_only_is_mutually_exclusive_with_base_only(self, tmp_path) -> None:
        """``--masters-only`` joins the existing mode group, so combining it with
        another mode is an argparse error (SystemExit 2)."""
        from unittest.mock import patch

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        with (
            patch(
                "sys.argv",
                ["import_csv.py", "--masters-only", "--base-only", str(csv_dir)],
            ),
            pytest.raises(SystemExit),
        ):
            _ic.main()

    def test_masters_only_with_truncate_existing_does_not_wipe_base(self, tmp_path) -> None:
        """``import_masters`` self-truncates only its two tables, so the
        top-level ``--truncate-existing`` base wipe — which includes
        ``release`` — must NOT fire in ``--masters-only`` mode. Guards against
        an ad-hoc ``--masters-only --truncate-existing`` blowing away the cache
        (WXYC/discogs-etl#317)."""
        from unittest.mock import MagicMock, patch

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        mock_conn = MagicMock()

        with (
            patch(
                "sys.argv",
                [
                    "import_csv.py",
                    "--masters-only",
                    "--truncate-existing",
                    str(csv_dir),
                    "postgresql:///test",
                ],
            ),
            patch.object(_ic.psycopg, "connect", return_value=mock_conn),
            patch.object(_ic, "import_masters", return_value=42),
            patch.object(_ic, "_truncate_tables") as mock_truncate,
        ):
            _ic.main()

        mock_truncate.assert_not_called()


# ---------------------------------------------------------------------------
# --truncate-existing flag
# ---------------------------------------------------------------------------


class TestTruncateExisting:
    """``--truncate-existing`` wipes the cache tables before COPY so a partial
    state from a prior failed rebuild doesn't fail the import with a
    duplicate-key violation. Excludes entity.identity (WXYC-side data) and
    alembic_version (migration history).
    """

    def test_base_truncate_list_excludes_entity_schema(self) -> None:
        """The entity schema holds the WXYC-side identity records — they must
        survive a rebuild. Neither truncate set may target it."""
        for name in _ic.CACHE_TABLES_TO_TRUNCATE_BASE:
            assert not name.startswith("entity."), (
                f"entity-schema table {name!r} must not be in CACHE_TABLES_TO_TRUNCATE_BASE"
            )
        for name in _ic.CACHE_TABLES_TO_TRUNCATE_TRACKS:
            assert not name.startswith("entity."), (
                f"entity-schema table {name!r} must not be in CACHE_TABLES_TO_TRUNCATE_TRACKS"
            )

    def test_truncate_lists_exclude_alembic_version(self) -> None:
        """alembic_version tracks migration history and must persist across
        rebuilds. Truncating it would break the dual-write convention."""
        assert "alembic_version" not in _ic.CACHE_TABLES_TO_TRUNCATE_BASE
        assert "alembic_version" not in _ic.CACHE_TABLES_TO_TRUNCATE_TRACKS

    def test_base_truncate_list_covers_full_cache(self) -> None:
        """The base set wipes the FULL cache so a rerun after a partial
        failed --base-only attempt clears everything from any prior state."""
        expected_tables = {
            # BASE_TABLES
            "release",
            "release_artist",
            "release_label",
            "release_genre",
            "release_style",
            # TRACK_TABLES + VIDEO_TABLES (cleared because partial prior tracks would conflict too)
            "release_track",
            "release_track_artist",
            "release_video",
            # ARTIST_TABLES + import_artist_details stub-row target
            "artist",
            "artist_alias",
            "artist_member",
            "artist_name_variation",
            "artist_url",
            # MASTER_TABLES
            "master",
            "master_artist",
            # populate_cache_metadata target
            "cache_metadata",
        }
        actual = set(_ic.CACHE_TABLES_TO_TRUNCATE_BASE)
        assert actual == expected_tables, (
            f"missing: {expected_tables - actual}, extra: {actual - expected_tables}"
        )

    def test_tracks_truncate_list_is_tracks_subset_only(self) -> None:
        """The tracks set wipes ONLY track-domain tables. In a full pipeline
        run the tracks step runs AFTER base+dedup; wiping base tables here
        would erase the deduped data and the SELECT id FROM release filter
        would find zero rows."""
        expected_tables = {
            "release_track",
            "release_track_artist",
            "release_video",
        }
        actual = set(_ic.CACHE_TABLES_TO_TRUNCATE_TRACKS)
        assert actual == expected_tables, (
            f"missing: {expected_tables - actual}, extra: {actual - expected_tables}"
        )

    def test_tracks_truncate_list_does_not_include_base_tables(self) -> None:
        """Explicit guard: the tracks set must not include any base table,
        because the orchestrator runs tracks AFTER base+dedup. This is the
        regression that the pre-PR-review hook caught on 2026-05-13."""
        base_only_tables = {
            "release",
            "release_artist",
            "release_label",
            "release_genre",
            "release_style",
            "artist",
            "artist_alias",
            "artist_member",
            "artist_name_variation",
            "artist_url",
            "master",
            "master_artist",
            "cache_metadata",
        }
        leaked = base_only_tables & set(_ic.CACHE_TABLES_TO_TRUNCATE_TRACKS)
        assert not leaked, f"--tracks-only --truncate-existing must not wipe base tables: {leaked}"

    def test_truncate_tables_issues_single_cascade_statement(self) -> None:
        """_truncate_tables emits one TRUNCATE ... CASCADE statement. One
        statement keeps the operation atomic and CASCADE handles FK
        dependencies for any table not in the explicit list."""
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        _ic._truncate_tables(mock_conn, ["release", "release_artist"])

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "TRUNCATE" in sql.upper()
        assert "CASCADE" in sql.upper()
        assert "release" in sql
        assert "release_artist" in sql

    def test_truncate_tables_commits_immediately(self) -> None:
        """The truncate must commit on its own conn before any parallel
        workers open new conns — otherwise MVCC isolation would let workers
        still see the pre-TRUNCATE rows and the COPY would still fail."""
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        _ic._truncate_tables(mock_conn, ["release"])

        mock_conn.commit.assert_called_once()

    def test_truncate_tables_empty_list_is_noop(self) -> None:
        """Defensive: empty table list short-circuits without issuing SQL."""
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        _ic._truncate_tables(mock_conn, [])

        mock_conn.cursor.assert_not_called()
        mock_conn.commit.assert_not_called()

    def test_flag_with_base_only_truncates_full_cache_set(self, tmp_path) -> None:
        """--base-only --truncate-existing wipes the FULL cache (the BASE
        set) so a partial failed run is fully cleared before reimport."""
        from unittest.mock import MagicMock, patch

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        mock_conn = MagicMock()

        with (
            patch(
                "sys.argv",
                [
                    "import_csv.py",
                    "--base-only",
                    "--truncate-existing",
                    str(csv_dir),
                    "postgresql:///test",
                ],
            ),
            patch.object(_ic.psycopg, "connect", return_value=mock_conn),
            patch.object(_ic, "_truncate_tables") as mock_truncate,
            patch.object(_ic, "_import_tables_parallel", return_value=100),
            patch.object(_ic, "import_artwork", return_value=10),
            patch.object(_ic, "populate_release_year", return_value=50),
            patch.object(_ic, "populate_cache_metadata", return_value=50),
            patch.object(_ic, "create_track_count_table", return_value=20),
            patch.object(_ic, "import_artist_details", return_value=20),
        ):
            _ic.main()

        mock_truncate.assert_called_once()
        call_args = mock_truncate.call_args
        assert call_args[0][0] is mock_conn
        assert call_args[0][1] == _ic.CACHE_TABLES_TO_TRUNCATE_BASE

    def test_flag_with_tracks_only_truncates_tracks_subset(self, tmp_path) -> None:
        """--tracks-only --truncate-existing wipes ONLY the track-domain
        tables, preserving base+dedup output. This is the bug the
        pre-PR-review hook caught: an earlier draft passed the full cache
        set unconditionally, so a tracks rerun erased the deduped base
        data and tracks ended up empty."""
        from unittest.mock import MagicMock, patch

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(5001,)]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "sys.argv",
                [
                    "import_csv.py",
                    "--tracks-only",
                    "--truncate-existing",
                    str(csv_dir),
                    "postgresql:///test",
                ],
            ),
            patch.object(_ic.psycopg, "connect", return_value=mock_conn),
            patch.object(_ic, "_truncate_tables") as mock_truncate,
            patch.object(_ic, "_import_tables_parallel", return_value=200),
        ):
            _ic.main()

        mock_truncate.assert_called_once()
        call_args = mock_truncate.call_args
        assert call_args[0][1] == _ic.CACHE_TABLES_TO_TRUNCATE_TRACKS

    def test_flag_omitted_does_not_truncate(self, tmp_path) -> None:
        """Default (flag absent) preserves prior behavior — no TRUNCATE on
        a fresh-DB rebuild, where the schema-create step yields empty tables
        and TRUNCATE would be wasted DDL."""
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
            patch.object(_ic, "_truncate_tables") as mock_truncate,
            patch.object(_ic, "_import_tables_parallel", return_value=100),
            patch.object(_ic, "import_artwork", return_value=10),
            patch.object(_ic, "populate_release_year", return_value=50),
            patch.object(_ic, "populate_cache_metadata", return_value=50),
            patch.object(_ic, "create_track_count_table", return_value=20),
            patch.object(_ic, "import_artist_details", return_value=20),
        ):
            _ic.main()

        mock_truncate.assert_not_called()


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

    def test_artist_name_variation_entry_exists(self) -> None:
        """ARTIST_TABLES must include artist_name_variation.

        Pre-#215 the table was never populated by the rebuild (the converter
        folded namevariations into artist_alias.csv). With the converter
        emitting a separate artist_name_variation.csv, the importer needs a
        matching config or the file would silently drop on the floor.
        """
        config = next((t for t in ARTIST_TABLES if t["table"] == "artist_name_variation"), None)
        assert config is not None, (
            "ARTIST_TABLES is missing an entry for artist_name_variation — "
            "see WXYC/discogs-etl#215 + WXYC/discogs-xml-converter#54"
        )
        assert config["csv_file"] == "artist_name_variation.csv"
        assert config["csv_columns"] == ["artist_id", "name"]
        assert config["db_columns"] == ["artist_id", "name"]
        assert "artist_id" in config["required"]
        assert "name" in config["required"]
        assert config["unique_key"] == ["artist_id", "name"]

    def test_artist_url_entry_exists(self) -> None:
        """ARTIST_TABLES must include artist_url.

        WXYC/discogs-xml-converter#68 extends the converter to extract Discogs
        `<urls>` (Wikipedia, official sites, social) into `artist_url.csv`.
        Without a matching ARTIST_TABLES entry, the file would silently drop
        on the floor at rebuild time. Step 3 of WXYC/library-metadata-lookup#497.
        """
        config = next((t for t in ARTIST_TABLES if t["table"] == "artist_url"), None)
        assert config is not None, (
            "ARTIST_TABLES is missing an entry for artist_url — "
            "see WXYC/library-metadata-lookup#497 + WXYC/discogs-xml-converter#68"
        )
        assert config["csv_file"] == "artist_url.csv"
        assert config["csv_columns"] == ["artist_id", "url"]
        assert config["db_columns"] == ["artist_id", "url"]
        assert "artist_id" in config["required"]
        assert "url" in config["required"]
        assert config["unique_key"] == ["artist_id", "url"]


class TestReleaseTrackUniqueKey:
    """release_track must have unique_key for dedup."""

    def test_release_track_has_unique_key(self) -> None:
        """release_track must dedup on (release_id, sequence)."""
        config = next(t for t in TRACK_TABLES if t["table"] == "release_track")
        assert "unique_key" in config
        assert config["unique_key"] == ["release_id", "sequence"]


class TestVideoTablesConfig:
    """VIDEO_TABLES must have correct structure for release_video import."""

    def test_video_tables_has_release_video(self) -> None:
        """VIDEO_TABLES contains exactly one entry for release_video."""
        assert len(VIDEO_TABLES) == 1
        assert VIDEO_TABLES[0]["table"] == "release_video"

    def test_release_video_csv_file(self) -> None:
        config = VIDEO_TABLES[0]
        assert config["csv_file"] == "release_video.csv"

    def test_release_video_has_all_columns(self) -> None:
        config = VIDEO_TABLES[0]
        expected = ["release_id", "sequence", "src", "title", "duration", "embed"]
        assert config["csv_columns"] == expected
        assert config["db_columns"] == expected

    def test_release_video_matching_column_lengths(self) -> None:
        config = VIDEO_TABLES[0]
        assert len(config["csv_columns"]) == len(config["db_columns"])

    def test_release_video_required_columns(self) -> None:
        """release_id and src are required; title/duration/embed are optional."""
        config = VIDEO_TABLES[0]
        assert "release_id" in config["required"]
        assert "src" in config["required"]
        assert "title" not in config["required"]
        assert "duration" not in config["required"]
        assert "embed" not in config["required"]

    def test_release_video_no_transforms(self) -> None:
        config = VIDEO_TABLES[0]
        assert config["transforms"] == {}

    def test_release_video_unique_key(self) -> None:
        """release_video must dedup on (release_id, sequence)."""
        config = VIDEO_TABLES[0]
        assert "unique_key" in config
        assert config["unique_key"] == ["release_id", "sequence"]

    def test_release_video_unique_key_subset_of_csv_columns(self) -> None:
        config = VIDEO_TABLES[0]
        key_set = set(config["unique_key"])
        csv_set = set(config["csv_columns"])
        assert key_set.issubset(csv_set)

    def test_release_video_has_all_required_keys(self) -> None:
        required_keys = {"csv_file", "table", "csv_columns", "db_columns", "required", "transforms"}
        assert required_keys.issubset(VIDEO_TABLES[0].keys())

    def test_release_video_csv_has_expected_headers(self) -> None:
        import csv as csv_mod

        csv_path = CSV_DIR / "release_video.csv"
        assert csv_path.exists(), "release_video.csv fixture must exist"
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            headers = reader.fieldnames
        assert headers is not None
        for col in VIDEO_TABLES[0]["csv_columns"]:
            assert col in headers, f"Column {col!r} missing from release_video.csv"


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


class TestImportArtistDetailsProfileCopy:
    """The profile UPDATE path in import_artist_details (LML#497) reads
    artist.csv, filters to known artist IDs, dedups via dict (last-value-wins),
    and stages the rows via COPY-to-temp + UPDATE FROM JOIN.

    These tests mock the connection so the dict-build branch executes
    end-to-end without needing a live Postgres; we capture the writes to the
    `copy.write_row` mock and the args to the staging-table SQL to verify
    behavior. The PG-backed integration test for the actual UPDATE lives in
    tests/integration/ (pg marker)."""

    def _setup_mock_conn(self, db_artist_ids: list[int]):
        """Build a MagicMock conn whose cursor's `cur.copy(...)` context yields
        a mock with a `.write_row` we can inspect, and whose first fetchall
        on `SELECT id FROM artist` returns `db_artist_ids`."""
        from unittest.mock import MagicMock

        mock_copy = MagicMock()
        mock_copy_ctx = MagicMock()
        mock_copy_ctx.__enter__ = MagicMock(return_value=mock_copy)
        mock_copy_ctx.__exit__ = MagicMock(return_value=False)

        mock_cursor = MagicMock()
        mock_cursor.copy.return_value = mock_copy_ctx
        mock_cursor.rowcount = len(db_artist_ids)
        mock_cursor.fetchall.return_value = [(aid,) for aid in db_artist_ids]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_copy

    def test_copy_filters_unknown_artist_ids(self, tmp_path) -> None:
        """artist.csv rows whose artist_id is not in `artist_ids` (the SELECT
        id FROM artist snapshot) must NOT be staged. Avoids COPYing the full
        ~3-4M-row Discogs artist dump when only ~50K rows survive
        release_artist filtering."""
        from unittest.mock import patch

        (tmp_path / "artist.csv").write_text(
            "artist_id,artist_name,profile\n"
            "1,Known Artist,Known artist bio\n"
            "999,Unknown Artist,Unknown artist bio\n"
        )

        mock_conn, mock_copy = self._setup_mock_conn(db_artist_ids=[1])

        with patch.object(_ic, "_import_tables", return_value=0):
            import_artist_details(mock_conn, tmp_path)

        written = [c.args[0] for c in mock_copy.write_row.call_args_list]
        assert written == [(1, "Known artist bio")], (
            f"Only artist_id=1 (in DB) should be staged; got {written}"
        )

    def test_copy_dedups_duplicate_artist_id_last_value_wins(self, tmp_path) -> None:
        """If artist.csv ever ships duplicate rows for the same artist_id
        (shouldn't, but defense), the staging table must end up with one row
        per artist_id and the LATER row wins. Matches v1 per-row UPDATE
        semantics."""
        from unittest.mock import patch

        (tmp_path / "artist.csv").write_text(
            "artist_id,artist_name,profile\n"
            "1,Artist v1,First write\n"
            "1,Artist v2,Second write (wins)\n"
        )

        mock_conn, mock_copy = self._setup_mock_conn(db_artist_ids=[1])

        with patch.object(_ic, "_import_tables", return_value=0):
            import_artist_details(mock_conn, tmp_path)

        written = [c.args[0] for c in mock_copy.write_row.call_args_list]
        assert written == [(1, "Second write (wins)")], (
            f"Last-value-wins dedup should keep the second row; got {written}"
        )

    def test_copy_skips_non_integer_artist_id(self, tmp_path) -> None:
        """A malformed CSV row (non-integer artist_id) is skipped without
        aborting the rebuild. The skip is logged at WARNING so silent data
        loss is visible."""
        from unittest.mock import patch

        (tmp_path / "artist.csv").write_text(
            "artist_id,artist_name,profile\n1,Good Row,Good bio\nabc,Bad Row,Bad bio\n"
        )

        mock_conn, mock_copy = self._setup_mock_conn(db_artist_ids=[1])

        with patch.object(_ic, "_import_tables", return_value=0):
            with caplog_at_warning(_ic.logger.name) as caplog:
                import_artist_details(mock_conn, tmp_path)

        written = [c.args[0] for c in mock_copy.write_row.call_args_list]
        assert written == [(1, "Good bio")], f"Non-int artist_id must be skipped; got {written}"
        # The skip must be logged so it's visible to operators
        assert any("non-integer artist_id" in r.message for r in caplog.records), (
            f"Skipped non-int row must be logged at WARNING; got {[r.message for r in caplog.records]}"
        )

    def test_copy_skips_empty_profile_and_unknown(self, tmp_path) -> None:
        """Rows with empty/whitespace-only profile, or artist_id missing
        entirely, are skipped silently — these are normal and shouldn't
        generate noise. Only unknown-artist skips get an INFO log line."""
        from unittest.mock import patch

        (tmp_path / "artist.csv").write_text(
            "artist_id,artist_name,profile\n"
            "1,No Profile,\n"
            "2,Whitespace Profile,   \n"
            ",Missing ID,Some bio\n"
            "3,Known Artist,Real bio\n"
        )

        mock_conn, mock_copy = self._setup_mock_conn(db_artist_ids=[1, 2, 3])

        with patch.object(_ic, "_import_tables", return_value=0):
            import_artist_details(mock_conn, tmp_path)

        written = [c.args[0] for c in mock_copy.write_row.call_args_list]
        assert written == [(3, "Real bio")], (
            f"Only the row with both artist_id and non-empty profile should "
            f"land in COPY; got {written}"
        )

    def test_pins_staging_table_sql_contract(self, tmp_path) -> None:
        """Pin the SQL shape. The other tests only inspect copy.write_row
        tuples — they'd pass against a regression that COPYs directly into
        `artist` and bypasses the staging table entirely, or that queries
        `release_artist` instead of `artist` for the artist_ids snapshot.
        This test asserts the production code actually targets the expected
        relations."""
        from unittest.mock import patch

        (tmp_path / "artist.csv").write_text("artist_id,artist_name,profile\n1,X,Y\n")

        mock_conn, mock_copy = self._setup_mock_conn(db_artist_ids=[1])

        with patch.object(_ic, "_import_tables", return_value=0):
            import_artist_details(mock_conn, tmp_path)

        cursor = mock_conn.cursor.return_value.__enter__.return_value
        executed_sql = " ".join(c.args[0] for c in cursor.execute.call_args_list if c.args)
        # The artist_ids snapshot must query the `artist` table, NOT release_artist
        # (a SELECT id FROM release_artist would return raw FK IDs without the
        # stub-INSERT's DISTINCT filter and without the ON CONFLICT dedup).
        assert "SELECT id FROM artist" in executed_sql, (
            f"artist_ids snapshot must query the artist table; SQL was: {executed_sql}"
        )
        # The profile UPDATE must go via the temp staging table, not direct.
        assert "_artist_profile" in executed_sql, (
            f"Profile UPDATE must use the _artist_profile staging table; SQL was: {executed_sql}"
        )
        # COPY must target the staging table, not `artist` directly.
        copied_sql = " ".join(c.args[0] for c in cursor.copy.call_args_list if c.args)
        assert "COPY _artist_profile" in copied_sql, (
            f"COPY must target _artist_profile (staging), not artist directly; got: {copied_sql}"
        )


class TestReleasePruneAntiJoin:
    """Pin the release prune as an indexed anti-join.

    ``DELETE FROM release WHERE id NOT IN (SELECT id FROM release_staging)``
    full-scans an unindexed temp table and prunes ~nothing (staging is a
    superset of ``release``). At ~682K releases it ran 2h20m before Railway
    admin-killed the connection (2026-07-06 rebuild), aborting *after* the
    child-table TRUNCATE committed and leaving ``release_track`` /
    ``release_artist`` empty. The prune must be a ``NOT EXISTS`` anti-join
    against an indexed ``release_staging``.
    """

    def _mock_conn(self):
        from unittest.mock import MagicMock

        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_cursor

    def test_prune_is_indexed_anti_join_not_in_subquery(self, tmp_path) -> None:
        from unittest.mock import patch

        (tmp_path / "release.csv").write_text("id,title\n1,X\n")
        mock_conn, mock_cursor = self._mock_conn()
        with patch.object(_ic, "import_csv", return_value=100):
            _ic.import_release_via_upsert(mock_conn, tmp_path)

        executed = " ".join(c.args[0] for c in mock_cursor.execute.call_args_list if c.args)
        norm = " ".join(executed.split()).lower()
        assert "not in (select" not in norm, (
            f"prune must not use a NOT IN subquery (unindexed full-scan anti-join); SQL: {executed}"
        )
        assert "not exists" in norm, f"prune must use a NOT EXISTS anti-join; SQL: {executed}"
        assert "create index" in norm and "release_staging" in norm, (
            f"release_staging must be indexed so the anti-join is fast; SQL: {executed}"
        )


import contextlib  # noqa: E402 — module-level imports already at top; this is for test-only helpers


@contextlib.contextmanager
def caplog_at_warning(logger_name: str):
    """Light shim so the WARNING-skip-count test doesn't depend on the
    pytest `caplog` fixture being threaded through. Captures records emitted
    by the named logger at WARNING+ within the `with` block."""
    import logging

    records: list[logging.LogRecord] = []

    class _Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Handler(level=logging.WARNING)
    logger = logging.getLogger(logger_name)
    prior_level = logger.level
    logger.addHandler(handler)
    if prior_level > logging.WARNING or prior_level == logging.NOTSET:
        logger.setLevel(logging.WARNING)
    try:

        class _Captured:
            @property
            def records(self_inner):  # noqa: N805 — closure over outer `records`
                return records

        yield _Captured()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prior_level)
