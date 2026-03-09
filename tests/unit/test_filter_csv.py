"""Unit tests for scripts/filter_csv.py."""

from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import pytest

# Load filter_csv module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "filter_csv.py"
_spec = importlib.util.spec_from_file_location("filter_csv", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fc)

normalize_artist = _fc.normalize_artist
load_library_artists = _fc.load_library_artists
find_matching_release_ids = _fc.find_matching_release_ids
filter_csv_file = _fc.filter_csv_file
get_release_id_column = _fc.get_release_id_column

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# ---------------------------------------------------------------------------
# normalize_artist
# ---------------------------------------------------------------------------


class TestNormalizeArtist:
    """Artist normalization for matching."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("Radiohead", "radiohead"),
            ("  Radiohead  ", "radiohead"),
            ("RADIOHEAD", "radiohead"),
            ("  Mixed Case  ", "mixed case"),
            ("", ""),
            ("Björk", "bjork"),
            ("Sigur Rós", "sigur ros"),
            ("Motörhead", "motorhead"),
            ("Hüsker Dü", "husker du"),
            ("Café Tacvba", "cafe tacvba"),
            ("Zoé", "zoe"),
        ],
        ids=[
            "lowercase",
            "strip-spaces",
            "all-caps",
            "mixed-case-strip",
            "empty",
            "bjork",
            "sigur-ros",
            "motorhead",
            "husker-du",
            "cafe-tacvba",
            "zoe",
        ],
    )
    def test_normalize(self, raw: str, expected: str) -> None:
        assert normalize_artist(raw) == expected


# ---------------------------------------------------------------------------
# load_library_artists
# ---------------------------------------------------------------------------


class TestLoadLibraryArtists:
    """Loading artist names from library_artists.txt."""

    def test_loads_fixture_file(self) -> None:
        path = FIXTURES_DIR / "library_artists.txt"
        artists = load_library_artists(path)
        assert isinstance(artists, set)
        assert len(artists) > 0

    def test_names_are_normalized(self) -> None:
        path = FIXTURES_DIR / "library_artists.txt"
        artists = load_library_artists(path)
        # All names should be lowercase and stripped
        for name in artists:
            assert name == name.lower().strip()

    def test_radiohead_in_set(self) -> None:
        path = FIXTURES_DIR / "library_artists.txt"
        artists = load_library_artists(path)
        assert "radiohead" in artists

    def test_blank_lines_excluded(self, tmp_path: Path) -> None:
        txt = tmp_path / "artists.txt"
        txt.write_text("Alpha\n\n  \nBeta\n")
        artists = load_library_artists(txt)
        assert artists == {"alpha", "beta"}


# ---------------------------------------------------------------------------
# find_matching_release_ids
# ---------------------------------------------------------------------------


class TestFindMatchingReleaseIds:
    """Finding release IDs with matching artists from release_artist.csv."""

    def test_finds_matching_ids(self) -> None:
        release_artist_path = FIXTURES_DIR / "csv" / "release_artist.csv"
        library_artists = {"radiohead"}
        ids = find_matching_release_ids(release_artist_path, library_artists)
        # Radiohead is on releases 1001, 1002, 1003, 3001, 4001
        assert ids == {1001, 1002, 1003, 3001, 4001}

    def test_no_matches(self) -> None:
        release_artist_path = FIXTURES_DIR / "csv" / "release_artist.csv"
        library_artists = {"nonexistent artist xyz"}
        ids = find_matching_release_ids(release_artist_path, library_artists)
        assert ids == set()

    def test_multiple_artists(self) -> None:
        release_artist_path = FIXTURES_DIR / "csv" / "release_artist.csv"
        library_artists = {"radiohead", "joy division"}
        ids = find_matching_release_ids(release_artist_path, library_artists)
        assert {1001, 1002, 1003, 3001, 4001, 2001, 2002}.issubset(ids)

    def test_extra_artists_not_matched_for_id(self) -> None:
        """Extra artists (credit=1) still use their artist_name for matching."""
        release_artist_path = FIXTURES_DIR / "csv" / "release_artist.csv"
        library_artists = {"some producer"}
        ids = find_matching_release_ids(release_artist_path, library_artists)
        # "Some Producer" is an extra artist on release 1001
        assert 1001 in ids

    def test_normalize_cache_avoids_redundant_calls(self, tmp_path: Path) -> None:
        """Duplicate artist names should only be normalized once (via cache)."""
        csv_path = tmp_path / "release_artist.csv"
        # Write a CSV with the same artist name repeated many times
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["release_id", "artist_id", "artist_name", "extra", "anv", "position"])
            for i in range(1, 101):
                writer.writerow([i, 1, "Juana Molina", 0, "", 1])

        from unittest.mock import patch

        call_count = 0
        original_normalize = normalize_artist

        def counting_normalize(name):
            nonlocal call_count
            call_count += 1
            return original_normalize(name)

        with patch.object(_fc, "normalize_artist", side_effect=counting_normalize):
            find_matching_release_ids(csv_path, {"juana molina"})

        # With caching, normalize should be called once for the unique name,
        # not 100 times for every row.
        assert call_count == 1


# ---------------------------------------------------------------------------
# get_release_id_column
# ---------------------------------------------------------------------------


class TestGetReleaseIdColumn:
    """Column name detection for different CSV files."""

    @pytest.mark.parametrize(
        "filename, expected",
        [
            ("release.csv", "id"),
            ("release_artist.csv", "release_id"),
            ("release_track.csv", "release_id"),
            ("release_track_artist.csv", "release_id"),
            ("release_image.csv", "release_id"),
        ],
        ids=["release", "release_artist", "release_track", "release_track_artist", "release_image"],
    )
    def test_column_name(self, filename: str, expected: str) -> None:
        assert get_release_id_column(filename) == expected


# ---------------------------------------------------------------------------
# filter_csv_file
# ---------------------------------------------------------------------------


class TestFilterCsvFile:
    """Filtering a CSV file to only matching release IDs."""

    def test_filters_to_matching_ids(self, tmp_path: Path) -> None:
        matching_ids = {1001, 3001}
        input_path = FIXTURES_DIR / "csv" / "release.csv"
        output_path = tmp_path / "release_filtered.csv"

        input_count, output_count = filter_csv_file(input_path, output_path, matching_ids, "id")
        assert input_count > 0
        assert output_count == 2

        # Verify output contains only matching IDs
        with open(output_path) as f:
            reader = csv.DictReader(f)
            ids = {int(row["id"]) for row in reader}
        assert ids == {1001, 3001}

    def test_preserves_all_columns(self, tmp_path: Path) -> None:
        matching_ids = {1001}
        input_path = FIXTURES_DIR / "csv" / "release.csv"
        output_path = tmp_path / "release_filtered.csv"

        filter_csv_file(input_path, output_path, matching_ids, "id")

        with open(input_path) as f:
            original_headers = csv.DictReader(f).fieldnames

        with open(output_path) as f:
            filtered_headers = csv.DictReader(f).fieldnames

        assert original_headers == filtered_headers

    def test_empty_matching_set(self, tmp_path: Path) -> None:
        input_path = FIXTURES_DIR / "csv" / "release.csv"
        output_path = tmp_path / "release_filtered.csv"

        input_count, output_count = filter_csv_file(input_path, output_path, set(), "id")
        assert input_count > 0
        assert output_count == 0

    def test_filters_child_table(self, tmp_path: Path) -> None:
        matching_ids = {1001}
        input_path = FIXTURES_DIR / "csv" / "release_track.csv"
        output_path = tmp_path / "release_track_filtered.csv"

        _, output_count = filter_csv_file(input_path, output_path, matching_ids, "release_id")
        assert output_count == 5  # Release 1001 has 5 tracks

    def test_missing_id_column_raises_clear_error(self, tmp_path: Path) -> None:
        """When id_column is not in the CSV header, a ValueError is raised
        with a message listing the available columns."""
        input_path = FIXTURES_DIR / "csv" / "release.csv"
        output_path = tmp_path / "out.csv"

        with pytest.raises(ValueError, match="Column 'nonexistent'.*not found"):
            filter_csv_file(input_path, output_path, {1001}, "nonexistent")
