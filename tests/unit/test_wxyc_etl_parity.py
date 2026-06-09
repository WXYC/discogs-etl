"""Characterization tests: verify wxyc-etl exports match the old local modules.

These tests ensure behavioral parity between the deleted lib/matching.py,
lib/artist_splitting.py, and lib/pipeline_state.py and their replacements in
the wxyc-etl package (Rust/PyO3).

The legacy normalizer surface (`normalize_artist_name`, `strip_diacritics`,
`normalize_title`, `batch_normalize`) was removed by WX-4.1.1 in wxyc-etl 0.7.0;
the parity tests that targeted it have been retired here. The WX-2 Normalizer
Charter forms (`to_match_form`, `to_storage_form`, `to_ascii_form`) are the
canonical replacements and are exercised against live data in their own suites.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from wxyc_etl.state import PipelineState
from wxyc_etl.text import (
    is_compilation_artist,
    split_artist_name,
    split_artist_name_contextual,
)

# ---------------------------------------------------------------------------
# is_compilation_artist (was lib/matching.py)
# ---------------------------------------------------------------------------


class TestIsCompilationArtistParity:
    """Cases for the anchored `is_compilation_artist` contract.

    wxyc-etl 0.5.0 (WXYC/wxyc-etl#129/#130) tightened the matcher from a
    substring scan to two rules:

    - LEADING prefix terminated by end-of-string or a non-alphanumeric
      boundary: "various artists", "v/a", "v.a", "soundtracks".
    - EXACT match (case-insensitive equality): "various", "soundtrack",
      "compilation".

    The "Original Motion Picture Soundtrack" / "Compilation Hits" cases
    that the old substring rule routed to True now classify as real
    artists; the test matrix locks that in alongside the WXYC false-
    positive regressions ("The Soundtrack of Our Lives", "Various
    Production") flagged in the PR.
    """

    @pytest.mark.parametrize(
        "artist, expected",
        [
            # Leading-prefix matches (the WXYC catalog V/A shapes)
            ("Various Artists", True),
            ("Various Artists - Hiphop", True),
            ("V/A", True),
            ("v.a.", True),
            ("Soundtracks", True),
            # Exact-only matches
            ("various", True),
            ("Various", True),
            ("Soundtrack", True),
            ("Compilation", True),
            # Old substring-rule false positives that are now real artists
            ("Original Motion Picture Soundtrack", False),
            ("Compilation Hits", False),
            ("The Soundtrack of Our Lives", False),
            ("Various Production", False),
            # Plain real artists
            ("Stereolab", False),
            ("Juana Molina", False),
            ("Cat Power", False),
            ("", False),
        ],
        ids=[
            "various-artists",
            "various-artists-genre-suffix",
            "v-slash-a",
            "v-dot-a",
            "soundtracks-prefix",
            "various-lowercase",
            "various-titlecase",
            "soundtrack-exact",
            "compilation-exact",
            "soundtrack-in-phrase-now-false",
            "compilation-keyword-now-false",
            "the-soundtrack-of-our-lives",
            "various-production",
            "stereolab",
            "juana-molina",
            "cat-power",
            "empty-string",
        ],
    )
    def test_is_compilation_artist(self, artist: str, expected: bool) -> None:
        assert is_compilation_artist(artist) == expected


# ---------------------------------------------------------------------------
# split_artist_name (was lib/artist_splitting.py)
# ---------------------------------------------------------------------------


class TestSplitArtistNameParity:
    """Same cases as the deleted test_artist_splitting.py TestSplitArtistName."""

    def test_comma_split(self) -> None:
        result = split_artist_name("Mike Vainio, Ryoji, Alva Noto")
        assert result == ["Mike Vainio", "Ryoji", "Alva Noto"]

    def test_plus_split(self) -> None:
        result = split_artist_name("Mika Vainio + Ryoji Ikeda + Alva Noto")
        assert result == ["Mika Vainio", "Ryoji Ikeda", "Alva Noto"]

    def test_slash_split(self) -> None:
        result = split_artist_name("J Dilla / Jay Dee")
        assert result == ["J Dilla", "Jay Dee"]

    def test_no_split_single_artist(self) -> None:
        """Single artist names return None (no split needed)."""
        assert split_artist_name("Stereolab") is None

    def test_no_split_numeric_comma(self) -> None:
        """Commas in numeric contexts don't trigger splitting."""
        result = split_artist_name("10,000 Maniacs")
        # Should not split on numeric commas
        assert result is None or result == []


class TestSplitArtistNameContextualParity:
    """Context-aware splitting uses known_artists to validate ampersand splits."""

    def test_ampersand_with_known_artists(self) -> None:
        known = {"duke ellington", "john coltrane"}
        result = split_artist_name_contextual("Duke Ellington & John Coltrane", known)
        assert result is not None
        assert len(result) == 2

    def test_ampersand_without_known_artists(self) -> None:
        """Ampersand splits are rejected when components aren't known."""
        result = split_artist_name_contextual("Duke Ellington & John Coltrane", set())
        assert result is None


# ---------------------------------------------------------------------------
# PipelineState (was lib/pipeline_state.py)
# ---------------------------------------------------------------------------


STEP_NAMES = [
    "create_schema",
    "import_csv",
    "create_indexes",
    "dedup",
    "import_tracks",
    "create_track_indexes",
    "prune",
    "vacuum",
    "set_logged",
]


class TestPipelineStateParity:
    """Same cases as the deleted test_pipeline_state.py."""

    def test_fresh_state_no_steps_completed(self) -> None:
        state = PipelineState(
            db_url="postgresql://localhost/test",
            csv_dir="/tmp/csv",
            steps=STEP_NAMES,
        )
        for step in STEP_NAMES:
            assert not state.is_completed(step)

    def test_mark_completed(self) -> None:
        state = PipelineState(
            db_url="postgresql://localhost/test",
            csv_dir="/tmp/csv",
            steps=STEP_NAMES,
        )
        state.mark_completed("import_csv")
        assert state.is_completed("import_csv")
        assert not state.is_completed("create_schema")

    def test_mark_failed(self) -> None:
        state = PipelineState(
            db_url="postgresql://localhost/test",
            csv_dir="/tmp/csv",
            steps=STEP_NAMES,
        )
        state.mark_failed("dedup", "connection refused")
        assert state.step_status("dedup") == "failed"
        assert state.step_error("dedup") == "connection refused"

    def test_save_and_load_roundtrip(self, tmp_path: Path) -> None:
        state_file = str(tmp_path / "state.json")
        state = PipelineState(
            db_url="postgresql://localhost/test",
            csv_dir="/tmp/csv",
            steps=STEP_NAMES,
        )
        state.mark_completed("create_schema")
        state.mark_completed("import_csv")
        state.save(state_file)

        loaded = PipelineState.load(state_file)
        assert loaded.is_completed("create_schema")
        assert loaded.is_completed("import_csv")
        assert not loaded.is_completed("create_indexes")

    def test_validate_resume_same_params(self) -> None:
        state = PipelineState(
            db_url="postgresql://localhost/test",
            csv_dir="/tmp/csv",
            steps=STEP_NAMES,
        )
        # Should not raise
        state.validate_resume("postgresql://localhost/test", "/tmp/csv")

    def test_validate_resume_different_params(self) -> None:
        state = PipelineState(
            db_url="postgresql://localhost/test",
            csv_dir="/tmp/csv",
            steps=STEP_NAMES,
        )
        with pytest.raises((ValueError, Exception)):
            state.validate_resume("postgresql://localhost/other", "/tmp/csv")
