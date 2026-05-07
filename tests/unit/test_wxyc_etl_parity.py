"""Characterization tests: verify wxyc-etl exports match the old local modules.

These tests ensure behavioral parity between the deleted lib/matching.py,
lib/artist_splitting.py, and lib/pipeline_state.py and their replacements in
the wxyc-etl package (Rust/PyO3).

Note: as of wxyc-etl 0.2.0 the legacy normalizer (`normalize_artist_name`,
`strip_diacritics`, `normalize_title`, `batch_normalize`) is deprecated in favor
of the WX-2 Normalizer Charter forms (`to_match_form`, `to_storage_form`,
`to_ascii_form`). These tests intentionally exercise the legacy normalizer to
verify parity with the old `lib/matching.py:normalize_artist()` until WX-4.1.1
removes the legacy API. The DeprecationWarnings are silenced module-wide so the
parity suite stays a regression net rather than turning CI yellow. The parallel
charter-vs-legacy parity test below documents whether the new and old forms
agree on the same input matrix.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from wxyc_etl.state import PipelineState
from wxyc_etl.text import (
    is_compilation_artist,
    normalize_artist_name,
    split_artist_name,
    split_artist_name_contextual,
    to_match_form,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

# ---------------------------------------------------------------------------
# is_compilation_artist (was lib/matching.py)
# ---------------------------------------------------------------------------


class TestIsCompilationArtistParity:
    """Same cases as the deleted test_matching.py TestIsCompilationArtist."""

    @pytest.mark.parametrize(
        "artist, expected",
        [
            ("Various Artists", True),
            ("various", True),
            ("Soundtrack", True),
            ("Original Motion Picture Soundtrack", True),
            ("V/A", True),
            ("v.a.", True),
            ("Compilation Hits", True),
            ("Stereolab", False),
            ("Juana Molina", False),
            ("Cat Power", False),
            ("", False),
        ],
        ids=[
            "various-artists",
            "various-lowercase",
            "soundtrack",
            "soundtrack-in-phrase",
            "v-slash-a",
            "v-dot-a",
            "compilation-keyword",
            "stereolab",
            "juana-molina",
            "cat-power",
            "empty-string",
        ],
    )
    def test_is_compilation_artist(self, artist: str, expected: bool) -> None:
        assert is_compilation_artist(artist) == expected


# ---------------------------------------------------------------------------
# normalize_artist_name (was lib/matching.py used indirectly)
# ---------------------------------------------------------------------------


class TestNormalizeArtistNameParity:
    """Normalization produces lowercase, stripped, diacritics-removed output."""

    @pytest.mark.parametrize(
        "input_name, expected",
        [
            ("Duke Ellington", "duke ellington"),
            ("STEREOLAB", "stereolab"),
            ("Nilüfer Yanya", "nilufer yanya"),
            ("  Cat Power  ", "cat power"),
            ("Chuquimamani-Condori", "chuquimamani-condori"),
        ],
        ids=["lowercase", "all-caps", "diacritics", "whitespace", "hyphen"],
    )
    def test_normalize(self, input_name: str, expected: str) -> None:
        assert normalize_artist_name(input_name) == expected

    def test_none_returns_empty(self) -> None:
        assert normalize_artist_name(None) == ""


# ---------------------------------------------------------------------------
# Charter parity: to_match_form vs legacy normalize_artist_name
# ---------------------------------------------------------------------------
#
# WX-2 Normalizer Charter migration. The legacy `normalize_artist_name` will be
# removed by WX-4.1.1; this test surfaces any behavioral drift between the
# legacy form and the charter `to_match_form` over the same input matrix the
# old `lib/matching.py:normalize_artist()` was characterized against. As long
# as both forms agree, downstream consumers that still call the legacy API can
# be migrated mechanically. If they diverge, this test fails loudly and the
# diff has to be triaged before bumping wxyc-etl.


class TestToMatchFormParityWithLegacy:
    """Charter `to_match_form` matches legacy `normalize_artist_name` on the
    same input matrix as `TestNormalizeArtistNameParity`."""

    @pytest.mark.parametrize(
        "input_name, expected",
        [
            ("Duke Ellington", "duke ellington"),
            ("STEREOLAB", "stereolab"),
            ("Nilüfer Yanya", "nilufer yanya"),
            ("  Cat Power  ", "cat power"),
            ("Chuquimamani-Condori", "chuquimamani-condori"),
        ],
        ids=["lowercase", "all-caps", "diacritics", "whitespace", "hyphen"],
    )
    def test_to_match_form_matches_legacy(self, input_name: str, expected: str) -> None:
        # Charter form produces the same output as legacy form...
        assert to_match_form(input_name) == expected
        # ...and they agree element-wise on this input matrix.
        assert to_match_form(input_name) == normalize_artist_name(input_name)

    def test_none_handling_agrees(self) -> None:
        """Charter and legacy both return '' for None as of wxyc-etl 0.2.1.

        Earlier 0.2.0 charter forms raised TypeError on None while legacy returned
        ''. The 0.2.1 PyO3 wrappers accept Option<&str> and return '' on None,
        unifying the contract — this test guards against regression.
        """
        assert normalize_artist_name(None) == ""
        assert to_match_form(None) == ""


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
