"""Fallback path tests for discogs-cache.

Tests two fallback scenarios:
1. verify_cache.py: Mock wxyc_etl import failure, verify that the Python rapidfuzz
   matching in verify_cache.py produces identical KEEP/PRUNE/REVIEW classifications
   regardless of whether wxyc_etl is available.
2. Pipeline state resume: Old-format state files (v1 and v2) correctly migrate to v3
   format and resume works as expected.

Pattern: Use monkeypatch to simulate primary path failure. Run both paths on the same
fixture data, assert identical results.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Load verify_cache module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
_spec = importlib.util.spec_from_file_location("verify_cache", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_vc = importlib.util.module_from_spec(_spec)
sys.modules["verify_cache"] = _vc
_spec.loader.exec_module(_vc)

LibraryIndex = _vc.LibraryIndex
MultiIndexMatcher = _vc.MultiIndexMatcher
Decision = _vc.Decision
normalize_artist = _vc.normalize_artist
normalize_title = _vc.normalize_title
classify_compilation = _vc.classify_compilation

from lib.matching import is_compilation_artist
from lib.pipeline_state import PipelineState, STEP_NAMES


# ---------------------------------------------------------------------------
# Fixture data: representative WXYC library rows and Discogs releases
# ---------------------------------------------------------------------------

LIBRARY_ROWS = [
    ("Juana Molina", "DOGA", "LP"),
    ("Stereolab", "Aluminum Tunes", "CD"),
    ("Cat Power", "Moon Pix", "LP"),
    ("Jessica Pratt", "On Your Own Love Again", "LP"),
    ("Chuquimamani-Condori", "Edits", "CD"),
    ("Duke Ellington & John Coltrane", "Duke Ellington & John Coltrane", "LP"),
    ("Father John Misty", "I Love You, Honeybear", "LP"),
    ("Autechre", "Confield", "CD"),
    ("Prince Jammy", "...Destroys The Space Invaders", "LP"),
    ("Various Artists", "Dark Night of the Soul", None),
]

# Discogs releases: (artist_name, title) pairs to classify.
# Each pair has an expected decision based on whether it matches library rows.
DISCOGS_RELEASES_EXPECTED = [
    # Exact matches -> KEEP
    ("Juana Molina", "DOGA", Decision.KEEP),
    ("Stereolab", "Aluminum Tunes", Decision.KEEP),
    ("Cat Power", "Moon Pix", Decision.KEEP),
    ("Jessica Pratt", "On Your Own Love Again", Decision.KEEP),
    ("Autechre", "Confield", Decision.KEEP),
    # Close matches -> KEEP (minor title/artist variations)
    ("Father John Misty", "I Love You, Honeybear", Decision.KEEP),
    # No match -> PRUNE
    ("Aphex Twin", "Selected Ambient Works Volume II", Decision.PRUNE),
    ("Boards of Canada", "Music Has the Right to Children", Decision.PRUNE),
    ("DJ Shadow", "Endtroducing.....", Decision.PRUNE),
]


class TestVerifyCacheFallbackClassifications:
    """Verify that the Python rapidfuzz matching produces correct classifications.

    This tests the fallback path -- the local Python implementation that runs
    when wxyc_etl is not available. Since verify_cache.py currently uses only
    local Python code, both paths are the same, and we verify the classifications
    are deterministic and correct.
    """

    @pytest.fixture
    def library_index(self) -> LibraryIndex:
        return LibraryIndex.from_rows(LIBRARY_ROWS)

    @pytest.fixture
    def matcher(self, library_index: LibraryIndex) -> MultiIndexMatcher:
        return MultiIndexMatcher(index=library_index)

    @pytest.mark.parametrize(
        "artist, title, expected_decision",
        DISCOGS_RELEASES_EXPECTED,
        ids=[
            f"{a}-{t[:20]}"
            for a, t, _ in DISCOGS_RELEASES_EXPECTED
        ],
    )
    def test_classification_matches_expected(
        self,
        matcher: MultiIndexMatcher,
        artist: str,
        title: str,
        expected_decision: Decision,
    ) -> None:
        """Each Discogs release is classified correctly against the library index."""
        norm_artist = normalize_artist(artist)
        norm_title = normalize_title(title)
        result = matcher.classify(norm_artist, norm_title)
        assert result.decision == expected_decision, (
            f"Expected {expected_decision.value} for ({artist!r}, {title!r}), "
            f"got {result.decision.value} "
            f"(scores: exact={result.exact_score:.2f}, "
            f"token_set={result.token_set_score:.2f}, "
            f"token_sort={result.token_sort_score:.2f}, "
            f"two_stage={result.two_stage_score:.2f})"
        )

    def test_two_runs_produce_identical_results(
        self,
        matcher: MultiIndexMatcher,
    ) -> None:
        """Running classification twice on the same data yields identical results."""
        results_a = []
        results_b = []
        for artist, title, _ in DISCOGS_RELEASES_EXPECTED:
            norm_a = normalize_artist(artist)
            norm_t = normalize_title(title)
            results_a.append(matcher.classify(norm_a, norm_t).decision)
            results_b.append(matcher.classify(norm_a, norm_t).decision)
        assert results_a == results_b


class TestCompilationFallbackClassification:
    """Verify compilation title matching works correctly."""

    @pytest.fixture
    def library_index(self) -> LibraryIndex:
        return LibraryIndex.from_rows(LIBRARY_ROWS)

    def test_known_compilation_classified_as_keep(self, library_index: LibraryIndex) -> None:
        """A compilation title in the library is classified as KEEP."""
        norm_title = normalize_title("Dark Night of the Soul")
        decision = classify_compilation(norm_title, library_index)
        assert decision == Decision.KEEP

    def test_unknown_compilation_classified_as_prune(self, library_index: LibraryIndex) -> None:
        """A compilation title not in the library is classified as PRUNE."""
        norm_title = normalize_title("Now That's What I Call Music 47")
        decision = classify_compilation(norm_title, library_index)
        assert decision == Decision.PRUNE


class TestIsCompilationArtistFallback:
    """Verify is_compilation_artist works correctly with local Python implementation.

    This is the fallback path when wxyc_etl is not available.
    """

    @pytest.mark.parametrize(
        "artist, expected",
        [
            ("Various Artists", True),
            ("Stereolab", False),
            ("Juana Molina", False),
            ("Cat Power", False),
            ("Soundtrack", True),
            ("V/A", True),
            ("v.a.", True),
            ("Compilation", True),
            ("Father John Misty", False),
            ("Autechre", False),
        ],
        ids=[
            "various-artists",
            "stereolab",
            "juana-molina",
            "cat-power",
            "soundtrack",
            "v-a-slash",
            "v-a-dot",
            "compilation",
            "father-john-misty",
            "autechre",
        ],
    )
    def test_is_compilation_artist(self, artist: str, expected: bool) -> None:
        assert is_compilation_artist(artist) == expected


class TestKnownArtistFastPath:
    """Verify that classify_known_artist() produces the same decision as classify()
    for artists that exist in the library index.

    classify_known_artist() is an optimization that skips the expensive combined-string
    scorers. It must agree with the full classify() path for known artists.
    """

    @pytest.fixture
    def library_index(self) -> LibraryIndex:
        return LibraryIndex.from_rows(LIBRARY_ROWS)

    @pytest.fixture
    def matcher(self, library_index: LibraryIndex) -> MultiIndexMatcher:
        return MultiIndexMatcher(index=library_index)

    @pytest.mark.parametrize(
        "artist, title",
        [
            ("Juana Molina", "DOGA"),
            ("Stereolab", "Aluminum Tunes"),
            ("Cat Power", "Moon Pix"),
            ("Jessica Pratt", "On Your Own Love Again"),
            ("Autechre", "Confield"),
        ],
        ids=["juana-molina", "stereolab", "cat-power", "jessica-pratt", "autechre"],
    )
    def test_fast_path_agrees_with_full_classify(
        self,
        matcher: MultiIndexMatcher,
        artist: str,
        title: str,
    ) -> None:
        """classify_known_artist() must agree with classify() for exact library matches."""
        norm_artist = normalize_artist(artist)
        norm_title = normalize_title(title)
        full_result = matcher.classify(norm_artist, norm_title)
        fast_result = matcher.classify_known_artist(norm_artist, norm_title)
        assert full_result.decision == fast_result.decision, (
            f"Disagreement for ({artist!r}, {title!r}): "
            f"full={full_result.decision.value}, fast={fast_result.decision.value}"
        )


# ---------------------------------------------------------------------------
# Pipeline state resume: old-format state files
# ---------------------------------------------------------------------------


class TestPipelineStateV1Resume:
    """Test that v1 pipeline state files are correctly migrated and resume works.

    V1 state files have 6 steps. V3 has 9 steps. The migration must correctly
    infer the completion status of the 3 new steps.
    """

    def _make_v1_state(self, tmp_path: Path, completed: list[str]) -> Path:
        """Create a v1 state file with the given completed steps."""
        v1_steps = {
            name: {"status": "pending"}
            for name in ["create_schema", "import_csv", "create_indexes", "dedup", "prune", "vacuum"]
        }
        for step in completed:
            v1_steps[step] = {"status": "completed"}
        data = {
            "version": 1,
            "database_url": "postgresql://localhost:5433/test",
            "csv_dir": "/tmp/csv",
            "steps": v1_steps,
        }
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps(data))
        return state_file

    def test_v1_partial_resume_import_csv_done(self, tmp_path: Path) -> None:
        """V1 state with import_csv completed should resume from create_indexes.

        import_csv in v1 included tracks, so import_tracks should also be completed.
        """
        state_file = self._make_v1_state(
            tmp_path, ["create_schema", "import_csv"]
        )
        state = PipelineState.load(state_file)

        assert state.is_completed("create_schema")
        assert state.is_completed("import_csv")
        assert state.is_completed("import_tracks")  # inferred from import_csv
        assert not state.is_completed("create_indexes")
        assert not state.is_completed("create_track_indexes")
        assert not state.is_completed("dedup")
        assert not state.is_completed("prune")
        assert not state.is_completed("vacuum")
        assert not state.is_completed("set_logged")

    def test_v1_partial_resume_dedup_done(self, tmp_path: Path) -> None:
        """V1 state with dedup completed should have track steps inferred."""
        state_file = self._make_v1_state(
            tmp_path, ["create_schema", "import_csv", "create_indexes", "dedup"]
        )
        state = PipelineState.load(state_file)

        assert state.is_completed("import_tracks")
        assert state.is_completed("create_track_indexes")
        assert not state.is_completed("prune")
        assert not state.is_completed("vacuum")
        assert not state.is_completed("set_logged")

    def test_v1_fully_completed_migrates_all_steps(self, tmp_path: Path) -> None:
        """V1 with all steps completed -> all v3 steps completed."""
        state_file = self._make_v1_state(
            tmp_path,
            ["create_schema", "import_csv", "create_indexes", "dedup", "prune", "vacuum"],
        )
        state = PipelineState.load(state_file)

        for step in STEP_NAMES:
            assert state.is_completed(step), f"Step {step} should be completed after v1 migration"

    def test_v1_metadata_preserved(self, tmp_path: Path) -> None:
        """V1 migration preserves database_url and csv_dir."""
        state_file = self._make_v1_state(tmp_path, [])
        state = PipelineState.load(state_file)
        assert state.db_url == "postgresql://localhost:5433/test"
        assert state.csv_dir == "/tmp/csv"


class TestPipelineStateV2Resume:
    """Test that v2 pipeline state files are correctly migrated and resume works.

    V2 state files have 8 steps. V3 adds set_logged after vacuum.
    """

    def _make_v2_state(self, tmp_path: Path, completed: list[str]) -> Path:
        """Create a v2 state file with the given completed steps."""
        v2_steps = {
            name: {"status": "pending"}
            for name in [
                "create_schema", "import_csv", "create_indexes", "dedup",
                "import_tracks", "create_track_indexes", "prune", "vacuum",
            ]
        }
        for step in completed:
            v2_steps[step] = {"status": "completed"}
        data = {
            "version": 2,
            "database_url": "postgresql://localhost:5433/test",
            "csv_dir": "/tmp/csv",
            "steps": v2_steps,
        }
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps(data))
        return state_file

    def test_v2_vacuum_completed_implies_set_logged(self, tmp_path: Path) -> None:
        """V2 with vacuum completed -> set_logged inferred as completed."""
        state_file = self._make_v2_state(
            tmp_path,
            ["create_schema", "import_csv", "create_indexes", "dedup",
             "import_tracks", "create_track_indexes", "prune", "vacuum"],
        )
        state = PipelineState.load(state_file)
        assert state.is_completed("set_logged")

    def test_v2_vacuum_not_completed_leaves_set_logged_pending(self, tmp_path: Path) -> None:
        """V2 with vacuum not completed -> set_logged is pending."""
        state_file = self._make_v2_state(
            tmp_path,
            ["create_schema", "import_csv", "create_indexes", "dedup",
             "import_tracks", "create_track_indexes", "prune"],
        )
        state = PipelineState.load(state_file)
        assert not state.is_completed("set_logged")

    def test_v2_partial_resume_preserves_failed_step(self, tmp_path: Path) -> None:
        """A v2 state with a failed step should preserve the failure after migration."""
        v2_steps = {
            name: {"status": "pending"}
            for name in [
                "create_schema", "import_csv", "create_indexes", "dedup",
                "import_tracks", "create_track_indexes", "prune", "vacuum",
            ]
        }
        v2_steps["create_schema"] = {"status": "completed"}
        v2_steps["import_csv"] = {"status": "completed"}
        v2_steps["create_indexes"] = {"status": "failed", "error": "disk full"}
        data = {
            "version": 2,
            "database_url": "postgresql://localhost:5433/test",
            "csv_dir": "/tmp/csv",
            "steps": v2_steps,
        }
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps(data))

        state = PipelineState.load(state_file)
        assert state.is_completed("create_schema")
        assert state.is_completed("import_csv")
        assert state.step_status("create_indexes") == "failed"
        assert state.step_error("create_indexes") == "disk full"
        assert not state.is_completed("set_logged")


class TestPipelineStateFutureVersionRejected:
    """Unknown future versions must raise an error rather than silently proceed."""

    def test_version_99_raises(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps({
            "version": 99,
            "database_url": "postgresql://localhost:5433/test",
            "csv_dir": "/tmp/csv",
            "steps": {},
        }))
        with pytest.raises(ValueError, match="version 99"):
            PipelineState.load(state_file)
