"""Characterization tests verifying behavioral parity between local modules and shared packages.

These tests run the same inputs through both the old (lib/) and new (wxyc_etl/wxyc_catalog)
import paths, asserting identical results. Once all deletions are complete and the old
imports no longer exist, these tests should be removed.
"""

import pytest

# Old imports (local)
from lib.matching import is_compilation_artist as local_is_compilation
from lib.artist_splitting import split_artist_name as local_split
from lib.artist_splitting import split_artist_name_contextual as local_split_contextual

# New imports (shared packages)
from wxyc_etl.text import is_compilation_artist as etl_is_compilation
from wxyc_etl.text import split_artist_name as etl_split
from wxyc_etl.text import split_artist_name_contextual as etl_split_contextual


class TestIsCompilationArtistParity:
    """Verify wxyc_etl.text.is_compilation_artist matches lib.matching."""

    @pytest.mark.parametrize(
        "name",
        [
            "Various Artists",
            "various",
            "VARIOUS",
            "Original Soundtrack",
            "Compilation",
            "V/A",
            "V.A.",
            "Autechre",
            "Cat Power",
            "Duke Ellington & John Coltrane",
            "",
            "Los Naturales (2)",
            "Bjork",
        ],
    )
    def test_parity(self, name):
        assert etl_is_compilation(name) == local_is_compilation(name), (
            f"Mismatch for {name!r}: etl={etl_is_compilation(name)}, local={local_is_compilation(name)}"
        )


class TestSplitArtistNameParity:
    """Verify wxyc_etl.text.split_artist_name matches lib.artist_splitting."""

    @pytest.mark.parametrize(
        "name",
        [
            "Mike Vainio, Ryoji, Alva Noto",
            "Emerson, Lake, and Palmer",
            "J Dilla / Jay Dee",
            "Sonic Youth + Lydia Lunch",
            "Duke Ellington & John Coltrane",
            "10,000 Maniacs",
            "Cat Power",
            "",
            "  whitespace  ",
            "A / B / C",
            "X + X + Y",
        ],
    )
    def test_parity(self, name):
        local_result = local_split(name)
        etl_result = etl_split(name)
        # wxyc_etl returns None instead of [] for no-split
        if etl_result is None:
            etl_result = []
        assert etl_result == local_result, (
            f"Mismatch for {name!r}: etl={etl_result}, local={local_result}"
        )


class TestSplitArtistNameContextualParity:
    """Verify wxyc_etl.text.split_artist_name_contextual matches lib.artist_splitting."""

    @pytest.mark.parametrize(
        "name,known_artists",
        [
            ("Duke Ellington & John Coltrane", {"duke ellington", "john coltrane"}),
            ("Duke Ellington & John Coltrane", set()),
            ("Sonic Youth & Thurston Moore", {"sonic youth", "thurston moore"}),
            ("Mike Vainio, Ryoji, Alva Noto", {"mike vainio", "ryoji"}),
            ("Cat Power", {"cat power"}),
        ],
    )
    def test_parity(self, name, known_artists):
        local_result = local_split_contextual(name, known_artists)
        etl_result = etl_split_contextual(name, known_artists)
        # wxyc_etl returns None instead of [] for no-split
        if etl_result is None:
            etl_result = []
        assert etl_result == local_result, (
            f"Mismatch for {name!r}: etl={etl_result}, local={local_result}"
        )


class TestPipelineStateParity:
    """Verify wxyc_etl.state.PipelineState matches lib.pipeline_state.PipelineState."""

    def test_basic_lifecycle(self, tmp_path):
        from lib.pipeline_state import PipelineState as LocalState, STEP_NAMES
        from wxyc_etl.state import PipelineState as EtlState

        db_url = "postgresql://test:5432/discogs"
        csv_dir = "/tmp/test"

        local = LocalState(db_url=db_url, csv_dir=csv_dir)
        etl = EtlState(db_url=db_url, csv_dir=csv_dir, steps=STEP_NAMES)

        # All steps start as pending
        for step in STEP_NAMES:
            assert local.is_completed(step) == etl.is_completed(step) == False

        # Mark some steps
        local.mark_completed("create_schema")
        etl.mark_completed("create_schema")
        assert local.is_completed("create_schema") == etl.is_completed("create_schema") == True
        assert local.step_status("create_schema") == etl.step_status("create_schema") == "completed"

        local.mark_failed("import_csv", "disk full")
        etl.mark_failed("import_csv", "disk full")
        assert local.step_status("import_csv") == etl.step_status("import_csv") == "failed"
        assert local.step_error("import_csv") == etl.step_error("import_csv") == "disk full"

    def test_save_load_roundtrip(self, tmp_path):
        from lib.pipeline_state import PipelineState as LocalState, STEP_NAMES
        from wxyc_etl.state import PipelineState as EtlState

        db_url = "postgresql://test:5432/discogs"
        csv_dir = "/tmp/test"

        # Save from etl, verify state survives
        etl = EtlState(db_url=db_url, csv_dir=csv_dir, steps=STEP_NAMES)
        etl.mark_completed("create_schema")
        etl.mark_completed("import_csv")

        state_file = tmp_path / "state.json"
        etl.save(str(state_file))

        loaded = EtlState.load(str(state_file))
        assert loaded.is_completed("create_schema")
        assert loaded.is_completed("import_csv")
        assert not loaded.is_completed("dedup")

    def test_validate_resume(self):
        from lib.pipeline_state import STEP_NAMES
        from wxyc_etl.state import PipelineState as EtlState

        etl = EtlState(db_url="postgresql://a", csv_dir="/a", steps=STEP_NAMES)
        etl.validate_resume(db_url="postgresql://a", csv_dir="/a")

        with pytest.raises(Exception):
            etl.validate_resume(db_url="postgresql://b", csv_dir="/a")
