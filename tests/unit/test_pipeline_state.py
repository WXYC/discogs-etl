"""Unit tests for lib/pipeline_state.PipelineState."""

from __future__ import annotations

import json

import pytest

from lib.pipeline_state import STEP_NAMES, PipelineState

STEPS = STEP_NAMES


class TestFreshState:
    """A freshly created PipelineState has all steps pending."""

    def test_no_steps_completed(self) -> None:
        state = PipelineState(
            db_url="postgresql://localhost/test",
            csv_dir="/tmp/csv",
        )
        for step in STEPS:
            assert not state.is_completed(step)

    def test_step_count(self) -> None:
        """V3 pipeline has 9 steps."""
        assert len(STEPS) == 9

    def test_step_order(self) -> None:
        """Steps are in correct execution order."""
        assert STEPS == [
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


class TestMarkCompleted:
    """mark_completed() / is_completed() round-trip."""

    def test_mark_completed(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_completed("import_csv")
        assert state.is_completed("import_csv")

    def test_other_steps_remain_pending(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_completed("import_csv")
        assert not state.is_completed("create_schema")
        assert not state.is_completed("dedup")

    def test_new_steps_can_be_marked(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_completed("import_tracks")
        assert state.is_completed("import_tracks")
        state.mark_completed("create_track_indexes")
        assert state.is_completed("create_track_indexes")


class TestMarkFailed:
    """mark_failed() records error message."""

    def test_mark_failed(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_failed("create_indexes", "disk full")
        assert not state.is_completed("create_indexes")
        assert state.step_status("create_indexes") == "failed"
        assert state.step_error("create_indexes") == "disk full"


class TestSaveLoad:
    """save() writes valid JSON; load() restores state."""

    def test_save_creates_valid_json(self, tmp_path) -> None:
        state_file = tmp_path / "state.json"
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_completed("create_schema")
        state.save(state_file)

        data = json.loads(state_file.read_text())
        assert data["version"] == 3
        assert data["database_url"] == "postgresql://localhost/test"
        assert data["csv_dir"] == "/tmp/csv"
        assert data["steps"]["create_schema"]["status"] == "completed"
        assert data["steps"]["import_csv"]["status"] == "pending"

    def test_load_restores_state(self, tmp_path) -> None:
        state_file = tmp_path / "state.json"
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_completed("create_schema")
        state.mark_completed("import_csv")
        state.mark_failed("create_indexes", "disk full")
        state.save(state_file)

        loaded = PipelineState.load(state_file)
        assert loaded.db_url == "postgresql://localhost/test"
        assert loaded.csv_dir == "/tmp/csv"
        assert loaded.is_completed("create_schema")
        assert loaded.is_completed("import_csv")
        assert loaded.step_status("create_indexes") == "failed"
        assert loaded.step_error("create_indexes") == "disk full"
        assert not loaded.is_completed("dedup")

    def test_save_is_atomic(self, tmp_path) -> None:
        """save() writes to a temp file then renames, so partial writes don't corrupt."""
        state_file = tmp_path / "state.json"
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.save(state_file)

        # The temp file should not linger
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []

    def test_v2_round_trip(self, tmp_path) -> None:
        """Save and load a v2 state with all steps."""
        state_file = tmp_path / "state.json"
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        for step in STEPS:
            state.mark_completed(step)
        state.save(state_file)

        loaded = PipelineState.load(state_file)
        for step in STEPS:
            assert loaded.is_completed(step), f"Step {step} should be completed"

    def test_v2_has_all_steps_in_file(self, tmp_path) -> None:
        """State file contains all 8 v2 steps."""
        state_file = tmp_path / "state.json"
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.save(state_file)

        data = json.loads(state_file.read_text())
        assert set(data["steps"].keys()) == set(STEPS)


class TestV1Migration:
    """load() migrates v1 state files to v2."""

    def _make_v1_state(self, tmp_path, completed_steps: list[str]) -> dict:
        """Create a v1 state file and return its data."""
        v1_steps = {
            name: {"status": "pending"}
            for name in [
                "create_schema",
                "import_csv",
                "create_indexes",
                "dedup",
                "prune",
                "vacuum",
            ]
        }
        for step in completed_steps:
            v1_steps[step] = {"status": "completed"}

        data = {
            "version": 1,
            "database_url": "postgresql://localhost/test",
            "csv_dir": "/tmp/csv",
            "steps": v1_steps,
        }
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps(data))
        return data

    def test_all_completed_v1(self, tmp_path) -> None:
        """All v1 steps completed -> all v2 steps completed."""
        self._make_v1_state(
            tmp_path,
            ["create_schema", "import_csv", "create_indexes", "dedup", "prune", "vacuum"],
        )
        state = PipelineState.load(tmp_path / "state.json")

        for step in STEPS:
            assert state.is_completed(step), f"Step {step} should be completed after v1 migration"

    def test_import_csv_completed_implies_import_tracks(self, tmp_path) -> None:
        """V1 import_csv completed -> import_tracks also completed."""
        self._make_v1_state(tmp_path, ["create_schema", "import_csv"])
        state = PipelineState.load(tmp_path / "state.json")

        assert state.is_completed("import_csv")
        assert state.is_completed("import_tracks")
        assert not state.is_completed("create_track_indexes")

    def test_create_indexes_completed_implies_create_track_indexes(self, tmp_path) -> None:
        """V1 create_indexes completed -> create_track_indexes also completed."""
        self._make_v1_state(tmp_path, ["create_schema", "import_csv", "create_indexes"])
        state = PipelineState.load(tmp_path / "state.json")

        assert state.is_completed("create_indexes")
        assert state.is_completed("create_track_indexes")

    def test_dedup_completed_implies_create_track_indexes(self, tmp_path) -> None:
        """V1 dedup completed -> create_track_indexes also completed."""
        self._make_v1_state(tmp_path, ["create_schema", "import_csv", "create_indexes", "dedup"])
        state = PipelineState.load(tmp_path / "state.json")

        assert state.is_completed("dedup")
        assert state.is_completed("import_tracks")
        assert state.is_completed("create_track_indexes")

    def test_partial_v1_only_schema(self, tmp_path) -> None:
        """V1 with only schema completed."""
        self._make_v1_state(tmp_path, ["create_schema"])
        state = PipelineState.load(tmp_path / "state.json")

        assert state.is_completed("create_schema")
        assert not state.is_completed("import_csv")
        assert not state.is_completed("import_tracks")
        assert not state.is_completed("create_track_indexes")

    def test_v1_preserves_metadata(self, tmp_path) -> None:
        """V1 migration preserves db_url and csv_dir."""
        self._make_v1_state(tmp_path, [])
        state = PipelineState.load(tmp_path / "state.json")

        assert state.db_url == "postgresql://localhost/test"
        assert state.csv_dir == "/tmp/csv"


class TestV2Migration:
    """load() migrates v2 state files to v3."""

    def _make_v2_state(self, tmp_path, completed_steps: list[str]) -> None:
        """Create a v2 state file."""
        v2_steps = {
            name: {"status": "pending"}
            for name in [
                "create_schema",
                "import_csv",
                "create_indexes",
                "dedup",
                "import_tracks",
                "create_track_indexes",
                "prune",
                "vacuum",
            ]
        }
        for step in completed_steps:
            v2_steps[step] = {"status": "completed"}

        data = {
            "version": 2,
            "database_url": "postgresql://localhost/test",
            "csv_dir": "/tmp/csv",
            "steps": v2_steps,
        }
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps(data))

    def test_all_completed_v2(self, tmp_path) -> None:
        """All v2 steps completed -> all v3 steps completed (set_logged inferred from vacuum)."""
        self._make_v2_state(
            tmp_path,
            [
                "create_schema",
                "import_csv",
                "create_indexes",
                "dedup",
                "import_tracks",
                "create_track_indexes",
                "prune",
                "vacuum",
            ],
        )
        state = PipelineState.load(tmp_path / "state.json")

        for step in STEPS:
            assert state.is_completed(step), f"Step {step} should be completed after v2 migration"

    def test_vacuum_not_completed_leaves_set_logged_pending(self, tmp_path) -> None:
        """V2 with vacuum not completed -> set_logged is pending."""
        self._make_v2_state(
            tmp_path,
            ["create_schema", "import_csv", "create_indexes", "dedup"],
        )
        state = PipelineState.load(tmp_path / "state.json")

        assert state.is_completed("dedup")
        assert not state.is_completed("vacuum")
        assert not state.is_completed("set_logged")

    def test_v2_preserves_metadata(self, tmp_path) -> None:
        """V2 migration preserves db_url and csv_dir."""
        self._make_v2_state(tmp_path, [])
        state = PipelineState.load(tmp_path / "state.json")

        assert state.db_url == "postgresql://localhost/test"
        assert state.csv_dir == "/tmp/csv"


class TestValidateResume:
    """validate_resume() rejects mismatched db_url or csv_dir."""

    def test_matching_config_passes(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.validate_resume(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")

    def test_mismatched_db_url_raises(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        with pytest.raises(ValueError, match="database_url"):
            state.validate_resume(db_url="postgresql://localhost/other", csv_dir="/tmp/csv")

    def test_mismatched_csv_dir_raises(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        with pytest.raises(ValueError, match="csv_dir"):
            state.validate_resume(db_url="postgresql://localhost/test", csv_dir="/tmp/other")


class TestUnknownStep:
    """Operations on unknown step names raise KeyError."""

    def test_is_completed_unknown_step(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        with pytest.raises(KeyError):
            state.is_completed("nonexistent")

    def test_mark_completed_unknown_step(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        with pytest.raises(KeyError):
            state.mark_completed("nonexistent")


class TestVersionValidation:
    """load() rejects unknown state file versions."""

    def test_load_rejects_future_version(self, tmp_path) -> None:
        state_file = tmp_path / "state.json"
        state_file.write_text(
            json.dumps(
                {
                    "version": 99,
                    "database_url": "postgresql://localhost/test",
                    "csv_dir": "/tmp/csv",
                    "steps": {},
                }
            )
        )
        with pytest.raises(ValueError, match="version 99"):
            PipelineState.load(state_file)
