"""End-to-end CLI integration tests for resuming the pipeline from a v1 or v2
pipeline state file (the pre-migration JSON formats).

run_pipeline.py supports ``--resume --state-file <path>``. When the state file
is v1 (6 steps) or v2 (8 steps), ``PipelineState.load`` migrates it to the
current v3 (9 steps) format and the pipeline should:

1. Exit 0.
2. Skip steps marked as completed in the migrated state ("Skipping <step>
   (already completed)" log messages).
3. Persist a v3-format state file with all steps completed at the end.

These tests run the pipeline as a subprocess against a fresh test PostgreSQL
database, using the canonical fixture CSVs and library.db.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"
FIXTURE_LIBRARY_DB = FIXTURES_DIR / "library.db"
RUN_PIPELINE = Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py"

ADMIN_URL = os.environ.get("DATABASE_URL_TEST", "postgresql://localhost:5433/postgres")

pytestmark = [pytest.mark.postgres, pytest.mark.e2e]


# v1 had 6 steps (no separate import_tracks/create_track_indexes/set_logged).
V1_STEP_NAMES = [
    "create_schema",
    "import_csv",
    "create_indexes",
    "dedup",
    "prune",
    "vacuum",
]

# v2 had 8 steps (set_logged was added in v3).
V2_STEP_NAMES = [
    "create_schema",
    "import_csv",
    "create_indexes",
    "dedup",
    "import_tracks",
    "create_track_indexes",
    "prune",
    "vacuum",
]

# v3 -- the current canonical step list.
V3_STEP_NAMES = [
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


def _postgres_available() -> bool:
    try:
        conn = psycopg.connect(ADMIN_URL, connect_timeout=3, autocommit=True)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture
def fresh_db_url():
    """Create a fresh test database, yield its URL, drop on teardown."""
    if not _postgres_available():
        pytest.skip("PostgreSQL not available (set DATABASE_URL_TEST)")

    db_name = f"discogs_resume_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    base = ADMIN_URL.rsplit("/", 1)[0]
    test_url = f"{base}/{db_name}"

    yield test_url

    with admin_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = {} AND pid <> pg_backend_pid()"
            ).format(sql.Literal(db_name))
        )
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
    admin_conn.close()


def _write_v1_state(
    state_path: Path,
    db_url: str,
    csv_dir: Path,
    completed: list[str],
) -> None:
    """Write a v1-format pipeline state JSON file to ``state_path``."""
    steps = {name: {"status": "pending"} for name in V1_STEP_NAMES}
    for name in completed:
        steps[name] = {"status": "completed"}
    data = {
        "version": 1,
        "database_url": db_url,
        "csv_dir": str(csv_dir.resolve()),
        "steps": steps,
    }
    state_path.write_text(json.dumps(data, indent=2))


def _write_v2_state(
    state_path: Path,
    db_url: str,
    csv_dir: Path,
    completed: list[str],
) -> None:
    """Write a v2-format pipeline state JSON file to ``state_path``."""
    steps = {name: {"status": "pending"} for name in V2_STEP_NAMES}
    for name in completed:
        steps[name] = {"status": "completed"}
    data = {
        "version": 2,
        "database_url": db_url,
        "csv_dir": str(csv_dir.resolve()),
        "steps": steps,
    }
    state_path.write_text(json.dumps(data, indent=2))


def _run_pipeline_resume(
    db_url: str,
    csv_dir: Path,
    state_file: Path,
    library_db: Path,
    timeout_s: int = 180,
) -> subprocess.CompletedProcess:
    """Invoke run_pipeline.py with --resume against the given state file.

    Returns the CompletedProcess; the caller is responsible for assertions on
    stdout/stderr/returncode.
    """
    return subprocess.run(
        [
            sys.executable,
            str(RUN_PIPELINE),
            "--csv-dir",
            str(csv_dir),
            "--library-db",
            str(library_db),
            "--database-url",
            db_url,
            "--resume",
            "--state-file",
            str(state_file),
        ],
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )


def _assert_pipeline_succeeded(result: subprocess.CompletedProcess) -> None:
    if result.returncode != 0:
        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)
    assert result.returncode == 0, (
        f"Pipeline exited {result.returncode}; see captured stdout/stderr above"
    )


def _final_state_is_v3_complete(state_file: Path) -> dict:
    """Load and validate that the post-run state file is v3 with all steps complete."""
    assert state_file.exists(), f"Pipeline did not persist state file at {state_file}"
    data = json.loads(state_file.read_text())
    assert data.get("version") == 3, f"Expected v3 state file, got version={data.get('version')}"
    for step in V3_STEP_NAMES:
        step_state = data["steps"].get(step)
        assert step_state is not None, f"Step {step!r} missing from final state"
        assert step_state.get("status") == "completed", (
            f"Step {step!r} not completed in final state: {step_state!r}"
        )
    return data


class TestStateResumeOldFormat:
    """Resume from v1 / v2 state files via the run_pipeline CLI."""

    def test_v1_state_file_resumes_via_cli(self, fresh_db_url, tmp_path) -> None:
        """A v1 state file with create_schema completed resumes from import_csv,
        completes the run, and writes a v3 state file with all steps marked done."""
        # Bootstrap: run the pipeline with no --resume to populate the database
        # up through the steps we want to mark completed in the v1 state file.
        # Easiest path: run the full pipeline once (it will create the v3 state
        # alongside), then *replace* the state file with a hand-crafted v1 file
        # whose completed-step set is a subset of what's actually been done.
        state_file = tmp_path / "v1_state.json"

        # Bootstrap full pipeline run (no --resume; no state file written by
        # default unless --state-file is passed). We pass --state-file so the
        # bootstrap state file is written here, then we overwrite it.
        bootstrap = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                fresh_db_url,
                "--state-file",
                str(state_file),
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        _assert_pipeline_succeeded(bootstrap)

        # Now overwrite the persisted state file with a v1 file claiming only
        # create_schema is completed. The migration will infer that import_csv,
        # import_tracks, etc. are NOT completed -- so on resume the pipeline
        # should re-run those steps. Since the database itself is already
        # populated, re-running idempotent steps must still succeed.
        _write_v1_state(
            state_file,
            db_url=fresh_db_url,
            csv_dir=CSV_DIR,
            completed=["create_schema"],
        )

        # Resume.
        result = _run_pipeline_resume(fresh_db_url, CSV_DIR, state_file, FIXTURE_LIBRARY_DB)
        _assert_pipeline_succeeded(result)

        combined = result.stdout + result.stderr
        # create_schema was marked complete in the v1 file -- verify the
        # resume path skipped it.
        assert "Skipping create_schema" in combined, (
            "Expected 'Skipping create_schema' in resume output; "
            "v1 migration may not be honouring completed steps.\n"
            f"Combined output:\n{combined[:2000]}"
        )

        # Final state file is v3 with all steps completed.
        _final_state_is_v3_complete(state_file)

    def test_v2_state_file_resumes_via_cli(self, fresh_db_url, tmp_path) -> None:
        """A v2 state file with steps through create_track_indexes completed
        resumes from prune, completes the run, and writes a v3 state file with
        all steps marked done (including the new set_logged step)."""
        state_file = tmp_path / "v2_state.json"

        # Bootstrap full pipeline run to populate the database.
        bootstrap = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                fresh_db_url,
                "--state-file",
                str(state_file),
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        _assert_pipeline_succeeded(bootstrap)

        # Overwrite with a v2 state where everything through
        # create_track_indexes is completed. After migration, prune / vacuum /
        # set_logged remain pending and should be re-run.
        _write_v2_state(
            state_file,
            db_url=fresh_db_url,
            csv_dir=CSV_DIR,
            completed=[
                "create_schema",
                "import_csv",
                "create_indexes",
                "dedup",
                "import_tracks",
                "create_track_indexes",
            ],
        )

        result = _run_pipeline_resume(fresh_db_url, CSV_DIR, state_file, FIXTURE_LIBRARY_DB)
        _assert_pipeline_succeeded(result)

        combined = result.stdout + result.stderr
        for step in [
            "create_schema",
            "import_csv",
            "create_indexes",
            "dedup",
            "import_tracks",
            "create_track_indexes",
        ]:
            assert f"Skipping {step}" in combined, (
                f"Expected 'Skipping {step}' in v2-resume output; "
                f"v2 migration may not be honouring completed steps.\n"
                f"Combined output:\n{combined[:2000]}"
            )

        # Final state file is v3 with all 9 steps completed (including the
        # new set_logged step that did not exist in v2).
        final = _final_state_is_v3_complete(state_file)
        assert final["steps"]["set_logged"]["status"] == "completed"
