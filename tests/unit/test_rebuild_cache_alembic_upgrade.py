"""Pin the alembic-upgrade invariants of ``scripts/rebuild-cache.sh``.

The 2026-05-05 disable of the GH Actions cron (``.github/workflows/rebuild-cache.yml``)
silently severed the rebuild path from its ``alembic upgrade head`` step. The
active path is now the EC2 ephemeral stack (``infra/ephemeral-rebuild/``), which
execs ``rebuild-cache.sh``; neither that script nor its bootstrap wrapper runs
``alembic upgrade head`` before the pipeline. When a migration adds a new column
between rebuilds (e.g. ``0005_release_track_artist_role.py``), the next cron
tick fails at the COPY for the affected table because Railway's schema is
behind ``main``. See #222.

These tests are static-structural: they parse the script and assert the
relevant fragments exist in the required order. They do not execute alembic --
the live behavior is covered by the next ephemeral rebuild's S3 log archive.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache.sh"


@pytest.fixture(scope="module")
def script_lines() -> list[str]:
    return SCRIPT_PATH.read_text().splitlines()


def _first_index(lines: list[str], needle: str) -> int:
    """Return the index of the first non-comment line containing ``needle``."""
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if needle in line:
            return i
    raise AssertionError(f"{needle!r} not found in non-comment lines of {SCRIPT_PATH}")


def test_runs_alembic_upgrade_head(script_lines: list[str]) -> None:
    """#222: the script must invoke ``alembic upgrade head`` before the pipeline.

    Without this, a new column added in a migration between rebuilds is not
    present on the destination DB when the converter starts emitting CSVs
    that reference it, and the COPY fails. The runbook claim that the
    rebuild applies pending migrations automatically becomes true again
    only when this command is on the active script path.
    """
    non_comment = [line for line in script_lines if not line.lstrip().startswith("#")]
    assert any("alembic upgrade head" in line for line in non_comment), (
        "rebuild-cache.sh must invoke 'alembic upgrade head' between dep "
        "refresh and the pipeline so destination-DB schema drift can't "
        "surface as a COPY error. See #222."
    )


def test_alembic_upgrade_runs_before_pipeline(script_lines: list[str]) -> None:
    """#222: alembic upgrade must precede ``run_pipeline.py``.

    A migration applied after the pipeline starts is useless -- the COPY
    has already failed. Pin the ordering so a future refactor can't
    accidentally invert it.
    """
    upgrade_idx = _first_index(script_lines, "alembic upgrade head")
    pipeline_idx = _first_index(script_lines, "run_pipeline.py")
    assert upgrade_idx < pipeline_idx, (
        f"'alembic upgrade head' (line {upgrade_idx + 1}) must precede the "
        f"run_pipeline.py invocation (line {pipeline_idx + 1}). Otherwise "
        f"the COPY runs against a stale schema. See #222."
    )


def test_alembic_upgrade_skipped_under_smoke(script_lines: list[str]) -> None:
    """#222: ``REBUILD_SMOKE=1`` must not write to prod.

    Smoke mode is read-only by contract — the existing curl Range-request
    smoke block exits before any DB writes. ``alembic upgrade head`` is a
    DB write, so the upgrade line must only be reachable when
    ``REBUILD_SMOKE`` is unset or not ``1``. Pin by requiring a
    ``REBUILD_SMOKE`` reference on one of the few non-comment lines
    immediately preceding the upgrade call.
    """
    upgrade_idx = _first_index(script_lines, "alembic upgrade head")
    # Walk back through non-comment lines until we find a REBUILD_SMOKE
    # guard, allowing a short blank-line gap. 8 non-comment lines is more
    # than enough for an `if` + `echo` + `(cd ... && ...)` shape and small
    # enough that the existing smoke block 60+ lines below can't match.
    preceding_non_comment = [
        line
        for line in script_lines[:upgrade_idx]
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert any("REBUILD_SMOKE" in line for line in preceding_non_comment[-8:]), (
        "The 'alembic upgrade head' call must be guarded by a REBUILD_SMOKE "
        "check so a smoke run does not write to the destination DB. See #222."
    )


def test_alembic_upgrade_runs_in_repo_dir(script_lines: list[str]) -> None:
    """#222: alembic must run from ``$REPO_DIR`` so it picks up ``alembic.ini``.

    Without ``cd "$REPO_DIR"`` (or its subshell equivalent), alembic
    resolves ``alembic.ini`` against cron's cwd, which is not guaranteed
    to be the repo root.
    """
    upgrade_idx = _first_index(script_lines, "alembic upgrade head")
    assert "$REPO_DIR" in script_lines[upgrade_idx], (
        "The 'alembic upgrade head' line must mention $REPO_DIR (e.g. "
        "via 'cd \"$REPO_DIR\" && alembic upgrade head') so alembic "
        "picks up the repo's alembic.ini. See #222."
    )
