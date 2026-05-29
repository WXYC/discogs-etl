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
def script_text() -> str:
    return SCRIPT_PATH.read_text()


@pytest.fixture(scope="module")
def script_lines() -> list[str]:
    return SCRIPT_PATH.read_text().splitlines()


def _non_comment_lines(lines: list[str]) -> list[str]:
    return [line for line in lines if not line.lstrip().startswith("#")]


def _first_index(lines: list[str], needle: str) -> int:
    """Return the index of the first non-comment line containing ``needle``."""
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if needle in line:
            return i
    raise AssertionError(f"{needle!r} not found in non-comment lines of {SCRIPT_PATH}")


def test_runs_alembic_upgrade_head(script_text: str) -> None:
    """#222: the script must invoke ``alembic upgrade head`` before the pipeline.

    Without this, a new column added in a migration between rebuilds is not
    present on the destination DB when the converter starts emitting CSVs
    that reference it, and the COPY fails. The runbook claim that the
    rebuild applies pending migrations automatically becomes true again
    only when this command is on the active script path.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert "alembic upgrade head" in code, (
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


def test_alembic_upgrade_skipped_under_smoke(script_text: str) -> None:
    """#222: ``REBUILD_SMOKE=1`` must not write to prod.

    The smoke mode exits before any DB writes (validated by the existing
    ``REBUILD_SMOKE=1`` short-circuit at the curl Range-request step).
    ``alembic upgrade head`` is a DB write -- gate it behind the same
    smoke check so a smoke run stays read-only against the destination.
    The suggested-approach guard is ``[ "${REBUILD_SMOKE:-}" != "1" ]``;
    any equivalent (e.g. inverted ``if [ ... = "1" ]`` early exit on the
    block) is fine, but the upgrade line itself must be reachable only
    when ``REBUILD_SMOKE`` is unset or not ``1``.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    upgrade_idx = code.find("alembic upgrade head")
    assert upgrade_idx >= 0, "alembic upgrade head not present"
    # Look at the ~400 chars preceding the upgrade for the smoke guard. This
    # is a structural proxy: a guard close to the call site documents intent
    # and survives ordinary refactors. The smoke mode's existing curl-Range
    # block lives much further down the file, so this window won't match
    # that unrelated guard.
    window_start = max(0, upgrade_idx - 400)
    window = code[window_start:upgrade_idx]
    assert "REBUILD_SMOKE" in window, (
        "The 'alembic upgrade head' call must be guarded by a REBUILD_SMOKE "
        "check so a smoke run does not write to the destination DB. See #222."
    )


def test_alembic_upgrade_runs_in_repo_dir(script_text: str) -> None:
    """#222: alembic must run from ``$REPO_DIR`` so it picks up ``alembic.ini``.

    Without ``cd "$REPO_DIR"`` (or its subshell equivalent), alembic
    resolves ``alembic.ini`` against the cwd of the cron invocation,
    which is not guaranteed to be the repo root. The dep-refresh step
    just above already shows the convention: operate inside ``$REPO_DIR``
    rather than relying on cron's cwd.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    upgrade_idx = code.find("alembic upgrade head")
    assert upgrade_idx >= 0, "alembic upgrade head not present"
    # The call site must mention $REPO_DIR within a small window -- either as
    # a leading 'cd "$REPO_DIR" &&' or wrapped in a subshell '(cd "$REPO_DIR" && ...)'.
    window_start = max(0, upgrade_idx - 200)
    window = code[window_start : upgrade_idx + len("alembic upgrade head")]
    assert "$REPO_DIR" in window, (
        "The 'alembic upgrade head' call must run inside $REPO_DIR so it "
        "picks up the repo's alembic.ini. See #222."
    )
