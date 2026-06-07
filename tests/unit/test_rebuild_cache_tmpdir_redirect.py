"""Pin TMPDIR-redirect invariant of ``scripts/rebuild-cache.sh``.

The 2026-06-07 manual jumpstart rebuild (#267) failed twice with
``No space left on device (os error 28)`` at ~94% of the release scan.
Both failures had the same shape regardless of the EBS root volume size
(100 GB on run 1, 200 GB on run 2 after #268 bumped the launch template).

Root cause: ``run_pipeline.py:821`` uses
``tempfile.TemporaryDirectory(prefix="discogs_pipeline_")``, which honors
``$TMPDIR`` and defaults to ``/tmp``. Amazon Linux 2023 (the
launch-template AMI) mounts ``/tmp`` on **tmpfs** sized at ~50% of RAM.
On the ``c6i.large`` instance (4 GB RAM) the tmpfs cap is ~2 GB. The
converter's CSV output crosses that line at the same point in the
release scan every time — independent of EBS size.

The fix is to redirect ``TMPDIR`` to a path under ``$WORK_DIR`` (which
``rebuild-cache.sh`` creates under ``$REPO_DIR`` on the EBS volume) so
the converter's CSV staging lands on disk, not in RAM. The existing
WORK_DIR cleanup trap already covers the new subdir, so the
TemporaryDirectory lifecycle stays self-consistent.

This test pins three invariants so the redirect can't silently regress:

1. ``TMPDIR=`` is set somewhere in the script (presence test).
2. The TMPDIR value references ``$WORK_DIR`` (or ``$REPO_DIR``) — i.e.,
   it points at the EBS-backed path, not ``/tmp`` or another tmpfs
   location.
3. ``TMPDIR`` is set BEFORE ``python ... run_pipeline.py`` so the
   redirect is in scope when the converter spawns.

See #271 (real fix), #268 (incomplete prior diagnosis), #269 (bash-trap
exit-code propagation; verified working when this fix runs end-to-end).
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
    """Return index of the first non-comment line containing ``needle``."""
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if needle in line:
            return i
    raise AssertionError(f"{needle!r} not found in non-comment lines of {SCRIPT_PATH}")


def _find_tmpdir_assignment(lines: list[str]) -> tuple[int, str]:
    """Return (line index, full line) of the TMPDIR assignment.

    Matches both ``TMPDIR=...`` and ``export TMPDIR=...`` on a
    non-comment line.
    """
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if "TMPDIR=" in stripped and not stripped.startswith("#"):
            return i, line
    raise AssertionError(
        "no TMPDIR assignment found in rebuild-cache.sh. The converter's "
        "tempfile.TemporaryDirectory defaults to /tmp, which is tmpfs (RAM) "
        "on Amazon Linux 2023 — see #271."
    )


def test_tmpdir_is_set(script_lines: list[str]) -> None:
    """#271: rebuild-cache.sh must set TMPDIR.

    Without it, ``tempfile.TemporaryDirectory`` falls back to ``/tmp`` —
    tmpfs on AL2023 — and the converter's CSV staging hits the ~2 GB
    RAM cap on c6i.large before the release scan completes.
    """
    _find_tmpdir_assignment(script_lines)


def test_tmpdir_points_to_ebs_backed_path(script_lines: list[str]) -> None:
    """#271: TMPDIR's value must reference WORK_DIR or REPO_DIR.

    Either anchor keeps the temp dir on the EBS root volume:
      - ``$WORK_DIR`` is the per-rebuild mktemp directory under
        ``$REPO_DIR``; cleaned up by the existing WORK_DIR trap.
      - ``$REPO_DIR`` (or any subpath of it) is also EBS-backed.

    Hard-coding a different path (``/var/tmp``, ``/opt/tmp``, etc.) is
    technically also off-tmpfs but is brittle to LT/AMI changes.
    Tying TMPDIR to the script's own WORK_DIR keeps the redirect
    self-contained and reuses the existing cleanup.
    """
    _, line = _find_tmpdir_assignment(script_lines)
    assert "$WORK_DIR" in line or "$REPO_DIR" in line or "${WORK_DIR}" in line or "${REPO_DIR}" in line, (
        f"TMPDIR assignment must reference $WORK_DIR or $REPO_DIR so the "
        f"converter's temp dir lives on EBS, not tmpfs. Got: {line!r}. "
        f"See #271."
    )


def test_tmpdir_set_before_run_pipeline(script_lines: list[str]) -> None:
    """#271: TMPDIR must be exported BEFORE ``python ... run_pipeline.py``.

    The TemporaryDirectory call inside ``run_pipeline.py`` reads ``$TMPDIR``
    from its own process env, inherited from the bash that exec'd it. A
    TMPDIR assignment AFTER the python invocation has no effect on that
    process — it would only affect anything spawned later (currently
    nothing in this script).
    """
    tmpdir_idx, _ = _find_tmpdir_assignment(script_lines)
    pipeline_idx = _first_index(script_lines, "run_pipeline.py")
    assert tmpdir_idx < pipeline_idx, (
        f"TMPDIR assignment (line {tmpdir_idx + 1}) must precede the "
        f"run_pipeline.py invocation (line {pipeline_idx + 1}). Otherwise "
        f"the redirect is out of scope when the converter spawns and "
        f"tempfile.TemporaryDirectory still falls back to /tmp (tmpfs). "
        f"See #271."
    )


def test_tmpdir_set_after_work_dir_mkdir(script_lines: list[str]) -> None:
    """#271: TMPDIR must be set AFTER ``WORK_DIR=...`` is initialized.

    A TMPDIR=$WORK_DIR line that runs before WORK_DIR is assigned
    resolves to an empty string and tempfile falls back to /tmp.
    Pin the ordering so a refactor can't accidentally invert it.
    """
    tmpdir_idx, _ = _find_tmpdir_assignment(script_lines)
    workdir_idx = _first_index(script_lines, 'WORK_DIR="$(mktemp')
    assert workdir_idx < tmpdir_idx, (
        f"TMPDIR assignment (line {tmpdir_idx + 1}) must follow the "
        f"WORK_DIR creation (line {workdir_idx + 1}); otherwise "
        f"TMPDIR=$WORK_DIR expands to empty and tempfile falls back to "
        f"/tmp. See #271."
    )
