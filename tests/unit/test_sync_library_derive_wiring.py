"""Pin the va_release derive wiring in the daily sync (#344).

The daily ``sync-library.sh`` run is the delivery mechanism for the one-off
prod backfill of ``va_release`` AND its ongoing freshness (LML inserts
API-fetched VA releases at runtime, and the monthly rebuild's cron is
disabled). Failure modes reviewed out of the original design would have
silently or loudly defeated it, so each is pinned here:

1. The derive invocation must use a path that resolves from ``$REPO_DIR``
   (the script ``cd``s there; there is no ``SCRIPT_DIR`` variable and no
   ``set -u``, so an undefined-variable path would expand empty and the
   soft-fail would swallow the "No such file" forever).
2. ``sync-library.yml`` must install psycopg BEFORE the "Run library sync"
   step — the job interpreter is a bare setup-python ``python3``, every
   pre-existing sync-path script is stdlib-only, and ``derive_va_release.py``
   imports psycopg. The install must be minimal and soft-fail: the full
   ``pip install -e ".[dev]"`` stays AFTER the sync, where a transient pip
   or resolver failure can only cost the metrics step, never the core
   library.db sync/upload (the invariant the LML-venv provisioning step
   documents).
3. The derive block must not touch ``EXIT_CODE``, which gates both the
   recall-index build and the library.db release upload.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNC_SCRIPT = REPO_ROOT / "scripts" / "sync-library.sh"
SYNC_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "sync-library.yml"

# The derive invocation line: an if-!-guarded $PYTHON (quoted or not) run of
# the repo-relative script path. Tolerant of quoting/brace style so a
# legitimate reformat doesn't read as a wiring regression.
_INVOCATION_RE = re.compile(r'if\s+!\s+"?\$\{?PYTHON\}?"?\s+scripts/derive_va_release\.py')


def _derive_block(source: str) -> str:
    """The shell block from the derive's if-line through its closing ``fi``.

    Anchors the end on a line consisting solely of ``fi`` (the file's style
    for every soft-fail block) rather than the first \"fi\" substring, which
    would truncate mid-word on message text like \"backfill\".
    """
    match = _INVOCATION_RE.search(source)
    assert match, (
        "sync-library.sh must invoke derive_va_release.py via an if-!-guarded "
        '"$PYTHON" run of the repo-relative path (the script has already '
        "cd'd to $REPO_DIR)"
    )
    end = re.compile(r"^fi\s*$", re.MULTILINE).search(source, match.start())
    assert end, "unterminated derive block: no closing 'fi' line found"
    return source[match.start() : end.end()]


class TestSyncLibraryDeriveInvocation:
    def test_derive_invoked_with_repo_relative_path(self) -> None:
        _derive_block(SYNC_SCRIPT.read_text())

    def test_derive_runs_before_recall_index_build(self) -> None:
        source = SYNC_SCRIPT.read_text()
        derive_pos = source.index("derive_va_release.py")
        recall_pos = source.index("build_compilation_track_location")
        assert derive_pos < recall_pos, (
            "the derive must run before the recall-index build that reads va_release"
        )

    def test_derive_soft_fail_does_not_touch_exit_code(self) -> None:
        """EXIT_CODE gates the recall-index build and the library.db release
        upload; the derive block must warn-and-continue without setting it."""
        block = _derive_block(SYNC_SCRIPT.read_text())
        assert "EXIT_CODE" not in block


class TestSyncWorkflowInstallOrdering:
    def test_psycopg_install_precedes_run_library_sync(self) -> None:
        source = SYNC_WORKFLOW.read_text()
        install_pos = source.index('pip install --quiet "psycopg[binary]')
        sync_pos = source.index("name: Run library sync")
        assert install_pos < sync_pos, (
            "the psycopg install must run before the sync step so "
            "derive_va_release.py finds psycopg in the job interpreter"
        )

    def test_psycopg_install_is_soft_fail(self) -> None:
        """A pip failure for the derive's dependency must never take the core
        library.db sync/upload down (the LML-venv step's documented
        invariant); the derive then soft-fails inside sync-library.sh."""
        source = SYNC_WORKFLOW.read_text()
        step_start = source.index("Install psycopg for the va_release derive step")
        step_end = source.index("name: Run library sync", step_start)
        block = source[step_start:step_end]
        assert "set +e" in block and "::warning::" in block

    def test_dev_install_stays_after_the_sync(self) -> None:
        """The full [dev] install serves only the post-upload metrics step; a
        resolver failure in its ~11-package tree must stay unable to block
        the sync (its pre-#344 blast radius)."""
        source = SYNC_WORKFLOW.read_text()
        assert source.count('pip install -e ".[dev]"') == 1
        dev_install_pos = source.index('pip install -e ".[dev]"')
        sync_pos = source.index("name: Run library sync")
        assert dev_install_pos > sync_pos

    def test_floor_knob_forwarded_to_sync_env(self) -> None:
        """docs/automation.md documents VA_RELEASE_FLOOR as the operator knob;
        the daily GH Actions run is the only production derivation path, so
        the workflow must actually forward it."""
        source = SYNC_WORKFLOW.read_text()
        sync_pos = source.index("name: Run library sync")
        release_pos = source.index("name: Update library.db in LML release", sync_pos)
        sync_step = source[sync_pos:release_pos]
        assert "VA_RELEASE_FLOOR" in sync_step
