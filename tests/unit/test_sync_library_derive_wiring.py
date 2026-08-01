"""Pin the va_release derive wiring in the daily sync (#344).

The daily ``sync-library.sh`` run is the delivery mechanism for the one-off
prod backfill of ``va_release`` AND its ongoing freshness (LML inserts
API-fetched VA releases at runtime, and the monthly rebuild's cron is
disabled). Two failure modes reviewed out of the original design would have
silently defeated it, so both are pinned here:

1. The derive invocation must use a path that resolves from ``$REPO_DIR``
   (the script ``cd``s there; there is no ``SCRIPT_DIR`` variable and no
   ``set -u``, so an undefined-variable path would expand empty and the
   soft-fail would swallow the "No such file" forever).
2. ``sync-library.yml`` must install this repo's dependencies BEFORE the
   "Run library sync" step — the job interpreter is a bare setup-python
   ``python3``, every pre-existing sync-path script is stdlib-only, and
   ``derive_va_release.py`` imports psycopg. With the install after the
   sync (its pre-#344 position, serving only the metrics step), the derive
   would ``ModuleNotFoundError`` into the soft-fail WARN on every run.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNC_SCRIPT = REPO_ROOT / "scripts" / "sync-library.sh"
SYNC_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "sync-library.yml"


class TestSyncLibraryDeriveInvocation:
    def test_derive_invoked_with_repo_relative_path(self) -> None:
        source = SYNC_SCRIPT.read_text()
        assert 'if ! "$PYTHON" scripts/derive_va_release.py' in source, (
            "sync-library.sh must invoke derive_va_release.py via the "
            "repo-relative path under the file's if-!-WARN soft-fail idiom"
        )

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
        source = SYNC_SCRIPT.read_text()
        start = source.index('if ! "$PYTHON" scripts/derive_va_release.py')
        block = source[start : source.index("fi", start) + 2]
        assert "EXIT_CODE" not in block


class TestSyncWorkflowInstallOrdering:
    def test_dependency_install_precedes_run_library_sync(self) -> None:
        source = SYNC_WORKFLOW.read_text()
        install_pos = source.index('pip install -e ".[dev]"')
        sync_pos = source.index("name: Run library sync")
        assert install_pos < sync_pos, (
            "pip install -e .[dev] must run before the sync step so "
            "derive_va_release.py finds psycopg in the job interpreter"
        )

    def test_install_step_not_duplicated(self) -> None:
        source = SYNC_WORKFLOW.read_text()
        assert source.count('pip install -e ".[dev]"') == 1, (
            "move the install step, don't duplicate it — one install serves "
            "both the derive step and the cache-health metrics step"
        )
