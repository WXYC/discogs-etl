"""Smoke tests for the Sentry/JSON-logger wireup in this repo.

These tests verify that:

* Each script entrypoint imports cleanly (i.e. the ``lib.observability``
  shim and ``wxyc_etl`` re-exports resolve).
* The shim's ``init_logger`` returns without raising when
  ``SENTRY_DSN`` is unset.
* When ``wxyc_etl.logger`` is actually installed (gates Sentry init),
  calling it produces a JSON log line carrying the four contract tags
  ``repo`` / ``tool`` / ``step`` / ``run_id``.

The third test skips automatically when ``wxyc_etl.logger`` is missing —
this is the situation in CI today, since the install ref pinned in
``.github/workflows/ci.yml`` predates WXYC/wxyc-etl#50. Once that ref is
bumped, the test becomes active without further changes.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def _has_wxyc_logger() -> bool:
    try:
        import wxyc_etl.logger  # noqa: F401
    except Exception:
        return False
    return True


def _reset_root_logger() -> None:
    """Strip handlers/filters off the root logger so init_logger reinstalls."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(logging.WARNING)
    if _has_wxyc_logger():
        import wxyc_etl.logger as logger_mod

        logger_mod._INITIALIZED = False


def test_init_logger_shim_does_not_raise(monkeypatch):
    """The shim must always be safely callable, regardless of install state."""
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    _reset_root_logger()

    from lib.observability import init_logger

    init_logger(repo="discogs-etl", tool="discogs-etl test")


@pytest.mark.skipif(
    not _has_wxyc_logger(),
    reason="wxyc_etl.logger not installed (needs WXYC/wxyc-etl#50)",
)
def test_init_logger_emits_json_with_repo_tag(capfd, monkeypatch):
    """When wxyc_etl.logger is wired up, JSON logs carry the contract tags."""
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    _reset_root_logger()

    from lib.observability import init_logger

    guard = init_logger(repo="discogs-etl", tool="discogs-etl test")
    assert guard is not None, "expected wxyc_etl.logger guard"
    assert guard.sentry_enabled is False
    assert guard.run_id

    log = logging.getLogger("discogs-etl.smoke")
    log.info("hello", extra={"step": "smoke"})

    captured = capfd.readouterr()
    assert captured.err, "expected JSON log line on stderr"
    line = captured.err.strip().splitlines()[-1]
    payload = json.loads(line)

    assert payload["repo"] == "discogs-etl"
    assert payload["tool"] == "discogs-etl test"
    assert payload["step"] == "smoke"
    assert payload["run_id"] == guard.run_id
    assert payload["message"] == "hello"


@pytest.mark.parametrize(
    "script",
    [
        "run_pipeline.py",
        "import_csv.py",
        "dedup_releases.py",
        "verify_cache.py",
        "filter_csv.py",
        "resolve_collisions.py",
        "tsv_to_sqlite.py",
        "check_cache_drift.py",
    ],
)
def test_script_compiles(script):
    """Each entrypoint must compile (i.e. its imports resolve)."""
    path = SCRIPTS_DIR / script
    assert path.exists(), f"missing script: {path}"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import compileall, sys; "
                f"sys.exit(0 if compileall.compile_file('{path}', quiet=1) else 1)"
            ),
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, (
        f"{script} failed to compile:\nstdout={result.stdout}\nstderr={result.stderr}"
    )
