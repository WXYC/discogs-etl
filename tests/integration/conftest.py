"""Shared fixtures for integration tests.

Layered on top of ``tests/conftest.py`` (which provides ``db_url`` /
``fresh_db_url`` against the Docker PG). This file adds helpers that
several alembic-migration tests need.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


# Cap any single alembic invocation. A migration that deadlocks (e.g., a
# CREATE INDEX waiting on a lock it never gets) would otherwise hang the
# test process until CI's outer timeout — often tens of minutes — fires.
# 60s comfortably covers the slowest migration in the chain today
# (sub-second) with headroom for Docker-cold-start jitter.
_ALEMBIC_SUBPROCESS_TIMEOUT_SECONDS = 60


def _run_alembic(args: list[str], db_url: str) -> subprocess.CompletedProcess[str]:
    """Invoke ``python -m alembic <args>`` with the cache DB URL set.

    Pops ``DATABASE_URL`` so the deprecated fallback in ``alembic/env.py``
    can't accidentally point the subprocess at a different DB if it's
    inherited from the caller's shell. Raises ``subprocess.TimeoutExpired``
    rather than hanging if the subprocess deadlocks.
    """
    env = {**os.environ, "DATABASE_URL_DISCOGS": db_url}
    env.pop("DATABASE_URL", None)
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=_ALEMBIC_SUBPROCESS_TIMEOUT_SECONDS,
    )


@pytest.fixture()
def run_alembic():
    """Function-scoped fixture exposing the subprocess wrapper.

    Migration tests pass a per-test fresh DB URL through the wrapper to
    drive ``alembic stamp`` / ``alembic upgrade`` / ``alembic downgrade``
    against an isolated database.
    """
    return _run_alembic
