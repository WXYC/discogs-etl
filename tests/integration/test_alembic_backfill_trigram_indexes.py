"""Verify 0002_backfill_trigram_indexes lands the four GIN trigram indexes
that were missing from production after the baseline's populated-DB
short-circuit kept them from being applied.

The migration uses ``CREATE INDEX CONCURRENTLY IF NOT EXISTS`` via a
side-channel autocommit psycopg connection (mirrors 0001_initial), so the
guarded surface is: indexes land cleanly, the apply is idempotent on a DB
that already has them, downgrade drops them, and offline ``--sql`` mode is
refused.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

EXPECTED_INDEXES = {
    "idx_release_track_title_trgm",
    "idx_release_track_artist_name_trgm",
    "idx_release_artist_name_trgm",
    "idx_release_title_trgm",
}


def _run_alembic(args: list[str], db_url: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "DATABASE_URL_DISCOGS": db_url}
    env.pop("DATABASE_URL", None)
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


def _present_trigram_indexes(db_url: str) -> set[str]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND indexname = ANY(%s)",
            (list(EXPECTED_INDEXES),),
        )
        return {row[0] for row in cur.fetchall()}


@pytest.mark.pg
def test_upgrade_to_head_creates_all_four_trigram_indexes(db_url: str) -> None:
    """Fresh DB → upgrade head → all four indexes exist + version stamped at 0002."""
    result = _run_alembic(["upgrade", "head"], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    assert _present_trigram_indexes(db_url) == EXPECTED_INDEXES, (
        f"missing one or more trigram indexes; present: {_present_trigram_indexes(db_url)}"
    )

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone() == ("0002_backfill_trigram_indexes",)


@pytest.mark.pg
def test_upgrade_is_idempotent_when_indexes_already_exist(db_url: str) -> None:
    """Backfill scenario: tables + trigram indexes pre-exist, DB stamped at 0001.
    Running ``alembic upgrade head`` must succeed and leave the indexes alone."""
    # Apply the baseline first so we have the schema, then create the trigram
    # indexes by hand to simulate a DB that was patched by an operator before
    # this migration landed (i.e. exactly what was done to production).
    baseline = _run_alembic(["upgrade", "0001_initial"], db_url)
    assert baseline.returncode == 0, (
        f"baseline apply failed:\nstdout: {baseline.stdout}\nstderr: {baseline.stderr}"
    )
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for index_name, table, column in (
            ("idx_release_track_title_trgm", "release_track", "title"),
            ("idx_release_track_artist_name_trgm", "release_track_artist", "artist_name"),
            ("idx_release_artist_name_trgm", "release_artist", "artist_name"),
            ("idx_release_title_trgm", "release", "title"),
        ):
            cur.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} "
                f"ON {table} USING GIN (lower(f_unaccent({column})) gin_trgm_ops)"
            )

    result = _run_alembic(["upgrade", "head"], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade head should be a no-op when indexes pre-exist:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert _present_trigram_indexes(db_url) == EXPECTED_INDEXES


@pytest.mark.pg
def test_downgrade_drops_the_four_trigram_indexes(db_url: str) -> None:
    """Apply head, then downgrade -1, indexes should be gone and version backed off."""
    apply = _run_alembic(["upgrade", "head"], db_url)
    assert apply.returncode == 0, (
        f"upgrade head failed:\nstdout: {apply.stdout}\nstderr: {apply.stderr}"
    )

    down = _run_alembic(["downgrade", "-1"], db_url)
    assert down.returncode == 0, (
        f"downgrade -1 failed:\nstdout: {down.stdout}\nstderr: {down.stderr}"
    )

    assert _present_trigram_indexes(db_url) == set()
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone() == ("0001_initial",)


@pytest.mark.pg
def test_offline_mode_refused_for_upgrade(db_url: str) -> None:
    """``--sql`` (offline mode) cannot honestly dry-run a side-channel autocommit
    revision; refusing loud avoids the silent-no-op trap that bit 0001."""
    # First land the baseline so we don't conflate the offline-mode error with a
    # missing-down_revision error.
    apply = _run_alembic(["upgrade", "0001_initial"], db_url)
    assert apply.returncode == 0

    sql_run = _run_alembic(["upgrade", "head", "--sql"], db_url)
    assert sql_run.returncode != 0, (
        "alembic upgrade head --sql should fail; got returncode 0:\n"
        f"stdout: {sql_run.stdout}\nstderr: {sql_run.stderr}"
    )
    combined = sql_run.stdout + sql_run.stderr
    assert "offline mode" in combined.lower(), (
        f"expected offline-mode error, got:\nstdout: {sql_run.stdout}\nstderr: {sql_run.stderr}"
    )
