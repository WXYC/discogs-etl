"""Verify the alembic 0001_initial baseline applies cleanly to an empty Postgres.

The baseline replays ``schema/*.sql`` in pipeline order, so the surface this
guards is: CONCURRENTLY-stripping works, dollar-quoted DO blocks survive, and
alembic stamps ``version_num = '0001_initial'`` on success.

Marked ``pg`` because it needs a live Postgres; lives in ``tests/integration/``
because the unit of work spans an actual migration tool + database round-trip.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.pg
def test_alembic_upgrade_head_against_empty_db(db_url: str) -> None:
    env = {**os.environ, "DATABASE_URL_DISCOGS": db_url}
    # Drop any inherited DATABASE_URL so we can't accidentally validate the
    # deprecated-fallback path here.
    env.pop("DATABASE_URL", None)

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        row = cur.fetchone()
        assert row == ("0001_initial",), f"unexpected alembic_version row: {row}"

        # Spot-check the contract: the canonical tables and the trigram index
        # whose creation expression depends on f_unaccent() (issue #104).
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        tables = {row[0] for row in cur.fetchall()}
        for required in ("release", "release_artist", "release_track", "cache_metadata"):
            assert required in tables, f"missing table {required} after baseline"

        cur.execute(
            "SELECT 1 FROM pg_indexes "
            "WHERE schemaname = 'public' AND indexname = 'idx_release_artist_name_trgm'"
        )
        assert cur.fetchone() is not None, "trigram index missing after baseline"
