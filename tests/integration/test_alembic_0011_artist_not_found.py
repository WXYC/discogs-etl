"""Verify 0011 adds `artist.not_found` for LML#510's tombstone discriminator.

Parallel to 0010 but for the artist table. ``artist.name`` is ``text NOT NULL``
with no ``CHECK (name <> '')`` today; the tombstone INSERT writes ``name = ''``
+ ``not_found = TRUE``. The migration probes the same write so a future-added
CHECK trips this test (and the migration) rather than the prod LML write.

Tracked at WXYC/library-metadata-lookup#510.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0011_artist_not_found.py"
SCHEMA_DIR = REPO_ROOT / "schema"


def test_migration_file_exists() -> None:
    assert MIGRATION_PATH.exists(), f"0011 migration missing at {MIGRATION_PATH}."


def test_migration_adds_not_found_column() -> None:
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS not_found" in body
    assert "boolean" in body.lower()
    assert "NOT NULL" in body
    assert "DEFAULT FALSE" in body or "DEFAULT false" in body


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


@pytest.fixture()
def db_with_artist_table(fresh_db_url: str) -> str:
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    return fresh_db_url


@pytest.mark.pg
def test_not_found_column_created_at_head(db_with_artist_table: str) -> None:
    db_url = db_with_artist_table

    stamp = _run_alembic(["stamp", "0010_release_not_found"], db_url)
    assert stamp.returncode == 0, (
        f"alembic stamp failed:\nstdout: {stamp.stdout}\nstderr: {stamp.stderr}"
    )

    result = _run_alembic(["upgrade", "0011_artist_not_found"], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'artist'
              AND column_name = 'not_found'
            """
        )
        row = cur.fetchone()
        assert row is not None, "artist.not_found column missing after 0011 upgrade"
        data_type, is_nullable, column_default = row
        assert data_type == "boolean"
        assert is_nullable == "NO"
        assert column_default is not None and "false" in column_default.lower()


@pytest.mark.pg
def test_not_found_round_trip_with_empty_name(db_with_artist_table: str) -> None:
    """Pin the tombstone-write contract: ``name = ''`` + ``not_found = TRUE``."""
    db_url = db_with_artist_table

    stamp = _run_alembic(["stamp", "0010_release_not_found"], db_url)
    assert stamp.returncode == 0
    result = _run_alembic(["upgrade", "0011_artist_not_found"], db_url)
    assert result.returncode == 0

    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO artist (id, name, not_found, fetched_at)
            VALUES (-2147483648, '', TRUE, now())
            """
        )
        cur.execute("SELECT name, not_found FROM artist WHERE id = -2147483648")
        row = cur.fetchone()
        assert row == ("", True)
        cur.execute("DELETE FROM artist WHERE id = -2147483648")


@pytest.mark.pg
def test_not_found_idempotent_on_reapply(db_with_artist_table: str) -> None:
    db_url = db_with_artist_table

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'artist' AND column_name = 'not_found'"
        )
        assert cur.fetchone() is not None

    stamp = _run_alembic(["stamp", "0010_release_not_found"], db_url)
    assert stamp.returncode == 0

    result = _run_alembic(["upgrade", "0011_artist_not_found"], db_url)
    assert result.returncode == 0
