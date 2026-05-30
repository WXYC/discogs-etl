"""Integration tests for ``scripts/cache_health_metrics.py``.

Exercises ``count_artwork_states`` end-to-end against a PG database that
has the ``release`` table from ``schema/create_database.sql`` applied.
Uses ``DATABASE_URL_TEST`` per the repo's pg-marker convention.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import uuid
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "cache_health_metrics.py"
CREATE_DB_SQL = REPO_ROOT / "schema" / "create_database.sql"

pytestmark = pytest.mark.pg


def _load_module():
    spec = importlib.util.spec_from_file_location("cache_health_metrics", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["cache_health_metrics"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def isolated_db():
    """Create + drop a temporary database so each test gets a clean release table."""
    base_url = os.environ.get("DATABASE_URL_TEST")
    if not base_url:
        pytest.skip("DATABASE_URL_TEST not set")
    db_name = f"cache_health_metrics_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(base_url, autocommit=True)
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{db_name}"')
    finally:
        admin.close()
    test_url = base_url.rsplit("/", 1)[0] + "/" + db_name
    try:
        sql = CREATE_DB_SQL.read_text()
        with psycopg.connect(test_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        yield test_url
    finally:
        admin = psycopg.connect(base_url, autocommit=True)
        try:
            with admin.cursor() as cur:
                # Terminate any lingering connections from the worker before drop.
                cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (db_name,),
                )
                cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        finally:
            admin.close()


def _insert_release(conn, *, release_id: int, artwork_url, artwork_checked_at) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO release (id, title, artwork_url, artwork_checked_at) "
            "VALUES (%s, %s, %s, %s)",
            (release_id, f"title-{release_id}", artwork_url, artwork_checked_at),
        )


class TestCountArtworkStatesIntegration:
    def test_empty_release_returns_zeros(self, isolated_db: str) -> None:
        mod = _load_module()
        states = mod.count_artwork_states(isolated_db)
        assert states == mod.ArtworkStates(total=0, never_asked=0, imageless=0)

    def test_classifies_three_artwork_states(self, isolated_db: str) -> None:
        # 2 have artwork, 3 never_asked (NULL/NULL), 1 imageless (NULL url + NOT NULL ts).
        with psycopg.connect(isolated_db, autocommit=True) as conn:
            _insert_release(
                conn,
                release_id=5001,
                artwork_url="https://example/1.jpg",
                artwork_checked_at="2026-05-29 00:00:00+00",
            )
            _insert_release(
                conn,
                release_id=5002,
                artwork_url="https://example/2.jpg",
                artwork_checked_at="2026-05-29 00:00:00+00",
            )
            _insert_release(conn, release_id=5003, artwork_url=None, artwork_checked_at=None)
            _insert_release(conn, release_id=5004, artwork_url=None, artwork_checked_at=None)
            _insert_release(conn, release_id=5005, artwork_url=None, artwork_checked_at=None)
            _insert_release(
                conn, release_id=5006, artwork_url=None, artwork_checked_at="2026-05-29 00:00:00+00"
            )
        mod = _load_module()
        states = mod.count_artwork_states(isolated_db)
        assert states.total == 6
        assert states.never_asked == 3
        assert states.imageless == 1
