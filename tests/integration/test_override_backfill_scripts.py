"""End-to-end integration tests for the discogs-etl#329 backfill-prep tools:
``scripts/query_missing_override_release_ids.py`` and
``scripts/check_override_parity.py``, run against real PostgreSQL.

Complements ``tests/integration/test_library_release_overrides.py`` (which
tests the shared query functions directly) by proving the two operator-facing
CLI entry points wire those functions together correctly end-to-end,
including writing a real ids-file.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
pytestmark = [pytest.mark.pg]


def _load_module(name: str, rel_path: str):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / rel_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _set_up_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE release (id integer PRIMARY KEY, title text)")
        cur.execute("CREATE SCHEMA lml_cache")
        cur.execute("""
            CREATE TABLE lml_cache.library_release_override (
                library_id integer NOT NULL,
                discogs_release_id integer NOT NULL,
                source text NOT NULL
            )
        """)


class TestQueryMissingOverrideReleaseIdsCli:
    def test_writes_missing_ids_consumable_by_the_seeder(
        self, fresh_db_url: str, tmp_path: Path, capsys
    ) -> None:
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Present')")
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES "
                "(10, 1, 'alex-l-2026'), (11, 2, 'alex-l-2026'), (12, 3, 'alex-l-2026-masters-api')"
            )
        conn.close()

        query_mod = _load_module(
            "query_missing_override_release_ids_it",
            "scripts/query_missing_override_release_ids.py",
        )
        out_file = tmp_path / "missing_ids.txt"
        rc = query_mod.main(["--database-url", fresh_db_url, "--output", str(out_file)])
        assert rc == 0

        printed = capsys.readouterr().out
        assert "alex-l-2026" in printed

        seed_mod = _load_module("seed_cache_from_clone_it", "scripts/seed_cache_from_clone.py")
        assert seed_mod._read_ids_file(out_file) == [2, 3]


class TestCheckOverrideParityCli:
    def test_exits_zero_once_backfill_closes_the_gap(self, fresh_db_url: str) -> None:
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Present')")
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES (10, 1, 'alex-l-2026')"
            )
        conn.close()

        parity_mod = _load_module("check_override_parity_it", "scripts/check_override_parity.py")
        rc = parity_mod.main(["--database-url", fresh_db_url])
        assert rc == 0

    def test_exits_nonzero_before_the_backfill_runs(self, fresh_db_url: str) -> None:
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES (10, 1, 'alex-l-2026')"
            )
        conn.close()

        parity_mod = _load_module("check_override_parity_it", "scripts/check_override_parity.py")
        rc = parity_mod.main(["--database-url", fresh_db_url])
        assert rc == 1

    def test_seeding_the_missing_release_flips_parity_to_healthy(self, fresh_db_url: str) -> None:
        """Simulates the backfill's effect directly (INSERT INTO release) to
        prove the parity check reflects the seeder's real postcondition,
        without invoking the seeder itself (covered by
        tests/integration/test_seed_cache_from_clone.py)."""
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES (10, 1, 'alex-l-2026')"
            )

        parity_mod = _load_module("check_override_parity_it", "scripts/check_override_parity.py")
        assert parity_mod.main(["--database-url", fresh_db_url]) == 1

        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Backfilled')")
        conn.close()

        assert parity_mod.main(["--database-url", fresh_db_url]) == 0
