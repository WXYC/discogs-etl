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


def _run_alembic(args: list[str], db_url: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "DATABASE_URL_DISCOGS": db_url}
    # Drop any inherited DATABASE_URL so we can't accidentally validate the
    # deprecated-fallback path here.
    env.pop("DATABASE_URL", None)
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.mark.pg
def test_alembic_upgrade_head_against_empty_db(db_url: str) -> None:
    result = _run_alembic(["upgrade", "0001_initial"], db_url)
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


@pytest.mark.pg
def test_alembic_upgrade_head_sql_against_populated_db_is_safe(db_url: str) -> None:
    # Regression for the 2026-04-28 prod-cache wipe: 0001_initial.upgrade()
    # opens its own psycopg connection in autocommit, which bypasses alembic's
    # `--sql` (offline mode) interception. Running `alembic upgrade head --sql`
    # against a populated DB used to silently DROP every release/artist table.
    # The defensive guard in 0001_initial.py refuses to run in offline mode.
    apply = _run_alembic(["upgrade", "0001_initial"], db_url)
    assert apply.returncode == 0, (
        f"baseline apply failed:\nstdout: {apply.stdout}\nstderr: {apply.stderr}"
    )

    sql_run = _run_alembic(["upgrade", "head", "--sql"], db_url)
    assert sql_run.returncode != 0, (
        "alembic upgrade head --sql should fail against this baseline; instead got "
        f"returncode 0:\nstdout: {sql_run.stdout}\nstderr: {sql_run.stderr}"
    )
    combined = sql_run.stdout + sql_run.stderr
    assert "offline mode" in combined.lower(), (
        f"expected offline-mode error, got:\nstdout: {sql_run.stdout}\nstderr: {sql_run.stderr}"
    )

    # The schema and version row must be untouched after the failed --sql call.
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.release')")
        assert cur.fetchone() == ("release",), "release table dropped by --sql attempt"
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone() == ("0001_initial",), "alembic_version mutated by --sql attempt"


@pytest.mark.pg
def test_alembic_upgrade_head_against_populated_unstamped_db_is_safe(db_url: str) -> None:
    # The other half of the 2026-04-28 risk: an operator running
    # `alembic upgrade head` (no --sql) against a populated DB that was never
    # stamped — exactly the situation that exists in prod before the one-time
    # stamp procedure runs. Without the populated-DB short-circuit guard, the
    # side-channel would re-execute schema/create_database.sql, whose first
    # statements are `DROP TABLE IF EXISTS release ... CASCADE`.
    schema_dir = REPO_ROOT / "schema"
    schema_files = (
        "create_functions.sql",
        "create_database.sql",
        "create_indexes.sql",
        "create_track_indexes.sql",
    )
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for name in schema_files:
            sql = (schema_dir / name).read_text().replace(" CONCURRENTLY", "")
            cur.execute(sql)
        cur.execute("INSERT INTO release (id, title) VALUES (424242, 'Sentinel')")

    result = _run_alembic(["upgrade", "0001_initial"], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade head failed against populated-unstamped DB:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT title FROM release WHERE id = 424242")
        row = cur.fetchone()
        assert row == ("Sentinel",), (
            "sentinel row vanished — schema was re-applied against populated DB"
        )
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone() == ("0001_initial",), (
            "alembic should still record the baseline as applied after the short-circuit"
        )
