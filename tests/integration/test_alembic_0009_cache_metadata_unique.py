"""Verify the 0009_cache_metadata_unique migration.

``import_csv.py:524``'s ``populate_cache_metadata`` does
``INSERT ... ON CONFLICT (release_id) DO NOTHING`` to tolerate the
LML race (#188 / #207). For Postgres to honor that, ``cache_metadata``
must have a UNIQUE or PRIMARY KEY constraint on ``release_id``.

``schema/create_database.sql`` defines ``release_id integer PRIMARY KEY``,
so a fresh ground-up rebuild has the constraint. But Railway's existing
cache_metadata table predates that definition (alembic 0001's
schema-presence guard only references the table, doesn't create it) and
**doesn't have the constraint**. The 2026-06-07 #267 manual jumpstart
run 3 (i-0cb8b5600672eecd5) hit this:

    psycopg.errors.InvalidColumnReference:
      there is no unique or exclusion constraint matching the ON CONFLICT
      specification at scripts/import_csv.py:524

0009 fixes the drift idempotently:
  1. Dedupe any pre-existing duplicates, **keeping the api_fetch row**
     (cached LML API data must be preserved across rebuilds per the
     populate_cache_metadata docstring).
  2. Add ``UNIQUE (release_id)`` only if no PRIMARY KEY or UNIQUE already
     covers ``release_id`` alone (no-op on fresh DBs that ran
     create_database.sql, real work on Railway).

See #273 (this migration), #188 (LML race), #207 (the missing PG
integration test that would have caught this), #267 (driving incident).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0009_cache_metadata_unique.py"
SCHEMA_DIR = REPO_ROOT / "schema"


# ---------------------------------------------------------------------------
# Static (no DB) checks
# ---------------------------------------------------------------------------


def test_migration_file_exists() -> None:
    assert MIGRATION_PATH.exists(), (
        f"0009 migration missing at {MIGRATION_PATH}. The Railway-side cache_metadata "
        "table lacks the UNIQUE(release_id) constraint that populate_cache_metadata's "
        "ON CONFLICT requires; without this migration the load step crashes (#273)."
    )


def test_migration_has_dedupe_step() -> None:
    """0009 must dedupe before adding the constraint.

    Otherwise pre-existing duplicate release_id rows (e.g., a 'bulk_import'
    row plus an 'api_fetch' row from a partial prior rebuild) would make
    the UNIQUE constraint creation fail with 23505. The dedupe must
    preserve the api_fetch row (LML cached data — see #188).
    """
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    # Look for the dedupe pattern shape, not exact whitespace.
    assert "DELETE FROM cache_metadata" in body, (
        "0009 must DELETE FROM cache_metadata to dedupe before adding the constraint."
    )
    assert "api_fetch" in body, (
        "0009 dedupe must reference 'api_fetch' so the LML cached rows survive. "
        "See populate_cache_metadata docstring + #188."
    )


def test_migration_adds_unique_constraint() -> None:
    """0009 must add UNIQUE (or PRIMARY KEY) on cache_metadata.release_id."""
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "cache_metadata" in body and "UNIQUE" in body and "release_id" in body, (
        "0009 must add a UNIQUE constraint on cache_metadata(release_id) so "
        "populate_cache_metadata's ON CONFLICT matches. See #273."
    )


def test_migration_revision_chain() -> None:
    """0009 must revise 0008 so `alembic upgrade head` picks it up."""
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "0009_cache_metadata_unique" in body, "revision id missing/wrong"
    assert "0008_release_artwork_checked_at" in body, (
        "down_revision must point at 0008 — without the chain, alembic upgrade head won't run 0009."
    )


# ---------------------------------------------------------------------------
# Live-PG assertions
# ---------------------------------------------------------------------------


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


def _setup_drifted_schema(db_url: str) -> None:
    """Apply create_database.sql, then DROP the cache_metadata PK to mimic Railway.

    Railway's cache_metadata predates the canonical schema definition and
    lacks the PRIMARY KEY constraint. Production drift simulation: apply
    the full schema, then drop the PK so the constraint-search returns nothing.
    """
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        # Drop the PK so the test env looks like the drifted Railway state.
        # The constraint name is auto-generated by Postgres for inline PRIMARY KEY.
        cur.execute(
            """
            DO $$
            DECLARE pk_name text;
            BEGIN
                SELECT conname INTO pk_name
                FROM pg_constraint
                WHERE conrelid = 'cache_metadata'::regclass AND contype = 'p';
                IF pk_name IS NOT NULL THEN
                    EXECUTE format('ALTER TABLE cache_metadata DROP CONSTRAINT %I', pk_name);
                END IF;
            END $$;
            """
        )


def _has_unique_or_pk_on_release_id(conn) -> bool:
    """True iff cache_metadata has a PK or UNIQUE constraint covering release_id alone."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM pg_constraint c
            WHERE c.conrelid = 'cache_metadata'::regclass
              AND c.contype IN ('p', 'u')
              AND c.conkey = ARRAY[(
                  SELECT a.attnum FROM pg_attribute a
                  WHERE a.attrelid = 'cache_metadata'::regclass AND a.attname = 'release_id'
              )]::smallint[]
            LIMIT 1
            """
        )
        return cur.fetchone() is not None


@pytest.mark.pg
def test_constraint_added_when_missing(fresh_db_url: str) -> None:
    """Drifted Railway state: cache_metadata exists without UNIQUE/PK. Upgrade fixes it."""
    _setup_drifted_schema(fresh_db_url)

    with psycopg.connect(fresh_db_url) as conn:
        assert not _has_unique_or_pk_on_release_id(conn), (
            "Pre-condition: drift simulation should have left cache_metadata.release_id "
            "without a UNIQUE/PK constraint. If this fires, the drift setup is wrong "
            "and the rest of this test wouldn't actually exercise the upgrade path."
        )

    stamp = _run_alembic(["stamp", "0008_release_artwork_checked_at"], fresh_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"

    result = _run_alembic(["upgrade", "head"], fresh_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"

    with psycopg.connect(fresh_db_url) as conn:
        assert _has_unique_or_pk_on_release_id(conn), (
            "0009 upgrade must add UNIQUE/PK on cache_metadata.release_id. Without "
            "this, populate_cache_metadata's ON CONFLICT still crashes (#273)."
        )


@pytest.mark.pg
def test_dedupe_keeps_api_fetch_row(fresh_db_url: str) -> None:
    """Pre-existing duplicates: upgrade dedupes preserving api_fetch (LML cached data)."""
    _setup_drifted_schema(fresh_db_url)

    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        # release_id 1 = both 'bulk_import' and 'api_fetch' — api_fetch must win.
        # release_id 2 = only 'bulk_import' — must survive.
        cur.execute("INSERT INTO release (id, title) VALUES (1, 'r1'), (2, 'r2'), (3, 'r3')")
        cur.execute(
            """
            INSERT INTO cache_metadata (release_id, source) VALUES
                (1, 'bulk_import'),
                (1, 'api_fetch'),
                (2, 'bulk_import'),
                (3, 'api_fetch'),
                (3, 'api_fetch')
            """
        )

    stamp = _run_alembic(["stamp", "0008_release_artwork_checked_at"], fresh_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"
    result = _run_alembic(["upgrade", "head"], fresh_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"

    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT release_id, source FROM cache_metadata ORDER BY release_id")
        rows = cur.fetchall()

    # Each release_id appears exactly once; api_fetch wins when both existed.
    assert rows == [
        (1, "api_fetch"),  # bulk_import dropped, api_fetch kept
        (2, "bulk_import"),  # only source, kept
        (
            3,
            "api_fetch",
        ),  # both api_fetch; one kept (the dedupe is arbitrary across same-source dupes)
    ], (
        f"Dedupe must drop non-api_fetch rows when an api_fetch row exists for the "
        f"same release_id, and collapse same-source dupes to one row. Got: {rows!r}. "
        f"See populate_cache_metadata docstring + #188."
    )


@pytest.mark.pg
def test_on_conflict_works_after_upgrade(fresh_db_url: str) -> None:
    """Smoke test: after upgrade, the exact INSERT ON CONFLICT from populate_cache_metadata succeeds.

    This is the bug from #273 reduced to its minimum failing test: without
    the constraint, the statement errors with InvalidColumnReference; with
    it, the statement returns cleanly (any subsequent attempt is a no-op
    thanks to DO NOTHING).
    """
    _setup_drifted_schema(fresh_db_url)

    stamp = _run_alembic(["stamp", "0008_release_artwork_checked_at"], fresh_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"
    result = _run_alembic(["upgrade", "head"], fresh_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"

    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO release (id, title) VALUES (1, 'r1'), (2, 'r2')")
        # The exact shape from import_csv.py:524 — if this raises, #273 isn't fixed.
        cur.execute(
            """
            INSERT INTO cache_metadata (release_id, source)
            SELECT id, 'bulk_import' FROM release
            ON CONFLICT (release_id) DO NOTHING
            """
        )
        # Second run is the LML-race scenario: must also succeed silently.
        cur.execute(
            """
            INSERT INTO cache_metadata (release_id, source)
            SELECT id, 'bulk_import' FROM release
            ON CONFLICT (release_id) DO NOTHING
            """
        )
        cur.execute("SELECT count(*) FROM cache_metadata")
        assert cur.fetchone() == (2,), (
            "cache_metadata should have one row per release after both inserts."
        )
