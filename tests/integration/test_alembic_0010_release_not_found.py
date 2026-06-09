"""Verify 0010 adds `release.not_found` for LML#510's tombstone discriminator.

`release.not_found` distinguishes two cases that previously collapsed to
"no cache row":

* **Never asked** — no row in ``release`` at all (the bulk loader hasn't
  imported it, or LML hasn't fetched it).
* **Asked, Discogs returned 404** — row exists with
  ``not_found = TRUE``. LML's negative-cache stays warm so subsequent
  callers don't re-burn the rate-limit budget on the same 404.

Without this column, LML's ``_api_fetch`` collapses 404 to ``None`` and
the fallthrough seam refuses to write ``None`` to PG, so every caller
re-asks Discogs forever. The companion change in LML's
``cache_service.write_release`` writes a *narrow* tombstone row whose
identifier columns intentionally stay NULL/empty so that a future
hydrated 200 response can fill them in without losing them mid-flight.

The default ``FALSE`` makes the migration safe to ship before LML is
updated: existing prod LML reads the column as ``FALSE`` (no tombstones)
and behaves exactly as it did pre-migration.

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
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0010_release_not_found.py"
SCHEMA_DIR = REPO_ROOT / "schema"


# ---------------------------------------------------------------------------
# Static (no DB) checks
# ---------------------------------------------------------------------------


def test_migration_file_exists() -> None:
    assert MIGRATION_PATH.exists(), (
        f"0010 migration missing at {MIGRATION_PATH}. Per the dual-write "
        "convention, the alembic chain and schema/create_database.sql must "
        "agree on the release-table shape."
    )


def test_migration_adds_not_found_column() -> None:
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS not_found" in body, (
        "0010 must ADD COLUMN not_found with IF NOT EXISTS so it's idempotent "
        "against schema/create_database.sql (which the legacy rebuild path "
        "applies directly)."
    )
    assert "boolean" in body.lower(), (
        "not_found must be boolean — LML's discriminator is a binary state."
    )
    assert "NOT NULL" in body, (
        "not_found must be NOT NULL so the predicate `not_found = TRUE` is "
        "unambiguous; a NULL tristate would force every read site to handle "
        "the third case."
    )
    assert "DEFAULT FALSE" in body or "DEFAULT false" in body, (
        "not_found must default to FALSE so the migration is backward-"
        "compatible: existing prod LML treats every row as a real row, "
        "exactly as it did pre-migration."
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


@pytest.fixture()
def db_with_release_table(fresh_db_url: str) -> str:
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    return fresh_db_url


@pytest.mark.pg
def test_not_found_column_created_at_head(db_with_release_table: str) -> None:
    db_url = db_with_release_table

    stamp = _run_alembic(["stamp", "0009_cache_metadata_unique"], db_url)
    assert stamp.returncode == 0, (
        f"alembic stamp failed:\nstdout: {stamp.stdout}\nstderr: {stamp.stderr}"
    )

    result = _run_alembic(["upgrade", "0010_release_not_found"], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'release'
              AND column_name = 'not_found'
            """
        )
        row = cur.fetchone()
        assert row is not None, "release.not_found column missing after 0010 upgrade"
        data_type, is_nullable, column_default = row
        assert data_type == "boolean", (
            f"not_found must be boolean; got {data_type!r}. A non-boolean type "
            f"would force LML's predicate into a NULL-or-cast comparison."
        )
        assert is_nullable == "NO", (
            "not_found must be NOT NULL. NULL would force every read site to "
            "handle a third 'maybe a tombstone' state."
        )
        assert column_default is not None and "false" in column_default.lower(), (
            f"not_found must default to FALSE so the migration is backward-"
            f"compatible with prod LML running the pre-510 code. Got "
            f"{column_default!r}."
        )


@pytest.mark.pg
def test_not_found_round_trip_with_empty_title(db_with_release_table: str) -> None:
    """Pin the tombstone-write contract: ``title = ''`` + ``not_found = TRUE``.

    LML#510's tombstone INSERT writes ``title = ''`` as a sentinel. ``release.title``
    is ``text NOT NULL`` with no ``CHECK (title <> '')``, so this works today.
    The migration's ``upgrade()`` probes the same write so a future-added
    CHECK trips this test (and the migration) rather than the prod LML write.
    """
    db_url = db_with_release_table

    stamp = _run_alembic(["stamp", "0009_cache_metadata_unique"], db_url)
    assert stamp.returncode == 0
    result = _run_alembic(["upgrade", "0010_release_not_found"], db_url)
    assert result.returncode == 0

    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO release (id, title, not_found, artwork_checked_at)
            VALUES (-2147483648, '', TRUE, now())
            """
        )
        cur.execute("SELECT title, not_found FROM release WHERE id = -2147483648")
        row = cur.fetchone()
        assert row == ("", True), (
            "Tombstone round-trip failed; tombstone semantics require "
            "(title='', not_found=TRUE) to persist. The migration probe "
            "should have caught this."
        )
        cur.execute("DELETE FROM release WHERE id = -2147483648")


@pytest.mark.pg
def test_not_found_idempotent_on_reapply(db_with_release_table: str) -> None:
    """Applying 0010 against a DB already on the dual-written schema must not error."""
    db_url = db_with_release_table

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'release' AND column_name = 'not_found'"
        )
        assert cur.fetchone() is not None, (
            "Pre-condition: schema/create_database.sql must dual-write "
            "release.not_found. Without that, this test no longer covers "
            "the idempotence-against-legacy-schema case."
        )

    stamp = _run_alembic(["stamp", "0009_cache_metadata_unique"], db_url)
    assert stamp.returncode == 0

    result = _run_alembic(["upgrade", "0010_release_not_found"], db_url)
    assert result.returncode == 0, (
        f"0010 must be idempotent against a pre-existing column. "
        f"alembic output:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
