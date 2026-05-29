"""Verify the 0008_artwork_checked_at migration adds the column + partial index.

`release.artwork_checked_at` distinguishes two NULL states of `artwork_url`:

* **never asked** — `artwork_checked_at IS NULL` (bulk loader left it that
  way; the row predates any LML lookup).
* **asked, genuinely no image** — `artwork_checked_at IS NOT NULL` (LML hit
  the Discogs API and the release legitimately has no cover).

Without this distinction, LML's cache-hit predicate either over-fetches
(re-asks Discogs forever on imageless releases — current state after
WXYC/library-metadata-lookup PR #415) or under-fetches (serves stale NULL
for releases the bulk loader never populated — pre-#414 state).

The partial index `release_artwork_null_idx` supports the LML#221 one-shot
top-up scan without a full sequential scan of the `release` table.

Tracked at WXYC/discogs-etl#239 + WXYC/library-metadata-lookup#423.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0008_release_artwork_checked_at.py"
SCHEMA_DIR = REPO_ROOT / "schema"


# ---------------------------------------------------------------------------
# Static (no DB) checks — kept in this file alongside the integration tests
# so a future migration-body change surfaces all assertions from the same
# `pytest` invocation.
# ---------------------------------------------------------------------------


def test_migration_file_exists() -> None:
    assert MIGRATION_PATH.exists(), (
        f"0008 migration missing at {MIGRATION_PATH}. Per the dual-write "
        "convention, the alembic chain and schema/create_database.sql must "
        "agree on the release-table shape."
    )


def test_migration_adds_artwork_checked_at_column() -> None:
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS artwork_checked_at" in body, (
        "0008 must ADD COLUMN artwork_checked_at with IF NOT EXISTS so it's "
        "idempotent against schema/create_database.sql (which the legacy "
        "rebuild path applies directly)."
    )
    assert "timestamptz" in body or "TIMESTAMPTZ" in body, (
        "artwork_checked_at must be timestamptz so NOW() values land "
        "timezone-aware (LML stamps `now()` in UTC; consumers compare in "
        "destination TZ via the timestamptz semantics)."
    )


def test_migration_creates_partial_index() -> None:
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "release_artwork_null_idx" in body, (
        "0008 must create release_artwork_null_idx — the partial index that "
        "lets LML#221's top-up drain scan the never-asked tail without a "
        "full sequential scan on the release table."
    )
    assert "WHERE artwork_url IS NULL AND artwork_checked_at IS NULL" in body, (
        "The partial index predicate must be exactly "
        "`WHERE artwork_url IS NULL AND artwork_checked_at IS NULL` so it "
        "captures only the never-asked rows. A wider predicate would index "
        "rows the drain doesn't care about; a narrower one would miss them."
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
    """Apply schema/create_database.sql so `release` exists before 0008 runs.

    0008 modifies the existing `release` table — we can't stamp at 0007 and
    upgrade without first creating `release`. The lighter alternative to
    running the full alembic chain (0001 → 0007 includes 0007's wxyc-postgres
    image gate) is to apply the canonical schema directly, then stamp at
    0007 and upgrade only 0008. This mirrors the dual-write convention: a
    destination DB that ran the legacy `schema/*.sql` path is what alembic
    is bridging from.
    """
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    return fresh_db_url


@pytest.mark.pg
def test_artwork_checked_at_column_created_at_head(db_with_release_table: str) -> None:
    db_url = db_with_release_table

    stamp = _run_alembic(["stamp", "0007_wxyc_postgres_image_gate"], db_url)
    assert stamp.returncode == 0, (
        f"alembic stamp failed:\nstdout: {stamp.stdout}\nstderr: {stamp.stderr}"
    )

    result = _run_alembic(["upgrade", "head"], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        # Column contract.
        cur.execute(
            """
            SELECT data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'release'
              AND column_name = 'artwork_checked_at'
            """
        )
        row = cur.fetchone()
        assert row is not None, "release.artwork_checked_at column missing after 0008 upgrade"
        data_type, is_nullable, column_default = row
        assert data_type == "timestamp with time zone", (
            f"artwork_checked_at must be timestamp with time zone (timestamptz); "
            f"got {data_type!r}. Without TZ-awareness, NOW() values written by "
            f"LML drift relative to consumers in non-UTC sessions."
        )
        assert is_nullable == "YES", (
            "artwork_checked_at must be nullable — NULL is the load-bearing "
            "'never asked' signal that LML's predicate distinguishes from "
            "'checked, no image' (timestamp set)."
        )
        assert column_default is None, (
            f"artwork_checked_at must have no DEFAULT; existing rows must "
            f"remain NULL ('never asked') without a backfill. Got "
            f"{column_default!r}."
        )

        # Partial index supports LML#221's never-asked top-up scan.
        cur.execute(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'release'
              AND indexname = 'release_artwork_null_idx'
            """
        )
        idx_row = cur.fetchone()
        assert idx_row is not None, (
            "release_artwork_null_idx missing after 0008 upgrade — LML#221's "
            "top-up drain would seq-scan the release table."
        )
        indexdef = idx_row[0]
        # `WHERE …` clause is the contract: index must cover the never-asked
        # set exactly, no narrower and no wider. PG round-trips the predicate
        # through pg_get_expr, which canonicalizes to per-clause parens
        # ("((a IS NULL) AND (b IS NULL))"); we assert on the two sub-clauses
        # and the AND so we're robust to that formatting but still pin the
        # logical predicate.
        assert "artwork_url IS NULL" in indexdef, (
            f"release_artwork_null_idx predicate missing artwork_url IS NULL: {indexdef!r}"
        )
        assert "artwork_checked_at IS NULL" in indexdef, (
            f"release_artwork_null_idx predicate missing artwork_checked_at IS NULL: {indexdef!r}"
        )
        assert " AND " in indexdef, (
            f"release_artwork_null_idx predicate must AND both clauses (not OR): {indexdef!r}"
        )


@pytest.mark.pg
def test_artwork_checked_at_idempotent_on_reapply(db_with_release_table: str) -> None:
    """Applying 0008 against a DB already on the dual-written schema must
    not error.

    The dual-write convention means destinations that ran the legacy
    ``schema/create_database.sql`` path land on the column + index before
    alembic ever runs. The ``db_with_release_table`` fixture already
    applied that legacy path, so the column and index already exist when
    this test stamps + upgrades. ``ADD COLUMN IF NOT EXISTS`` +
    ``CREATE INDEX IF NOT EXISTS`` must absorb the duplicate without
    erroring.
    """
    db_url = db_with_release_table

    # Sanity: the legacy schema path already created the column + index.
    # If this changes (schema/create_database.sql stops dual-writing the
    # column), this test mutates into a redundant copy of the upgrade-from-
    # empty test above — the pre-condition pin keeps the dual-write
    # intent visible.
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'release' AND column_name = 'artwork_checked_at'"
        )
        assert cur.fetchone() is not None, (
            "Pre-condition: schema/create_database.sql must dual-write "
            "artwork_checked_at. Without that, this test no longer covers "
            "the idempotence-against-legacy-schema case."
        )

    stamp = _run_alembic(["stamp", "0007_wxyc_postgres_image_gate"], db_url)
    assert stamp.returncode == 0, (
        f"alembic stamp failed:\nstdout: {stamp.stdout}\nstderr: {stamp.stderr}"
    )

    result = _run_alembic(["upgrade", "head"], db_url)
    assert result.returncode == 0, (
        f"0008 must be idempotent against a pre-existing column + index. "
        f"alembic output:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
