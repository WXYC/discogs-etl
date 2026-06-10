"""Verify 0012 brings entity.release_identity / entity.release_reconciliation_log
under the alembic chain for LML#526's release-side identity layer.

LML's ``POST /api/v1/identity/resolve`` returns 503 until both tables exist on
the discogs-cache PostgreSQL instance. The migration:

* Creates the ``entity`` schema if absent — the LML canonical
  ``entity/release_identity.sql`` assumes the schema is already present
  (existing prod has it from out-of-band bootstrap; fresh dev DBs do not).
* Creates ``entity.release_identity`` with six per-source UNIQUE columns
  that LML's mint protocol (``INSERT ... ON CONFLICT ({col}) DO NOTHING
  RETURNING id``) binds against. Dropping any UNIQUE breaks the mint
  surface — see ``entity/store.py::mint_or_get_release_identity`` in LML.
* Creates ``entity.release_reconciliation_log`` with an FK to
  ``release_identity.id`` (``NO ACTION``, matching the artist-side
  convention).
* Creates the FK index ``idx_release_reconciliation_log_identity_id``.
  Postgres does not auto-index FK referencing columns; every
  ``WHERE identity_id = $1`` lookup needs it.

The migration must be re-application-safe (existing prod already has the
schema, plus a fresh dev DB after a prior partial run). All DDL uses
``IF NOT EXISTS``.

The downgrade drops in FK order — index, then log, then identity — and
intentionally leaves the ``entity`` schema in place because the artist-side
tables (``entity.identity`` / ``entity.reconciliation_log``) still live
there. Adopting those into the alembic chain is tracked at
WXYC/discogs-etl#279.

Tracked at WXYC/discogs-etl#278.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0012_entity_release_identity.py"


# Per-source UNIQUE columns load-bearing for LML's mint protocol.
# Order matches the canonical entity/release_identity.sql in LML#530.
PER_SOURCE_UNIQUE_COLUMNS: tuple[str, ...] = (
    "discogs_release_id",
    "discogs_master_id",
    "musicbrainz_release_id",
    "spotify_album_id",
    "apple_music_album_id",
    "bandcamp_album_url",
)


# ---------------------------------------------------------------------------
# Static (no DB) checks
# ---------------------------------------------------------------------------


def test_migration_file_exists() -> None:
    assert MIGRATION_PATH.exists(), (
        f"0012 migration missing at {MIGRATION_PATH}. The LML release-identity "
        "surface returns 503 in prod until both entity tables exist on the "
        "discogs-cache PG instance."
    )


def test_migration_creates_schema_before_tables() -> None:
    """The LML canonical DDL omits the schema-create; this migration adds it.

    Fresh dev DBs would fail otherwise.
    """
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "CREATE SCHEMA IF NOT EXISTS entity" in body, (
        "0012 must bootstrap the entity schema. The canonical "
        "entity/release_identity.sql in LML#530 assumes the schema is "
        "already present; against a fresh dev DB without prior out-of-band "
        "bootstrap, the canonical DDL would fail."
    )


def test_migration_declares_all_per_source_uniques() -> None:
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    for col in PER_SOURCE_UNIQUE_COLUMNS:
        assert col in body, (
            f"0012 must declare {col} on entity.release_identity. LML's mint "
            f"protocol uses INSERT ... ON CONFLICT ({col}) DO NOTHING RETURNING "
            f"id; dropping the column breaks the write surface with a loud "
            f"500, not a silent duplicate-mint."
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


def _stamp_and_upgrade(db_url: str) -> None:
    """Stamp at 0011 (skipping the prior chain) then upgrade to 0012.

    Stamping avoids dragging the whole 0001→0011 chain into a test that only
    cares about 0012's surface. The migration is self-contained: it does
    not depend on any column added by earlier revisions.
    """
    stamp = _run_alembic(["stamp", "0011_artist_not_found"], db_url)
    assert stamp.returncode == 0, (
        f"alembic stamp failed:\nstdout: {stamp.stdout}\nstderr: {stamp.stderr}"
    )
    result = _run_alembic(["upgrade", "0012_entity_release_identity"], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


@pytest.mark.pg
def test_entity_schema_exists(fresh_db_url: str) -> None:
    _stamp_and_upgrade(fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
            ("entity",),
        )
        assert cur.fetchone() is not None, (
            "entity schema missing after 0012 upgrade. The canonical LML DDL "
            "omits the schema-create; 0012 must add it for fresh dev DBs."
        )


@pytest.mark.pg
def test_release_identity_columns(fresh_db_url: str) -> None:
    _stamp_and_upgrade(fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'entity' AND table_name = 'release_identity'
            ORDER BY ordinal_position
            """
        )
        rows = {name: (data_type, is_nullable) for name, data_type, is_nullable in cur.fetchall()}

    expected_columns = {
        "id",
        "discogs_release_id",
        "discogs_master_id",
        "musicbrainz_release_id",
        "spotify_album_id",
        "apple_music_album_id",
        "bandcamp_album_url",
        "reconciliation_status",
        "created_at",
        "updated_at",
    }
    assert expected_columns.issubset(rows.keys()), (
        f"entity.release_identity missing columns: {expected_columns - rows.keys()}"
    )
    assert rows["reconciliation_status"] == ("text", "NO")
    assert rows["created_at"][0] == "timestamp with time zone"
    assert rows["updated_at"][0] == "timestamp with time zone"


@pytest.mark.pg
def test_six_per_source_unique_constraints(fresh_db_url: str) -> None:
    """All six per-source UNIQUEs are load-bearing for LML's mint protocol.

    Dropping any one breaks the ON CONFLICT clause in
    ``entity/store.py::mint_or_get_release_identity`` and raises a loud
    500 on the LML write surface.
    """
    _stamp_and_upgrade(fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        # Per the wxyc-shared convention, walk pg_constraint rather than
        # information_schema.table_constraints — the latter loses the
        # column-list mapping behind a join we'd have to rebuild here.
        cur.execute(
            """
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE n.nspname = 'entity'
              AND t.relname = 'release_identity'
              AND c.contype = 'u'
              AND cardinality(c.conkey) = 1
            ORDER BY a.attname
            """
        )
        unique_columns = {row[0] for row in cur.fetchall()}

    assert unique_columns == set(PER_SOURCE_UNIQUE_COLUMNS), (
        f"Per-source UNIQUE drift on entity.release_identity. Got {unique_columns}, "
        f"expected {set(PER_SOURCE_UNIQUE_COLUMNS)}. LML's mint protocol binds "
        f"each ON CONFLICT clause to one of these columns; missing one raises "
        f"a loud 500 (RuntimeError 'UNIQUE constraint appears broken') in "
        f"entity/store.py."
    )


@pytest.mark.pg
def test_reconciliation_log_fk_no_action(fresh_db_url: str) -> None:
    """FK has no ON DELETE CASCADE — matches the artist-side convention."""
    _stamp_and_upgrade(fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT rc.delete_rule
            FROM information_schema.referential_constraints rc
            JOIN information_schema.table_constraints tc
              ON tc.constraint_name = rc.constraint_name
             AND tc.constraint_schema = rc.constraint_schema
            WHERE tc.table_schema = 'entity'
              AND tc.table_name = 'release_reconciliation_log'
              AND tc.constraint_type = 'FOREIGN KEY'
            """
        )
        rows = cur.fetchall()
    assert len(rows) == 1, (
        f"Expected exactly one FK on entity.release_reconciliation_log, got {len(rows)}."
    )
    assert rows[0][0] == "NO ACTION", (
        f"FK delete_rule must be NO ACTION, got {rows[0][0]!r}. CASCADE would "
        "silently delete the audit log; consumers expect explicit cleanup."
    )


@pytest.mark.pg
def test_fk_index_present(fresh_db_url: str) -> None:
    """idx_release_reconciliation_log_identity_id covers WHERE identity_id = $1."""
    _stamp_and_upgrade(fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = 'entity'
              AND tablename = 'release_reconciliation_log'
              AND indexname = 'idx_release_reconciliation_log_identity_id'
            """
        )
        assert cur.fetchone() is not None, (
            "FK index missing. Postgres does not auto-index FK referencing "
            "columns, so every WHERE identity_id = $1 lookup would seq-scan."
        )


@pytest.mark.pg
def test_mint_then_remint_smoke(fresh_db_url: str) -> None:
    """End-to-end mint protocol shape: first INSERT mints, second is a no-op.

    Mirrors ``entity/store.py::mint_or_get_release_identity`` in LML#530:
    ``INSERT ... ON CONFLICT (discogs_release_id) DO NOTHING RETURNING id``.
    The second call must return zero rows so the LML caller falls through
    to the SELECT-for-conflict-loser branch.
    """
    _stamp_and_upgrade(fresh_db_url)
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO entity.release_identity (discogs_release_id)
            VALUES (12345)
            ON CONFLICT (discogs_release_id) DO NOTHING
            RETURNING id
            """
        )
        first = cur.fetchone()
        assert first is not None, "First mint must return the new id."
        identity_id = first[0]

        cur.execute(
            """
            INSERT INTO entity.release_identity (discogs_release_id)
            VALUES (12345)
            ON CONFLICT (discogs_release_id) DO NOTHING
            RETURNING id
            """
        )
        second = cur.fetchone()
        assert second is None, (
            "Second mint must return zero rows (conflict loser path). "
            "If this returns a row, the UNIQUE constraint is missing and "
            "the LML write surface would silently double-mint."
        )

        cur.execute("SELECT id FROM entity.release_identity WHERE discogs_release_id = 12345")
        rows = cur.fetchall()
        assert rows == [(identity_id,)], (
            f"Expected exactly one row after mint+remint, got {rows!r}."
        )


@pytest.mark.pg
def test_reapply_is_noop(fresh_db_url: str) -> None:
    """Re-running the migration against an already-upgraded DB must not fail.

    Production prereq: discogs-cache PG already has the entity schema +
    artist-side tables from the out-of-band bootstrap. The migration must
    no-op against that state rather than perturbing it.
    """
    _stamp_and_upgrade(fresh_db_url)

    # Seed a sentinel row so we can prove re-application doesn't perturb data.
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO entity.release_identity (discogs_release_id) VALUES (99999) RETURNING id"
        )
        sentinel_id = cur.fetchone()[0]

    # Downgrade alembic state and re-upgrade so the IF NOT EXISTS branches fire.
    downgrade = _run_alembic(["stamp", "0011_artist_not_found"], fresh_db_url)
    assert downgrade.returncode == 0
    reupgrade = _run_alembic(["upgrade", "0012_entity_release_identity"], fresh_db_url)
    assert reupgrade.returncode == 0, (
        f"Re-application failed:\nstdout: {reupgrade.stdout}\nstderr: {reupgrade.stderr}"
    )

    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT discogs_release_id FROM entity.release_identity WHERE id = %s",
            (sentinel_id,),
        )
        assert cur.fetchone() == (99999,), (
            "Sentinel row lost across re-application. The migration must use "
            "CREATE TABLE IF NOT EXISTS so existing rows survive."
        )
