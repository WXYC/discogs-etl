"""Migration test for 0015_release_master_id_idx.

0015 creates ``idx_release_master_id`` on databases that do not have it. Unlike
its sibling ``test_alembic_0014_repair_artwork_null_idx.py``, the drift here has
*two* independent causes, and the tests below cover both:

1. **The alembic-only database.** ``schema/create_database.sql`` is the sole
   declaration of this index; no revision has ever created it. A database built
   by ``alembic upgrade head`` has therefore never had it at any point. This is
   the case ``alembic_only_db_url`` reproduces, and it is what makes 0015 a
   *create* rather than a repair.
2. **The copy-swap.** ``CREATE TABLE new_release AS SELECT ...`` carries no
   indexes, so even a cold-built database lost the index at its first rebuild.
   ``drifted_db_url`` reproduces that by applying the canonical schema and then
   dropping the index by hand, which stands in for the CTAS + RENAME.

Recurrence prevention — both rebuild scripts now recreating the index — is
covered by ``tests/integration/test_copy_swap_index_parity.py`` (source-text)
and ``tests/integration/test_copy_swap_preserves_master_id_index.py``
(behavioral). This file covers only the migration.

See WXYC/discogs-etl#412.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
SCHEMA_DIR = REPO_ROOT / "schema"
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0015_release_master_id_idx.py"

_REVISION = "0015_release_master_id_idx"
_PRIOR_REVISION = "0014_repair_artwork_null_idx"
_INDEX_NAME = "idx_release_master_id"


def test_migration_file_exists() -> None:
    assert MIGRATION_PATH.exists(), f"{MIGRATION_PATH.name} missing"


def test_migration_uses_concurrently_and_if_not_exists() -> None:
    """Both properties are load-bearing, for different reasons.

    CONCURRENTLY keeps the build off an ACCESS EXCLUSIVE lock against a live
    cache LML is reading. IF NOT EXISTS makes the revision a no-op on a
    database that already has the index — a cold-built one, or prod, where it
    was created by hand on 2026-08-20 ahead of this revision — which is what
    lets it ship everywhere rather than only to drifted databases.
    """
    body = MIGRATION_PATH.read_text()
    assert f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_INDEX_NAME}" in body, (
        "0015 must create the index CONCURRENTLY and IF NOT EXISTS"
    )
    assert "master_id IS NOT NULL" in body, (
        "0015's predicate must match create_database.sql's exactly, or the "
        "master_id filter stops being index-covered"
    )
    assert "ON release(master_id)" in body, (
        "0015 must index the master_id COLUMN. A partial index keyed on any "
        "other column with the same WHERE clause satisfies the predicate "
        "assertion above while leaving a master_id filter to full-scan."
    )


def test_migration_chains_from_0014() -> None:
    body = MIGRATION_PATH.read_text()
    assert f'revision: str = "{_REVISION}"' in body
    assert f'down_revision: str | Sequence[str] | None = "{_PRIOR_REVISION}"' in body


def _indexdef(db_url: str) -> str | None:
    """Return the full index definition for idx_release_master_id, or None."""
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = 'release' AND indexname = %s",
            (_INDEX_NAME,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _assert_index_shape(indexdef: str | None, *, context: str) -> None:
    """Assert the index is on `release(master_id)` AND carries the partial predicate.

    Both halves matter and neither implies the other: an index on the right
    column without the predicate is a different (larger) index, and an index
    carrying the predicate on the wrong column cannot serve a master_id filter
    at all. Asserting only one half is how a silent degradation gets through.
    """
    assert indexdef is not None, f"{_INDEX_NAME} is missing {context}"
    assert "USING btree (master_id)" in indexdef, (
        f"{_INDEX_NAME} {context} is not a btree on master_id: {indexdef!r}"
    )
    assert "master_id IS NOT NULL" in indexdef, (
        f"{_INDEX_NAME} {context} lost its partial predicate: {indexdef!r}"
    )


@pytest.fixture()
def alembic_only_db_url(fresh_db_url: str) -> str:
    """A DB that has run alembic and never seen create_database.sql.

    This is cause 1 in the module docstring, and the case that distinguishes
    0015 from 0014: there is no earlier revision to repair from, because no
    revision has ever created this index.
    """
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS release ("
            "  id integer PRIMARY KEY,"
            "  master_id integer,"
            "  title text"
            ")"
        )
    return fresh_db_url


@pytest.fixture()
def drifted_db_url(fresh_db_url: str) -> str:
    """A cold-built DB whose copy-swap ate the index (cause 2)."""
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        cur.execute(f"DROP INDEX IF EXISTS {_INDEX_NAME}")
    return fresh_db_url


@pytest.mark.pg
def test_alembic_only_fixture_really_lacks_the_index(alembic_only_db_url: str) -> None:
    """Vacuity guard: if the fixture had the index, the test below proves nothing."""
    assert _indexdef(alembic_only_db_url) is None, (
        f"alembic_only_db_url fixture already has {_INDEX_NAME} — the create "
        f"test below would pass without 0015 doing anything"
    )


@pytest.mark.pg
def test_drifted_fixture_really_is_missing_the_index(drifted_db_url: str) -> None:
    """Vacuity guard for the copy-swap drift case."""
    assert _indexdef(drifted_db_url) is None, (
        f"drifted_db_url fixture still has {_INDEX_NAME} — the repair test "
        f"below would pass without 0015 doing anything"
    )


@pytest.mark.pg
def test_upgrade_creates_the_index_on_an_alembic_only_db(
    run_alembic, alembic_only_db_url: str
) -> None:
    """Cause 1: the database that never had the index in its history."""
    stamp = run_alembic(["stamp", _PRIOR_REVISION], alembic_only_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"

    result = run_alembic(["upgrade", _REVISION], alembic_only_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"

    _assert_index_shape(_indexdef(alembic_only_db_url), context=f"after upgrading to {_REVISION}")


@pytest.mark.pg
def test_upgrade_repairs_a_copy_swap_drifted_db(run_alembic, drifted_db_url: str) -> None:
    """Cause 2: the cold-built database whose rebuild dropped the index."""
    stamp = run_alembic(["stamp", _PRIOR_REVISION], drifted_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"

    result = run_alembic(["upgrade", _REVISION], drifted_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"

    _assert_index_shape(_indexdef(drifted_db_url), context=f"after upgrading to {_REVISION}")


@pytest.mark.pg
def test_upgrade_is_a_noop_when_the_index_already_exists(run_alembic, fresh_db_url: str) -> None:
    """The cold-built case — and prod's, where the index was created by hand."""
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    assert _indexdef(fresh_db_url) is not None, (
        "create_database.sql did not create the index — fixture assumption broken"
    )

    stamp = run_alembic(["stamp", _PRIOR_REVISION], fresh_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"
    result = run_alembic(["upgrade", _REVISION], fresh_db_url)
    assert result.returncode == 0, (
        f"upgrade must no-op cleanly when the index already exists:\n"
        f"{result.stdout}\n{result.stderr}"
    )
    _assert_index_shape(_indexdef(fresh_db_url), context="after a no-op upgrade")


@pytest.mark.pg
def test_downgrade_keeps_the_index(run_alembic, drifted_db_url: str) -> None:
    """Downgrade is a deliberate no-op.

    Unlike 0014 — which declines to drop because dropping would undo 0008 —
    0015 declines because dropping would put the DB out of parity with
    create_database.sql, which declares the index unconditionally. The prior
    revision specifies nothing about this index either way.
    """
    run_alembic(["stamp", _PRIOR_REVISION], drifted_db_url)
    run_alembic(["upgrade", _REVISION], drifted_db_url)
    assert _indexdef(drifted_db_url) is not None, "precondition: upgrade created it"

    result = run_alembic(["downgrade", _PRIOR_REVISION], drifted_db_url)
    assert result.returncode == 0, f"downgrade failed:\n{result.stdout}\n{result.stderr}"
    assert _indexdef(drifted_db_url) is not None, (
        f"downgrade dropped {_INDEX_NAME} — 0015's downgrade must be a no-op, "
        f"since create_database.sql declares the index unconditionally"
    )


def _index_is_valid(db_url: str) -> bool | None:
    """Return ``pg_index.indisvalid`` for idx_release_master_id, or None if absent.

    ``pg_indexes`` (used by :func:`_indexdef`) lists INVALID indexes exactly
    like valid ones, so shape assertions cannot tell the two apart. This reads
    the flag that decides whether the planner will actually use the index.
    """
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT i.indisvalid FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relname = %s",
            (_INDEX_NAME,),
        )
        row = cur.fetchone()
    return row[0] if row else None


@pytest.fixture()
def invalid_index_db_url(fresh_db_url: str) -> str:
    """A DB carrying an INVALID ``idx_release_master_id``.

    The state PostgreSQL leaves when a ``CREATE INDEX CONCURRENTLY`` build is
    interrupted — SIGTERM on the rebuild instance, a job timeout, a cancelled
    statement. Flipping the catalog flag by hand reproduces it without racing a
    real CONCURRENTLY build. Requires superuser, which the local docker PG and
    the CI ``wxyc-postgres`` service both provide.
    """
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        cur.execute(
            "UPDATE pg_index SET indisvalid = false WHERE indexrelid = %s::regclass",
            (_INDEX_NAME,),
        )
    return fresh_db_url


@pytest.mark.pg
def test_invalid_index_fixture_really_is_invalid(invalid_index_db_url: str) -> None:
    """Vacuity guard: if the catalog UPDATE didn't take, the test below proves nothing."""
    assert _index_is_valid(invalid_index_db_url) is False, (
        f"invalid_index_db_url fixture did not mark {_INDEX_NAME} INVALID "
        f"(catalog UPDATE needs superuser) — the repair test below would pass "
        f"without 0015 doing anything"
    )


@pytest.mark.pg
def test_upgrade_replaces_an_invalid_leftover_index(run_alembic, invalid_index_db_url: str) -> None:
    """An interrupted CONCURRENTLY build must not poison the retry.

    ``CREATE INDEX CONCURRENTLY IF NOT EXISTS`` alone is not enough: PostgreSQL
    counts an INVALID index as existing, so ``IF NOT EXISTS`` no-ops, alembic
    stamps the revision, and the operator is left with an index the planner
    will never use. 0015 must drop the invalid leftover first.
    """
    stamp = run_alembic(["stamp", _PRIOR_REVISION], invalid_index_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"

    result = run_alembic(["upgrade", _REVISION], invalid_index_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"

    assert _index_is_valid(invalid_index_db_url) is True, (
        f"{_INDEX_NAME} is still INVALID after upgrade — the planner will ignore "
        f"it. add_index_concurrently_safely must drop the invalid leftover first."
    )
    _assert_index_shape(_indexdef(invalid_index_db_url), context="after replacing an INVALID index")
