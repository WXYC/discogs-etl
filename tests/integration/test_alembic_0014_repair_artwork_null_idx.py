"""Migration test for 0014_repair_artwork_null_idx.

0014 repairs ``release_artwork_null_idx`` on databases where the copy-swap
rebuild destroyed it. The scenario these tests reproduce is the one observed on
production 2026-08-19: a database stamped well past 0008 (which creates the
index) whose ``release`` table nonetheless has no such index, because
``CREATE TABLE new_release AS SELECT ...`` carries no indexes and neither
rebuild script recreated it afterwards.

The live-DB tests therefore *drop the index explicitly* after applying the
canonical schema — that drop stands in for the copy-swap — then stamp at 0013
and upgrade, asserting 0014 puts it back with the right predicate.

Recurrence prevention (both rebuild scripts now recreate the index) is covered
separately by ``tests/integration/test_copy_swap_index_parity.py``. This file
covers only the repair path.

See WXYC/discogs-etl#239 for the index's original rationale and
``tests/integration/test_alembic_0008_artwork_checked_at.py`` for the revision
that first creates it.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
SCHEMA_DIR = REPO_ROOT / "schema"
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0014_repair_artwork_null_idx.py"

_REVISION = "0014_repair_artwork_null_idx"
_PRIOR_REVISION = "0013_adopt_entity_identity"
_INDEX_NAME = "release_artwork_null_idx"


def test_migration_file_exists() -> None:
    assert MIGRATION_PATH.exists(), f"{MIGRATION_PATH.name} missing"


def test_migration_uses_concurrently_and_if_not_exists() -> None:
    """Both properties are load-bearing, for different reasons.

    CONCURRENTLY keeps the build off an ACCESS EXCLUSIVE lock against a live
    cache LML is reading. IF NOT EXISTS makes the revision a no-op on a
    database that still has the index (one built from create_database.sql and
    never pruned), which is what lets it ship to every environment rather than
    only the drifted ones.

    IF NOT EXISTS is necessary but not sufficient: it also no-ops against an
    INVALID index left by an interrupted build, which is why the DDL goes
    through lib/pg_concurrent_ddl. See
    test_upgrade_replaces_an_invalid_leftover_index.
    """
    body = MIGRATION_PATH.read_text()
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS release_artwork_null_idx" in body, (
        "0014 must create the index CONCURRENTLY and IF NOT EXISTS"
    )
    assert "artwork_url IS NULL AND artwork_checked_at IS NULL" in body, (
        "0014's predicate must match 0008's exactly, or topup_artwork.py's "
        "candidate query stops being index-covered"
    )


def test_migration_chains_from_0013() -> None:
    body = MIGRATION_PATH.read_text()
    assert f'revision: str = "{_REVISION}"' in body
    assert f'down_revision: str | Sequence[str] | None = "{_PRIOR_REVISION}"' in body


def _index_predicate(db_url: str) -> str | None:
    """Return the index definition for release_artwork_null_idx, or None."""
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = 'release' AND indexname = %s",
            (_INDEX_NAME,),
        )
        row = cur.fetchone()
    return row[0] if row else None


@pytest.fixture()
def drifted_db_url(fresh_db_url: str) -> str:
    """A DB in the exact shape prod was found in: schema applied, index gone.

    Dropping the index by hand stands in for the copy-swap's CTAS + RENAME,
    which is what actually removed it in production.
    """
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        cur.execute(f"DROP INDEX IF EXISTS {_INDEX_NAME}")
    return fresh_db_url


@pytest.mark.pg
def test_drifted_fixture_really_is_missing_the_index(drifted_db_url: str) -> None:
    """Vacuity guard: if the fixture didn't drop it, the repair tests prove nothing."""
    assert _index_predicate(drifted_db_url) is None, (
        "drifted_db_url fixture still has release_artwork_null_idx — the repair "
        "tests below would pass without 0014 doing anything"
    )


@pytest.mark.pg
def test_upgrade_repairs_the_missing_index(run_alembic, drifted_db_url: str) -> None:
    stamp = run_alembic(["stamp", _PRIOR_REVISION], drifted_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"

    result = run_alembic(["upgrade", _REVISION], drifted_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"

    indexdef = _index_predicate(drifted_db_url)
    assert indexdef is not None, f"{_INDEX_NAME} still missing after upgrading to {_REVISION}"
    assert "artwork_url IS NULL" in indexdef, f"predicate lost artwork_url clause: {indexdef!r}"
    assert "artwork_checked_at IS NULL" in indexdef, (
        f"predicate lost artwork_checked_at clause: {indexdef!r}"
    )
    assert " AND " in indexdef, f"predicate must AND both clauses, not OR: {indexdef!r}"


@pytest.mark.pg
def test_upgrade_is_a_noop_when_the_index_survived(run_alembic, fresh_db_url: str) -> None:
    """The undrifted case: schema applied, index intact, upgrade must not fail."""
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    assert _index_predicate(fresh_db_url) is not None, (
        "create_database.sql did not create the index — fixture assumption broken"
    )

    stamp = run_alembic(["stamp", _PRIOR_REVISION], fresh_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"
    result = run_alembic(["upgrade", _REVISION], fresh_db_url)
    assert result.returncode == 0, (
        f"upgrade must no-op cleanly when the index already exists:\n"
        f"{result.stdout}\n{result.stderr}"
    )
    assert _index_predicate(fresh_db_url) is not None, "no-op upgrade dropped the index"


@pytest.mark.pg
def test_downgrade_keeps_the_index(run_alembic, drifted_db_url: str) -> None:
    """Downgrade is a deliberate no-op — dropping would undo 0008, not 0014."""
    run_alembic(["stamp", _PRIOR_REVISION], drifted_db_url)
    run_alembic(["upgrade", _REVISION], drifted_db_url)
    assert _index_predicate(drifted_db_url) is not None, "precondition: upgrade created it"

    result = run_alembic(["downgrade", _PRIOR_REVISION], drifted_db_url)
    assert result.returncode == 0, f"downgrade failed:\n{result.stdout}\n{result.stderr}"
    assert _index_predicate(drifted_db_url) is not None, (
        "downgrade dropped release_artwork_null_idx — 0014's downgrade must be a "
        "no-op, since 0008 (still applied at this point) specifies the index exists"
    )


def _index_is_valid(db_url: str) -> bool | None:
    """Return ``pg_index.indisvalid`` for release_artwork_null_idx, or None if absent.

    ``pg_indexes`` (used by :func:`_index_predicate`) lists INVALID indexes
    exactly like valid ones, so predicate-shape assertions cannot tell the two
    apart. This reads the flag that decides whether the planner will actually
    use the index.
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
    """A DB carrying an INVALID ``release_artwork_null_idx``.

    This is the state PostgreSQL leaves behind when a ``CREATE INDEX
    CONCURRENTLY`` build is interrupted — SIGTERM on the rebuild EC2 instance,
    a job timeout, a cancelled statement: the index row stays in ``pg_class``
    with ``pg_index.indisvalid = false`` and the planner ignores it.

    Flipping the catalog flag by hand reproduces that without having to race a
    real CONCURRENTLY build, and is the standard way to exercise the recovery
    path. Requires superuser, which both the local docker PG and the CI
    ``wxyc-postgres`` service provide.
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
        "invalid_index_db_url fixture did not mark release_artwork_null_idx INVALID "
        "(catalog UPDATE needs superuser) — the repair test below would pass without "
        "0014 doing anything"
    )


@pytest.mark.pg
def test_upgrade_replaces_an_invalid_leftover_index(run_alembic, invalid_index_db_url: str) -> None:
    """The re-run case: an interrupted CONCURRENTLY build must not poison the retry.

    ``CREATE INDEX CONCURRENTLY IF NOT EXISTS`` alone is not enough here.
    PostgreSQL counts an INVALID index as existing, so ``IF NOT EXISTS``
    no-ops, alembic stamps 0014, and the operator is left with an index the
    planner will never use — the exact silent state this revision exists to
    repair. 0014 must drop the invalid leftover first.
    """
    stamp = run_alembic(["stamp", _PRIOR_REVISION], invalid_index_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"

    result = run_alembic(["upgrade", _REVISION], invalid_index_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"

    assert _index_is_valid(invalid_index_db_url) is True, (
        "release_artwork_null_idx is still INVALID after upgrading to "
        f"{_REVISION}. A re-run following an interrupted CONCURRENTLY build must "
        "drop the invalid leftover before recreating; IF NOT EXISTS treats it as "
        "present and silently no-ops."
    )
