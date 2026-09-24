"""Verify 0016 adds release.status / notes / data_quality (WXYC/discogs-etl#428).

The converter's ``release.csv`` writer has always emitted ``status``, ``notes``
and ``data_quality``; the import dropped them for want of somewhere to put
them. This revision is that somewhere.

What the assertions here are actually protecting:

* **Nullable, no default.** Not cosmetic. ``scripts/import_csv.py`` registers
  the three as ``optional_csv_columns``, so a ``release.csv`` from a converter
  predating them omits the columns from its COPY entirely and the DB-side
  behaviour decides what lands. NULL is the honest answer — "this dump said
  nothing about the field". A ``NOT NULL`` or a ``DEFAULT ''`` would forge a
  value the dump never carried, and ``NOT NULL`` would additionally make the
  legacy COPY fail outright.
* **Dual-write parity.** ``schema/create_database.sql`` (fresh-rebuild path)
  and the alembic chain (upgrade path) must land the same ``release`` shape,
  or which path a database was built by becomes load-bearing. ``alembic
  upgrade head`` exiting 0 does not establish that, and the copy-swap tests
  build their schema from ``create_database.sql`` rather than the chain, so
  without the parity test below nothing compares the two.

Follows the per-revision convention of
``tests/integration/test_alembic_0010_release_not_found.py`` and its siblings.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0016_release_status_notes_dq.py"
SCHEMA_DIR = REPO_ROOT / "schema"

REVISION = "0016_release_status_notes_dq"
PREVIOUS_REVISION = "0015_release_master_id_idx"
QUALIFIER_COLUMNS = ("status", "notes", "data_quality")


# ---------------------------------------------------------------------------
# Static (no DB) checks
# ---------------------------------------------------------------------------


def test_migration_file_exists() -> None:
    assert MIGRATION_PATH.exists(), (
        f"0016 migration missing at {MIGRATION_PATH}. Per the dual-write "
        f"convention, the alembic chain and schema/create_database.sql must "
        f"agree on the release-table shape."
    )


@pytest.mark.parametrize("column", QUALIFIER_COLUMNS)
def test_migration_adds_each_column_idempotently(column: str) -> None:
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert f"ADD COLUMN IF NOT EXISTS {column} text" in body, (
        f"0016 must ADD COLUMN {column} text with IF NOT EXISTS so it no-ops "
        f"against a database already carrying the dual-written schema, which "
        f"the legacy rebuild path applies directly."
    )


def test_migration_declares_no_default_and_no_not_null() -> None:
    """A default or a NOT NULL would defeat the legacy-CSV guard in import_csv."""
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    upgrade_sql = body.split("_UPGRADE_SQL", 1)[1].split('"""', 2)[1]
    assert "DEFAULT" not in upgrade_sql.upper(), (
        "0016 must not give the qualifier columns a DEFAULT. A dump that does "
        "not carry the field should leave NULL, not a fabricated value."
    )
    assert "NOT NULL" not in upgrade_sql.upper(), (
        "0016 must leave the qualifier columns nullable. NOT NULL would make a "
        "COPY from a release.csv that predates them fail outright."
    )


def test_migration_chains_onto_the_current_head() -> None:
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert f'revision: str = "{REVISION}"' in body
    assert f'down_revision: str | Sequence[str] | None = "{PREVIOUS_REVISION}"' in body, (
        f"0016 must revise {PREVIOUS_REVISION}. A gap or a fork in the chain "
        f"leaves `alembic upgrade head` ambiguous."
    )


# ---------------------------------------------------------------------------
# Live-PG assertions
# ---------------------------------------------------------------------------


def _release_columns(db_url: str) -> dict[str, tuple]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'release'
            """
        )
        return {row[0]: row[1:] for row in cur.fetchall()}


@pytest.fixture()
def db_with_release_table(fresh_db_url: str) -> str:
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    return fresh_db_url


@pytest.fixture()
def second_db_url(fresh_db_url: str) -> Iterator[str]:
    """A *second* ephemeral database on the same server.

    ``fresh_db_url`` is function-scoped, so a test requesting it alongside a
    fixture derived from it gets one database, not two — which silently turns
    any A-versus-B comparison into A-versus-A. The parity test below did
    exactly that until a deliberate drift (commenting ``data_quality`` out of
    ``create_database.sql``) failed to make it fail. Minted here instead,
    reusing the server ``fresh_db_url`` already resolved.
    """
    admin_url = fresh_db_url.rsplit("/", 1)[0] + "/postgres"
    db_name = f"discogs_test_parity_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(admin_url, autocommit=True)
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{db_name}"')
        yield fresh_db_url.rsplit("/", 1)[0] + "/" + db_name
    finally:
        with admin.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        admin.close()


@pytest.mark.pg
def test_upgrade_adds_three_nullable_text_columns(fresh_db_url: str, run_alembic) -> None:
    """Build from the chain alone — the path a database that was never cold-built takes."""
    result = run_alembic(["upgrade", REVISION], fresh_db_url)
    assert result.returncode == 0, (
        f"alembic upgrade failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    columns = _release_columns(fresh_db_url)
    for column in QUALIFIER_COLUMNS:
        assert column in columns, f"release.{column} missing after the 0016 upgrade"
        data_type, is_nullable, column_default = columns[column]
        assert data_type == "text", (
            f"release.{column} must be text; got {data_type!r}. Discogs ships "
            f"these as free strings and the import applies no transform."
        )
        assert is_nullable == "YES", (
            f"release.{column} must stay nullable — a release.csv that predates "
            f"#428 omits the column from its COPY, and NULL is the honest "
            f"record of a dump that said nothing about the field."
        )
        assert column_default is None, (
            f"release.{column} must have no default; got {column_default!r}. A "
            f"default would fabricate a value the dump never carried."
        )


@pytest.mark.pg
def test_upgrade_is_idempotent_against_the_dual_written_schema(
    db_with_release_table: str, run_alembic
) -> None:
    """The cold-built case: create_database.sql already declared the columns."""
    db_url = db_with_release_table
    assert set(QUALIFIER_COLUMNS) <= set(_release_columns(db_url)), (
        "Pre-condition: schema/create_database.sql must dual-write the three "
        "columns. Without that this test no longer covers re-application."
    )

    assert run_alembic(["stamp", PREVIOUS_REVISION], db_url).returncode == 0
    result = run_alembic(["upgrade", REVISION], db_url)
    assert result.returncode == 0, (
        f"0016 must be idempotent against pre-existing columns. alembic "
        f"output:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


@pytest.mark.pg
def test_downgrade_removes_only_the_three_columns(fresh_db_url: str, run_alembic) -> None:
    assert run_alembic(["upgrade", REVISION], fresh_db_url).returncode == 0
    before = _release_columns(fresh_db_url)

    result = run_alembic(["downgrade", PREVIOUS_REVISION], fresh_db_url)
    assert result.returncode == 0, (
        f"alembic downgrade failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    after = _release_columns(fresh_db_url)
    assert set(before) - set(after) == set(QUALIFIER_COLUMNS), (
        "downgrade must drop exactly status / notes / data_quality. Dropping "
        "more would undo an earlier revision; dropping fewer leaves the "
        "database out of parity with 0015."
    )


@pytest.mark.pg
def test_alembic_chain_and_create_database_sql_agree_on_release(
    db_with_release_table: str, second_db_url: str, run_alembic
) -> None:
    """The dual-write contract, asserted rather than assumed.

    ``alembic upgrade head`` exiting 0 says the chain ran, not that it landed
    the same table ``schema/create_database.sql`` declares. The copy-swap and
    import tests all build their schema from the SQL file, so a drift between
    the two paths would be invisible to every other test in the suite and
    would surface only on a database built the other way.

    The two databases must be genuinely distinct — see ``second_db_url``.
    """
    result = run_alembic(["upgrade", "head"], second_db_url)
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    from_chain = _release_columns(second_db_url)
    from_sql = _release_columns(db_with_release_table)

    assert set(from_chain) == set(from_sql), (
        f"`release` differs between the alembic chain and "
        f"schema/create_database.sql. Only in the chain: "
        f"{sorted(set(from_chain) - set(from_sql))}; only in the SQL file: "
        f"{sorted(set(from_sql) - set(from_chain))}. Both paths are live — the "
        f"monthly rebuild cold-builds from the SQL file, prod upgrades through "
        f"the chain — so they must land the same table."
    )
    for column in QUALIFIER_COLUMNS:
        assert from_chain[column] == from_sql[column], (
            f"release.{column} has a different shape depending on how the "
            f"database was built: chain {from_chain[column]!r} vs "
            f"create_database.sql {from_sql[column]!r}."
        )
