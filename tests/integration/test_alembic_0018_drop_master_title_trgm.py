"""Migration test for 0018_drop_master_title_trgm.

0018 drops ``idx_master_title_trgm``, a trigram GIN index on ``master`` that
production never used (``idx_scan = 0``), and its declaration is deleted from
``schema/create_database.sql`` in the same change. ``0001_initial`` replays the
live ``schema/*.sql`` files, so a plain ``upgrade head`` on an empty database
never has the index at all -- "absent after upgrade" would pass with an empty
migration. The fixture therefore builds production's state by hand: the schema
files, plus the index created with its old definition.

See WXYC/discogs-etl#437.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
SCHEMA_DIR = REPO_ROOT / "schema"
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0018_drop_master_title_trgm.py"

_REVISION = "0018_drop_master_title_trgm"
_PRIOR_REVISION = "0017_artist_child_unique"
_INDEX_NAME = "idx_master_title_trgm"
_OLD_DEFINITION = (
    f"CREATE INDEX IF NOT EXISTS {_INDEX_NAME} "
    "ON master USING GIN (lower(f_unaccent(title)) gin_trgm_ops)"
)


def test_migration_chains_from_0017() -> None:
    body = MIGRATION_PATH.read_text()
    assert f'revision: str = "{_REVISION}"' in body
    assert f'down_revision: str | Sequence[str] | None = "{_PRIOR_REVISION}"' in body
    assert len(_REVISION) <= 32, "alembic_version is varchar(32)"


def test_migration_drops_concurrently_and_if_exists() -> None:
    body = MIGRATION_PATH.read_text()
    assert f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX_NAME}" in body


def test_create_database_sql_no_longer_declares_the_index() -> None:
    """Left in, the next monthly rebuild would recreate the index 0018 dropped."""
    assert _INDEX_NAME not in SCHEMA_DIR.joinpath("create_database.sql").read_text()


def _index_names(db_url: str, table: str = "master") -> set[str]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = %s",
            (table,),
        )
        return {row[0] for row in cur.fetchall()}


def _indexdefs(db_url: str, table: str = "master") -> dict[str, str]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = %s",
            (table,),
        )
        return dict(cur.fetchall())


@pytest.fixture()
def prod_state_db_url(fresh_db_url: str) -> str:
    """Production's pre-0018 state: the schema files plus the old index, by hand."""
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        for name in ("create_functions.sql", "create_database.sql"):
            cur.execute(SCHEMA_DIR.joinpath(name).read_text())
        cur.execute(_OLD_DEFINITION)
    return fresh_db_url


@pytest.mark.pg
def test_fixture_really_has_the_index(prod_state_db_url: str) -> None:
    """Vacuity guard: without the index the drop test proves nothing."""
    assert _INDEX_NAME in _index_names(prod_state_db_url)


@pytest.mark.pg
def test_upgrade_drops_the_index(run_alembic, prod_state_db_url: str) -> None:
    stamp = run_alembic(["stamp", _PRIOR_REVISION], prod_state_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"

    result = run_alembic(["upgrade", _REVISION], prod_state_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"
    assert _INDEX_NAME not in _index_names(prod_state_db_url)


@pytest.mark.pg
def test_upgrade_is_a_noop_when_the_index_is_already_absent(run_alembic, fresh_db_url: str) -> None:
    """A database built from the current schema files never had it."""
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    assert _INDEX_NAME not in _index_names(fresh_db_url)

    run_alembic(["stamp", _PRIOR_REVISION], fresh_db_url)
    result = run_alembic(["upgrade", _REVISION], fresh_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"
    assert _INDEX_NAME not in _index_names(fresh_db_url)


@pytest.mark.pg
def test_downgrade_recreates_the_index_and_reupgrade_drops_it(
    run_alembic, prod_state_db_url: str
) -> None:
    run_alembic(["stamp", _PRIOR_REVISION], prod_state_db_url)
    run_alembic(["upgrade", _REVISION], prod_state_db_url)
    assert _INDEX_NAME not in _index_names(prod_state_db_url), "precondition: upgrade dropped it"

    down = run_alembic(["downgrade", _PRIOR_REVISION], prod_state_db_url)
    assert down.returncode == 0, f"downgrade failed:\n{down.stdout}\n{down.stderr}"
    indexdef = _indexdefs(prod_state_db_url).get(_INDEX_NAME)
    assert indexdef is not None, "downgrade did not recreate the index"
    assert (
        "USING gin" in indexdef and "lower(f_unaccent(" in indexdef and "gin_trgm_ops" in indexdef
    )

    up = run_alembic(["upgrade", _REVISION], prod_state_db_url)
    assert up.returncode == 0, f"re-upgrade failed:\n{up.stdout}\n{up.stderr}"
    assert _INDEX_NAME not in _index_names(prod_state_db_url)


@pytest.mark.pg
def test_master_indexes_match_between_schema_build_and_alembic_head(
    run_alembic, fresh_db_url: str
) -> None:
    """The full ``pg_indexes`` set on ``master`` agrees across both build paths.

    Fails if the declaration is left in ``create_database.sql``: the schema
    build would carry the index while the migrated path (which drops it)
    would not.
    """
    import uuid

    from psycopg import sql as psycopg_sql

    schema_built = fresh_db_url
    with psycopg.connect(schema_built, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())

    admin_url, _, _ = fresh_db_url.rpartition("/")
    migrated_name = f"discogs_parity_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(f"{admin_url}/postgres", autocommit=True) as admin:
        admin.execute(
            psycopg_sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                psycopg_sql.Identifier(migrated_name)
            )
        )
    migrated = f"{admin_url}/{migrated_name}"
    try:
        result = run_alembic(["upgrade", "head"], migrated)
        assert result.returncode == 0, f"upgrade head failed:\n{result.stdout}\n{result.stderr}"
        assert _indexdefs(schema_built) == _indexdefs(migrated)
    finally:
        with psycopg.connect(f"{admin_url}/postgres", autocommit=True) as admin:
            admin.execute(
                psycopg_sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    psycopg_sql.Identifier(migrated_name)
                )
            )
