"""Verify the 0017_artist_child_unique migration (WXYC/discogs-etl#433).

Every monthly rebuild re-COPYed the dump's ``artist_alias`` /
``artist_name_variation`` / ``artist_member`` / ``artist_url`` rows on top of
what was already there, so prod carried ~4.8 copies of each row (3,988,085
``artist_name_variation`` rows for 809,820 distinct keys, measured
2026-09-25). 0017 is the half that stops it recurring: a ``UNIQUE (artist_id,
<key>)`` per table, which is also the arbiter the rebuilt loader's ``ON
CONFLICT`` needs.

Three properties are load-bearing and each has a test below.

**It refuses on duplicates rather than deleting them.** The rebuild applies
migrations unattended at step 2b of ``scripts/rebuild-cache.sh``; a
multi-million-row DELETE must not run from there. Refusing also keeps 0017
pure DDL, which is what clears it for the out-of-band apply that
``docs/migrations-runbook.md`` gates on. ``scripts/dedup_artist_children.py``
is the dedupe, and the error message has to name it.

**The constraint names match PostgreSQL's own.** ``schema/create_database.sql``
declares the same uniqueness inline, and an inline ``UNIQUE (a, b)`` is named
``<table>_a_b_key``. If 0017 chose anything else the two build paths would
diverge in ``pg_indexes`` while both looked correct.

**The old plain ``(artist_id)`` indexes go.** Each is a strict prefix of its
new UNIQUE and therefore dead weight — 38 MB of it on
``artist_name_variation`` alone. ``create_database.sql`` drops the
declarations in the same change, and the parity test compares the *whole*
``pg_indexes`` set rather than only the UNIQUE ones: comparing UNIQUEs alone
is exactly what would let a surviving ``CREATE INDEX`` declaration pass
unnoticed and be recreated by the next rebuild.
"""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from tests.conftest import _ephemeral_database

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = REPO_ROOT / "schema"

REVISION = "0017_artist_child_unique"
PRIOR_REVISION = "0016_release_status_notes_dq"

# Literal on purpose: the unit suite pins these against the ARTIST_TABLES key
# mapping, so restating them here catches a rename that updated both the
# migration and its derivation but nothing else.
CONSTRAINTS = {
    "artist_alias": "artist_alias_artist_id_alias_name_key",
    "artist_name_variation": "artist_name_variation_artist_id_name_key",
    "artist_member": "artist_member_artist_id_member_id_key",
    "artist_url": "artist_url_artist_id_url_key",
}
SUPERSEDED_INDEXES = {table: f"idx_{table}_artist_id" for table in CONSTRAINTS}

pytestmark = pytest.mark.pg


@pytest.fixture()
def second_db_url() -> Iterator[str]:
    """A second throwaway database, for the two-build-path parity test."""
    yield from _ephemeral_database()


def _apply_schema(db_url: str) -> None:
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())


def _setup_pre_0017(db_url: str) -> None:
    """Apply the schema, then undo 0017's end state to mimic production.

    Prod ran ``create_database.sql`` long before #433, so it has the four
    plain ``(artist_id)`` indexes and no UNIQUE constraints. The current file
    declares the opposite, so the drift has to be recreated by hand.
    """
    _apply_schema(db_url)
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for table, constraint in CONSTRAINTS.items():
            cur.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}")
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {SUPERSEDED_INDEXES[table]} ON {table} (artist_id)"
            )


def _seed_duplicates(db_url: str) -> None:
    """One artist with a duplicated row in each of the four child tables."""
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO artist (id, name) VALUES (1, 'Juana Molina')")
        cur.execute(
            "INSERT INTO artist_alias (artist_id, alias_name) VALUES (1, 'Juana'), (1, 'Juana')"
        )
        cur.execute(
            "INSERT INTO artist_name_variation (artist_id, name) VALUES (1, 'J. Molina'), "
            "(1, 'J. Molina')"
        )
        cur.execute(
            "INSERT INTO artist_member (artist_id, member_id, member_name) "
            "VALUES (1, 9, 'Juana'), (1, 9, 'Juana')"
        )
        cur.execute(
            "INSERT INTO artist_url (artist_id, url) VALUES (1, 'https://x/'), (1, 'https://x/')"
        )


def _constraint_names(db_url: str) -> set[str]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT c.conname FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            "WHERE c.contype = 'u' AND n.nspname = 'public' AND t.relname = ANY(%s)",
            (list(CONSTRAINTS),),
        )
        return {row[0] for row in cur.fetchall()}


def _index_names(db_url: str) -> set[str]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = ANY(%s)",
            (list(CONSTRAINTS),),
        )
        return {row[0] for row in cur.fetchall()}


def _index_definitions(db_url: str) -> set[tuple[str, str]]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename = ANY(%s)",
            (list(CONSTRAINTS),),
        )
        return set(cur.fetchall())


def _upgrade(run_alembic, db_url: str) -> subprocess.CompletedProcess[str]:
    stamp = run_alembic(["stamp", PRIOR_REVISION], db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"
    return run_alembic(["upgrade", "head"], db_url)


def test_refuses_to_run_while_duplicates_remain(run_alembic, fresh_db_url: str) -> None:
    """(a) The guard refuses and names the dedupe script; nothing is created.

    Deleting a "small residue" here would make 0017 row-mutating and cost it
    the pure-DDL clearance that lets it be applied out of band at all.
    """
    _setup_pre_0017(fresh_db_url)
    _seed_duplicates(fresh_db_url)

    result = _upgrade(run_alembic, fresh_db_url)

    assert result.returncode != 0, (
        "0017 must refuse against duplicated artist_* rows. Silently proceeding would "
        f"fail on the UNIQUE build instead.\n{result.stdout}\n{result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "scripts/dedup_artist_children.py" in combined, (
        "the refusal must name the script that fixes it — an operator reading a "
        f"rebuild log at 23:00 has nothing else to go on.\n{combined}"
    )
    assert _constraint_names(fresh_db_url) == set(), (
        "a refused upgrade must leave no constraint behind; a partially-applied 0017 "
        "would make the retry's guard read a different table set."
    )
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM artist_name_variation")
        assert cur.fetchone()[0] == 2, "0017 must not delete rows"


def test_clean_upgrade_adds_constraints_and_drops_superseded_indexes(
    run_alembic, fresh_db_url: str
) -> None:
    """(b) Four UNIQUE constraints in, four plain (artist_id) indexes out."""
    _setup_pre_0017(fresh_db_url)
    assert set(SUPERSEDED_INDEXES.values()) <= _index_names(fresh_db_url), (
        "drift setup should have created the pre-0017 indexes"
    )

    result = _upgrade(run_alembic, fresh_db_url)
    assert result.returncode == 0, f"upgrade failed:\n{result.stdout}\n{result.stderr}"

    assert _constraint_names(fresh_db_url) == set(CONSTRAINTS.values())
    assert set(SUPERSEDED_INDEXES.values()).isdisjoint(_index_names(fresh_db_url)), (
        "each idx_artist_*_artist_id is a strict prefix of its new UNIQUE; leaving them "
        "keeps paying for 38 MB (artist_name_variation, measured on prod) of duplicate index."
    )


def test_rerunning_the_migration_body_is_a_no_op(run_alembic, fresh_db_url: str) -> None:
    """(c) Idempotent: re-stamping back and upgrading again changes nothing.

    ``alembic upgrade head`` twice would no-op in alembic rather than in the
    migration, which tests nothing — the stamp back to 0016 forces the body
    to run a second time against a database that already has its end state.
    """
    _setup_pre_0017(fresh_db_url)
    first = _upgrade(run_alembic, fresh_db_url)
    assert first.returncode == 0, f"upgrade failed:\n{first.stdout}\n{first.stderr}"
    before = _index_definitions(fresh_db_url)

    second = _upgrade(run_alembic, fresh_db_url)
    assert second.returncode == 0, f"re-run failed:\n{second.stdout}\n{second.stderr}"

    assert _index_definitions(fresh_db_url) == before
    assert _constraint_names(fresh_db_url) == set(CONSTRAINTS.values())


def test_both_build_paths_agree_on_every_index(
    run_alembic, fresh_db_url: str, second_db_url: str
) -> None:
    """(d) Dual-write parity over the FULL pg_indexes set, not just the UNIQUEs.

    A database cold-built from ``create_database.sql`` and one built by
    ``alembic upgrade head`` must carry identical indexes on these four
    tables. Restricting the comparison to UNIQUE constraints is the failure
    this test is shaped to catch: both paths would agree on those while the
    schema file silently kept declaring four superseded plain indexes for the
    next rebuild to recreate.
    """
    _apply_schema(fresh_db_url)

    migrated = run_alembic(["upgrade", "head"], second_db_url)
    assert migrated.returncode == 0, f"upgrade failed:\n{migrated.stdout}\n{migrated.stderr}"

    assert _index_definitions(second_db_url) == _index_definitions(fresh_db_url)


def test_downgrade_drops_the_constraints_and_restores_the_plain_indexes(
    run_alembic, fresh_db_url: str
) -> None:
    """(e) Downgrade returns the schema to 0016's shape (not to duplicates)."""
    _setup_pre_0017(fresh_db_url)
    upgraded = _upgrade(run_alembic, fresh_db_url)
    assert upgraded.returncode == 0, f"upgrade failed:\n{upgraded.stdout}\n{upgraded.stderr}"

    result = run_alembic(["downgrade", PRIOR_REVISION], fresh_db_url)
    assert result.returncode == 0, f"downgrade failed:\n{result.stdout}\n{result.stderr}"

    assert _constraint_names(fresh_db_url) == set()
    assert set(SUPERSEDED_INDEXES.values()) <= _index_names(fresh_db_url)


UNIQUE_KEY_COLUMNS = {
    "artist_alias": "artist_id, alias_name",
    "artist_name_variation": "artist_id, name",
    "artist_member": "artist_id, member_id",
    "artist_url": "artist_id, url",
}


def _leave_indexes_without_constraints(db_url: str) -> None:
    """The shape ``upgrade`` leaves if it dies between the two DDL halves.

    ``_build_unique_index`` succeeds, then ``add_constraint_safely`` exhausts
    its lock_timeout retries under LML contention -- so the unique *index*
    exists and no constraint does.
    """
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for table, columns in UNIQUE_KEY_COLUMNS.items():
            cur.execute(f"CREATE UNIQUE INDEX {CONSTRAINTS[table]} ON {table} ({columns})")


def test_downgrade_clears_a_unique_index_left_without_its_constraint(
    run_alembic, fresh_db_url: str
) -> None:
    """A half-applied upgrade must not survive its own downgrade.

    ``DROP CONSTRAINT IF EXISTS`` no-ops when the promote never happened, so
    without an explicit index drop the downgrade reports success, recreates
    the plain indexes -- and leaves a unique index still enforcing on every
    write. An operator downgrading precisely to unblock writes would get no
    relief and a green exit saying otherwise.
    """
    _setup_pre_0017(fresh_db_url)
    _leave_indexes_without_constraints(fresh_db_url)
    stamp = run_alembic(["stamp", REVISION], fresh_db_url)
    assert stamp.returncode == 0, f"stamp failed:\n{stamp.stdout}\n{stamp.stderr}"

    result = run_alembic(["downgrade", PRIOR_REVISION], fresh_db_url)
    assert result.returncode == 0, f"downgrade failed:\n{result.stdout}\n{result.stderr}"

    survivors = _index_names(fresh_db_url) & set(CONSTRAINTS.values())
    assert survivors == set(), f"unique index survived downgrade and still enforces: {survivors}"
    assert set(SUPERSEDED_INDEXES.values()) <= _index_names(fresh_db_url)


def test_constraint_check_is_scoped_to_its_own_table(run_alembic, fresh_db_url: str) -> None:
    """``conname`` is unique per table, not per database.

    The decoy has to live in *another schema*: index names are unique per
    schema, so a same-named constraint in ``public`` would collide on the
    index build long before the probe mattered. Across schemas nothing
    collides, and an unscoped ``pg_constraint`` probe then treats the decoy as
    proof this constraint exists -- skipping the promote and stamping 0017
    with a bare unique index where a constraint was promised.
    """
    _setup_pre_0017(fresh_db_url)
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA decoy")
        cur.execute("CREATE TABLE decoy.unrelated (artist_id int, alias_name text)")
        cur.execute(
            f"ALTER TABLE decoy.unrelated ADD CONSTRAINT {CONSTRAINTS['artist_alias']} "
            "UNIQUE (artist_id, alias_name)"
        )

    upgraded = _upgrade(run_alembic, fresh_db_url)
    assert upgraded.returncode == 0, f"upgrade failed:\n{upgraded.stdout}\n{upgraded.stderr}"
    assert _constraint_names(fresh_db_url) == set(CONSTRAINTS.values())
