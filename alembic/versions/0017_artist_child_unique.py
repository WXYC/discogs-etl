"""artist_* child tables: UNIQUE (artist_id, <key>), dropping the prefix indexes

Nothing in the database stopped the monthly rebuild re-COPYing the dump's
``artist_alias`` / ``artist_name_variation`` / ``artist_member`` /
``artist_url`` rows on top of the ones already there, so it did, every month:
3,988,085 ``artist_name_variation`` rows for 809,820 distinct keys, and the
same ~4.8x on the other three -- 4.87M surplus rows and ~312 MB across the
four tables, measured read-only against prod on 2026-09-25
(WXYC/discogs-etl#433). This revision adds the missing key, which is also the
arbiter that ``scripts/import_csv.py::_import_artist_children``'s ``ON
CONFLICT (artist_id, <key>) DO NOTHING`` infers -- so the loader in the same
change cannot ship without it.

**It refuses on duplicates rather than deduping them.**
``scripts/rebuild-cache.sh`` runs ``alembic upgrade head`` unattended at step
2b, before ``run_pipeline.py`` takes the rebuild advisory lock, and a
multi-million-row DELETE does not belong there. Deleting rows would also make
this revision row-mutating and cost it the pure-DDL clearance
(``docs/migrations-runbook.md``) that is the only reason it may be applied
out of band -- which is how it is meant to be applied, in the same operator
session as ``scripts/dedup_artist_children.py``, minutes after it. The guard
raises, names that script, and the operator loops dedupe -> upgrade.

**Each index build is retried on 23505.** ``CREATE UNIQUE INDEX
CONCURRENTLY`` begins enforcing uniqueness once the index is
ready-for-inserts, well before it is valid, while ``ON CONFLICT``'s arbiter
inference considers only *valid* indexes. So for the seconds-to-a-minute a
build takes, an LML ``write_artist_details`` whose Discogs response repeats a
URL or an alias raises ``unique_violation`` despite the target-less ``ON
CONFLICT DO NOTHING`` it now carries (library-metadata-lookup#1361), and
takes the build down with it, leaving an INVALID index behind.
:func:`lib.pg_concurrent_ddl.add_index_concurrently_safely` drops that
leftover on the way in, so the retry starts from a clean catalog.

**Names are PostgreSQL's own** ``<table>_<col>_<col>_key``, which is what the
matching inline ``UNIQUE (a, b)`` added to ``schema/create_database.sql`` in
this change produces -- so a cold-built database and an ``alembic upgrade
head`` one stay identical in ``pg_indexes``. The four
``idx_artist_*_artist_id`` indexes go in the same step: each is a strict
prefix of its table's new UNIQUE. Their declarations leave
``create_database.sql`` too, without which the next rebuild would run that
file and quietly recreate all four.

Prior art: ``alembic/versions/0009_cache_metadata_unique.py``, the in-repo
dedupe-then-UNIQUE precedent.

Revision ID: 0017_artist_child_unique
Revises: 0016_release_status_notes_dq
Create Date: 2026-09-26

"""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence

import psycopg

from lib.alembic_helpers import refuse_offline, resolve_db_url
from lib.pg_concurrent_ddl import (
    _drop_invalid_index_if_present,
    add_constraint_safely,
    add_index_concurrently_safely,
)
from scripts.import_csv import ARTIST_CHILD_KEYS

# revision identifiers, used by Alembic.
revision: str = "0017_artist_child_unique"
down_revision: str | Sequence[str] | None = "0016_release_status_notes_dq"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DEDUPE_SCRIPT = "scripts/dedup_artist_children.py"

CONSTRAINT_NAMES: dict[str, str] = {
    table: f"{table}_{'_'.join(key)}_key" for table, key in ARTIST_CHILD_KEYS.items()
}

CREATE_UNIQUE_INDEX_DDL: dict[str, str] = {
    table: (
        f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {CONSTRAINT_NAMES[table]} "
        f"ON {table} ({', '.join(key)})"
    )
    for table, key in ARTIST_CHILD_KEYS.items()
}

ADD_CONSTRAINT_DDL: dict[str, str] = {
    table: f"ALTER TABLE {table} ADD CONSTRAINT {name} UNIQUE USING INDEX {name}"
    for table, name in CONSTRAINT_NAMES.items()
}

SUPERSEDED_INDEX_NAMES: dict[str, str] = {
    table: f"idx_{table}_artist_id" for table in ARTIST_CHILD_KEYS
}

# A UNIQUE index changes the failure class for an oversized key. A1 measured
# the widest artist_url.url at 1,017 bytes against btree's ~2,704-byte limit,
# so no md5() expression branch is warranted -- but if a future dump ever
# carries a longer url or alias_name, the symptom is
# `index row size ... exceeds btree version 4 maximum` raised inside
# _import_artist_children's merge, which aborts the unattended monthly
# rebuild rather than storing the row as it used to.
SQLSTATE_UNIQUE_VIOLATION = "23505"
BUILD_ATTEMPTS = 3
BUILD_BACKOFF_SECONDS: tuple[float, ...] = (10.0, 30.0)


def _refuse_on_duplicates(conn: psycopg.Connection) -> None:
    """Raise unless every table already satisfies the key (see the docstring)."""
    offenders: list[str] = []
    with conn.cursor() as cur:
        for table, key in ARTIST_CHILD_KEYS.items():
            columns = ", ".join(key)
            # Probe before counting. The exact surplus needs a full scan plus
            # an aggregate over ~4M rows on artist_name_variation, and the
            # runbook's retry loop pays it per iteration -- including for
            # tables already constrained by a partly-applied run, which
            # cannot have duplicates at all. The probe stops at the first
            # duplicate; only a table that has one pays for the count.
            cur.execute(f"SELECT 1 FROM {table} GROUP BY {columns} HAVING count(*) > 1 LIMIT 1")
            if cur.fetchone() is None:
                continue
            cur.execute(f"SELECT count(*) - count(DISTINCT ({columns})) FROM {table}")
            surplus = cur.fetchone()[0]  # type: ignore[index]
            if surplus:
                offenders.append(f"{table}: {surplus:,} surplus rows over ({columns})")
    if offenders:
        raise RuntimeError(
            f"{revision} refuses to run — {'; '.join(offenders)}. Run "
            f"`python {DEDUPE_SCRIPT} --execute` against this database, then retry. "
            "This revision deliberately does not dedupe: deleting rows would make it "
            "row-mutating and forfeit the pure-DDL clearance (docs/migrations-runbook.md) "
            "that lets it be applied out of band, and an unattended rebuild is no place "
            "for a multi-million-row DELETE."
        )


def _constraint_exists(conn: psycopg.Connection, table: str, name: str) -> bool:
    """Is ``name`` already a constraint *on this table*?

    Scoped by ``conrelid`` deliberately: ``conname`` is unique per table, not
    per database, so an unscoped probe treats a same-named constraint on any
    other relation as proof this one exists — skipping the promote and
    stamping the revision with a bare unique index where a constraint was
    promised.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_constraint WHERE conname = %s AND conrelid = %s::regclass",
            (name, table),
        )
        return cur.fetchone() is not None


def _build_unique_index(conn: psycopg.Connection, table: str, log: logging.Logger) -> None:
    """CONCURRENTLY-build the UNIQUE index, retrying a concurrent 23505."""
    for attempt in range(1, BUILD_ATTEMPTS + 1):
        try:
            add_index_concurrently_safely(conn, CREATE_UNIQUE_INDEX_DDL[table])
            return
        except psycopg.Error as exc:
            if exc.sqlstate != SQLSTATE_UNIQUE_VIOLATION or attempt == BUILD_ATTEMPTS:
                # A CONCURRENTLY build that fails its *second* scan leaves an
                # index with indisready = true, indisvalid = false: ignored for
                # queries, but still maintained on every write. Later attempts
                # preclean it (add_index_concurrently_safely does), so only the
                # give-up path can strand one -- indefinitely, and visible
                # nowhere but pg_index.
                _drop_invalid_index_if_present(conn, CONSTRAINT_NAMES[table])
                raise
            delay = BUILD_BACKOFF_SECONDS[attempt - 1]
            log.warning(
                "0017: %s index build hit 23505 on attempt %d/%d — a live write landed a "
                "duplicate while the index was enforcing but not yet valid; retrying in %.0fs",
                table,
                attempt,
                BUILD_ATTEMPTS,
                delay,
            )
            time.sleep(delay)


def upgrade() -> None:
    refuse_offline(revision, "upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn:
        _refuse_on_duplicates(conn)
        for table in ARTIST_CHILD_KEYS:
            log.info("0017: add %s", CONSTRAINT_NAMES[table])
            _build_unique_index(conn, table, log)
            if not _constraint_exists(conn, table, CONSTRAINT_NAMES[table]):
                add_constraint_safely(conn, ADD_CONSTRAINT_DDL[table], lock_tables=(table,))
        with conn.cursor() as cur:
            for table, index in SUPERSEDED_INDEX_NAMES.items():
                log.info("0017: drop %s, superseded by the new UNIQUE on %s", index, table)
                cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index}")


def downgrade() -> None:
    """Drop the four constraints and put the plain ``(artist_id)`` indexes back.

    This restores 0016's *schema*, not its data: the duplicate rows the
    dedupe removed are gone and nothing recorded what they were. For
    ``artist_alias`` and ``artist_member`` they were not even byte-identical
    to the survivors -- the dedupe kept the row carrying the strictly greater
    information (see ``scripts/dedup_artist_children.py``), so a downgrade
    could not reconstruct them even in principle.
    """
    refuse_offline(revision, "downgrade")

    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn:
        for table, name in CONSTRAINT_NAMES.items():
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
                # Dropping the constraint takes its index with it -- but if
                # `upgrade` died between the build and the promote there is an
                # index and no constraint, and the DROP above no-ops on it.
                # Left behind it keeps enforcing, so a downgrade run precisely
                # to unblock writes would report success and change nothing.
                cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
            add_index_concurrently_safely(
                conn,
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {SUPERSEDED_INDEX_NAMES[table]} "
                f"ON {table} (artist_id)",
            )
