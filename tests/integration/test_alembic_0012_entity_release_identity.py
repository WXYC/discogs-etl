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

The downgrade drops in FK order — child table, then parent — and
intentionally leaves the ``entity`` schema in place because the artist-side
tables (``entity.identity`` / ``entity.reconciliation_log``) still live
there. Adopting those into the alembic chain is tracked at
WXYC/discogs-etl#279.

Tracked at WXYC/discogs-etl#278.
"""

from __future__ import annotations

import re
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0012_entity_release_identity.py"


def _read_revision_metadata() -> tuple[str, str]:
    """Return (revision, down_revision) parsed from 0012's source file.

    Source-of-truth lookup keeps the test from hardcoding revision strings
    that might be shortened later (e.g., 0009's revision id was trimmed to
    fit alembic_version varchar(32) in commit c228b47). If the chain is
    renamed, this test tracks automatically instead of failing eight cases
    in lockstep on a stale stamp target.

    Regex over the file rather than importlib because the migration's
    imports (``from lib.alembic_helpers import ...``) require alembic-aware
    sys.path setup that pytest collection does not guarantee.

    Called lazily from ``_stamp_and_upgrade`` rather than at module load so
    a missing or malformed migration file shows up as the dedicated
    ``test_migration_file_exists`` failure instead of an opaque collection
    error that swallows every other test in the file.
    """
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    rev = re.search(r'^revision:\s*str\s*=\s*"([^"]+)"', body, re.MULTILINE)
    down = re.search(r'^down_revision:\s*str\s*\|[^=]+=\s*"([^"]+)"', body, re.MULTILINE)
    assert rev is not None, f"Could not find revision assignment in {MIGRATION_PATH}"
    assert down is not None, f"Could not find down_revision assignment in {MIGRATION_PATH}"
    return rev.group(1), down.group(1)


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


# Expected column shape on entity.release_identity:
# (data_type, is_nullable). Keys are column names.
# Pins types so a future widening (id INTEGER → BIGINT, discogs_*_id INTEGER →
# BIGINT) trips this test before reaching LML's mint protocol.
RELEASE_IDENTITY_COLUMN_SHAPE: dict[str, tuple[str, str]] = {
    "id": ("integer", "NO"),
    "discogs_release_id": ("integer", "YES"),
    "discogs_master_id": ("integer", "YES"),
    "musicbrainz_release_id": ("text", "YES"),
    "spotify_album_id": ("text", "YES"),
    "apple_music_album_id": ("text", "YES"),
    "bandcamp_album_url": ("text", "YES"),
    "reconciliation_status": ("text", "NO"),
    "created_at": ("timestamp with time zone", "NO"),
    "updated_at": ("timestamp with time zone", "NO"),
}


# Expected column shape on entity.release_reconciliation_log.
# LML's audit-log INSERT binds the (source, external_id, confidence, method)
# tuple positionally; drift in nullability or types corrupts the audit trail.
RECONCILIATION_LOG_COLUMN_SHAPE: dict[str, tuple[str, str]] = {
    "id": ("integer", "NO"),
    "identity_id": ("integer", "NO"),
    "source": ("text", "NO"),
    "external_id": ("text", "NO"),
    "confidence": ("real", "YES"),
    "method": ("text", "NO"),
    "created_at": ("timestamp with time zone", "NO"),
}


# ---------------------------------------------------------------------------
# Static (no DB) checks
# ---------------------------------------------------------------------------


def test_migration_file_exists() -> None:
    """Fast static gate so a missing-file regression doesn't masquerade as a
    pg-skip when the Docker DB isn't available."""
    assert MIGRATION_PATH.exists(), (
        f"0012 migration missing at {MIGRATION_PATH}. The LML release-identity "
        "surface returns 503 in prod until both entity tables exist on the "
        "discogs-cache PG instance."
    )


# ---------------------------------------------------------------------------
# Live-PG assertions
# ---------------------------------------------------------------------------


def _stamp_and_upgrade(run_alembic, db_url: str) -> None:
    """Stamp at 0011 (skipping the prior chain) then upgrade to 0012.

    Stamping avoids dragging the whole 0001→0011 chain into a test that only
    cares about 0012's surface. The migration is self-contained: it does
    not depend on any column added by earlier revisions.
    """
    revision, prior_revision = _read_revision_metadata()
    stamp = run_alembic(["stamp", prior_revision], db_url)
    assert stamp.returncode == 0, (
        f"alembic stamp failed:\nstdout: {stamp.stdout}\nstderr: {stamp.stderr}"
    )
    result = run_alembic(["upgrade", revision], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


def _seed_bootstrap_state(db_url: str) -> None:
    """Pre-create the entity schema + a mock artist-side ``entity.identity``
    table so we exercise the documented prod scenario: existing prod has the
    schema and artist-side tables from out-of-band bootstrap before 0012
    runs. The migration must no-op cleanly against that state.

    The mock ``entity.identity`` table is shaped after LML's canonical
    artist-side SQL — just enough to verify the migration leaves
    pre-existing entity tables alone.
    """
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA entity")
        cur.execute(
            """
            CREATE TABLE entity.identity (
                id SERIAL PRIMARY KEY,
                discogs_artist_id INTEGER UNIQUE,
                musicbrainz_artist_id TEXT UNIQUE,
                reconciliation_status TEXT NOT NULL DEFAULT 'unreconciled',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute("INSERT INTO entity.identity (discogs_artist_id) VALUES (777) RETURNING id")


@pytest.mark.pg
def test_entity_schema_exists(run_alembic, fresh_db_url: str) -> None:
    _stamp_and_upgrade(run_alembic, fresh_db_url)
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
def test_release_identity_columns(run_alembic, fresh_db_url: str) -> None:
    """Pin every column's type, nullability, and (where load-bearing) default.

    Strict equality on the column set: a stray added column trips this test
    instead of slipping through as a silent schema drift.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'entity' AND table_name = 'release_identity'
            ORDER BY ordinal_position
            """
        )
        rows = {
            name: (data_type, is_nullable, column_default)
            for name, data_type, is_nullable, column_default in cur.fetchall()
        }

    assert rows.keys() == RELEASE_IDENTITY_COLUMN_SHAPE.keys(), (
        f"entity.release_identity column-set drift. "
        f"Missing: {RELEASE_IDENTITY_COLUMN_SHAPE.keys() - rows.keys()}; "
        f"Unexpected: {rows.keys() - RELEASE_IDENTITY_COLUMN_SHAPE.keys()}."
    )
    for col, (expected_type, expected_nullable) in RELEASE_IDENTITY_COLUMN_SHAPE.items():
        actual_type, actual_nullable, _ = rows[col]
        assert (actual_type, actual_nullable) == (expected_type, expected_nullable), (
            f"entity.release_identity.{col} drifted: "
            f"got ({actual_type!r}, {actual_nullable!r}), "
            f"expected ({expected_type!r}, {expected_nullable!r}). "
            f"A SERIAL→BIGSERIAL widening or INTEGER↔TEXT swap on any per-source "
            f"identifier breaks LML's mint protocol or the FK alignment with "
            f"entity.release_reconciliation_log.identity_id INTEGER."
        )

    # reconciliation_status DEFAULT 'unreconciled' is load-bearing for LML's
    # state machine — newly-minted rows must carry the canonical sentinel so
    # the reconciler routes them to the right branch. Extract the literal
    # portion of the column_default expression and assert exact equality so
    # a drift to 'unreconciled_v2' or similar (which would still pass a
    # substring check that didn't anchor the closing quote in PG syntax)
    # cannot slip through.
    status_default = rows["reconciliation_status"][2] or ""
    literal_match = re.fullmatch(r"'([^']*)'::text", status_default)
    assert literal_match is not None, (
        f"entity.release_identity.reconciliation_status DEFAULT expression "
        f"is not a bare text literal. Got column_default={status_default!r}; "
        f"expected 'unreconciled'::text. LML's mint INSERT omits this column, "
        f"so the DEFAULT is the only place the seed sentinel is written."
    )
    assert literal_match.group(1) == "unreconciled", (
        f"entity.release_identity.reconciliation_status DEFAULT literal "
        f"drifted: got {literal_match.group(1)!r}, expected 'unreconciled'. "
        f"LML's reconciler routes on the canonical sentinel; any rename "
        f"strands newly-minted rows in the wrong state-machine branch."
    )


@pytest.mark.pg
def test_release_reconciliation_log_columns(run_alembic, fresh_db_url: str) -> None:
    """Pin entity.release_reconciliation_log column shape end-to-end.

    LML's audit-log writer binds (identity_id, source, external_id, confidence,
    method) positionally; any drift in column existence, type, or nullability
    corrupts the audit trail.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'entity' AND table_name = 'release_reconciliation_log'
            ORDER BY ordinal_position
            """
        )
        rows = {name: (data_type, is_nullable) for name, data_type, is_nullable in cur.fetchall()}

    assert rows.keys() == RECONCILIATION_LOG_COLUMN_SHAPE.keys(), (
        f"entity.release_reconciliation_log column-set drift. "
        f"Missing: {RECONCILIATION_LOG_COLUMN_SHAPE.keys() - rows.keys()}; "
        f"Unexpected: {rows.keys() - RECONCILIATION_LOG_COLUMN_SHAPE.keys()}."
    )
    for col, expected in RECONCILIATION_LOG_COLUMN_SHAPE.items():
        assert rows[col] == expected, (
            f"entity.release_reconciliation_log.{col} drifted: got {rows[col]!r}, "
            f"expected {expected!r}."
        )


@pytest.mark.pg
def test_six_per_source_unique_constraints(run_alembic, fresh_db_url: str) -> None:
    """All six per-source UNIQUEs are load-bearing for LML's mint protocol.

    Dropping any one breaks the ON CONFLICT clause in
    ``entity/store.py::mint_or_get_release_identity`` and raises a loud
    500 on the LML write surface.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)
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
def test_reconciliation_log_fk_rules(run_alembic, fresh_db_url: str) -> None:
    """FK has neither ON DELETE CASCADE nor ON UPDATE CASCADE — matches the
    artist-side convention. CASCADE would silently delete the audit log or
    propagate id renumbering through it; consumers expect explicit cleanup.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT rc.delete_rule, rc.update_rule
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
    delete_rule, update_rule = rows[0]
    assert delete_rule == "NO ACTION", (
        f"FK delete_rule must be NO ACTION, got {delete_rule!r}. CASCADE would "
        "silently delete the audit log; consumers expect explicit cleanup."
    )
    assert update_rule == "NO ACTION", (
        f"FK update_rule must be NO ACTION, got {update_rule!r}. CASCADE would "
        "silently propagate id renumbering through the audit log."
    )


@pytest.mark.pg
def test_fk_index_present(run_alembic, fresh_db_url: str) -> None:
    """idx_release_reconciliation_log_identity_id covers WHERE identity_id = $1."""
    _stamp_and_upgrade(run_alembic, fresh_db_url)
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
def test_mint_then_remint_smoke(run_alembic, fresh_db_url: str) -> None:
    """End-to-end mint protocol shape: first INSERT mints, second is a no-op.

    Mirrors ``entity/store.py::mint_or_get_release_identity`` in LML#530:
    ``INSERT ... ON CONFLICT (discogs_release_id) DO NOTHING RETURNING id``.
    The second call must return zero rows so the LML caller falls through
    to the SELECT-for-conflict-loser branch.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)
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
def test_upgrade_against_prod_bootstrap_state(run_alembic, fresh_db_url: str) -> None:
    """Simulate the documented prod scenario before upgrade.

    Per 0012's docstring, prod already has the entity schema + artist-side
    ``entity.identity`` / ``entity.reconciliation_log`` tables from
    out-of-band bootstrap. The upgrade must:

    * succeed without error,
    * create entity.release_identity / entity.release_reconciliation_log,
    * leave the pre-existing entity.identity rows untouched.
    """
    _seed_bootstrap_state(fresh_db_url)
    _stamp_and_upgrade(run_alembic, fresh_db_url)

    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        # Release-side tables landed.
        cur.execute(
            "SELECT to_regclass('entity.release_identity'), "
            "to_regclass('entity.release_reconciliation_log')"
        )
        release_identity, log = cur.fetchone()
        assert release_identity is not None, (
            "0012 upgrade failed to create entity.release_identity against a "
            "prod-bootstrap-shaped DB. The IF NOT EXISTS path should still "
            "create the new release-side tables alongside artist-side ones."
        )
        assert log is not None, "0012 upgrade failed to create entity.release_reconciliation_log."

        # Pre-existing artist-side data survived.
        cur.execute("SELECT discogs_artist_id FROM entity.identity")
        assert cur.fetchall() == [(777,)], (
            "Pre-existing entity.identity data was perturbed by 0012 upgrade. "
            "The artist-side tables (out-of-band bootstrap) must survive."
        )


@pytest.mark.pg
def test_reapply_is_noop(run_alembic, fresh_db_url: str) -> None:
    """Re-running the migration against an already-upgraded DB must not fail.

    Stamps the alembic_version row back to 0011 (no DDL — the actual tables
    are not dropped) and re-runs the upgrade so the IF NOT EXISTS branches
    fire against existing tables. Existing rows must survive.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)

    # Seed a sentinel row so we can prove re-application doesn't perturb data.
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO entity.release_identity (discogs_release_id) VALUES (99999) RETURNING id"
        )
        sentinel_id = cur.fetchone()[0]

    # Rewind alembic state (stamp, not downgrade — tables stay) and re-upgrade
    # so the IF NOT EXISTS branches fire against existing tables.
    revision, prior_revision = _read_revision_metadata()
    rewind = run_alembic(["stamp", prior_revision], fresh_db_url)
    assert rewind.returncode == 0
    reupgrade = run_alembic(["upgrade", revision], fresh_db_url)
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


@pytest.mark.pg
def test_downgrade_drops_release_side_tables(run_alembic, fresh_db_url: str) -> None:
    """Exercise _DOWNGRADE_SQL end-to-end.

    After upgrade → downgrade:

    * ``entity.release_identity`` and ``entity.release_reconciliation_log``
      are dropped (the FK index goes with the child table).
    * The ``entity`` schema is preserved (artist-side tables outlive this
      migration; their adoption is tracked at WXYC/discogs-etl#279).

    This is the only test that runs the actual ``alembic downgrade`` —
    without it, a typo in _DOWNGRADE_SQL would ship green.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)

    _, prior_revision = _read_revision_metadata()
    downgrade = run_alembic(["downgrade", prior_revision], fresh_db_url)
    assert downgrade.returncode == 0, (
        f"alembic downgrade failed:\nstdout: {downgrade.stdout}\nstderr: {downgrade.stderr}"
    )

    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        # Release-side tables and FK index gone.
        cur.execute(
            "SELECT to_regclass('entity.release_identity'), "
            "to_regclass('entity.release_reconciliation_log')"
        )
        release_identity, log = cur.fetchone()
        assert release_identity is None, "entity.release_identity should be dropped"
        assert log is None, "entity.release_reconciliation_log should be dropped"

        cur.execute(
            """
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'entity'
              AND indexname = 'idx_release_reconciliation_log_identity_id'
            """
        )
        assert cur.fetchone() is None, "FK index should be gone with its table"

        # Schema preserved.
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'entity'"
        )
        assert cur.fetchone() is not None, (
            "entity schema should outlive the downgrade — the artist-side "
            "tables (entity.identity / entity.reconciliation_log) still live "
            "there. See migration docstring."
        )


# ---------------------------------------------------------------------------
# Dual-write drift detection (schema/create_database.sql direction)
# ---------------------------------------------------------------------------


CREATE_DATABASE_SQL_PATH = REPO_ROOT / "schema" / "create_database.sql"


def _shape_from_db(conn, table_name: str) -> dict[str, tuple[str, str]]:
    """Return ``{column_name: (data_type, is_nullable)}`` for ``entity.<table>``."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'entity' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return {name: (data_type, is_nullable) for name, data_type, is_nullable in cur.fetchall()}


@pytest.mark.pg
def test_create_database_sql_matches_migration_shape(fresh_db_url: str) -> None:
    """Apply schema/create_database.sql against a fresh DB and assert the
    same entity-table shape the migration produces.

    Catches the OTHER direction of dual-write drift: the migration tests
    already pin the alembic path; this test pins the create_database.sql
    path. If a future PR edits one but not the other, this test fails
    instead of letting the divergence ship to dev DBs built by
    ``--fresh-rebuild``.
    """
    sql_body = CREATE_DATABASE_SQL_PATH.read_text(encoding="utf-8")
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql_body)

    with psycopg.connect(fresh_db_url) as conn:
        identity_shape = _shape_from_db(conn, "release_identity")
        log_shape = _shape_from_db(conn, "release_reconciliation_log")

    assert identity_shape == RELEASE_IDENTITY_COLUMN_SHAPE, (
        "schema/create_database.sql's entity.release_identity diverged from "
        "the migration's shape. Dual-write convention requires both produce "
        "the same end state.\n"
        f"Got: {identity_shape}\n"
        f"Expected: {RELEASE_IDENTITY_COLUMN_SHAPE}"
    )
    assert log_shape == RECONCILIATION_LOG_COLUMN_SHAPE, (
        "schema/create_database.sql's entity.release_reconciliation_log "
        "diverged from the migration's shape. Dual-write convention requires "
        "both produce the same end state.\n"
        f"Got: {log_shape}\n"
        f"Expected: {RECONCILIATION_LOG_COLUMN_SHAPE}"
    )
