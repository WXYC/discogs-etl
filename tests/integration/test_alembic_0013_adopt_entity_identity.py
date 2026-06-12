"""Verify 0013 brings the artist-side entity tables under the alembic chain.

``entity.identity`` and ``entity.reconciliation_log`` exist on prod
discogs-cache via out-of-band bootstrap (no revision in this repo created
them). 0012 already adopted the release-side counterparts; 0013 closes the
asymmetry on the artist side. LML treats the artist-side tables as
read-only — ``identity/dependencies.py`` issues a
``SELECT 1 FROM entity.identity LIMIT 0`` probe that flips the routes to
503 when the table is missing, so on a fresh dev DB (no out-of-band
bootstrap) LML would otherwise return 503 until the alembic chain
catches up.

This adoption migration:

* Creates ``entity.identity`` mirroring ``wxyc-etl/src/schema/entity.rs::
  ENTITY_IDENTITY_DDL`` — the canonical mirror of the prod artist-side
  shape — including the load-bearing ``library_name TEXT UNIQUE``
  constraint that LML's resolve protocol binds against.
* Creates ``entity.reconciliation_log`` mirroring
  ``RECONCILIATION_LOG_DDL`` with the FK to ``entity.identity(id)``
  (``NO ACTION`` delete/update, matching the release-side convention from
  0012).
* Creates ``idx_entity_identity_status`` on ``reconciliation_status`` —
  present in the LML integration test fixture but unverified in prod
  before this revision lands; the pre-flight ``pg_indexes`` probe in the
  PR body records actual prod state.
* Creates ``idx_entity_reconciliation_log_identity_id`` on the FK column
  — Postgres does not auto-index FK referencing columns; every
  ``WHERE identity_id = $1`` lookup would otherwise seq-scan.

All DDL uses ``CREATE TABLE IF NOT EXISTS`` / ``CREATE INDEX IF NOT
EXISTS`` so re-application against existing prod tables is a no-op (the
adoption case) AND the fresh-dev case lands the schema from scratch.

Downgrade is intentionally a no-op. This revision adopts existing tables
into alembic ownership; downgrading is "alembic forgets about them," not
"remove them." Dropping the indexes alone would mean a perf cliff on
downgrade for tables that physically remain — and the revision can't
distinguish indexes it created from inherited ones, so
``DROP INDEX IF EXISTS`` would unconditionally drop regardless of origin.

Tracked at WXYC/discogs-etl#279.
"""

from __future__ import annotations

import re
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0013_adopt_entity_identity.py"


def _read_revision_metadata() -> tuple[str, str]:
    """Return (revision, down_revision) parsed from 0013's source file.

    Source-of-truth lookup keeps the test from hardcoding revision strings
    that might be shortened later (0009 trimmed its revision id to fit
    alembic_version varchar(32) in commit c228b47); if the chain is
    renamed, this test tracks automatically.

    Regex over the file rather than importlib because the migration's
    imports (``from lib.alembic_helpers import ...``) require alembic-aware
    sys.path setup that pytest collection does not guarantee.
    """
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    rev = re.search(r'^revision:\s*str\s*=\s*"([^"]+)"', body, re.MULTILINE)
    down = re.search(r'^down_revision:\s*str\s*\|[^=]+=\s*"([^"]+)"', body, re.MULTILINE)
    assert rev is not None, f"Could not find revision assignment in {MIGRATION_PATH}"
    assert down is not None, f"Could not find down_revision assignment in {MIGRATION_PATH}"
    return rev.group(1), down.group(1)


# Expected column shape on entity.identity, mirroring
# wxyc-etl/src/schema/entity.rs::ENTITY_IDENTITY_DDL.
# (data_type, is_nullable). Pinning types so a future widening
# (e.g. SERIAL→BIGSERIAL on id) trips this test before reaching LML.
IDENTITY_COLUMN_SHAPE: dict[str, tuple[str, str]] = {
    "id": ("integer", "NO"),
    "library_name": ("text", "NO"),
    "discogs_artist_id": ("integer", "YES"),
    "wikidata_qid": ("text", "YES"),
    "musicbrainz_artist_id": ("text", "YES"),
    "spotify_artist_id": ("text", "YES"),
    "apple_music_artist_id": ("text", "YES"),
    "bandcamp_id": ("text", "YES"),
    "reconciliation_status": ("text", "NO"),
    "created_at": ("timestamp with time zone", "NO"),
    "updated_at": ("timestamp with time zone", "NO"),
}


# Expected column shape on entity.reconciliation_log.
# LML's artist-side audit-log writer binds positionally; nullability or type
# drift corrupts the audit trail.
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
        f"0013 migration missing at {MIGRATION_PATH}. Fresh dev DBs would "
        "fall back to a 503 on LML's artist-side identity routes until the "
        "tables are created out-of-band."
    )


# ---------------------------------------------------------------------------
# Live-PG assertions
# ---------------------------------------------------------------------------


def _stamp_and_upgrade(run_alembic, db_url: str) -> None:
    """Stamp at the prior revision then upgrade to 0013.

    Stamping avoids dragging the whole 0001→prior chain into a test that
    only cares about 0013's surface. 0013 is self-contained: it does not
    depend on any column added by earlier revisions.
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


# Prod-bootstrap simulation: DDL approximating the prod shape of the
# artist-side entity tables before 0013 runs. Mirrors the wxyc-etl Rust
# constants — the canonical reference for what's actually in prod. A
# pre-existing row is seeded so we can assert the upgrade preserves data.
_PROD_BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS entity;

CREATE TABLE IF NOT EXISTS entity.identity (
    id SERIAL PRIMARY KEY,
    library_name TEXT NOT NULL UNIQUE,
    discogs_artist_id INTEGER,
    wikidata_qid TEXT,
    musicbrainz_artist_id TEXT,
    spotify_artist_id TEXT,
    apple_music_artist_id TEXT,
    bandcamp_id TEXT,
    reconciliation_status TEXT NOT NULL DEFAULT 'unreconciled',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entity.reconciliation_log (
    id SERIAL PRIMARY KEY,
    identity_id INTEGER NOT NULL REFERENCES entity.identity(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    method TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _seed_prod_bootstrap_state(db_url: str) -> int:
    """Pre-create the prod-shaped entity tables + seed a sentinel row.

    Returns the seeded identity row id so callers can assert it survives.

    Deliberately omits the two indexes 0013 adds (``idx_entity_identity_status``
    and ``idx_entity_reconciliation_log_identity_id``) — the issue documents
    their prod presence as unverified. Simulating the worst case (indexes
    absent) exercises the IF NOT EXISTS CREATE INDEX path against a
    pre-existing populated table.
    """
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_PROD_BOOTSTRAP_SQL)
        cur.execute(
            """
            INSERT INTO entity.identity (library_name, discogs_artist_id, reconciliation_status)
            VALUES ('Stereolab', 5432, 'reconciled')
            RETURNING id
            """
        )
        return cur.fetchone()[0]


@pytest.mark.pg
def test_entity_schema_exists(run_alembic, fresh_db_url: str) -> None:
    """Fresh-dev case: empty DB, upgrade creates the entity schema."""
    _stamp_and_upgrade(run_alembic, fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
            ("entity",),
        )
        assert cur.fetchone() is not None, (
            "entity schema missing after 0013 upgrade. The wxyc-etl Rust "
            "constants assume the schema is already present; 0013 must add "
            "a CREATE SCHEMA IF NOT EXISTS guard for fresh dev DBs that "
            "haven't been through #278's 0012 already."
        )


@pytest.mark.pg
def test_identity_columns(run_alembic, fresh_db_url: str) -> None:
    """Pin every column's type, nullability, and (where load-bearing) default.

    Strict equality on the column set: a stray added column trips this test
    instead of slipping through as silent schema drift, which would leave
    the prod adoption case and fresh-dev case writing different shapes.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'entity' AND table_name = 'identity'
            ORDER BY ordinal_position
            """
        )
        rows = {
            name: (data_type, is_nullable, column_default)
            for name, data_type, is_nullable, column_default in cur.fetchall()
        }

    assert rows.keys() == IDENTITY_COLUMN_SHAPE.keys(), (
        f"entity.identity column-set drift vs wxyc-etl/src/schema/entity.rs::"
        f"ENTITY_IDENTITY_DDL. Missing: {IDENTITY_COLUMN_SHAPE.keys() - rows.keys()}; "
        f"Unexpected: {rows.keys() - IDENTITY_COLUMN_SHAPE.keys()}."
    )
    for col, (expected_type, expected_nullable) in IDENTITY_COLUMN_SHAPE.items():
        actual_type, actual_nullable, _ = rows[col]
        assert (actual_type, actual_nullable) == (expected_type, expected_nullable), (
            f"entity.identity.{col} drifted: "
            f"got ({actual_type!r}, {actual_nullable!r}), "
            f"expected ({expected_type!r}, {expected_nullable!r})."
        )

    # reconciliation_status DEFAULT 'unreconciled' is load-bearing for LML's
    # state machine — newly-minted artist rows must carry the canonical
    # sentinel so the reconciler routes them to the right branch.
    status_default = rows["reconciliation_status"][2] or ""
    literal_match = re.fullmatch(r"'([^']*)'::text", status_default)
    assert literal_match is not None, (
        f"entity.identity.reconciliation_status DEFAULT expression is not a "
        f"bare text literal. Got column_default={status_default!r}; expected "
        f"'unreconciled'::text."
    )
    assert literal_match.group(1) == "unreconciled", (
        f"entity.identity.reconciliation_status DEFAULT literal drifted: "
        f"got {literal_match.group(1)!r}, expected 'unreconciled'."
    )


@pytest.mark.pg
def test_reconciliation_log_columns(run_alembic, fresh_db_url: str) -> None:
    """Pin entity.reconciliation_log column shape end-to-end."""
    _stamp_and_upgrade(run_alembic, fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'entity' AND table_name = 'reconciliation_log'
            ORDER BY ordinal_position
            """
        )
        rows = {name: (data_type, is_nullable) for name, data_type, is_nullable in cur.fetchall()}

    assert rows.keys() == RECONCILIATION_LOG_COLUMN_SHAPE.keys(), (
        f"entity.reconciliation_log column-set drift. "
        f"Missing: {RECONCILIATION_LOG_COLUMN_SHAPE.keys() - rows.keys()}; "
        f"Unexpected: {rows.keys() - RECONCILIATION_LOG_COLUMN_SHAPE.keys()}."
    )
    for col, expected in RECONCILIATION_LOG_COLUMN_SHAPE.items():
        assert rows[col] == expected, (
            f"entity.reconciliation_log.{col} drifted: got {rows[col]!r}, expected {expected!r}."
        )


@pytest.mark.pg
def test_library_name_unique(run_alembic, fresh_db_url: str) -> None:
    """``library_name UNIQUE`` is load-bearing — LML's resolve path keys off it.

    Asserted against ``pg_constraint`` rather than ``information_schema``
    because the latter loses the column-list mapping behind a join we'd
    have to rebuild here.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE n.nspname = 'entity'
              AND t.relname = 'identity'
              AND c.contype = 'u'
              AND cardinality(c.conkey) = 1
            ORDER BY a.attname
            """
        )
        unique_columns = {row[0] for row in cur.fetchall()}

    assert "library_name" in unique_columns, (
        f"library_name UNIQUE missing on entity.identity. Got {unique_columns}. "
        f"LML's artist-side resolve keys off this constraint; dropping it "
        f"would let duplicate library_name rows accumulate silently."
    )


@pytest.mark.pg
def test_reconciliation_log_fk_rules(run_alembic, fresh_db_url: str) -> None:
    """FK has neither ON DELETE CASCADE nor ON UPDATE CASCADE — matches the
    release-side convention (0012). CASCADE would silently delete the
    audit log or propagate id renumbering through it.
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
              AND tc.table_name = 'reconciliation_log'
              AND tc.constraint_type = 'FOREIGN KEY'
            """
        )
        rows = cur.fetchall()
    assert len(rows) == 1, f"Expected exactly one FK on entity.reconciliation_log, got {len(rows)}."
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
def test_indexes_present(run_alembic, fresh_db_url: str) -> None:
    """Both 0013-managed indexes land on a fresh dev DB.

    * ``idx_entity_identity_status`` powers LML's reconciler dashboard
      scans (``WHERE reconciliation_status = ?``).
    * ``idx_entity_reconciliation_log_identity_id`` covers
      ``WHERE identity_id = $1`` — Postgres does not auto-index FK
      referencing columns.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)
    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = 'entity'
              AND indexname IN (
                'idx_entity_identity_status',
                'idx_entity_reconciliation_log_identity_id'
              )
            """
        )
        found = {row[0] for row in cur.fetchall()}
    assert found == {
        "idx_entity_identity_status",
        "idx_entity_reconciliation_log_identity_id",
    }, f"Index drift: got {found}."


@pytest.mark.pg
def test_upgrade_against_prod_bootstrap_state(run_alembic, fresh_db_url: str) -> None:
    """Adoption case: tables already exist in the prod shape; upgrade no-ops.

    Pre-existing rows must survive — the prod tables hold LML's
    source-of-truth reconciliation records and an INSERT/DDL that
    perturbed them would destroy real reconciliation state.
    """
    sentinel_id = _seed_prod_bootstrap_state(fresh_db_url)
    _stamp_and_upgrade(run_alembic, fresh_db_url)

    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        # Pre-existing artist-side row survived.
        cur.execute(
            "SELECT library_name, discogs_artist_id, reconciliation_status "
            "FROM entity.identity WHERE id = %s",
            (sentinel_id,),
        )
        row = cur.fetchone()
        assert row == ("Stereolab", 5432, "reconciled"), (
            f"Pre-existing entity.identity row was perturbed by 0013 upgrade. "
            f"Got {row!r}. The prod tables hold LML's source-of-truth "
            f"reconciliation records — the upgrade must be a strict no-op "
            f"against the documented prod shape."
        )

        # Both indexes landed (the bootstrap omits them — see docstring).
        cur.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = 'entity'
              AND indexname IN (
                'idx_entity_identity_status',
                'idx_entity_reconciliation_log_identity_id'
              )
            """
        )
        found = {row[0] for row in cur.fetchall()}
        assert found == {
            "idx_entity_identity_status",
            "idx_entity_reconciliation_log_identity_id",
        }, (
            f"Index drift after prod-bootstrap adoption: got {found}. The "
            f"migration must idempotently land both indexes regardless of "
            f"whether the prod DB already had them."
        )


@pytest.mark.pg
def test_reapply_is_noop(run_alembic, fresh_db_url: str) -> None:
    """Re-running 0013 against an already-upgraded DB must not fail.

    Stamps the alembic_version row back to the prior revision (no DDL —
    the tables physically remain) and re-runs upgrade so the IF NOT
    EXISTS branches fire against existing tables. Existing rows survive.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)

    # Seed a sentinel row so we can prove re-application doesn't perturb data.
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO entity.identity (library_name) VALUES ('Juana Molina') RETURNING id"
        )
        sentinel_id = cur.fetchone()[0]

    revision, prior_revision = _read_revision_metadata()
    rewind = run_alembic(["stamp", prior_revision], fresh_db_url)
    assert rewind.returncode == 0
    reupgrade = run_alembic(["upgrade", revision], fresh_db_url)
    assert reupgrade.returncode == 0, (
        f"Re-application failed:\nstdout: {reupgrade.stdout}\nstderr: {reupgrade.stderr}"
    )

    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT library_name FROM entity.identity WHERE id = %s",
            (sentinel_id,),
        )
        assert cur.fetchone() == ("Juana Molina",), (
            "Sentinel row lost across re-application. The migration must use "
            "CREATE TABLE IF NOT EXISTS so existing rows survive."
        )


@pytest.mark.pg
def test_downgrade_is_noop(run_alembic, fresh_db_url: str) -> None:
    """Downgrade preserves the tables, indexes, and rows.

    0013 is an adoption migration: downgrading is "alembic forgets about
    them," not "remove them." Without this assertion a future PR could
    introduce a stray DROP statement that quietly destroys LML's
    source-of-truth reconciliation state on a routine downgrade.
    """
    _stamp_and_upgrade(run_alembic, fresh_db_url)

    # Seed a sentinel so we can prove the downgrade does not touch data.
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO entity.identity (library_name) VALUES ('Jessica Pratt') RETURNING id"
        )
        sentinel_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO entity.reconciliation_log "
            "(identity_id, source, external_id, method) "
            "VALUES (%s, 'musicbrainz', 'mb-xyz', 'manual')",
            (sentinel_id,),
        )

    _, prior_revision = _read_revision_metadata()
    downgrade = run_alembic(["downgrade", prior_revision], fresh_db_url)
    assert downgrade.returncode == 0, (
        f"alembic downgrade failed:\nstdout: {downgrade.stdout}\nstderr: {downgrade.stderr}"
    )

    with psycopg.connect(fresh_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('entity.identity'), to_regclass('entity.reconciliation_log')"
        )
        identity_reg, log_reg = cur.fetchone()
        assert identity_reg is not None, (
            "entity.identity disappeared on downgrade. 0013 is an adoption "
            "migration — downgrade must be a no-op."
        )
        assert log_reg is not None, "entity.reconciliation_log disappeared on downgrade."

        cur.execute(
            "SELECT library_name FROM entity.identity WHERE id = %s",
            (sentinel_id,),
        )
        assert cur.fetchone() == ("Jessica Pratt",), (
            "Sentinel row lost on downgrade. The migration must not touch "
            "rows — they hold LML's source-of-truth reconciliation state."
        )

        cur.execute(
            "SELECT external_id FROM entity.reconciliation_log WHERE identity_id = %s",
            (sentinel_id,),
        )
        assert cur.fetchone() == ("mb-xyz",), "Reconciliation log row lost on downgrade."

        # Indexes also preserved — dropping them on downgrade would mean
        # re-introducing a perf cliff against tables that physically remain.
        cur.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = 'entity'
              AND indexname IN (
                'idx_entity_identity_status',
                'idx_entity_reconciliation_log_identity_id'
              )
            """
        )
        found = {row[0] for row in cur.fetchall()}
        assert found == {
            "idx_entity_identity_status",
            "idx_entity_reconciliation_log_identity_id",
        }, (
            f"Indexes dropped on downgrade: got {found}. Asymmetric drop "
            f"(indexes go, tables stay) is intentionally avoided — see the "
            f"migration docstring."
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
    same artist-side entity-table shape the migration produces.

    Catches the OTHER direction of dual-write drift: the migration tests
    above pin the alembic path; this test pins the create_database.sql
    path. Without this guard, a future PR that edited the migration but
    not the SQL (or vice versa) would silently let ``--fresh-rebuild`` dev
    DBs and alembic-upgrade dev DBs diverge on the artist-side schema.
    """
    sql_body = CREATE_DATABASE_SQL_PATH.read_text(encoding="utf-8")
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql_body)

    with psycopg.connect(fresh_db_url) as conn:
        identity_shape = _shape_from_db(conn, "identity")
        log_shape = _shape_from_db(conn, "reconciliation_log")

    assert identity_shape == IDENTITY_COLUMN_SHAPE, (
        "schema/create_database.sql's entity.identity diverged from the "
        "migration's shape. Dual-write convention requires both produce the "
        "same end state.\n"
        f"Got: {identity_shape}\n"
        f"Expected: {IDENTITY_COLUMN_SHAPE}"
    )
    assert log_shape == RECONCILIATION_LOG_COLUMN_SHAPE, (
        "schema/create_database.sql's entity.reconciliation_log diverged "
        "from the migration's shape. Dual-write convention requires both "
        "produce the same end state.\n"
        f"Got: {log_shape}\n"
        f"Expected: {RECONCILIATION_LOG_COLUMN_SHAPE}"
    )
