"""entity.identity / entity.reconciliation_log: adopt the artist-side
LML identity layer into the alembic chain.

These tables already exist on the prod discogs-cache PostgreSQL instance
via out-of-band bootstrap (no revision in this repo created them). 0012
adopted the release-side counterparts (``entity.release_identity`` /
``entity.release_reconciliation_log``); this revision closes the
asymmetry on the artist side so:

* A future cold rebuild of the cache schema picks up the artist-side
  tables automatically rather than needing a manual ``psql`` step.
* The canonical DDL lives in the repo. Before this revision, the closest
  records were the e2e test fixture (documentation, not enforcement) and
  the wxyc-etl Rust constants in a different repo.
* Future tweaks to the artist-side schema can live in subsequent alembic
  revisions instead of out-of-band patches.

DDL is mirrored from ``wxyc-etl/src/schema/entity.rs`` —
``ENTITY_IDENTITY_DDL`` and ``RECONCILIATION_LOG_DDL`` constants — which
is the canonical mirror of the prod artist-side shape. Embedded by copy,
not by import: the revision's DDL must survive future wxyc-etl bumps
without silently changing.

``CREATE SCHEMA IF NOT EXISTS entity`` precedes the table DDL because
the Rust constants assume the schema is already present. On existing
prod, 0012 (or earlier out-of-band bootstrap) will have already created
the schema; on fresh dev DBs without 0012 having run yet, this guard
makes 0013 safe to apply standalone.

All DDL uses ``IF NOT EXISTS`` so re-application against existing prod
tables is a no-op (the adoption case) AND a fresh dev DB lands the schema
from scratch. Existing rows are never touched — the prod tables hold
LML's source-of-truth reconciliation records, and an unchecked CREATE
TABLE would crash the apply rather than corrupt them, but the guard is
defense in depth.

Downgrade is intentionally a **no-op**. This revision adopts existing
tables into alembic ownership; downgrading is "alembic forgets about
them," not "remove them." Two reasons:

1. Dropping the indexes alone would mean re-introducing a perf cliff on
   downgrade for tables that physically remain — confusing asymmetry
   without a benefit.
2. ``DROP INDEX IF EXISTS`` cannot distinguish indexes this revision
   created from inherited ones, so it would unconditionally drop
   regardless of origin — punishing prod for adopting tables that
   already had the indexes via out-of-band bootstrap.

Sequencing after ``0012_entity_release_identity`` is hygiene (keeps the
entity-schema adoption arc co-located in the chain), not correctness.
The DDL is genuinely independent of 0012; a future hot-fix renumber
would not break anything.

Defensive pattern: same ``is_offline_mode()`` refuse + autocommit
``psycopg.connect`` side-channel as 0010 / 0011 / 0012 so
``alembic upgrade --sql`` (offline mode) fails fast rather than silently
emitting no-op SQL while the migration's side-channel connection never
runs.

Operational note: ``CREATE INDEX`` without ``CONCURRENTLY`` takes a
``SHARE`` lock that blocks writes (not reads — fine for LML's read-heavy
artist-side traffic). If the indexes don't already exist in prod, the
build window is bounded by table size: the artist-side ``entity.identity``
is one row per WXYC library artist (thousands, not millions), so
sub-second in practice. The pre-flight ``pg_indexes`` probe in the PR
body records whether the indexes already exist; if they do, this
revision is a known no-op.

Tracked at WXYC/discogs-etl#279.

Revision ID: 0013_adopt_entity_identity
Revises: 0012_entity_release_identity
Create Date: 2026-06-11

"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import psycopg

from lib.alembic_helpers import refuse_offline, resolve_db_url

revision: str = "0013_adopt_entity_identity"
down_revision: str | Sequence[str] | None = "0012_entity_release_identity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Mirrors wxyc-etl/src/schema/entity.rs::ENTITY_IDENTITY_DDL +
# RECONCILIATION_LOG_DDL with IF NOT EXISTS guards and an explicit
# CREATE SCHEMA prefix. Embedded by copy: the revision must not silently
# change when wxyc-etl bumps.
_UPGRADE_SQL = """
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

CREATE INDEX IF NOT EXISTS idx_entity_identity_status
    ON entity.identity(reconciliation_status);

CREATE TABLE IF NOT EXISTS entity.reconciliation_log (
    id SERIAL PRIMARY KEY,
    identity_id INTEGER NOT NULL REFERENCES entity.identity(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    method TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_entity_reconciliation_log_identity_id
    ON entity.reconciliation_log(identity_id);
"""


def upgrade() -> None:
    refuse_offline(revision, "upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn, conn.cursor() as cur:
        log.info(
            "0013: adopt entity.identity + entity.reconciliation_log + indexes "
            "(IF NOT EXISTS — no-op against prod adoption case)"
        )
        cur.execute(_UPGRADE_SQL)


def downgrade() -> None:
    """No-op. Adoption migrations don't drop the tables they adopt.

    Still refuses offline mode so ``alembic downgrade --sql`` doesn't
    pretend to emit useful SQL — it would emit nothing, which an
    operator could misread as "downgrade complete."
    """
    refuse_offline(revision, "downgrade")
    log = logging.getLogger("alembic.runtime.migration")
    log.info(
        "0013: downgrade is a no-op — adoption migration. Tables, indexes, "
        "and rows survive. See migration docstring for rationale."
    )
