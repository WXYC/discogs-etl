"""entity.release_identity / entity.release_reconciliation_log: release-side
identity layer for LML#526.

Brings the release-side counterpart of the existing artist-side
``entity.identity`` surface under the alembic chain. After this lands,
LML's ``POST /api/v1/identity/resolve`` flips from 503 (probe miss in
``identity/dependencies.py``) to 200.

DDL is mirrored from LML's canonical ``entity/release_identity.sql``
(WXYC/library-metadata-lookup#530), with one addition: ``CREATE SCHEMA
IF NOT EXISTS entity`` precedes the table DDL because the canonical file
assumes the schema is already present. Existing prod has the schema from
out-of-band bootstrap; fresh dev DBs do not.

The columns ``discogs_release_id`` and ``discogs_master_id`` are
``INTEGER`` but **not** FKs to the cache's ``release.id`` / ``master.id``.
They are external identifiers — the identity layer's lifecycle is
independent of the monthly cache rebuild, and the FKs would couple the
two.

Per-source UNIQUE columns are load-bearing for LML's mint protocol
concurrency safety. ``entity/store.py::mint_or_get_release_identity``
issues ``INSERT ... ON CONFLICT ({col}) DO NOTHING RETURNING id`` per
source; dropping any UNIQUE raises a loud 500 (``RuntimeError("UNIQUE
constraint appears broken")``) rather than silently double-minting.

``updated_at`` currently always equals ``created_at`` — the v1 mint
protocol never updates an existing row, only conflict-fallthrough-SELECTs
it. The column exists for forward compatibility with LML#207's
cross-source joiner and the upcoming reconciliation_status writer; no
trigger needed in v1.

The downgrade drops in FK order — child table (release_reconciliation_log)
first, then parent (release_identity). The DROP TABLE on the child also
drops its FK index, so no explicit DROP INDEX is needed. The ``entity``
schema is intentionally left in place: the artist-side tables
(``entity.identity`` / ``entity.reconciliation_log``) still live there.
Adopting those into the alembic chain is tracked at
WXYC/discogs-etl#279.

Defensive pattern: same ``is_offline_mode()`` refuse + autocommit
``psycopg.connect`` side-channel as 0010 / 0011 so ``alembic upgrade
--sql`` (offline mode) fails fast rather than silently emitting no-op
SQL.

Revision ID: 0012_entity_release_identity
Revises: 0011_artist_not_found
Create Date: 2026-06-10

"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import psycopg

from lib.alembic_helpers import refuse_offline, resolve_db_url

revision: str = "0012_entity_release_identity"
down_revision: str | Sequence[str] | None = "0011_artist_not_found"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_UPGRADE_SQL = """
CREATE SCHEMA IF NOT EXISTS entity;

CREATE TABLE IF NOT EXISTS entity.release_identity (
    id SERIAL PRIMARY KEY,
    discogs_release_id INTEGER UNIQUE,
    discogs_master_id INTEGER UNIQUE,
    musicbrainz_release_id TEXT UNIQUE,
    spotify_album_id TEXT UNIQUE,
    apple_music_album_id TEXT UNIQUE,
    bandcamp_album_url TEXT UNIQUE,
    reconciliation_status TEXT NOT NULL DEFAULT 'unreconciled',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entity.release_reconciliation_log (
    id SERIAL PRIMARY KEY,
    identity_id INTEGER NOT NULL REFERENCES entity.release_identity(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    method TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_release_reconciliation_log_identity_id
    ON entity.release_reconciliation_log(identity_id);
"""

# FK order: child table first, then parent. The DROP TABLE on the child also
# drops its FK index, so no explicit DROP INDEX is needed. Schema is left in
# place because the artist-side entity.identity / entity.reconciliation_log
# tables still live there (out-of-band bootstrap; see #279 for adoption).
_DOWNGRADE_SQL = """
DROP TABLE IF EXISTS entity.release_reconciliation_log;
DROP TABLE IF EXISTS entity.release_identity;
"""


def upgrade() -> None:
    refuse_offline(revision, "upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn, conn.cursor() as cur:
        log.info("0012: CREATE SCHEMA + entity.release_identity + reconciliation_log + FK index")
        cur.execute(_UPGRADE_SQL)


def downgrade() -> None:
    refuse_offline(revision, "downgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn, conn.cursor() as cur:
        log.info("0012: DROP entity.release_reconciliation_log + entity.release_identity")
        cur.execute(_DOWNGRADE_SQL)
