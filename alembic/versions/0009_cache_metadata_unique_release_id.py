"""cache_metadata: dedupe and add UNIQUE(release_id) for populate_cache_metadata's ON CONFLICT

Background
----------

``scripts/import_csv.py``'s ``populate_cache_metadata`` uses
``INSERT ... SELECT ... ON CONFLICT (release_id) DO NOTHING`` to tolerate
concurrent ``api_fetch`` writes from LML's runtime (#188). Postgres requires
a UNIQUE or PRIMARY KEY constraint matching the ON CONFLICT specification.

``schema/create_database.sql`` declares ``release_id integer PRIMARY KEY`` on
``cache_metadata``, so a fresh ground-up rebuild has the constraint. But
Railway's existing ``cache_metadata`` table predates that schema definition
(alembic 0001's schema-presence guard only references it, doesn't create
it) and **does not have the PK**. The 2026-06-07 #267 manual jumpstart
run 3 (i-0cb8b5600672eecd5) hit this — the load phase crashed with::

    psycopg.errors.InvalidColumnReference:
      there is no unique or exclusion constraint matching the ON CONFLICT
      specification (import_csv.py:524)

This migration fixes the drift idempotently:

1. **Dedupe** any pre-existing duplicates, keeping the ``api_fetch`` row
   when both exist. The populate_cache_metadata docstring guarantees that
   ``api_fetch`` rows (LML-cached API responses) are preserved across
   rebuilds — the dedupe respects that. For same-source duplicates one row
   is kept arbitrarily by ``ctid``. In practice Railway is unlikely to have
   any duplicates (cache_metadata is TRUNCATEd at the start of each
   rebuild), but the dedupe is a precondition for the constraint and is
   safe to run when there's nothing to dedupe.
2. **Add UNIQUE(release_id)** only when no PRIMARY KEY or UNIQUE constraint
   already covers ``release_id`` alone. No-op against fresh DBs that ran
   create_database.sql (PK already present); real work on Railway.

Downgrade drops the UNIQUE constraint by name (idempotent — does nothing
if not present). It does NOT restore duplicates; that would require
tracking what was dropped, which isn't reversible in any useful sense.

Dual-write convention
---------------------

``schema/create_database.sql`` already declares the PK on cache_metadata,
so a fresh-rebuild path produces the same end-state without needing a
matching change here. The dual-write convention is already satisfied.

Companion tickets
-----------------

- WXYC/discogs-etl#273 — this migration.
- WXYC/discogs-etl#188 — original LML race that motivated the ON CONFLICT.
- WXYC/discogs-etl#207 — open: "Add PG integration test for
  populate_cache_metadata's ON CONFLICT race-tolerance pattern" — the
  missing test this migration's tests now cover.
- WXYC/discogs-etl#267 — manual jumpstart that surfaced the drift.
- WXYC/discogs-etl#269 (closed) — bash-trap fix; without it run 3's
  failure would have been silent.
- WXYC/discogs-etl#271 (closed) — TMPDIR redirect that unblocked reaching
  the load phase where this crashed.

Revision ID: 0009_cache_metadata_unique_release_id
Revises: 0008_release_artwork_checked_at
Create Date: 2026-06-07

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

import psycopg

from alembic import context

revision: str = "0009_cache_metadata_unique_release_id"
down_revision: str | Sequence[str] | None = "0008_release_artwork_checked_at"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_UPGRADE_SQL = """
-- 1. Dedupe duplicates, preferring the api_fetch row (cached LML API data).
--
-- For each pair (a, b) of rows with the same release_id and a.ctid < b.ctid:
--   delete `a` when `a` is not api_fetch OR `b` is api_fetch.
-- This drops the non-api_fetch row in mixed pairs and the older ctid in
-- same-source pairs, leaving exactly one row per release_id with api_fetch
-- preserved wherever it existed.
DELETE FROM cache_metadata a USING cache_metadata b
WHERE a.ctid < b.ctid
  AND a.release_id = b.release_id
  AND (a.source != 'api_fetch' OR b.source = 'api_fetch');

-- 2. Add UNIQUE(release_id) only if no PRIMARY KEY or UNIQUE already covers
-- release_id alone. No-op on fresh DBs (create_database.sql declared the
-- PK); adds the constraint on Railway-style drifted DBs.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        WHERE c.conrelid = 'cache_metadata'::regclass
          AND c.contype IN ('p', 'u')
          AND c.conkey = ARRAY[(
              SELECT a.attnum FROM pg_attribute a
              WHERE a.attrelid = 'cache_metadata'::regclass
                AND a.attname = 'release_id'
          )]::smallint[]
    ) THEN
        ALTER TABLE cache_metadata
            ADD CONSTRAINT cache_metadata_release_id_key UNIQUE (release_id);
    END IF;
END $$;
"""

_DOWNGRADE_SQL = """
ALTER TABLE cache_metadata
    DROP CONSTRAINT IF EXISTS cache_metadata_release_id_key;
"""


def _resolve_db_url() -> str:
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply "
            "0009_cache_metadata_unique_release_id."
        )
    return db_url


def _refuse_offline(direction: str) -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            f"0009_cache_metadata_unique_release_id does not support --sql / "
            f"offline mode ({direction}): the migration opens its own "
            "autocommit psycopg connection to apply DDL + DML, mirroring 0008. "
            "Run `alembic upgrade head` (or `downgrade`) against a live DB instead."
        )


def upgrade() -> None:
    _refuse_offline("upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        log.info(
            "0009: dedupe cache_metadata (api_fetch-wins) + add UNIQUE(release_id) "
            "if not already covered"
        )
        cur.execute(_UPGRADE_SQL)


def downgrade() -> None:
    _refuse_offline("downgrade")

    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_DOWNGRADE_SQL)
