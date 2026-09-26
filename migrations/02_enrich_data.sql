-- Enrich Discogs cache: restore dropped columns, add artist detail tables
-- Run once against the existing database (post-migration 01).
-- Idempotent: safe to re-run.
--
-- The four artist child tables declare UNIQUE (artist_id, <key>) inline,
-- mirroring schema/create_database.sql and alembic 0017 (WXYC/discogs-etl#433).
-- The plain idx_artist_*_artist_id indexes this file used to create are gone:
-- each UNIQUE leads with artist_id and is therefore a strict superset, so
-- re-creating them would put back ~38 MB of dead weight that 0017 exists to
-- drop. The UNIQUE itself is NOT optional here -- scripts/import_csv.py loads
-- these tables with ON CONFLICT (artist_id, <key>), which raises 42P10
-- against a table that lacks the matching constraint.
--
-- Usage:
--   psql -U postgres -d discogs -f 02_enrich_data.sql

BEGIN;

-- ============================================
-- 1. Restore columns dropped by migration 01
-- ============================================

-- Full release date string (e.g. "2024-03-15")
ALTER TABLE release ADD COLUMN IF NOT EXISTS released text;

-- Discogs artist ID on release_artist (nullable for API-fetched releases)
ALTER TABLE release_artist ADD COLUMN IF NOT EXISTS artist_id integer;

-- Role for extra artists (e.g. "Producer", "Mixed By")
ALTER TABLE release_artist ADD COLUMN IF NOT EXISTS role text;

-- Restore country column on release (used by dedup ranking)
ALTER TABLE release ADD COLUMN IF NOT EXISTS country text;

-- ============================================
-- 2. Enrich release_label table
-- ============================================

ALTER TABLE release_label ADD COLUMN IF NOT EXISTS label_id integer;
ALTER TABLE release_label ADD COLUMN IF NOT EXISTS catno text;

-- ============================================
-- 3. Artist detail tables (new)
-- ============================================

CREATE TABLE IF NOT EXISTS artist (
    id         integer PRIMARY KEY,
    name       text NOT NULL,
    profile    text,
    image_url  text,
    fetched_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS artist_alias (
    artist_id  integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    alias_id   integer,
    alias_name text NOT NULL,
    UNIQUE (artist_id, alias_name)   -- WXYC/discogs-etl#433. Mirrors alembic/versions/0017_artist_child_unique.py. alias_id is out of the key on purpose: the ETL never loads it, so it is NULL on dump rows and set on LML's.
);

CREATE TABLE IF NOT EXISTS artist_name_variation (
    artist_id  integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    name       text NOT NULL,
    UNIQUE (artist_id, name)   -- WXYC/discogs-etl#433. Mirrors alembic/versions/0017_artist_child_unique.py.
);

CREATE TABLE IF NOT EXISTS artist_member (
    artist_id   integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    member_id   integer NOT NULL,
    member_name text NOT NULL,
    active      boolean DEFAULT true,
    UNIQUE (artist_id, member_id)   -- WXYC/discogs-etl#433. Mirrors alembic/versions/0017_artist_child_unique.py. member_name is functionally dependent on member_id and stays out of the key.
);

CREATE TABLE IF NOT EXISTS artist_url (
    artist_id integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    url       text NOT NULL,
    UNIQUE (artist_id, url)   -- WXYC/discogs-etl#433. Mirrors alembic/versions/0017_artist_child_unique.py.
);

COMMIT;
