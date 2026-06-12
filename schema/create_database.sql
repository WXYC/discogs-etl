-- Create Discogs cache database schema (optimized)
-- Run with: psql -U postgres -f 04-create-database.sql
--
-- This schema includes all columns used by cache_service.py and downstream consumers.
-- FK constraints with ON DELETE CASCADE enable single-table pruning.
-- Schema regression coverage: tests/integration/test_schema.py::TestSchemaProductionOrdering
-- guards against ordering bugs (e.g. #104) by applying these files to a pristine
-- template0-cloned database in CI.

-- Create database (run as superuser)
-- CREATE DATABASE discogs;

-- Connect to discogs database, then run the rest
-- \c discogs

-- Enable trigram extension for fuzzy text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Enable unaccent extension for accent-insensitive search
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ============================================
-- Helper functions
-- ============================================

-- Immutable wrapper for unaccent() so it can be used in index expressions
-- (the built-in unaccent() is STABLE, depending on search_path).
--
-- Defined here AND in create_functions.sql so that create_database.sql is
-- self-sufficient: the idx_master_title_trgm index expression below
-- references f_unaccent and would otherwise fail with "function
-- f_unaccent(text) does not exist" on a fresh database (see #104).
--
-- create_functions.sql remains the canonical source of this definition;
-- later steps (create_indexes.sql, create_track_indexes.sql) also depend
-- on f_unaccent and must be runnable independently of this file. The two
-- definitions are kept identical; both use CREATE OR REPLACE so whichever
-- runs last simply re-asserts the same body.
CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text AS $$
  SELECT public.unaccent('public.unaccent', $1)
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

-- ============================================
-- Core tables (idempotent — safe to apply against a populated DB)
-- ============================================
--
-- Every CREATE here is `IF NOT EXISTS`. On a fresh DB every statement
-- fires; on a populated cache every statement is a no-op and existing
-- rows (including LML-back-patched `release.artwork_url` /
-- `release.artwork_checked_at`) are left alone. The explicit wipe path
-- lives in `schema/drop_core_tables.sql`, invoked only by the
-- `--fresh-rebuild` flag on `scripts/run_pipeline.py`
-- (WXYC/discogs-etl#242).

-- Releases
CREATE TABLE IF NOT EXISTS release (
    id                  integer PRIMARY KEY,
    title               text NOT NULL,
    release_year        smallint,
    country             text,
    artwork_url         text,
    released            text,              -- full date string, e.g. "2024-03-15"
    format              text,              -- normalized format category: 'Vinyl', 'CD', etc.
    master_id           integer,           -- Discogs master ID; used by dedup partitioning, persists post-swap (see DEDUP_TABLES in scripts/dedup_releases.py)
    artwork_checked_at  timestamptz,       -- WXYC/discogs-etl#239. NULL = never asked, set = LML asked Discogs at lookup time. LML's predicate honors this so genuinely-imageless releases aren't refetched. Index below covers LML#221's never-asked drain.
    not_found           boolean NOT NULL DEFAULT FALSE   -- WXYC/library-metadata-lookup#510. Tombstone marker for Discogs 404s on get_release. LML's read short-circuits on TRUE; rebuild/UPSERT paths clear to FALSE on fresh data. Mirrored in alembic/versions/0010_release_not_found.py.
);

-- Partial index for LML#221's never-asked top-up drain (WXYC/discogs-etl#239).
-- Mirrored in alembic/versions/0008_release_artwork_checked_at.py; the
-- dual-write convention keeps the fresh-rebuild and alembic-upgrade paths
-- in parity.
CREATE INDEX IF NOT EXISTS release_artwork_null_idx
    ON release (id)
    WHERE artwork_url IS NULL AND artwork_checked_at IS NULL;

-- Artists on releases
CREATE TABLE IF NOT EXISTS release_artist (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    artist_id       integer,             -- Discogs artist ID (nullable for API-fetched releases)
    artist_name     text NOT NULL,
    extra           integer DEFAULT 0,  -- 0 = main artist, 1 = extra credit
    role            text               -- role for extra artists: "Producer", "Mixed By", etc.
);

-- Labels on releases
CREATE TABLE IF NOT EXISTS release_label (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    label_id        integer,
    label_name      text NOT NULL,
    catno           text
);

-- Genres on releases
CREATE TABLE IF NOT EXISTS release_genre (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    genre           text NOT NULL
);

-- Styles on releases (more specific than genres)
CREATE TABLE IF NOT EXISTS release_style (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    style           text NOT NULL
);

-- Tracks on releases
CREATE TABLE IF NOT EXISTS release_track (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    sequence        integer NOT NULL,
    position        text,              -- "A1", "B2", etc.
    title           text NOT NULL,
    duration        text
);

-- Videos on releases
CREATE TABLE IF NOT EXISTS release_video (
    release_id integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    sequence   integer NOT NULL,
    src        text NOT NULL,
    title      text,
    duration   integer,
    embed      boolean DEFAULT true
);

-- Artists on specific tracks (for compilations)
--
-- ``extra`` and ``role`` (WXYC/discogs-etl#218) distinguish main-artist
-- credits (``<artists>``) from extra-artist credits (``<extraartists>``).
-- Downstream consumers filter to main credits with ``WHERE extra = 0``.
-- Mirrors the column shape on ``release_artist`` for consistency.
--
-- Both columns are additive / NULL-tolerant so older (3-column) CSVs
-- and pre-migration cache rows continue to import and read correctly:
--   * ``extra`` defaults to 0, matching the legacy "everything was main"
--     interpretation under which existing consumers were already
--     operating.
--   * ``role`` is NULL for main credits and may be NULL for extra
--     credits when the XML omitted the ``<role>`` element.
CREATE TABLE IF NOT EXISTS release_track_artist (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    track_sequence  integer NOT NULL,
    artist_name     text NOT NULL,
    extra           integer DEFAULT 0,
    role            text
);

-- ============================================
-- Artist detail tables
-- ============================================

CREATE TABLE IF NOT EXISTS artist (
    id              integer PRIMARY KEY,
    name            text NOT NULL,
    profile         text,
    image_url       text,
    fetched_at      timestamptz NOT NULL DEFAULT now(),
    not_found       boolean NOT NULL DEFAULT FALSE   -- WXYC/library-metadata-lookup#510. Tombstone marker for Discogs 404s on get_artist_details. Mirrored in alembic/versions/0011_artist_not_found.py.
);

CREATE TABLE IF NOT EXISTS artist_alias (
    artist_id       integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    alias_id        integer,
    alias_name      text NOT NULL
);

CREATE TABLE IF NOT EXISTS artist_name_variation (
    artist_id       integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    name            text NOT NULL
);

CREATE TABLE IF NOT EXISTS artist_member (
    artist_id       integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    member_id       integer NOT NULL,
    member_name     text NOT NULL,
    active          boolean DEFAULT true
);

CREATE TABLE IF NOT EXISTS artist_url (
    artist_id       integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    url             text NOT NULL
);

-- ============================================
-- Masters (canonical album groupings)
-- ============================================

CREATE TABLE IF NOT EXISTS master (
    id              integer PRIMARY KEY,
    title           text NOT NULL,
    main_release_id integer,
    year            smallint
);

CREATE TABLE IF NOT EXISTS master_artist (
    master_id       integer NOT NULL REFERENCES master(id) ON DELETE CASCADE,
    artist_id       integer,
    artist_name     text NOT NULL
);

-- ============================================
-- Cache metadata (for tracking data freshness)
-- ============================================

CREATE TABLE IF NOT EXISTS cache_metadata (
    release_id      integer PRIMARY KEY REFERENCES release(id) ON DELETE CASCADE,
    cached_at       timestamptz NOT NULL DEFAULT now(),
    source          text NOT NULL,  -- 'bulk_import' or 'api_fetch'
    last_validated  timestamptz
);

-- ============================================
-- Negative-result cache for Discogs lookups
-- ============================================
-- Mirrors alembic/versions/0006_lookup_negative.py per the dual-write
-- convention. Persists "we asked Discogs and got nothing" verdicts across
-- LML process restarts. LML's read path consults this table after the
-- in-memory + positive PG caches and before the Discogs API. TTL is
-- enforced by the LML query inline (now() < attempted_at + ttl_seconds *
-- interval '1 second'); a future cron may sweep expired rows.
CREATE TABLE IF NOT EXISTS lookup_negative (
    key_hash          bytea PRIMARY KEY,
    artist            text,
    track             text,
    artist_as_keyword boolean,
    attempted_at      timestamptz NOT NULL DEFAULT now(),
    ttl_seconds       integer NOT NULL DEFAULT 604800
);

-- ============================================
-- WXYC library hook (cross-cache-identity §3.1)
-- ============================================
-- One row per library release; library_id mirrors Backend wxyc_schema.library.id.
-- Mirrored from alembic/versions/0003_wxyc_library_v2.py per the dual-write
-- convention so a fresh rebuild produces the same end state as the alembic
-- upgrade chain.
CREATE TABLE IF NOT EXISTS wxyc_library (
    library_id      integer PRIMARY KEY,
    artist_id       integer,
    artist_name     text NOT NULL,
    album_title     text NOT NULL,
    label_id        integer,
    label_name      text,
    format_id       integer,
    format_name     text,
    wxyc_genre      text,
    call_letters    text,
    call_numbers    integer,
    release_year    smallint,
    norm_artist     text NOT NULL,
    norm_title      text NOT NULL,
    norm_label      text,
    snapshot_at     timestamptz NOT NULL,
    snapshot_source text NOT NULL
        CHECK (snapshot_source IN ('backend', 'tubafrenzy', 'llm'))
);

-- ============================================
-- Indexes
-- ============================================

-- Foreign key indexes
CREATE INDEX IF NOT EXISTS idx_release_artist_release_id ON release_artist(release_id);
CREATE INDEX IF NOT EXISTS idx_release_label_release_id ON release_label(release_id);
CREATE INDEX IF NOT EXISTS idx_release_genre_release_id ON release_genre(release_id);
CREATE INDEX IF NOT EXISTS idx_release_style_release_id ON release_style(release_id);
CREATE INDEX IF NOT EXISTS idx_release_track_release_id ON release_track(release_id);
CREATE INDEX IF NOT EXISTS idx_release_track_artist_release_id ON release_track_artist(release_id);
CREATE INDEX IF NOT EXISTS idx_release_video_release_id ON release_video(release_id);

-- Artist detail indexes
CREATE INDEX IF NOT EXISTS idx_artist_alias_artist_id ON artist_alias(artist_id);
CREATE INDEX IF NOT EXISTS idx_artist_name_variation_artist_id ON artist_name_variation(artist_id);
CREATE INDEX IF NOT EXISTS idx_artist_member_artist_id ON artist_member(artist_id);
CREATE INDEX IF NOT EXISTS idx_artist_url_artist_id ON artist_url(artist_id);

-- Partial index on master_id for dedup performance.
-- Transient: dropped automatically by dedup copy-swap (which excludes master_id).
CREATE INDEX IF NOT EXISTS idx_release_master_id ON release(master_id) WHERE master_id IS NOT NULL;

-- Master indexes
CREATE INDEX IF NOT EXISTS idx_master_artist_master_id ON master_artist(master_id);
CREATE INDEX IF NOT EXISTS idx_master_title_trgm ON master USING GIN (lower(f_unaccent(title)) gin_trgm_ops);

-- Cache metadata indexes
CREATE INDEX IF NOT EXISTS idx_cache_metadata_cached_at ON cache_metadata(cached_at);
CREATE INDEX IF NOT EXISTS idx_cache_metadata_source ON cache_metadata(source);

-- Negative-cache TTL-sweep index (mirrors 0006_lookup_negative.py)
CREATE INDEX IF NOT EXISTS idx_lookup_negative_attempted_at ON lookup_negative(attempted_at);

-- ============================================
-- LML-owned identity layer (entity schema)
-- ============================================
-- Mirrors alembic/versions/0012_entity_release_identity.py (release-side) and
-- 0013_adopt_entity_identity.py (artist-side) per the dual-write convention so
-- a fresh rebuild (`run_pipeline.py --fresh-rebuild`) produces the same end
-- state as the alembic upgrade chain. Without the mirror, a dev DB built only
-- through create_database.sql leaves LML's POST /api/v1/identity/resolve probe
-- returning 503 and the artist-side `SELECT 1 FROM entity.identity LIMIT 0`
-- probe also failing.
--
-- Lifecycle note: entity.* tables hold LML-owned mint and reconciliation state
-- and survive the monthly cache rebuild. drop_core_tables.sql intentionally
-- does NOT drop them; --truncate-existing intentionally does NOT include them
-- in the TRUNCATE list (see CACHE_TABLES_TO_TRUNCATE_BASE in
-- scripts/import_csv.py).
--
-- Artist-side DDL (entity.identity / entity.reconciliation_log + indexes) is
-- copied from wxyc-etl/src/schema/entity.rs — the canonical mirror of the
-- prod shape — and embedded by copy (not import), matching 0013's policy.
-- Artist-side adoption tracked at WXYC/discogs-etl#279.
CREATE SCHEMA IF NOT EXISTS entity;

CREATE TABLE IF NOT EXISTS entity.release_identity (
    id                       SERIAL PRIMARY KEY,
    discogs_release_id       INTEGER UNIQUE,
    discogs_master_id        INTEGER UNIQUE,
    musicbrainz_release_id   TEXT UNIQUE,
    spotify_album_id         TEXT UNIQUE,
    apple_music_album_id     TEXT UNIQUE,
    bandcamp_album_url       TEXT UNIQUE,
    reconciliation_status    TEXT NOT NULL DEFAULT 'unreconciled',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entity.release_reconciliation_log (
    id           SERIAL PRIMARY KEY,
    identity_id  INTEGER NOT NULL REFERENCES entity.release_identity(id),
    source       TEXT NOT NULL,
    external_id  TEXT NOT NULL,
    confidence   REAL,
    method       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_release_reconciliation_log_identity_id
    ON entity.release_reconciliation_log(identity_id);

-- Artist-side: mirrors alembic/versions/0013_adopt_entity_identity.py.
-- library_name UNIQUE is load-bearing for LML's artist-side resolve path.
CREATE TABLE IF NOT EXISTS entity.identity (
    id                       SERIAL PRIMARY KEY,
    library_name             TEXT NOT NULL UNIQUE,
    discogs_artist_id        INTEGER,
    wikidata_qid             TEXT,
    musicbrainz_artist_id    TEXT,
    spotify_artist_id        TEXT,
    apple_music_artist_id    TEXT,
    bandcamp_id              TEXT,
    reconciliation_status    TEXT NOT NULL DEFAULT 'unreconciled',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_entity_identity_status
    ON entity.identity(reconciliation_status);

CREATE TABLE IF NOT EXISTS entity.reconciliation_log (
    id           SERIAL PRIMARY KEY,
    identity_id  INTEGER NOT NULL REFERENCES entity.identity(id),
    source       TEXT NOT NULL,
    external_id  TEXT NOT NULL,
    confidence   REAL,
    method       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_entity_reconciliation_log_identity_id
    ON entity.reconciliation_log(identity_id);
