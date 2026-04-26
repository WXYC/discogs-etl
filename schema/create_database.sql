-- Create Discogs cache database schema (optimized)
-- Run with: psql -U postgres -f 04-create-database.sql
--
-- This schema includes all columns used by cache_service.py and downstream consumers.
-- FK constraints with ON DELETE CASCADE enable single-table pruning.

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
-- Core tables (drop + recreate for clean ETL)
-- ============================================

-- Drop in FK order: children first, then parent
DROP TABLE IF EXISTS cache_metadata CASCADE;
DROP TABLE IF EXISTS master_artist CASCADE;
DROP TABLE IF EXISTS master CASCADE;
DROP TABLE IF EXISTS artist_url CASCADE;
DROP TABLE IF EXISTS artist_member CASCADE;
DROP TABLE IF EXISTS artist_name_variation CASCADE;
DROP TABLE IF EXISTS artist_alias CASCADE;
DROP TABLE IF EXISTS artist CASCADE;
DROP TABLE IF EXISTS release_video CASCADE;
DROP TABLE IF EXISTS release_track_artist CASCADE;
DROP TABLE IF EXISTS release_track CASCADE;
DROP TABLE IF EXISTS release_style CASCADE;
DROP TABLE IF EXISTS release_genre CASCADE;
DROP TABLE IF EXISTS release_label CASCADE;
DROP TABLE IF EXISTS release_artist CASCADE;
DROP TABLE IF EXISTS release CASCADE;

-- Releases
CREATE TABLE release (
    id              integer PRIMARY KEY,
    title           text NOT NULL,
    release_year    smallint,
    country         text,
    artwork_url     text,
    released        text,              -- full date string, e.g. "2024-03-15"
    format          text,              -- normalized format category: 'Vinyl', 'CD', etc.
    master_id       integer          -- used by dedup, dropped after dedup copy-swap
);

-- Artists on releases
CREATE TABLE release_artist (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    artist_id       integer,             -- Discogs artist ID (nullable for API-fetched releases)
    artist_name     text NOT NULL,
    extra           integer DEFAULT 0,  -- 0 = main artist, 1 = extra credit
    role            text               -- role for extra artists: "Producer", "Mixed By", etc.
);

-- Labels on releases
CREATE TABLE release_label (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    label_id        integer,
    label_name      text NOT NULL,
    catno           text
);

-- Genres on releases
CREATE TABLE release_genre (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    genre           text NOT NULL
);

-- Styles on releases (more specific than genres)
CREATE TABLE release_style (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    style           text NOT NULL
);

-- Tracks on releases
CREATE TABLE release_track (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    sequence        integer NOT NULL,
    position        text,              -- "A1", "B2", etc.
    title           text NOT NULL,
    duration        text
);

-- Videos on releases
CREATE TABLE release_video (
    release_id integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    sequence   integer NOT NULL,
    src        text NOT NULL,
    title      text,
    duration   integer,
    embed      boolean DEFAULT true
);

-- Artists on specific tracks (for compilations)
CREATE TABLE release_track_artist (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    track_sequence  integer NOT NULL,
    artist_name     text NOT NULL
);

-- ============================================
-- Artist detail tables
-- ============================================

CREATE TABLE artist (
    id              integer PRIMARY KEY,
    name            text NOT NULL,
    profile         text,
    image_url       text,
    fetched_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE artist_alias (
    artist_id       integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    alias_id        integer,
    alias_name      text NOT NULL
);

CREATE TABLE artist_name_variation (
    artist_id       integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    name            text NOT NULL
);

CREATE TABLE artist_member (
    artist_id       integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    member_id       integer NOT NULL,
    member_name     text NOT NULL,
    active          boolean DEFAULT true
);

CREATE TABLE artist_url (
    artist_id       integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    url             text NOT NULL
);

-- ============================================
-- Masters (canonical album groupings)
-- ============================================

CREATE TABLE master (
    id              integer PRIMARY KEY,
    title           text NOT NULL,
    main_release_id integer,
    year            smallint
);

CREATE TABLE master_artist (
    master_id       integer NOT NULL REFERENCES master(id) ON DELETE CASCADE,
    artist_id       integer,
    artist_name     text NOT NULL
);

-- ============================================
-- Cache metadata (for tracking data freshness)
-- ============================================

CREATE TABLE cache_metadata (
    release_id      integer PRIMARY KEY REFERENCES release(id) ON DELETE CASCADE,
    cached_at       timestamptz NOT NULL DEFAULT now(),
    source          text NOT NULL,  -- 'bulk_import' or 'api_fetch'
    last_validated  timestamptz
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
