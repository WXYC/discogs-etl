-- Create Discogs cache database schema (optimized)
-- Run with: psql -U postgres -f 04-create-database.sql
--
-- This schema only includes columns actively queried by cache_service.py.
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
-- Core tables (drop + recreate for clean ETL)
-- ============================================

-- Drop in FK order: children first, then parent
DROP TABLE IF EXISTS cache_metadata CASCADE;
DROP TABLE IF EXISTS release_track_artist CASCADE;
DROP TABLE IF EXISTS release_track CASCADE;
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
    master_id       integer          -- used by dedup, dropped after dedup copy-swap
);

-- Artists on releases
CREATE TABLE release_artist (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    artist_id       integer,             -- Discogs artist ID (nullable for API-fetched releases)
    artist_name     text NOT NULL,
    extra           integer DEFAULT 0  -- 0 = main artist, 1 = extra credit
);

-- Labels on releases
CREATE TABLE release_label (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    label_name      text NOT NULL
);

-- Tracks on releases
CREATE TABLE release_track (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    sequence        integer NOT NULL,
    position        text,              -- "A1", "B2", etc.
    title           text NOT NULL,
    duration        text
);

-- Artists on specific tracks (for compilations)
CREATE TABLE release_track_artist (
    release_id      integer NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    track_sequence  integer NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_release_track_release_id ON release_track(release_id);
CREATE INDEX IF NOT EXISTS idx_release_track_artist_release_id ON release_track_artist(release_id);

-- Partial index on master_id for dedup performance.
-- Transient: dropped automatically by dedup copy-swap (which excludes master_id).
CREATE INDEX IF NOT EXISTS idx_release_master_id ON release(master_id) WHERE master_id IS NOT NULL;

-- Cache metadata indexes
CREATE INDEX IF NOT EXISTS idx_cache_metadata_cached_at ON cache_metadata(cached_at);
CREATE INDEX IF NOT EXISTS idx_cache_metadata_source ON cache_metadata(source);
