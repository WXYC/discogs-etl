-- Drop the discogs-cache core-table subgraph.
--
-- Used by the explicit `--fresh-rebuild` path in scripts/run_pipeline.py to
-- wipe the schema before reapplying `schema/create_database.sql`. The
-- default rebuild path is now incremental (create_database.sql is
-- `CREATE TABLE IF NOT EXISTS`-only) so LML-back-patched artwork survives
-- across rebuilds (WXYC/discogs-etl#242). This file is the operator-
-- visible escape hatch when "I want today's drop+recreate behavior" is
-- the intent (e.g. recovering from a corrupted cache).
--
-- These DROPs were previously embedded in create_database.sql; splitting
-- them out keeps that file safe to apply against an already-populated DB.
-- Mirrors the single-job convention already used by create_functions.sql /
-- create_indexes.sql / create_track_indexes.sql.

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

DROP TABLE IF EXISTS lookup_negative CASCADE;
DROP TABLE IF EXISTS wxyc_library CASCADE;
