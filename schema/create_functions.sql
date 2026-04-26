-- Create immutable wrapper for unaccent() to allow use in index expressions.
--
-- PostgreSQL's built-in unaccent() is STABLE (depends on search_path), so it
-- can't be used directly in index expressions which require IMMUTABLE functions.
-- This wrapper pins the dictionary to public.unaccent, removing the search_path
-- variability.
--
-- Run BEFORE create_database.sql: create_database.sql defines the
-- idx_master_title_trgm index expression that references f_unaccent, so the
-- function must already exist (see #104).  Also run BEFORE create_indexes.sql
-- and create_track_indexes.sql, which use f_unaccent in their trigram index
-- expressions.
--
-- This file is self-contained: it creates the unaccent extension it depends
-- on so it can be applied to a brand-new database with no prior setup.
-- create_database.sql also issues CREATE EXTENSION IF NOT EXISTS, so there
-- is no conflict when it runs afterwards.

CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text AS $$
  SELECT public.unaccent('public.unaccent', $1)
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;
