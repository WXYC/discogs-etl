# Claude Code Instructions for discogs-cache

## Project Overview

ETL pipeline for building and maintaining a PostgreSQL cache of Discogs release data, filtered to artists in the WXYC radio library catalog. The cache database is a shared resource consumed by multiple services:

- **request-o-matic** (Python/FastAPI) - `discogs/cache_service.py` queries the cache for album lookups
- **Backend-Service** (TypeScript/Node.js) - future consumer for Discogs data

## Architecture

### Pipeline Steps

1. **Download** Discogs monthly data dumps (XML) from https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html
2. **Enrich** `library_artists.txt` with WXYC cross-references (`scripts/enrich_library_artists.py`, optional)
3. **Convert and filter** XML to CSV using [discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter) (Rust binary), with optional artist filtering via `--library-artists`. Accepts a single XML file or a directory containing releases.xml, artists.xml, and labels.xml. When artists.xml is present, alias-enhanced filtering is enabled automatically. When labels.xml is present, `label_hierarchy.csv` is produced for sublabel-aware dedup.
4. **Create schema** (`schema/create_database.sql`) and **functions** (`schema/create_functions.sql`)
5. **Import** filtered CSVs into PostgreSQL (`scripts/import_csv.py`)
6. **Create indexes** including accent-insensitive trigram GIN indexes (`schema/create_indexes.sql`)
7. **Deduplicate** by master_id (`scripts/dedup_releases.py`) -- prefers label match (with sublabel resolution via `--label-hierarchy`), then US releases, then most tracks, then lowest ID
8. **Prune or Copy-to** -- one of:
    - `--prune`: delete non-matching releases in place (~89% data reduction, 3 GB -> 340 MB)
    - `--copy-to`/`--target-db-url`: copy matched releases to a separate database, preserving the full import
9. **Vacuum** to reclaim disk space (`VACUUM FULL`)

`scripts/run_pipeline.py` supports two modes:
- `--xml` mode: runs steps 2-9 (enrich, convert+filter, database build through vacuum). `--xml` accepts a single file or a directory.
- `--csv-dir` mode: runs steps 4-9 (database build from pre-filtered CSVs)

Both modes support `--target-db-url` to copy matched releases to a separate database instead of pruning in place, and `--resume` (csv-dir only) to skip already-completed steps.

Step 1 (download) is always manual.

### master_id Column Lifecycle

The `release` table includes a `master_id` column used during import and dedup. The dedup copy-swap strategy (`CREATE TABLE AS SELECT ...` without `master_id`) drops the column automatically. After dedup, `master_id` no longer exists in the schema.

The `country` column, by contrast, is permanent -- it is included in the dedup copy-swap SELECT list and persists in the final schema for consumers.

### Database Schema (Shared Contract)

The SQL files in `schema/` define the contract between this ETL pipeline and all consumers:

- `schema/create_database.sql` -- Tables: `release`, `release_artist`, `release_track`, `release_track_artist`, `cache_metadata`; extensions: pg_trgm, unaccent
- `schema/create_functions.sql` -- `f_unaccent()` immutable wrapper for accent-insensitive index expressions
- `schema/create_indexes.sql` -- Trigram GIN indexes for accent-insensitive fuzzy text search (pg_trgm + unaccent)

Consumers connect via `DATABASE_URL_DISCOGS` environment variable.

### Docker Compose

`docker-compose.yml` provides a self-contained environment:
- **`db`** service: PostgreSQL 16 with pg_trgm + unaccent, port 5433:5432
- **`pipeline`** service: runs `scripts/run_pipeline.py` against the db

```bash
docker compose up --build   # full pipeline (needs data/ directory, builds Rust converter in Docker)
docker compose up db -d     # just the database (for tests)
```

### Key Files

- `scripts/run_pipeline.py` -- Pipeline orchestrator (--xml for steps 2-9, --csv-dir for steps 4-9)
- `scripts/enrich_library_artists.py` -- Enrich artist list with WXYC cross-references (pymysql)
- `scripts/filter_csv.py` -- Filter Discogs CSVs to library artists (standalone, used outside the pipeline)
- `scripts/import_csv.py` -- Import CSVs into PostgreSQL (psycopg COPY)
- `scripts/dedup_releases.py` -- Deduplicate releases by master_id, preferring label match + sublabel resolution, US releases (copy-swap with `DROP CASCADE`)
- `scripts/verify_cache.py` -- Multi-index fuzzy matching for KEEP/PRUNE classification; `--copy-to` streams matches to a target DB
- `scripts/csv_to_tsv.py` -- CSV to TSV conversion utility
- `scripts/fix_csv_newlines.py` -- Fix multiline CSV fields
- `lib/matching.py` -- Compilation detection utility
- `lib/pipeline_state.py` -- Pipeline state tracking for resumable runs
- `lib/db_introspect.py` -- Database introspection for inferring pipeline state on resume
- `docs/discogs-cache-technical-overview.md` -- Design rationale, benchmarks, and pipeline architecture details

### External Inputs

Two files are inputs to the ETL but produced by request-o-matic:

1. **`library_artists.txt`** -- One artist name per line, used by `discogs-xml-converter --library-artists` for filtering
2. **`library.db`** -- SQLite database, used by `verify_cache.py` for KEEP/PRUNE classification

Both are produced by request-o-matic's library sync (`scripts/sync-library.sh`).

## Development

### Testing

Three test layers with pytest markers:

```bash
# Unit tests (no external dependencies, run by default)
pytest tests/unit/ -v

# Integration tests (needs PostgreSQL on port 5433)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m postgres -v

# MySQL integration tests (needs WXYC MySQL on port 3307)
pytest -m mysql -v

# E2E tests (runs full pipeline as subprocess against test Postgres)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m e2e -v
```

Markers: `postgres` (needs PostgreSQL), `mysql` (needs WXYC MySQL), `e2e` (full pipeline), `integration` (needs library.db). Integration and E2E tests are excluded from the default `pytest` run via `addopts` in `pyproject.toml`.

Test fixtures are in `tests/fixtures/` (CSV files, library.db, library_artists.txt). Regenerate with `python tests/fixtures/create_fixtures.py`.

### Code Style

- Line length: 100 chars
- Use `ruff` for linting
- Python 3.11+

## Pipeline Lifecycle

This pipeline runs monthly (or when Discogs publishes new data dumps). It has a completely different lifecycle from the request-handling services that consume its output.

## Development Practices

### TDD (Required)

All code changes in this repo follow test-driven development. This is not optional.

1. **Red**: Write a failing test that describes the desired behavior. Run the test and confirm it fails for the right reason.
2. **Green**: Write the minimum implementation to make the test pass. Run the test and confirm it passes.
3. **Refactor**: Look for opportunities to improve the code while keeping tests green. Re-run tests after each change.
4. **Repeat**: Continue the cycle until the feature is complete.

**Key principle**: No production code without a failing test first.

## Example Music Data for Tests

WXYC is a freeform station. When creating test fixtures or mock data, use representative artists instead of mainstream acts like Queen, Radiohead, or The Beatles. The canonical data source is `wxyc-shared/src/test-utils/wxyc-example-data.json`. See the reference table in the org-level CLAUDE.md.

When writing inline test data or new fixture rows, use these defaults matching the repo's data structures:

**`release` table** (id, status, title, country, released, notes, data_quality, master_id, format):
```
5001,Accepted,DOGA,AR,2024-05-10,,Correct,8001,LP
5002,Accepted,Aluminum Tunes,UK,1998-09-01,,Correct,8002,CD
5003,Accepted,Moon Pix,US,1998-09-22,,Correct,8003,LP
5004,Accepted,On Your Own Love Again,US,2015-01-27,,Correct,8004,LP
5005,Accepted,Edits,US,2023,,Correct,,CD
5006,Accepted,Duke Ellington & John Coltrane,US,1963,,Correct,8005,LP
```

**`release_artist` table** (release_id, artist_id, artist_name, extra, anv, position, join_field):
```
5001,101,Juana Molina,0,,1,
5002,102,Stereolab,0,,1,
5003,103,Cat Power,0,,1,
5004,104,Jessica Pratt,0,,1,
5005,105,Chuquimamani-Condori,0,,1,
5006,106,Duke Ellington,0,,1,
5006,107,John Coltrane,0,,2, &
```

**`release_label` table** (release_id, label, catno):
```
5001,Sonamos,SON-001
5002,Duophonic,D-UHF-CD22
5003,Matador Records,OLE 325-1
5004,Drag City,DC575
5006,Impulse Records,A-30
```

**`release_track` table** (release_id, sequence, position, title, duration):
```
5001,1,A1,Cosoco,4:12
5002,1,1,Fuses,7:29
5003,1,1,American Flag,4:18
5004,1,A1,Back Baby Back,3:22
5005,1,1,Palqa,3:45
5006,1,A1,In A Sentimental Mood,4:19
```

**`library_artists.txt`**: `Juana Molina`, `Stereolab`, `Cat Power`, `Jessica Pratt`, `Chuquimamani-Condori`, `Duke Ellington`

**SQLite `library` rows** (artist, title): `("Juana Molina", "DOGA")`, `("Stereolab", "Aluminum Tunes")`, `("Cat Power", "Moon Pix")`, `("Jessica Pratt", "On Your Own Love Again")`, `("Chuquimamani-Condori", "Edits")`, `("Duke Ellington", "Duke Ellington & John Coltrane")`
