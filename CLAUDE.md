# Claude Code Instructions for discogs-etl

## Project Overview

ETL pipeline for building and maintaining a PostgreSQL cache of Discogs release data, filtered to artists in the WXYC radio library catalog. The cache database is a shared resource consumed by multiple services:

- **library-metadata-lookup** (Python/FastAPI) - `discogs/cache_service.py` queries the cache for album lookups
- **Backend-Service** (TypeScript/Node.js) - future consumer for Discogs data

## Architecture

### Pipeline Steps

1. **Download** Discogs monthly data dumps (XML) from https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html
2. **Enrich** `library_artists.txt` with WXYC cross-references (via `wxyc-enrich-library-artists` CLI from wxyc-catalog, optional)
3. **Convert and filter** XML to CSV using [discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter) (Rust binary), with optional artist filtering via `--library-artists`. Accepts a single XML file or a directory containing releases.xml, artists.xml, and labels.xml. When artists.xml is present, alias-enhanced filtering is enabled automatically. When labels.xml is present, `label_hierarchy.csv` is produced for sublabel-aware dedup.
4. **Create schema** (`schema/create_database.sql`) and **functions** (`schema/create_functions.sql`), then **SET UNLOGGED** on all tables to skip WAL writes during bulk import
5. **Import** filtered CSVs into PostgreSQL (`scripts/import_csv.py`)
6. **Create indexes** including accent-insensitive trigram GIN indexes (`schema/create_indexes.sql`)
7. **Deduplicate** by (master_id, format) (`scripts/dedup_releases.py`) -- partitions by master_id and normalized format so different formats (CD, Vinyl, etc.) of the same album survive dedup independently. Within each partition, prefers label match (with sublabel resolution via `--label-hierarchy`), then US releases, then most tracks, then lowest ID
8. **Prune or Copy-to** -- one of:
    - `--prune`: delete non-matching releases in place (~89% data reduction, 3 GB -> 340 MB)
    - `--copy-to`/`--target-db-url`: copy matched releases to a separate database, preserving the full import
9. **Vacuum** to reclaim disk space (`VACUUM FULL`)
10. **SET LOGGED** to restore WAL durability for consumers

`scripts/run_pipeline.py` supports two modes:
- `--xml` mode: runs steps 2-10 (enrich, convert+filter, database build through SET LOGGED). `--xml` accepts a single file or a directory.
- `--csv-dir` mode: runs steps 4-10 (database build from pre-filtered CSVs)

Both modes support `--target-db-url` to copy matched releases to a separate database instead of pruning in place, and `--resume` (csv-dir only) to skip already-completed steps. `--keep-csv` (xml mode only) writes converted CSVs to a persistent directory instead of a temp dir, so they survive pipeline failures.

Step 1 (download) is always manual.

### master_id Column Lifecycle

The `release` table includes a `master_id` column used during import and dedup. The dedup copy-swap strategy (`CREATE TABLE AS SELECT ...` without `master_id`) drops the column automatically. After dedup, `master_id` no longer exists in the schema.

The `country` column, by contrast, is permanent -- it is included in the dedup copy-swap SELECT list and persists in the final schema for consumers.

### format Column Lifecycle

The `format` column stores the normalized format category (Vinyl, CD, Cassette, 7", Digital). Unlike `master_id`, `format` persists after dedup and is available to consumers. During import, raw Discogs format strings are normalized via `lib/format_normalization.py` (e.g., "2xLP" → "Vinyl", "CD-R" → "CD"). During dedup, releases are partitioned by `(master_id, format)`, so a CD and Vinyl pressing of the same album both survive. During verify/prune, format-aware matching ensures only releases whose format matches the library's are kept (for exact artist+title matches). NULL format on either side is treated as "match anything" for backward compatibility.

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
- `scripts/filter_csv.py` -- Filter Discogs CSVs to library artists (standalone, used outside the pipeline)
- `scripts/import_csv.py` -- Import CSVs into PostgreSQL (psycopg COPY). Child tables are imported in parallel via ThreadPoolExecutor after parent tables. Artist detail tables (artist_alias, artist_member) are filtered to known artist IDs to prevent FK violations, since the converter's CSVs contain all Discogs artists. Tables with `unique_key` configs are deduped in-memory during COPY.
- `scripts/dedup_releases.py` -- Deduplicate releases by master_id, preferring label match + sublabel resolution, US releases (copy-swap with `DROP CASCADE`). Index/constraint creation is parallelized via ThreadPoolExecutor.
- `scripts/verify_cache.py` -- Multi-index fuzzy matching for KEEP/PRUNE classification; `--copy-to` streams matches to a target DB. Phase 4 (fuzzy matching) has two paths: when `wxyc-etl` is installed, `batch_classify_releases()` runs all scoring in Rust with rayon parallelism; otherwise, falls back to ProcessPoolExecutor with rapidfuzz. Set `WXYC_ETL_NO_RUST=1` to force the Python fallback. Large prune sets (>10K IDs) use copy-and-swap instead of CASCADE DELETE.
- `scripts/csv_to_tsv.py` -- CSV to TSV conversion utility
- `scripts/fix_csv_newlines.py` -- Fix multiline CSV fields
- `lib/format_normalization.py` -- Normalize raw Discogs/library format strings to broad categories (Vinyl, CD, Cassette, 7", Digital)
- `scripts/sync-library.sh` -- Daily library sync orchestrator: MySQL query (via MariaDB `mysql` CLI for MySQL 4.1 compat) → `tsv_to_sqlite.py` → streaming links enrichment → upload to LML. Automated by `.github/workflows/sync-library.yml` (daily at noon UTC).
- `scripts/tsv_to_sqlite.py` -- Converts MySQL TSV output to SQLite with FTS5 index. Called by sync-library.sh.
- `docs/discogs-etl-technical-overview.md` -- Design rationale, benchmarks, and pipeline architecture details

### Shared Package Dependencies

Functionality that was previously local to this repo has been extracted to shared packages:

- **wxyc-etl** (Rust/PyO3) -- Artist name normalization (`normalize_artist_name`), compilation detection (`is_compilation_artist`), artist name splitting (`split_artist_name`, `split_artist_name_contextual`), pipeline state tracking (`PipelineState`), and database introspection.
- **wxyc-catalog** -- Catalog source protocol (`CatalogSource`, `TubafrenzySource`, `BackendServiceSource`), library.db export (`wxyc-export-to-sqlite` CLI), library artist enrichment (`wxyc-enrich-library-artists` CLI), and label extraction (`wxyc-extract-library-labels` CLI).

### External Inputs

### Library Catalog (library.db)

`library.db` is a SQLite export of the WXYC library catalog, generated by the `wxyc-export-to-sqlite` CLI from the wxyc-catalog package. The default source is tubafrenzy's MySQL database on Kattare (via SSH), but Backend-Service's PostgreSQL database can be used instead via `--catalog-source backend-service --catalog-db-url <url>`. The `--catalog-source` flag is supported by `run_pipeline.py` and the wxyc-catalog CLI tools. The legacy `--wxyc-db-url` flag is an alias for `--catalog-source tubafrenzy --catalog-db-url <url>`.

`library.db` is used as input throughout the pipeline:

1. **`library_artists.txt`** -- Generated from `library.db` by `wxyc-enrich-library-artists` (wxyc-catalog CLI), one artist name per line, used by `discogs-xml-converter --library-artists` for filtering
2. **KEEP/PRUNE classification** -- `scripts/verify_cache.py` uses `library.db` to match cached releases against the WXYC catalog

`scripts/sync-library.sh` orchestrates the daily sync: query Kattare MySQL via the MariaDB `mysql` CLI (required for MySQL 4.1's old-format password hashes), convert TSV to SQLite via `scripts/tsv_to_sqlite.py`, enrich with streaming links from `streaming_availability.db` (a [GitHub Release artifact](https://github.com/WXYC/library-metadata-lookup/releases/tag/streaming-data-v1) in library-metadata-lookup, refreshed weekly), then upload to LML staging and production via `POST /admin/upload-library-db`. The Discogs pipeline can also generate `library.db` inline via `--generate-library-db` using the wxyc-catalog CLI.

## Development

### Testing

The repo follows architecture A from [the wiki test-patterns doc](https://github.com/WXYC/wiki/blob/main/plans/test-patterns.md): markers route CI by infrastructure, not by tier. Directory layout (`tests/unit/`, `tests/integration/`, `tests/e2e/`) documents the tier; markers describe operational requirements only.

Declared markers:

| Marker | Meaning | When to use |
|---|---|---|
| `pg` | needs a PostgreSQL service (`DATABASE_URL_TEST`) | every test that connects to Postgres, regardless of tier |
| `slow` | takes longer than ~10s (orthogonal to infra) | perf benchmarks; opt-out from CI sync-check, run manually |

Tests with no marker are the default pytest run (no infrastructure required).

```bash
# Default run: no-marker tests only (pure-logic unit tests + in-memory SQLite + library.db fixture tests)
pytest

# PG-backed tests (integration + E2E that touch Postgres)
docker compose up db -d
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m pg -v

# Everything that is not slow (PG-backed plus default tier in one run, for coverage)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m "pg or not slow" -v

# Perf benchmarks (rare, manual; opted-out from the marker sync-check)
pytest -m slow -v
```

CI runs four jobs per push/PR: `lint`, `test` (default no-marker run, unit dir only), `pg` (`-m "pg or not slow"` against PostgreSQL with `--cov-fail-under=60` -- the PG-and-default combined run is what hits the gate), and `marker-sync` (the reusable sync-check workflow from wxyc-etl that catches markers silently deselected by addopts).

Test fixtures are in `tests/fixtures/` (CSV files, library.db, library_artists.txt). Regenerate with `python tests/fixtures/create_fixtures.py`.

### Code Style

- Line length: 100 chars
- Use `ruff` for linting
- Python 3.11+

## Pipeline Lifecycle

This pipeline runs monthly (or when Discogs publishes new data dumps). It has a completely different lifecycle from the request-handling services that consume its output.

## Automation

### Library Sync (`sync-library.yml`)

A GitHub Actions cron workflow runs `scripts/sync-library.sh` daily at noon UTC (7 AM EST / 8 AM EDT) to export the WXYC library catalog to SQLite (via `wxyc-export-to-sqlite` from wxyc-catalog) and upload it to library-metadata-lookup staging and production environments.

The workflow can also be triggered manually: `gh workflow run sync-library.yml`

The `--notify` flag is always passed, so Slack notifications are sent on failure when `SLACK_MONITORING_WEBHOOK` is configured.

**Required GitHub secrets:**

| Secret | Description |
|--------|-------------|
| `SSH_PRIVATE_KEY` | Private key authorized on Kattare |
| `LIBRARY_SSH_HOST` | Kattare SSH hostname |
| `LIBRARY_SSH_USER` | SSH username |
| `LIBRARY_DB_HOST` | MySQL host (as seen from SSH host) |
| `LIBRARY_DB_USER` | MySQL username |
| `LIBRARY_DB_PASSWORD` | MySQL password |
| `LIBRARY_DB_NAME` | MySQL database name |
| `ADMIN_TOKEN` | Bearer token for library-metadata-lookup admin endpoints |
| `STAGING_URL` | Staging base URL for library-metadata-lookup |
| `PRODUCTION_URL` | Production base URL for library-metadata-lookup |
| `SLACK_MONITORING_WEBHOOK` | Slack webhook for error notifications (optional) |

After a successful run, verify the library-metadata-lookup health endpoint returns healthy with the expected row count.

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

**SQLite `library` rows** (artist, title, format): `("Juana Molina", "DOGA", "LP")`, `("Stereolab", "Aluminum Tunes", "CD")`, `("Cat Power", "Moon Pix", "LP")`, `("Jessica Pratt", "On Your Own Love Again", "LP")`, `("Chuquimamani-Condori", "Edits", "CD")`, `("Duke Ellington", "Duke Ellington & John Coltrane", "LP")`
