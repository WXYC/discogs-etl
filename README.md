# discogs-cache

ETL pipeline for building a PostgreSQL cache of Discogs release data, filtered to artists in the WXYC radio library catalog. Reduces Discogs API calls by providing a local cache for album lookups, track validation, and artwork URLs.

## Overview

The pipeline processes monthly Discogs data dumps (~63.3 GB XML) into a focused PostgreSQL database (~3 GB) containing only releases by artists in the WXYC library catalog. This provides:

- Fast local lookups instead of rate-limited Discogs API calls
- Accent-insensitive trigram fuzzy text search via pg_trgm + unaccent
- Shared data resource for multiple consuming services

## Prerequisites

- Python 3.11+
- PostgreSQL with the `pg_trgm` and `unaccent` extensions (or use [Docker Compose](#docker-compose))
- [discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter) -- Rust binary for XML-to-CSV conversion (build from source or install on PATH)
- Discogs monthly data dumps (XML) from https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html -- `releases.xml.gz` is required; `artists.xml.gz` and `labels.xml.gz` are optional for enhanced filtering and dedup

## Quick Start

```bash
uv sync
```

### 1. Generate library.db

`library.db` is a SQLite export of the WXYC library catalog (sourced from the MySQL database on Kattare):

```bash
python scripts/export_to_sqlite.py   # requires SSH/MySQL env vars; produces library.db
```

This runs automatically via `scripts/sync-library.sh` on a schedule. The pipeline can also generate it inline with `--generate-library-db`.

### 2. Run the pipeline

The full pipeline generates `library_artists.txt` from `library.db`, converts XML to CSV, builds a PostgreSQL database, deduplicates, and prunes to library matches. See [Pipeline](#pipeline) for details on each step.

**With Docker Compose** (builds the Rust converter automatically):

```bash
mkdir -p data
cp /path/to/releases.xml.gz data/
cp /path/to/library.db data/
# Optional: copy artists.xml.gz and labels.xml.gz for enhanced filtering/dedup
cp /path/to/artists.xml.gz data/
cp /path/to/labels.xml.gz data/

docker compose up --build
```

**With the orchestration script:**

```bash
# Single file (releases only)
python scripts/run_pipeline.py \
  --xml /path/to/releases.xml.gz \
  --library-db /path/to/library.db \
  --database-url postgresql://localhost:5432/discogs

# Directory (releases + optional artists/labels XMLs)
python scripts/run_pipeline.py \
  --xml /path/to/xml_dumps/ \
  --library-db /path/to/library.db \
  --database-url postgresql://localhost:5432/discogs
```

By default, `discogs-xml-converter` is expected on PATH. Use `--converter` to specify an alternate path. Use `--library-artists` to provide a pre-existing artist list instead of generating one from `library.db`.

When `--xml` points to a directory, the converter auto-detects XML files by their root element and processes them in order: artists first (to build alias maps), then labels, then releases. See [Artist Alias Filtering](#artist-alias-filtering) and [Sublabel Hierarchy](#sublabel-hierarchy) for details.

## Pipeline

All 8 steps are automated by `run_pipeline.py` (or [Docker Compose](#docker-compose)). The script supports two modes: full pipeline from XML, or database build from pre-filtered CSVs.

| Step | Script | Description |
|------|--------|-------------|
| 1. Generate artist list | `scripts/enrich_library_artists.py` | Extract artists from library.db, optionally enrich with cross-references |
| 2. Convert + Filter | `discogs-xml-converter` | XML to CSV with optional artist filtering (alias-enhanced when artists.xml is present) |
| 3. Create schema | `schema/create_database.sql`, `schema/create_functions.sql` | Set up tables, extensions, and functions |
| 4. Import | `scripts/import_csv.py` | Bulk load CSVs via psycopg COPY |
| 5. Create indexes | `schema/create_indexes.sql` | Accent-insensitive trigram GIN indexes for fuzzy search |
| 6. Deduplicate | `scripts/dedup_releases.py` | Keep best release per master_id (label match, US, most tracks) |
| 7. Prune/Copy | `scripts/verify_cache.py` | Remove non-library releases or copy matches to target DB |
| 8. Vacuum | `VACUUM FULL` | Reclaim disk space |

### Full Pipeline (--xml)

Runs steps 1-8. `--xml` accepts either a single XML file or a directory containing XML dumps. When `--library-db` is provided, the pipeline generates `library_artists.txt` automatically and uses it to filter during XML conversion:

```bash
# Single file (releases only)
python scripts/run_pipeline.py \
  --xml /path/to/releases.xml.gz \
  --library-db /path/to/library.db \
  --database-url postgresql://localhost:5432/discogs

# Directory (auto-detects releases, artists, labels XMLs)
python scripts/run_pipeline.py \
  --xml /path/to/xml_dumps/ \
  --library-db /path/to/library.db \
  --database-url postgresql://localhost:5432/discogs
```

When the directory contains `artists.xml.gz`, [alias-enhanced filtering](#artist-alias-filtering) is enabled automatically. When it contains `labels.xml.gz`, the converter produces `label_hierarchy.csv` which the pipeline uses for [sublabel-aware dedup](#sublabel-hierarchy).

To enrich the artist list with alternate names from the WXYC catalog database, add `--wxyc-db-url`:

```bash
python scripts/run_pipeline.py \
  --xml /path/to/xml_dumps/ \
  --library-db /path/to/library.db \
  --wxyc-db-url mysql://user:pass@host:port/wxycmusic \
  --database-url postgresql://localhost:5432/discogs
```

Use `--library-artists` to provide a pre-existing artist list instead of generating one from `library.db`.

### Database Build (--csv-dir)

Runs steps 3-8 from pre-filtered CSVs:

```bash
python scripts/run_pipeline.py \
  --csv-dir /path/to/filtered/ \
  --library-db /path/to/library.db \
  --database-url postgresql://localhost:5432/discogs
```

- `--library-db` is optional; if omitted, the prune step is skipped
- `--library-labels` accepts a pre-generated `library_labels.csv` for [label-aware dedup](#label-aware-dedup)
- `--database-url` defaults to `DATABASE_URL` env var or `postgresql://localhost:5432/discogs`

### Docker Compose

```bash
mkdir -p data
cp /path/to/releases.xml.gz data/
cp /path/to/library.db data/
# Optional: copy artists.xml.gz and labels.xml.gz for enhanced filtering/dedup
cp /path/to/artists.xml.gz data/
cp /path/to/labels.xml.gz data/

docker compose up --build
```

The Docker build compiles `discogs-xml-converter` from source in a Rust builder stage, so no local Rust toolchain is needed. When the `data/` directory contains artists.xml.gz and/or labels.xml.gz alongside releases.xml.gz, the converter automatically enables alias-enhanced filtering and produces label hierarchy data.

### Label-Aware Dedup

By default, dedup keeps the release with the most tracks per `master_id` group. When WXYC label preferences are available, dedup instead prefers the release whose Discogs label matches WXYC's known pressing -- ensuring the cached edition matches what the station actually owns.

Label preferences come from WXYC's `FLOWSHEET_ENTRY_PROD` MySQL table (rotation play entries include `LABEL_NAME`). The extraction script `scripts/extract_library_labels.py` produces a `library_labels.csv` with `(artist_name, release_title, label_name)` triples.

There are two ways to enable label-aware dedup:

1. **Automatic extraction** (when `--wxyc-db-url` is provided): the pipeline extracts labels from MySQL before the dedup step.

2. **Pre-generated CSV** (when `--library-labels` is provided): the pipeline uses the CSV directly, no MySQL connection needed.

```bash
# Automatic: extract labels from WXYC MySQL and use for dedup
python scripts/run_pipeline.py \
  --csv-dir /path/to/filtered/ \
  --library-db /path/to/library.db \
  --wxyc-db-url mysql://user:pass@host:port/wxycmusic \
  --database-url postgresql://localhost:5432/discogs

# Pre-generated: use an existing library_labels.csv
python scripts/run_pipeline.py \
  --csv-dir /path/to/filtered/ \
  --library-labels /path/to/library_labels.csv \
  --database-url postgresql://localhost:5432/discogs
```

The ranking order is: **label match** (prefer WXYC's pressing) > **US country** (domestic pressing) > **track count** (quality tiebreaker) > **release ID** (deterministic fallback).

### Artist Alias Filtering

When the converter processes a directory containing `artists.xml.gz` alongside `releases.xml.gz`, it uses Discogs artist aliases to improve filtering accuracy. For example, if WXYC's library has "Puff Daddy" but a Discogs release is credited to "P. Diddy" (an alias), alias-enhanced filtering catches the match using the `artist_id` as a join key.

The converter parses artists.xml first, building a map of `artist_id -> [aliases, name_variations]`. When filtering releases, each credited artist is checked by both canonical name and all known aliases. This is precise because `artist_id` links the release credit to the correct alias list, avoiding cross-artist false positives.

This is enabled automatically when `--xml` points to a directory containing `artists.xml.gz` and `--library-artists` is provided. No additional flags needed.

### Sublabel Hierarchy

When `labels.xml.gz` is present in the XML directory, the converter produces `label_hierarchy.csv` mapping sublabels to parent labels (e.g., Parlophone -> EMI). The pipeline uses this during dedup for bidirectional sublabel resolution:

- If WXYC says "EMI" but Discogs has "Parlophone" (a sublabel of EMI), the label match is recognized
- If WXYC says "Parlophone" but Discogs has "EMI" (the parent), the label match is also recognized

This is one level of parent resolution. Discogs label hierarchies are generally shallow; multi-level resolution can be added later if needed.

For `--csv-dir` mode, you can pass a pre-generated hierarchy file via `--label-hierarchy`:

```bash
python scripts/run_pipeline.py \
  --csv-dir /path/to/filtered/ \
  --library-labels /path/to/library_labels.csv \
  --label-hierarchy /path/to/label_hierarchy.csv \
  --database-url postgresql://localhost:5432/discogs
```

### Copy to Target Database

Instead of pruning releases in place (which destroys the full imported dataset), you can copy only matched releases to a separate target database:

```bash
python scripts/run_pipeline.py \
  --csv-dir /path/to/filtered/ \
  --library-db /path/to/library.db \
  --database-url postgresql://localhost:5432/discogs \
  --target-db-url postgresql://localhost:5432/discogs_cache
```

This preserves the full `discogs` database and creates a lean `discogs_cache` database with only KEEP and REVIEW releases, complete with schema, FK constraints, and trigram indexes. The target database is created automatically if it doesn't exist.

You can also use `--copy-to` directly with `verify_cache.py`:

```bash
python scripts/verify_cache.py \
  --copy-to postgresql://localhost:5432/discogs_cache \
  /path/to/library.db \
  postgresql://localhost:5432/discogs
```

`--copy-to` and `--prune` are mutually exclusive.

### Resuming a Failed Pipeline

If a pipeline run fails mid-way (e.g., disk full during index creation), you can resume from where it left off instead of restarting from scratch:

```bash
python scripts/run_pipeline.py \
  --csv-dir /path/to/filtered/ \
  --library-db /path/to/library.db \
  --database-url postgresql://localhost:5432/discogs \
  --resume
```

The pipeline tracks step completion in a JSON state file (default: `.pipeline_state.json`). On resume, completed steps are skipped. You can specify a custom state file path with `--state-file`:

```bash
python scripts/run_pipeline.py \
  --csv-dir /path/to/filtered/ \
  --resume \
  --state-file /tmp/my_pipeline_state.json
```

If no state file exists when `--resume` is used, the pipeline infers completed steps from database state (e.g., schema exists, tables have rows, indexes present, `master_id` column dropped by dedup).

`--resume` is only valid with `--csv-dir` mode, not `--xml` mode.

### Running Steps Manually

Individual steps can also be run directly:

```bash
# 1. Convert XML to CSV (with optional artist filtering)
# Single file (releases only)
discogs-xml-converter /path/to/releases.xml.gz --output-dir /path/to/csv/
# Directory (auto-detects releases, artists, labels XMLs)
discogs-xml-converter /path/to/xml_dumps/ --output-dir /path/to/csv/ \
  --library-artists /path/to/library_artists.txt

# 2. Create schema and functions
psql -d discogs -f schema/create_database.sql
psql -d discogs -f schema/create_functions.sql

# 3. Import CSVs
python scripts/import_csv.py /path/to/csv/ [database_url]

# 4. Create indexes (10-30 min on large datasets)
psql -d discogs -f schema/create_indexes.sql

# 5. Deduplicate (optionally with label matching and sublabel hierarchy)
python scripts/dedup_releases.py [database_url]
python scripts/dedup_releases.py --library-labels /path/to/library_labels.csv [database_url]
python scripts/dedup_releases.py --library-labels /path/to/library_labels.csv \
  --label-hierarchy /path/to/label_hierarchy.csv [database_url]

# 6. Prune (dry run first, then with --prune or --copy-to)
python scripts/verify_cache.py /path/to/library.db [database_url]
python scripts/verify_cache.py --prune /path/to/library.db [database_url]
# Or copy to a target database instead:
python scripts/verify_cache.py --copy-to postgresql:///discogs_cache /path/to/library.db [database_url]

# 7. Vacuum
psql -d discogs -c "VACUUM FULL;"
```

## Database Schema

The schema files in `schema/` define the shared contract between this ETL pipeline and all consumers.

### Tables

| Table | Description |
|-------|-------------|
| `release` | Release metadata: id, title, release_year, country, artwork_url |
| `release_artist` | Artists on releases (main + extra credits), with optional Discogs `artist_id` |
| `release_label` | Label names per release (e.g., Parlophone, Factory Records) |
| `release_track` | Tracks on releases with position and duration |
| `release_track_artist` | Artists on specific tracks (for compilations) |
| `cache_metadata` | Data freshness tracking (cached_at, source) |

### Indexes

- Foreign key indexes on all child tables
- Accent-insensitive trigram GIN indexes (`pg_trgm` + `unaccent`) on `title` and `artist_name` columns for fuzzy text search. Uses an immutable `f_unaccent()` wrapper to enable index expressions with `lower(f_unaccent(column))`.
- Cache metadata indexes for freshness queries

### Consumer Integration

Consumers connect via the `DATABASE_URL_DISCOGS` environment variable:

```
DATABASE_URL_DISCOGS=postgresql://user:pass@host:5432/discogs
```

Current consumers:
- **library-metadata-lookup** (`discogs/cache_service.py`) - Python/asyncpg
- **Backend-Service** - TypeScript/Node.js (planned)

## Testing

Tests are organized into three layers:

```bash
# Unit tests (no external dependencies, run by default)
pytest tests/unit/ -v

# Integration tests (needs PostgreSQL)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m postgres -v

# MySQL integration tests (needs WXYC MySQL on port 3307)
pytest -m mysql -v

# E2E tests (needs PostgreSQL, runs full pipeline as subprocess)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m e2e -v
```

Integration and E2E tests are excluded by default (`pytest` with no args runs only unit tests). Start the test database with:

```bash
docker compose up db -d
```

## Migrations

The `migrations/` directory contains historical one-time migrations:

- `01_optimize_schema.sql` - Initial schema optimization (drops unused tables/columns, adds artwork_url and release_year, deduplicates by master_id). Already applied to the production database.

## Documentation

- [Cache Technical Overview](docs/discogs-cache-technical-overview.md) -- design rationale, benchmarks, and pipeline architecture details
