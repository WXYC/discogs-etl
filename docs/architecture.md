# Architecture

## Pipeline Steps

1. **Download** Discogs monthly data dumps (XML) from https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html
2. **(Optional, standalone) Enrich** `library_artists.txt` with WXYC cross-references (via `wxyc-enrich-library-artists` CLI from wxyc-catalog). Only useful when the operator wants the converter's artist-only filter (`--library-artists`); the pair-wise path on `--library-db` skips this step.
3. **Convert and filter** XML to CSV using [discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter) (Rust binary). Accepts a single XML file or a directory containing releases.xml, artists.xml, and labels.xml. The converter applies one of two release filters inside its streaming scanner so disk only ever holds the kept rows: `--library-artists library_artists.txt` for the artist-only filter (~4M kept), or `--library-db library.db` for the pair-wise (artist, title) filter (~50K kept; what the monthly rebuild uses so the import step fits on Railway-sized destination DBs without overflowing `COPY release_artist`, see #128). Pair-wise normalization is diacritic-insensitive on both sides; known false negatives are compound-artist library entries like "Duke Ellington & John Coltrane" whose Discogs releases split into separate `release_artist` rows. When artists.xml is present, alias-enhanced filtering is enabled automatically. When labels.xml is present, `label_hierarchy.csv` is produced for sublabel-aware dedup.
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
- `--xml` mode: runs steps 3-10 (convert+filter, database build through SET LOGGED). `--xml` accepts a single file or a directory. Step 2 (enrich) is no longer part of the orchestrator — operators who want the artist-only filter run `wxyc-enrich-library-artists` separately and pass the result via `--library-artists`.
- `--csv-dir` mode: runs steps 4-10 (database build from pre-filtered CSVs)

Both modes support `--target-db-url` (deprecated, see below) to copy matched releases to a separate database instead of pruning in place, and `--resume` (csv-dir only) to skip already-completed steps. `--keep-csv` (xml mode only) writes converted CSVs to a persistent directory instead of a temp dir, so they survive pipeline failures.

## Cache database CLI convention

discogs-etl follows the shared cache-builder CLI convention defined in `wxyc-etl::cli` (Rust) and mirrored here in Python:

| Flag / env | Status | Notes |
|---|---|---|
| `--database-url` | canonical | PostgreSQL URL for the cache database. |
| `DATABASE_URL_DISCOGS` | canonical | Service-specific env fallback; preferred over `DATABASE_URL`. |
| `DATABASE_URL` | deprecated fallback | Still works; emits a stderr warning that `DATABASE_URL_DISCOGS` is preferred. |
| `--target-db-url` | deprecated | Still functional but emits a stderr warning. The cache convention is consolidating on a single `--database-url`. |

Resolution order for `--database-url`: explicit flag > `DATABASE_URL_DISCOGS` > `DATABASE_URL` (deprecated) > `postgresql://localhost:5432/discogs`.

Step 1 (download) is always manual.

## master_id Column Lifecycle

The `release` table includes a `master_id` column populated during import (links a release to its Discogs "master" — the conceptual album, distinct from any specific pressing/edition). It is used during dedup (`PARTITION BY master_id, format` in `ensure_dedup_ids`) and persists through the dedup copy-swap so consumers can group editions of the same album. NULL is allowed (singles, demos, and obscure pressings often lack a master).

The dedup `CREATE TABLE new_release AS SELECT ... FROM release` SELECT list at `scripts/dedup_releases.py` (`DEDUP_TABLES` module constant) must include `master_id` for the column to survive the swap. Tests in `tests/integration/test_dedup.py::TestDedupCopySwapPreservesMasterId` pin this — they import `DEDUP_TABLES` from the production module rather than mirroring it, so the test cannot drift from production.

The `country` column behaves the same way — listed in the dedup SELECT and therefore permanent.

## artwork_checked_at Column Lifecycle

The `release` table includes an `artwork_checked_at timestamptz` column (nullable, no default) added by alembic [0008](https://github.com/WXYC/discogs-etl/issues/239). It distinguishes the two states `artwork_url IS NULL` can mean:

- **Never asked** (`artwork_checked_at IS NULL`): the bulk loader / converter never had artwork data for this row; LML has not looked it up yet either. The row falls through to a Discogs API call on the next LML lookup.
- **Asked, genuinely no image** (`artwork_checked_at IS NOT NULL` AND `artwork_url IS NULL`): LML hit the Discogs API and the release legitimately has no cover. Treated as a cache hit by LML's predicate — no further API calls.

The bulk loader (`import_csv.py:import_artwork`) leaves the column NULL on every row it writes. LML's runtime path stamps `artwork_checked_at = NOW()` in `write_release` whenever the cache is back-filled from a live Discogs API response — see [WXYC/library-metadata-lookup#423](https://github.com/WXYC/library-metadata-lookup/issues/423) for the predicate change that makes the column load-bearing.

The column survives the dedup + prune-copy-swap paths via the SELECT lists in `scripts/dedup_releases.py:DEDUP_TABLES`, `scripts/verify_cache.py:PRUNE_COPY_TABLES`, and `scripts/verify_cache.py:COPY_TABLE_SPEC` — same convention as `master_id`. Regression pins at `tests/integration/test_dedup.py::TestDedupCopySwapPreservesMasterId::test_artwork_checked_at_column_persists_after_copy_swap` and `tests/integration/test_verify_cache_columns.py::TestPruneCopyTablesCoversSchema::test_release_keeps_artwork_checked_at` prevent silent drops on future SELECT-list edits.

Partial index `release_artwork_null_idx ON release (id) WHERE artwork_url IS NULL AND artwork_checked_at IS NULL` covers the never-asked tail for [WXYC/library-metadata-lookup#221](https://github.com/WXYC/library-metadata-lookup/issues/221)'s top-up drain — without the index that scan would seq-scan the full `release` table (~82K rows in prod as of 2026-05-29).

## Artwork Preservation Across Rebuilds

The monthly rebuild is incremental by default: `release.artwork_url` and `release.artwork_checked_at` that LML's runtime back-patched between rebuilds (see [WXYC/library-metadata-lookup#423](https://github.com/WXYC/library-metadata-lookup/issues/423)) survive the next rebuild. The plumbing is two structural changes (per [#242](https://github.com/WXYC/discogs-etl/issues/242)):

1. `schema/create_database.sql` is `CREATE TABLE IF NOT EXISTS`-only — safe to apply against a populated DB. The destructive `DROP TABLE … CASCADE` block moved to `schema/drop_core_tables.sql`, invoked only by `--fresh-rebuild`.
2. `scripts/import_csv.py:import_release_via_upsert` reloads `release` via staging-table COPY + UPSERT with `artwork_url` + `artwork_checked_at` excluded from the SET list. Releases that fall out of the new dump are pruned via `DELETE … WHERE id NOT IN staging`; FK `ON DELETE CASCADE` removes their child rows. Child tables of `release` (`release_artist` + siblings, plus `cache_metadata`) are TRUNCATEd before re-COPY so duplicates don't accumulate.
3. `scripts/import_csv.py:import_artwork` also stamps `artwork_checked_at = now()` whenever it sets `artwork_url` from the dump — matches the semantics LML's runtime `write_release` applies (LML#423) so freshly-imported rows aren't treated as "never asked" on the first lookup.

Semantics matrix for `scripts/run_pipeline.py`:

| Flag | Schema | Data | LML back-patches |
| --- | --- | --- | --- |
| (default) | preserved (`CREATE TABLE IF NOT EXISTS` no-ops) | upserted; children TRUNCATE+re-COPY | preserved |
| `--truncate-existing` | preserved | wiped via `TRUNCATE CACHE_TABLES_TO_TRUNCATE_BASE` | wiped (re-COPY into empty tables) |
| `--fresh-rebuild` | dropped + recreated via `drop_core_tables.sql` | wiped via `DROP CASCADE` | wiped |

**Dead-URL edge case the issue calls out**: if a release's image is taken down on Discogs between rebuilds, `release_image.csv` omits the row, the `import_artwork` UPDATE skips the release, and the prior LML-back-patched URL is preserved (potentially a dead URL). LML's runtime path will eventually 404 on the URL and re-fetch, back-patching with the new state. Strictly better than today's "wipe to NULL → serve placeholder" behavior.

**Plan**: [`WXYC/wiki/plans/discogs-etl-242-rebuild-coalesce.md`](https://github.com/WXYC/wiki/blob/main/plans/discogs-etl-242-rebuild-coalesce.md) (PR [wiki#75](https://github.com/WXYC/wiki/pull/75)). Regression coverage: `tests/integration/test_import.py::TestImportArtworkPreservation` (the 5-case acceptance grid), `tests/integration/test_rebuild_idempotent.py` (--truncate-existing + --fresh-rebuild semantics), `tests/integration/test_schema.py::TestSchemaIdempotenceDriftGuards` (drift guards on the schema split).

## format Column Lifecycle

The `format` column stores the normalized format category (Vinyl, CD, Cassette, 7", Digital). Unlike `master_id`, `format` persists after dedup and is available to consumers. During import, raw Discogs format strings are normalized via `lib/format_normalization.py` (e.g., "2xLP" → "Vinyl", "CD-R" → "CD"). During dedup, releases are partitioned by `(master_id, format)`, so a CD and Vinyl pressing of the same album both survive. During verify/prune, format-aware matching ensures only releases whose format matches the library's are kept (for exact artist+title matches). NULL format on either side is treated as "match anything" for backward compatibility.

## Database Schema (Shared Contract)

The SQL files in `schema/` define the contract between this ETL pipeline and all consumers:

- `schema/create_database.sql` -- Tables: `release`, `release_artist`, `release_track`, `release_track_artist`, `cache_metadata`; extensions: pg_trgm, unaccent
- `schema/create_functions.sql` -- `f_unaccent()` immutable wrapper for accent-insensitive index expressions
- `schema/create_indexes.sql` -- Trigram GIN indexes for accent-insensitive fuzzy text search (pg_trgm + unaccent)

Consumers connect via `DATABASE_URL_DISCOGS` environment variable.

## Docker Compose

`docker-compose.yml` provides a self-contained environment:
- **`db`** service: `ghcr.io/wxyc/wxyc-postgres:pg16` (PG 16 + `pg_trgm` + `unaccent` + `wxyc_unaccent.rules` baked into `$SHAREDIR/tsearch_data/`), port 5433:5432. The wxyc-postgres image is built + published from [WXYC/wxyc-etl#127](https://github.com/WXYC/wxyc-etl/issues/127); operator runbook at [`docs/wxyc-postgres-image.md`](https://github.com/WXYC/wxyc-etl/blob/main/docs/wxyc-postgres-image.md). Same image is mirrored to Railway production (one-time service-source swap, tracked in the runbook) so destinations have the rules file the alembic 0007 dictionary gate expects.
- **`pipeline`** service: runs `scripts/run_pipeline.py` against the db

```bash
docker compose up --build   # full pipeline (needs data/ directory, builds Rust converter in Docker)
docker compose up db -d     # just the database (for tests)
```

## Key Files

- `scripts/run_pipeline.py` -- Pipeline orchestrator (--xml for steps 2-9, --csv-dir for steps 4-9)
- `scripts/filter_csv.py` -- Filter Discogs CSVs against the WXYC library. Two modes: (default) artist-only, takes `library_artists.txt`; (`--library-db`) pair-wise on `(artist, title)` against a SQLite library.db. Both filters now also exist on the Rust converter side (`discogs-xml-converter --library-artists` / `--library-db`); the converter applies them inside the streaming scanner so disk never holds the unfiltered output. This script remains as the Python parity reference (the converter's `tests/parity_test.rs` invokes it) and as a standalone tool for filtering pre-staged CSVs. Not on the rebuild-cache.sh path.
- `scripts/import_csv.py` -- Import CSVs into PostgreSQL (psycopg COPY). Child tables are imported in parallel via ThreadPoolExecutor after parent tables. Artist detail tables (artist_alias, artist_member) are filtered to known artist IDs to prevent FK violations, since the converter's CSVs contain all Discogs artists. Tables with `unique_key` configs are deduped in-memory during COPY.
- `scripts/dedup_releases.py` -- Deduplicate releases by master_id, preferring label match + sublabel resolution, US releases (copy-swap with `DROP CASCADE`). Index/constraint creation is parallelized via ThreadPoolExecutor.
- `scripts/verify_cache.py` -- Multi-index fuzzy matching for KEEP/PRUNE classification; `--copy-to` streams matches to a target DB. Phase 4 (fuzzy matching) has two paths: when `wxyc-etl` is installed, `batch_classify_releases()` runs all scoring in Rust with rayon parallelism; otherwise, falls back to ProcessPoolExecutor with rapidfuzz. Set `WXYC_ETL_NO_RUST=1` to force the Python fallback. Large prune sets (>10K IDs) use copy-and-swap instead of CASCADE DELETE.
- `scripts/csv_to_tsv.py` -- CSV to TSV conversion utility
- `scripts/fix_csv_newlines.py` -- Fix multiline CSV fields
- `lib/format_normalization.py` -- Normalize raw Discogs/library format strings to broad categories (Vinyl, CD, Cassette, 7", Digital)
- `scripts/sync-library.sh` -- Daily library sync orchestrator: MySQL query (via MariaDB `mysql` CLI for MySQL 4.1 compat) → `tsv_to_sqlite.py` → streaming links enrichment → upload to LML. Automated by `.github/workflows/sync-library.yml` (daily at noon UTC).
- `scripts/tsv_to_sqlite.py` -- Converts MySQL TSV output to SQLite with FTS5 index. Called by sync-library.sh.
- `scripts/check_cache_drift.py` -- Drift watchdog: compares `COUNT(DISTINCT artist) FROM library` (sqlite) to `COUNT(DISTINCT artist_name) FROM release_artist` (cache). Exits non-zero (and posts to `SLACK_MONITORING_WEBHOOK` when set) if the ratio falls below `--min-ratio` (default 0.7). Run as the final step of `rebuild-cache.sh` so coverage regressions surface as alerts.
- `scripts/cache_health_metrics.py` -- Publishes the artwork-state decomposition of `release` to CloudWatch under `WXYC/DiscogsCache`: `release_count`, `artwork_never_asked_count` (`artwork_url IS NULL AND artwork_checked_at IS NULL` — drainable by LML#221), and `artwork_imageless_count` (`artwork_url IS NULL AND artwork_checked_at IS NOT NULL` — genuinely no image, unfixable). Wired into `sync-library.yml` so the daily noon-UTC tick produces a time series. The alarm — "never_asked_count stagnates above 30% of release_count for 7 days" — is configured out-of-band via `aws cloudwatch put-metric-alarm` since it's stateful operator config rather than per-deploy code. Per [#241](https://github.com/WXYC/discogs-etl/issues/241).
- `scripts/wxyc_library_parity_check.py` / `scripts/wxyc_library_explain_analyze.py` -- Operator helpers for the v2 `wxyc_library` hook (E1 §4.1.4). The parity-check script runs the extended legacy-vs-new comparison (auto-falls-back to text comparison when `wxyc_norm_artist()` is not yet deployed); the EXPLAIN ANALYZE harness runs the top-5 LML query patterns. Both read `DATABASE_URL_DISCOGS` and are intended for the dual-write window + cutover gate on the full Homebrew cache (port 5432).
- `scripts/rebuild-cache.sh` -- EC2 wrapper: pulls latest discogs-etl + discogs-xml-converter, downloads library.db from LML release artifact, spools the Discogs dump from data.discogs.com to disk via `curl --continue-at - --retry-all-errors` (resumable on mid-stream HTTP/2 resets, see #181), then runs `run_pipeline.py --xml ... --library-db ...` against the spooled file (the converter does the pair-wise narrowing inside its scanner; no separate post-pass), then the drift watchdog. Invoked by `scripts/rebuild-cache-bootstrap.sh` on the spawned EC2 (current path); also still runnable as a standalone cron on a long-lived host (legacy).
- `scripts/rebuild-cache-bootstrap.sh` -- runs as user-data on the ephemeral EC2 spawned by the `wxyc-discogs-rebuild` SAM stack (`infra/ephemeral-rebuild/`). Installs deps, clones the converter, sources secrets from SSM, execs `rebuild-cache.sh`, uploads logs to S3, runs `shutdown -h now`. Setup runbook at `infra/ephemeral-rebuild/README.md`.
- `docs/discogs-etl-technical-overview.md` -- Design rationale, benchmarks, and pipeline architecture details

## Shared Package Dependencies

Functionality that was previously local to this repo has been extracted to shared packages:

- **wxyc-etl** (Rust/PyO3) -- Artist name normalization (`to_match_form`; legacy `normalize_artist_name` is `#[deprecated]` per the WX-2 Normalizer Charter), compilation detection (`is_compilation_artist`), artist name splitting (`split_artist_name`, `split_artist_name_contextual`), pipeline state tracking (`PipelineState`), and database introspection.
- **wxyc-catalog** -- Catalog source protocol (`CatalogSource`, `TubafrenzySource`, `BackendServiceSource`), library.db export (`wxyc-export-to-sqlite` CLI), library artist enrichment (`wxyc-enrich-library-artists` CLI), and label extraction (`wxyc-extract-library-labels` CLI).

## External Inputs

### Library Catalog (library.db)

`library.db` is a SQLite export of the WXYC library catalog, generated by the `wxyc-export-to-sqlite` CLI from the wxyc-catalog package. The default source is tubafrenzy's MySQL database on Kattare (via SSH), but Backend-Service's PostgreSQL database can be used instead via `--catalog-source backend-service --catalog-db-url <url>`. The `--catalog-source` flag is supported by `run_pipeline.py` and the wxyc-catalog CLI tools. The legacy `--wxyc-db-url` flag is an alias for `--catalog-source tubafrenzy --catalog-db-url <url>`.

`library.db` is used as input throughout the pipeline:

1. **`library_artists.txt`** -- Generated from `library.db` by `wxyc-enrich-library-artists` (wxyc-catalog CLI), one artist name per line, used by `discogs-xml-converter --library-artists` for filtering
2. **KEEP/PRUNE classification** -- `scripts/verify_cache.py` uses `library.db` to match cached releases against the WXYC catalog

`scripts/sync-library.sh` orchestrates the daily sync: query Kattare MySQL via the MariaDB `mysql` CLI (required for MySQL 4.1's old-format password hashes), convert TSV to SQLite via `scripts/tsv_to_sqlite.py`, enrich with streaming links from `streaming_availability.db` (a [GitHub Release artifact](https://github.com/WXYC/library-metadata-lookup/releases/tag/streaming-data-v1) in library-metadata-lookup, refreshed weekly), then upload to LML staging and production via `POST /admin/upload-library-db`. The Discogs pipeline can also generate `library.db` inline via `--generate-library-db` using the wxyc-catalog CLI.
