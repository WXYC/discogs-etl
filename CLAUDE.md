# Claude Code Instructions for discogs-etl

ETL pipeline for building and maintaining a PostgreSQL cache of Discogs release data, filtered to artists in the WXYC radio library catalog. The cache database is a shared resource consumed by multiple services (library-metadata-lookup runtime, Backend-Service).

## Topic guides

CLAUDE.md is a router for the always-loaded reference card. Topic depth lives in `docs/`:

- **[`docs/architecture.md`](docs/architecture.md)** — Pipeline steps (1-10), cache-database CLI convention (`--database-url` / `DATABASE_URL_DISCOGS` / `DATABASE_URL`), `master_id` / `artwork_checked_at` / `format` column lifecycles, artwork preservation across rebuilds (incremental upsert default, `--truncate-existing`, `--fresh-rebuild`), shared schema contract, Docker Compose, Key Files, shared package deps (`wxyc-etl`, `wxyc-catalog`), `library.db` external input
- **[`docs/migrations.md`](docs/migrations.md)** — Alembic layout + per-revision history (0001 baseline through 0013 `entity.identity` adoption), dual-write convention, one-shot stamp procedure, defensive guards in `0001_initial.py` (`is_offline_mode()` + schema-presence short-circuit), shared `lib/alembic_helpers.py` (`refuse_offline` / `resolve_db_url`) used by 0010 / 0011 / 0012 / 0013, deploy wiring via `rebuild-cache.yml`
- **[`docs/testing.md`](docs/testing.md)** — Architecture-A marker conventions (`pg`, `slow`), pytest commands per marker combo, CI jobs (`lint`, `test`, `pg`, `marker-sync`), fixture regeneration
- **[`docs/automation.md`](docs/automation.md)** — Monthly cache rebuild (`rebuild-cache.yml`, EC2 dispatch, secrets table, failure interpretation), library sync (`sync-library.yml`, secrets, daily noon UTC cadence)
- **[`docs/observability.md`](docs/observability.md)** — `lib.observability` shim, JSON logger contract tags (`repo`, `tool`, `step`, `run_id`), `SENTRY_DSN` wiring, list of scripts that init the logger
- **[`docs/test-fixtures.md`](docs/test-fixtures.md)** — Inline fixture data (CSV row examples for `release`, `release_artist`, `release_label`, `release_track`, `release_track_artist`, `library_artists.txt`, SQLite `library`) keyed off the canonical WXYC example artists

Operator-facing runbooks live alongside in `docs/`: [`migrations-runbook.md`](docs/migrations-runbook.md), [`ec2-rebuild-runbook.md`](docs/ec2-rebuild-runbook.md), [`topup-artwork-runbook.md`](docs/topup-artwork-runbook.md), [`discogs-etl-technical-overview.md`](docs/discogs-etl-technical-overview.md), [`plan-223-wxyc-unaccent-railway-fix.md`](docs/plan-223-wxyc-unaccent-railway-fix.md), [`plan-multi-artist-splitting.md`](docs/plan-multi-artist-splitting.md), [`plan-298-empty-child-index-recovery.md`](docs/plan-298-empty-child-index-recovery.md).

Read the relevant topic doc before doing work in that area.

## Pipeline Lifecycle

This pipeline runs monthly (or when Discogs publishes new data dumps). It has a completely different lifecycle from the request-handling services that consume its output.

## TDD (Required)

All code changes in this repo follow test-driven development. This is not optional.

1. **Red**: Write a failing test that describes the desired behavior. Run the test and confirm it fails for the right reason.
2. **Green**: Write the minimum implementation to make the test pass. Run the test and confirm it passes.
3. **Refactor**: Look for opportunities to improve the code while keeping tests green. Re-run tests after each change.
4. **Repeat**: Continue the cycle until the feature is complete.

**Key principle**: No production code without a failing test first.

## Code Style

- Line length: 100 chars
- Use `ruff` for linting
- Python 3.11+

## Example Music Data for Tests

WXYC is a freeform station — use representative artists (Stereolab, Juana Molina, Jessica Pratt, Cat Power, Chuquimamani-Condori, Duke Ellington & John Coltrane) rather than mainstream acts. Full canonical fixture tables (CSV rows keyed to fixed `release_id`s, `library_artists.txt`, SQLite `library` rows) live in [`docs/test-fixtures.md`](docs/test-fixtures.md). Canonical org-wide data source: `wxyc-shared/src/test-utils/wxyc-example-data.json`.

## Relationship to Other Repos

- **library-metadata-lookup** (Python/FastAPI) -- `discogs/cache_service.py` queries the cache for album lookups
- **Backend-Service** (TypeScript/Node.js) -- future consumer for Discogs data
- **[discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter)** -- Rust binary for XML-to-CSV conversion; applies the pair-wise / artist-only release filter inside its streaming scanner
- **[wxyc-etl](https://github.com/WXYC/wxyc-etl)** -- Rust/PyO3 shared library: artist name normalization, compilation detection, artist splitting, pipeline state tracking, DB introspection. Vendored canonical artifacts via `wxyc-etl-pin.txt`
- **[wxyc-catalog](https://github.com/WXYC/wxyc-catalog)** -- Catalog source protocol (tubafrenzy + Backend-Service backends), `wxyc-export-to-sqlite` / `wxyc-enrich-library-artists` / `wxyc-extract-library-labels` CLIs
- **[wxyc-shared](https://github.com/WXYC/wxyc-shared)** -- Cross-repo test-utility fixtures (canonical example artists)

## `entity.*` schema ownership

The `entity.*` schema in the cache database is **owned by this repo** and is created/migrated **only** through alembic (`alembic/versions/0012_entity_release_identity.py`, `0013_adopt_entity_identity.py`). It holds the cross-service identity contract (`entity.identity`, `entity.release_identity`, reconciliation logs) read by LML, Backend-Service, and semantic-index. Per [discogs-etl#288](https://github.com/WXYC/discogs-etl/issues/288) (Option 3):

- **discogs-etl does not adopt LML cache tables.** LML owns its application caches in a separate `lml_cache.*` schema that it lifespan-bootstraps itself; this repo never migrates, truncates, backs up, or audits `lml_cache.*`. (The earlier pattern of LML creating `entity.*` cache tables and promising a post-hoc adoption migration — which went unfiled after LML PR #571 — is retired.)
- **The truncate guard stays.** `scripts/import_csv.py` (`_validate_truncate_lists`) rejects any schema-qualified entry in the truncate lists, so neither `entity.*` nor `lml_cache.*` can be reached by a public-schema cache rebuild. Keep it.
- New `entity.*` columns/tables (e.g. LML#573's PR-3 `deezer_album_id` on `entity.release_identity`) land here via alembic first, then LML pulls the contract — the three-repo sequence in [WXYC/wiki#83](https://github.com/WXYC/wiki/issues/83).
