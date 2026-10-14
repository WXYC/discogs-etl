# Seed cache from clone — operator runbook

`scripts/seed_cache_from_clone.py` **additively** copies an artist-scoped release row-set from a full, unfiltered Discogs clone into an existing (populated) target discogs-cache. It is a one-shot operator action, not a pipeline step and not on a schedule.

Motivation: [Backend-Service#1631](https://github.com/WXYC/Backend-Service/issues/1631)'s Apple-Music-URL backfill stalls on a finite "cold tail" of non-library albums whose Discogs releases the library filter never admitted to prod discogs-cache. On a miss LML falls through to the live Discogs API + cold path (>15s), times out, and trips the health watchdog. A full local clone already holds those releases in the right shape; this seeds exactly those into prod so LML resolves them as fast cache hits and the paused backfill drains normally. See `plans/bs1631-tail-cache-seed.md` for the full design.

## When to run

When a bounded, known set of releases needs to exist in a populated cache and a full rebuild is not warranted. This is **not** the mechanism for widening the cache filter to open-ended demand (rejected as low-ROI). The target is a specific release_id list produced upstream.

Do not run while a bulk job is hammering LML on the shared Discogs token — this seed does not call Discogs (clone → PG copy only), but the downstream backfill it unblocks does.

## Guarantees (why it is safe against a populated prod cache)

- **Additive only.** `release` (id PK), `cache_metadata` (release_id PK), and `artist` (id PK) insert with `ON CONFLICT DO NOTHING`. The arbiter-less child tables (`release_artist`, `release_label`, `release_genre`, `release_style`, `release_track`, `release_track_artist`, `release_video`) have no PK/UNIQUE, so idempotency is driven off the parent: children are inserted only for the **new-parent-id set** = (requested ids) − (ids already on the target). A parent id is in that set only if it was absent, so its children were necessarily absent too — no duplicate rows, fully re-runnable. A `pg_advisory_xact_lock` (key `330001`, registered in this repo's `CLAUDE.md` "Advisory lock keys" table) spans the new-id computation through the write for every real run as a backstop against two overlapping invocations racing into duplicate child rows. In practice the rebuild lock (below) keeps them apart first: a real seed holds `354001` for its whole write, so a second overlapping real seed against the same target refuses with exit `75` rather than waiting. Re-run it once the first finishes.
- **Refuses to overlap a rebuild.** A real (non-dry-run) run takes the rebuild lock (`pg_try_advisory_lock(354001)`, `lib/rebuild_lock.py`) on `--target` and holds it for the whole write; if it is already held (by a rebuild, `dedup_artist_children.py`, an operator session, or another real seed), the seed logs a warning naming the lock and exits `75` without writing anything. Re-run once the holder finishes. `--dry-run` takes no lock and works during a rebuild. Without this, a seed could write rows the rebuild's import TRUNCATE and prune then drop, or interleave with the api_fetch restore into duplicate child rows.
- **Never overwrites.** No UPDATE, no DELETE, no TRUNCATE. Existing prod rows are byte-identical before and after.
- **Column-drift tolerant.** Column lists come from `verify_cache.COPY_TABLE_SPEC` (the canonical source of truth) intersected with the columns actually present on both databases. The clone predates `release.artwork_checked_at` / `release.not_found` / `artist.not_found` and has no `release_video` table; those simply fall out of the copy and default on the target. `artist.fetched_at` (NOT NULL on prod) is `COALESCE`d to `now()` on the copy in case the clone carries a NULL.
- **`--source` is genuinely read-only.** The source connection only ever runs `SELECT` / `COPY … TO STDOUT`; the new-id set is embedded as an `ANY(ARRAY[...]::integer[])` literal in each query rather than staged into a source-side temp table, so `--source` works against a strictly read-only role or a hot standby.
- **`verify_cache.py --prune` is prohibited against the seeded rows.** It classifies releases against WXYC library membership and DELETEs non-matches — tail artists are non-library by definition, so it would delete exactly what was seeded. Post-seed verification is a separate read-only integrity check.

## Inputs

| Flag | Required | Notes |
|---|---|---|
| `--source` | yes | Clone DSN, read-only (`SELECT` / `COPY … TO STDOUT` only — the script never writes to `--source`). e.g. `postgresql://localhost:5432/discogs`. |
| `--target` | yes | Target discogs-cache DSN. For prod, the Railway `Postgres` service `DATABASE_PUBLIC_URL` (reading prod requires explicit approval; writing is heavier — gate on a green dry-run first). Must differ from `--source`; the script refuses an identical `--source`/`--target` pair (a transposed pair would otherwise silently no-op). |
| `--ids-file` | yes | Release_ids (or, with `--seed-artists`, artist_ids) to seed, one per line (blank lines and `#` comments ignored). Selected upstream via LML's `lower(f_unaccent(artist_name)) % $artist` trigram predicate over the tail artists — artist-scoped (keep all releases by a tail artist), not album-scoped. |
| `--seed-artists` | no | Seed the `artist` family (see "Artist seeding" below) from `--ids-file` instead of the default `release` family. |
| `--preserve-provenance` | no | Release family only. For each newly seeded release, copy `cache_metadata.source` and `cached_at` from `--source` when the source row exists and both are non-NULL; otherwise fall back to the default fresh `bulk_import` row. Use when `--source` is a prod export whose provenance is reliable (e.g. restoring `api_fetch` rows); leave off for a clone, whose freshness rows are unreliable. Still additive (`ON CONFLICT DO NOTHING`); `--dry-run` logs how many rows would preserve vs fall back. |
| `--dry-run` | no | Report per-table row-set sizes without writing. **Always run first.** |

The script depends only on `psycopg` and `lib.observability` (installable via `pip install -e .`), imports `COPY_TABLE_SPEC` from `verify_cache`, and imports `parse_keep_release_ids` from `lib.keep_release_ids` for `--ids-file` parsing.

## Procedure

1. **Select the tail release_ids** upstream (LML `%` trigram over the distinct tail artists from `wxyc_schema.album_metadata`) and write them to `tail_release_ids.txt`.
2. **Dry-run against a scratch prod-shaped target** to confirm per-table row-set sizes look sane:
   ```bash
   python scripts/seed_cache_from_clone.py \
     --source postgresql://localhost:5432/discogs \
     --target postgresql://localhost:5433/discogs_seed_scratch \
     --ids-file tail_release_ids.txt \
     --dry-run
   ```
3. **Dry-run against prod** (still no writes) to see how many candidates are genuinely new vs already present.
4. **Seed prod** (drop `--dry-run`). The write is one transaction per family; per-table inserted counts are logged.
5. **Verify (read-only).** Referential-integrity check over the seeded release_ids (every child has a parent `release`; no orphans; `artwork_url` populated where the clone had it), then a live coverage check: prod LML `/api/v1/lookup` on ≥5 tail albums → expect cache hit, zero `discogs.fallthrough` events. **Do not** run `verify_cache.py`.
6. **Resume the paused backfill** from its cursor (`BACKFILL_ALBUM_AFTER_ID`) — separate go.

## Artist seeding

`seed_artists_additive` (same guarantees, invoked via `--seed-artists`) copies tail `artist` rows + their `artist_name_variation` children when LML enrichment JOINs them and they are absent on the target. `artist` (id PK) uses `ON CONFLICT`; so does `artist_name_variation`, on `(artist_id, name)` — [#433](https://github.com/WXYC/discogs-etl/issues/433) gave it a UNIQUE constraint, and a clone taken before that ticket's dedupe carries ~4.8 copies of every row, which an arbiter-less insert would now fail on. It stays parent-gated as well. Only seed artists if the column-diff/enrichment step confirms they are needed.

```bash
python scripts/seed_cache_from_clone.py \
  --source postgresql://localhost:5432/discogs \
  --target "$DATABASE_URL_DISCOGS" \
  --ids-file tail_artist_ids.txt \
  --seed-artists \
  --dry-run
```

Drop `--dry-run` to write. `--ids-file` here holds artist_ids, one per line, in the same format as the release-id file.

## Rollback

There is nothing to roll back on a partial run — it is additive and re-runnable; a re-run computes a smaller new-parent-id set and completes. If seeded rows must be removed, delete by the exact seeded release_id list (never `verify_cache.py --prune`), after a `SELECT` with the same predicate to confirm scope.

## Relation to the monthly rebuild

The monthly rebuild preserves LML's `api_fetch` releases itself: `rebuild-cache.sh` passes `--preserve-api-fetch` (`PRESERVE_API_FETCH=0` opts out), snapshotting them before the base import and restoring them after prune. Design and the run-log line are in [`architecture.md`](architecture.md#api_fetch-release-preservation); failure handling is in the [`api_fetch` troubleshooting entry](ec2-rebuild-runbook.md#run-fails-with-an-api_fetch-snapshot-or-restore-error) of the EC2 runbook. So an export-before / re-seed-after round trip around a `rebuild-cache.sh` run is no longer routine. Preservation is on only there. A manual re-seed is needed after any run without `--preserve-api-fetch` (`PRESERVE_API_FETCH=0`, the `rebuild-cache.yml` `workflow_dispatch` fallback, which does not pass the flag yet ([#460](https://github.com/WXYC/discogs-etl/issues/460)), or a standalone `run_pipeline.py`, where the flag is off by default), or when the snapshot was lost (for example the `*_api_fetch_snap` scratch tables were dropped by hand). A run without the flag takes no snapshot, so take a prod export before it: the re-seed reads that export as `--source`. Build `--ids-file` from the export with `SELECT release_id FROM cache_metadata WHERE source = 'api_fetch'`, and seed with `--preserve-provenance` (see the second limit below).

Two limits to know before relying on a manual re-seed:

- **It cannot rehydrate hollow pinned releases.** The seed skips every id already in `release`. A pinned release (#424) that survived a rebuild as a `release` row with no children and a `bulk_import` label is therefore skipped, not repaired. The pipeline's restore handles that case; the seed does not. A hand repair is optional: the import NULLs `release.artwork_checked_at` on a retained pin, so LML treats the pin as a miss the next time it reads it, re-fetches it from the Discogs API, and rewrites its children. A repair only saves those rate-limited fetches. To make one after a manual re-seed, restore those ids' child rows, their `cache_metadata` row, and `release.artwork_checked_at` from the export, guarded as the pipeline's restore is: in one transaction that first takes `LOCK TABLE release IN SHARE ROW EXCLUSIVE MODE`, repair only the ids that still have no row in any child table. The child tables have no PK/UNIQUE, so an insert for an id that LML has already re-fetched duplicates its children.
- **Seeded rows survive the next rebuild only as `source = 'api_fetch'`.** The rebuild's prunes go by dump membership and library match, not by `cache_metadata.source`: the import-stage prune drops a release the dump does not carry, and verify-prune (`verify_cache.py`) drops a non-library one, whatever its label. A pinned override is exempt from both (the import's pin exemption keeps its `release` row, and `apply_release_overrides` keeps pins that have a `release` row); it survives hollow if the dump does not carry it, as the bullet above describes. What lets an `api_fetch` release outlast a rebuild is the snapshot and restore above, which select on `source = 'api_fetch'`. A default seed writes fresh `bulk_import` provenance, so non-library rows seeded that way are not snapshotted and do not survive. Use `--preserve-provenance` when seeding an `api_fetch` export.
