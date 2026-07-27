# LML override one-time backfill — operator runbook

Backfills the 16,850 (as measured 2026-07-26) `discogs_release_id`s that LML's `lml_cache.library_release_override` pins but that are currently missing from prod's discogs-cache, using the additive seeder `scripts/seed_cache_from_clone.py`. This is a one-shot operator action, not a pipeline step and not on a schedule. See [discogs-etl#327](https://github.com/WXYC/discogs-etl/issues/327) (durable retention design) and [discogs-etl#329](https://github.com/WXYC/discogs-etl/issues/329) (this backfill).

## Why this is needed

The monthly cache rebuild loads Discogs releases scoped to WXYC library artists. Some releases LML pins via `library_release_override` fall outside that scope, so they are absent from the cache and cold-fetch from the live Discogs API on every LML lookup ([library-metadata-lookup#706](https://github.com/WXYC/library-metadata-lookup/issues/706) latency; fails entirely if Discogs is unavailable). [discogs-etl#328](https://github.com/WXYC/discogs-etl/pull/328) (merged) makes the dedup and prune pipeline seams retain a pinned release once it's in the cache, but does not get it there in the first place — that requires either the converter-side allowlist ([discogs-xml-converter#81](https://github.com/WXYC/discogs-xml-converter/issues/81), not yet landed) or this one-time backfill. This runbook is the bridge: durability now, without waiting on the next monthly rebuild + converter change.

## Hard constraints — read before running anything against prod

- **`verify_cache.py --prune` must never run against prod without `--keep-release-ids` applied to the backfilled ids.** Without it, the very next prune classifies every backfilled release PRUNE (non-library artist, by construction) and deletes exactly what this runbook just seeded. The orchestrated pipeline (`run_pipeline.py`) applies this automatically via `write_keep_release_ids` (reads `lml_cache.library_release_override` fresh every run) — but a **standalone** `verify_cache.py --prune` or `dedup_releases.py` invocation outside the pipeline applies **no** exemption unless `--keep-release-ids <FILE>` is passed explicitly. If you ever run either script by hand against prod after this backfill, generate a fresh allowlist first (`query_missing_override_release_ids.py`, or better, the full override set — every `discogs_release_id` in `library_release_override`, not just the previously-missing subset) and pass it.
- **discogs-etl#328 must be merged to `main`** before this backfill is durable across the next monthly rebuild. (Merged 2026-07-27 — verify with `git log --oneline -1 --grep '#328' origin/main` if this runbook is stale by the time you run it.)
- **The prod-mutating step (seeding prod) requires explicit human sign-off**, recorded on [discogs-etl#329](https://github.com/WXYC/discogs-etl/issues/329), separate from this runbook existing. Steps 1-3 below are read-only / dry-run and safe to run ahead of that sign-off; step 4 is not.
- Never run `verify_cache.py::copy_releases_to_target` (`--copy-to`) for this — `_create_target_schema` does `DROP TABLE ... CASCADE` on every release table. It is a rebuild-target tool, not an additive seeder.

## Prereqs

Environment:

| Variable | Required | Notes |
|---|---|---|
| `DATABASE_URL_DISCOGS` | yes | Prod discogs-cache PG URL (also holds `lml_cache.library_release_override` — same instance, different schema). |
| Source clone DSN | yes | A full (unscoped) Discogs cache with all 16,850 ids confirmed present (LML#858 resolution used the July 2026 dump: 2.53M masters / 19M releases / 174M tracks). Passed via `--source` to `seed_cache_from_clone.py`, never via an env var (avoids an accidental prod-as-source swap). |

The scripts below depend only on `psycopg` and `lib.observability` (installable via `pip install -e .`); no Discogs API credentials needed (this is a database-to-database copy, not a live fetch).

## Procedure

### 1. Regenerate the missing-ids list (read-only)

```bash
python scripts/query_missing_override_release_ids.py \
  --database-url "$DATABASE_URL_DISCOGS" \
  --output missing_override_release_ids.txt
```

Prints the per-source pinned/missing summary (same shape as #327's measurement table) and writes the ids to `missing_override_release_ids.txt`, one per line, consumable directly by `seed_cache_from_clone.py --ids-file`. Re-run this immediately before step 4 if any time has passed since it was first generated — the override table is LML-owned and can gain new pins at any time.

### 2. Dry-run against a scratch prod-shaped target

```bash
python scripts/seed_cache_from_clone.py \
  --source postgresql://localhost:5432/discogs \
  --target postgresql://localhost:5433/discogs_seed_scratch \
  --ids-file missing_override_release_ids.txt \
  --dry-run
```

Confirms per-table row-set sizes look sane (roughly 16,850 releases plus tracklists — a real tracklist per release, not a stub) before touching prod at all.

### 3. Dry-run against prod (still no writes)

```bash
python scripts/seed_cache_from_clone.py \
  --source postgresql://localhost:5432/discogs \
  --target "$DATABASE_URL_DISCOGS" \
  --ids-file missing_override_release_ids.txt \
  --dry-run
```

Confirms how many candidates are genuinely new vs already present on prod (should be close to the full ids-file count on the first run). This is the last checkpoint before the sign-off gate.

### 4. Human sign-off gate

**Stop here.** Do not run step 5 without recorded explicit approval on [discogs-etl#329](https://github.com/WXYC/discogs-etl/issues/329). Post the step 3 dry-run output (per-table counts) to the issue as the basis for sign-off.

### 5. Seed prod (drop `--dry-run`)

```bash
python scripts/seed_cache_from_clone.py \
  --source postgresql://localhost:5432/discogs \
  --target "$DATABASE_URL_DISCOGS" \
  --ids-file missing_override_release_ids.txt
```

Additive only (`ON CONFLICT DO NOTHING`), never deletes or overwrites. Safe to re-run if interrupted — a re-run computes a smaller new-parent-id set and completes (idempotent: a second run with the same `--ids-file` inserts 0 new rows). Per-table inserted counts are logged.

### 6. Verify parity (read-only)

```bash
python scripts/check_override_parity.py --database-url "$DATABASE_URL_DISCOGS"
```

Exits 0 only when every override source has `missing_from_cache == 0`. Non-zero means the backfill is incomplete (e.g. the source clone was itself missing some ids) — do not consider the backfill done until this passes.

### 7. Confirm the keep-release-ids exemption protects the backfill

This is exercised automatically the next time `run_pipeline.py` runs a rebuild: `write_keep_release_ids` reads the *entire* `library_release_override` table fresh at the start of every run (not just the previously-missing subset), so the backfilled ids are covered by construction — `apply_release_overrides` unconditionally unions every id in that allowlist into `keep_ids` regardless of fuzzy-match result. Nothing to do here for the pipeline path.

To check ahead of the next scheduled rebuild (e.g. same day as the backfill), generate a fresh allowlist directly (`run_pipeline.py` has no standalone flag for just this step — `write_keep_release_ids` only runs as part of a full pipeline invocation) and run `verify_cache.py` in its default dry-run mode (no `--prune`, no `--copy-to`):

```bash
psql "$DATABASE_URL_DISCOGS" -Atc \
  "SELECT DISTINCT discogs_release_id FROM lml_cache.library_release_override" \
  > /tmp/keep_release_ids.txt
python scripts/verify_cache.py <library_db> "$DATABASE_URL_DISCOGS" --keep-release-ids /tmp/keep_release_ids.txt
```

Confirm the printed `Releases to prune` count does not include the backfilled ids.

### 8. Confirm 0 trackless releases

```sql
SELECT count(*) AS trackless
FROM release r
WHERE r.id = ANY(:backfilled_ids)  -- from missing_override_release_ids.txt
  AND NOT EXISTS (SELECT 1 FROM release_track t WHERE t.release_id = r.id);
```

Should return 0 (discogs-etl#329 acceptance criterion). A nonzero count means the source clone had a stub release for some id — investigate before closing the issue.

## Rollback

Nothing to roll back on a partial run — the seed is additive and re-runnable (see step 5). If seeded rows must be removed for some other reason, delete by the exact seeded release_id list from `missing_override_release_ids.txt` (never `verify_cache.py --prune`), after a `SELECT` with the same predicate to confirm scope, and with explicit approval per this repo's data-safety conventions.

## Closing out

Once steps 1-8 all pass, update the [discogs-etl#329](https://github.com/WXYC/discogs-etl/issues/329) acceptance-criteria checklist and close it. [discogs-etl#327](https://github.com/WXYC/discogs-etl/issues/327) stays open until [discogs-xml-converter#81](https://github.com/WXYC/discogs-xml-converter/issues/81) (the converter allowlist — the durable going-forward fix for pins added *after* this backfill) also lands.
