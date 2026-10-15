# Seed cache from clone — operator runbook

`scripts/seed_cache_from_clone.py` **additively** copies an artist-scoped release row-set from a full, unfiltered Discogs clone into an existing (populated) target discogs-cache. It is a one-shot operator action, not a pipeline step and not on a schedule.

Motivation: [Backend-Service#1631](https://github.com/WXYC/Backend-Service/issues/1631)'s Apple-Music-URL backfill stalls on a finite "cold tail" of non-library albums whose Discogs releases the library filter never admitted to prod discogs-cache. On a miss LML falls through to the live Discogs API + cold path (>15s), times out, and trips the health watchdog. A full local clone already holds those releases in the right shape; this seeds exactly those into prod so LML resolves them as fast cache hits and the paused backfill drains normally. See `plans/bs1631-tail-cache-seed.md` for the full design.

## When to run

When a bounded, known set of releases needs to exist in a populated cache and a full rebuild is not warranted. This is **not** the mechanism for widening the cache filter to open-ended demand (rejected as low-ROI). The target is a specific release_id list produced upstream.

Avoid running a real seed across the monthly rebuild tick (06:00 UTC on the 4th = 22:00 PST / 23:00 PDT on the 3rd). The seed holds rebuild lock `354001` for its whole write (named `discogs-etl-seed` on its connection), and a rebuild that finds it held by a seed retries every 5 minutes for up to `REBUILD_LOCK_WAIT_MINUTES` (default 180) instead of skipping the month, so an overlap delays the rebuild. The real window is wider than the tick: the rebuild takes `354001` only after boot, the code refresh, alembic, the `library.db` download and the dump spool, often 30 or more minutes after 06:00 UTC. A seed that outlasts the wait still skips that month's rebuild, so schedule away from it, as the dedup runbook says for dedup. Seed releases and artists in one invocation (`--ids-file` plus `--artist-ids-file`): it takes `354001` once and holds it from the start of the release seed to the end of the artist seed, so the two families stay consistent with each other, with no rebuild between them. Two separate invocations (a release seed, then `--seed-artists`) each hold it only for their own write; a rebuild can run in the gap between them. Its prune then drops the just-seeded release rows, and the artist seed refuses with exit 75 while that rebuild still holds the lock, or runs against the rebuilt cache after it. The single invocation does **not** make a seed across the tick safe. A waiting rebuild takes `354001` right after the span ends, and its import prune and verify-prune drop seeded non-library releases recorded as `bulk_import` (see the end of this runbook), so still schedule the seed away from the tick. The span covers the two seed families only. The export restore (`restore_api_fetch_from_export.py`) takes `354001` itself under `discogs-etl-restore-api-fetch` and runs afterwards; the gap before it is harmless for `api_fetch` rows: `restore_api_fetch_releases` restores only absent or hollow ids and the dump wins, and a rebuild preserves the target's `api_fetch` releases itself (`--preserve-api-fetch`).

Do not run while a bulk job is hammering LML on the shared Discogs token — this seed does not call Discogs (clone → PG copy only), but the downstream backfill it unblocks does.

## Guarantees (why it is safe against a populated prod cache)

- **Additive only.** `release` (id PK), `cache_metadata` (release_id PK), and `artist` (id PK) insert with `ON CONFLICT DO NOTHING`. The arbiter-less child tables (`release_artist`, `release_label`, `release_genre`, `release_style`, `release_track`, `release_track_artist`, `release_video`) have no PK/UNIQUE, so idempotency is driven off the parent: children are inserted only for the **new-parent-id set** = (requested ids) − (ids already on the target). A parent id is in that set only if it was absent, so its children were necessarily absent too — no duplicate rows, fully re-runnable. A `pg_advisory_xact_lock` (key `330001`, registered in this repo's `CLAUDE.md` "Advisory lock keys" table) spans the new-id computation through the write for every real run as a backstop against two overlapping invocations racing into duplicate child rows. In practice the rebuild lock (below) keeps them apart first: a real seed holds `354001` for its whole write, so a second overlapping real seed against the same target refuses with exit `75` rather than waiting. Re-run it once the first finishes.
- **Refuses to overlap a rebuild.** A real (non-dry-run) run takes the rebuild lock (`pg_try_advisory_lock(354001)`, `lib/rebuild_lock.py`) on `--target` and holds it for the whole write; if it is already held (by a rebuild, `dedup_artist_children.py`, an api_fetch export restore, an operator session, or another real seed), the seed's refusal names the session that holds it (read from `pg_locks`) and exits `75` without writing anything. Re-run once the holder finishes (`python -m lib.rebuild_lock --describe-holder` names it too). `--dry-run` takes no lock and works during a rebuild. Without this, a seed could write rows the rebuild's import TRUNCATE and prune then drop, or interleave with the api_fetch restore into duplicate child rows.
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
| `--artist-ids-file` | no | Artist_ids to seed, one per line, after the releases in `--ids-file`, in the same run under one `354001` acquisition. Not combinable with `--seed-artists`; `--preserve-provenance` still applies to the release family only. |
| `--seed-artists` | no | Seed the `artist` family (see "Artist seeding" below) from `--ids-file` instead of the default `release` family. |
| `--preserve-provenance` | no | Release family only. For each newly seeded release, copy `cache_metadata.source` and `cached_at` from `--source` when the source row exists and both are non-NULL; otherwise fall back to the default fresh `bulk_import` row. Use when `--source` is a prod export whose provenance is reliable; leave off for a clone, whose freshness rows are unreliable. Do not use it to restore an export's `api_fetch` releases: use `scripts/restore_api_fetch_from_export.py` (see [Relation to the monthly rebuild](#relation-to-the-monthly-rebuild)), which also rehydrates hollow pins and restores the full `cache_metadata` row. Still additive (`ON CONFLICT DO NOTHING`); `--dry-run` logs how many rows would preserve vs fall back. |
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
4. **Seed prod** (drop `--dry-run`). To seed releases and artists, use one invocation: `--ids-file` for the releases plus `--artist-ids-file` for the artists (see Artist seeding), so `354001` is held across both. The write is one transaction per family; per-table inserted counts are logged.
5. **Verify (read-only).** Referential-integrity check over the seeded release_ids (every child has a parent `release`; no orphans; `artwork_url` populated where the clone had it), then a live coverage check: prod LML `/api/v1/lookup` on ≥5 tail albums → expect cache hit, zero `discogs.fallthrough` events. **Do not** run `verify_cache.py`.
6. **Resume the paused backfill** from its cursor (`BACKFILL_ALBUM_AFTER_ID`) — separate go.

## Artist seeding

`seed_artists_additive` (same guarantees) copies tail `artist` rows + their `artist_name_variation` children when LML enrichment JOINs them and they are absent on the target. `artist` (id PK) uses `ON CONFLICT`; so does `artist_name_variation`, on `(artist_id, name)` — [#433](https://github.com/WXYC/discogs-etl/issues/433) gave it a UNIQUE constraint, and a clone taken before that ticket's dedupe carries ~4.8 copies of every row, which an arbiter-less insert would now fail on. It stays parent-gated as well. Only seed artists if the column-diff/enrichment step confirms they are needed.

Seed releases and artists together with one invocation: `--ids-file` holds the release_ids and `--artist-ids-file` the artist_ids, one per line each. The run takes `354001` once for the release seed and the artist seed (#488).

```bash
python scripts/seed_cache_from_clone.py \
  --source postgresql://localhost:5432/discogs \
  --target "$DATABASE_URL_DISCOGS" \
  --ids-file tail_release_ids.txt \
  --artist-ids-file tail_artist_ids.txt \
  --dry-run
```

Drop `--dry-run` to write. For the artists-only case, use `--seed-artists`, where `--ids-file` holds artist_ids in the same format:

```bash
python scripts/seed_cache_from_clone.py \
  --source postgresql://localhost:5432/discogs \
  --target "$DATABASE_URL_DISCOGS" \
  --ids-file tail_artist_ids.txt \
  --seed-artists \
  --dry-run
```

## Rollback

There is nothing to roll back on a partial run — it is additive and re-runnable; a re-run computes a smaller new-parent-id set and completes. If seeded rows must be removed, delete by the exact seeded release_id list (never `verify_cache.py --prune`), after a `SELECT` with the same predicate to confirm scope.

## Relation to the monthly rebuild

The monthly rebuild preserves LML's `api_fetch` releases itself: `rebuild-cache.sh` passes `--preserve-api-fetch` (`PRESERVE_API_FETCH=0` opts out), snapshotting them before the base import and restoring them after prune. Design and the run-log line are in [`architecture.md`](architecture.md#api_fetch-release-preservation); failure handling is in the [`api_fetch` troubleshooting entry](ec2-rebuild-runbook.md#run-fails-with-an-api_fetch-snapshot-or-restore-error) of the EC2 runbook. So an export-before / re-seed-after round trip around a `rebuild-cache.sh` run is no longer routine. Preservation is on there and in the `rebuild-cache.yml` `workflow_dispatch` fallback (its `preserve_api_fetch` input, default true). A manual restore is needed after any run that took no snapshot: `PRESERVE_API_FETCH=0`, a `rebuild-cache.yml` dispatch with `preserve_api_fetch` set to false, a standalone `run_pipeline.py` (the flag is off by default), or a run with `--target-db-url` (`run_pipeline.py` turns `--preserve-api-fetch` off when it is set). It is also needed when the snapshot was lost (for example the `*_api_fetch_snap` scratch tables were dropped by hand). A run without a snapshot drops the `api_fetch` releases, so take a prod export before it.

Restore the export with `scripts/restore_api_fetch_from_export.py`, not with this seed. It stages the export's `api_fetch` release family on `--target` as the pipeline's own `*_api_fetch_snap` scratch tables and runs the pipeline's `restore_api_fetch_releases` over them (#462), so the manual and the in-pipeline restore share one rule set: the dump wins, hollow pinned releases (#424) are rehydrated with their children and `artwork_checked_at`, the full `cache_metadata` row (`source`, `cached_at`, `last_validated`) comes back, and the `release` lock is bounded by a 2-minute `lock_timeout`. This seed cannot do the same: it skips every id already in `release`, so a hollow pin stays hollow, and it sets `last_validated` to its default.

```bash
python scripts/restore_api_fetch_from_export.py \
  --source postgresql://localhost:5432/discogs_export \
  --target "$DATABASE_URL_DISCOGS" \
  --dry-run
```

`--dry-run` reads both databases, writes nothing and takes no lock; it logs `snapshotted=N preserved=N rehydrated=N superseded_by_dump=N` for the export against the cache as it stands. Drop it to restore. Both modes exit `2` without writing when `--source` holds no `api_fetch` releases (or has no `release` / `cache_metadata` table) or `--target` lacks one of the nine cache tables: that is the wrong database, and the log names the problem. A real run:

- takes the rebuild lock `354001` on `--target` for its whole run and exits `75` without writing if another session holds it, naming that session in its refusal; retry once that session is done;
- only reads `--source` (`SELECT` / `COPY ... TO STDOUT`, in a read-only transaction);
- restores a snapshot already on `--target` (a run that died before its restore) first, then stages the export, so it never merges into an existing snapshot. If that run is one you mean to `run_pipeline.py --resume`, resume it first: the resume reuses that snapshot, and this script would consume it;
- stages before the restore locks `release`, so the bounded `SHARE ROW EXCLUSIVE` lock is held only for the restore's own transaction. Staging is one target transaction that holds `ACCESS SHARE` on the nine live cache tables while the export streams in: LML reads and writes are not blocked, but DDL (an `alembic upgrade`, a `TRUNCATE`) waits for it, and LML's writes then queue behind that DDL. Do not run it alongside a migration;
- on a failed restore, rolls the restore back and keeps the `*_api_fetch_snap` tables it could not restore (exit `1`). The log says which: the staged export, or a snapshot already on `--target` whose restore failed before the export was staged. Fix the cause, then rerun. Do not leave it for the next rebuild: that rebuild's left-behind recovery runs the same restore, so while the cause persists it fails the same way and aborts that rebuild, and after a failure before staging it would not apply the export at all.

A hollow pin needs no restore to be correct: the import NULLs `release.artwork_checked_at` on a retained pin, so LML re-fetches it on its next read and rewrites its children. The restore only saves those rate-limited fetches.

Rows seeded with this script survive the next rebuild only as `source = 'api_fetch'`. The rebuild's prunes go by dump membership and library match, not by `cache_metadata.source`: the import-stage prune drops a release the dump does not carry, and verify-prune (`verify_cache.py`) drops a non-library one, whatever its label. A pinned override is exempt from both (the import's pin exemption keeps its `release` row, and `apply_release_overrides` keeps pins that have a `release` row); it survives hollow if the dump does not carry it. What lets an `api_fetch` release outlast a rebuild is the snapshot and restore above, which select on `source = 'api_fetch'`. A default seed writes fresh `bulk_import` provenance, so non-library rows seeded that way are not snapshotted and do not survive.
