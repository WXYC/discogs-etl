# Seed cache from clone — operator runbook

`scripts/seed_cache_from_clone.py` **additively** copies an artist-scoped release row-set from a full, unfiltered Discogs clone into an existing (populated) target discogs-cache. It is a one-shot operator action, not a pipeline step and not on a schedule.

Motivation: [Backend-Service#1631](https://github.com/WXYC/Backend-Service/issues/1631)'s Apple-Music-URL backfill stalls on a finite "cold tail" of non-library albums whose Discogs releases the library filter never admitted to prod discogs-cache. On a miss LML falls through to the live Discogs API + cold path (>15s), times out, and trips the health watchdog. A full local clone already holds those releases in the right shape; this seeds exactly those into prod so LML resolves them as fast cache hits and the paused backfill drains normally. See `plans/bs1631-tail-cache-seed.md` for the full design.

## When to run

When a bounded, known set of releases needs to exist in a populated cache and a full rebuild is not warranted. This is **not** the mechanism for widening the cache filter to open-ended demand (rejected as low-ROI). The target is a specific release_id list produced upstream.

Do not run while a bulk job is hammering LML on the shared Discogs token — this seed does not call Discogs (clone → PG copy only), but the downstream backfill it unblocks does.

## Guarantees (why it is safe against a populated prod cache)

- **Additive only.** `release` (id PK) and `cache_metadata` (release_id PK) insert with `ON CONFLICT DO NOTHING`. The arbiter-less child tables (`release_artist`, `release_label`, `release_genre`, `release_style`, `release_track`, `release_track_artist`, `release_video`; and `artist_name_variation` on the artist side) have no PK/UNIQUE, so idempotency is driven off the parent: children are inserted only for the **new-parent-id set** = (requested ids) − (ids already on the target). A parent id is in that set only if it was absent, so its children were necessarily absent too — no duplicate rows, fully re-runnable.
- **Never overwrites.** No UPDATE, no DELETE, no TRUNCATE. Existing prod rows are byte-identical before and after.
- **Column-drift tolerant.** Column lists come from `verify_cache.COPY_TABLE_SPEC` (the canonical source of truth) intersected with the columns actually present on both databases. The clone predates `release.artwork_checked_at` / `release.not_found` / `artist.not_found` and has no `release_video` table; those simply fall out of the copy and default on the target.
- **`verify_cache.py --prune` is prohibited against the seeded rows.** It classifies releases against WXYC library membership and DELETEs non-matches — tail artists are non-library by definition, so it would delete exactly what was seeded. Post-seed verification is a separate read-only integrity check.

## Inputs

| Flag | Required | Notes |
|---|---|---|
| `--source` | yes | Clone DSN, read-only (`SELECT` / `COPY … TO STDOUT` only). e.g. `postgresql://localhost:5432/discogs`. |
| `--target` | yes | Target discogs-cache DSN. For prod, the Railway `Postgres` service `DATABASE_PUBLIC_URL` (reading prod requires explicit approval; writing is heavier — gate on a green dry-run first). |
| `--ids-file` | yes | Release_ids to seed, one per line (blank lines and `#` comments ignored). Selected upstream via LML's `lower(f_unaccent(artist_name)) % $artist` trigram predicate over the tail artists — artist-scoped (keep all releases by a tail artist), not album-scoped. |
| `--dry-run` | no | Report per-table row-set sizes without writing. **Always run first.** |

The script depends only on `psycopg` and `lib.observability` (installable via `pip install -e .`) and imports `COPY_TABLE_SPEC` from `verify_cache`.

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

`seed_artists_additive` (same guarantees) copies tail `artist` rows + their `artist_name_variation` children when LML enrichment JOINs them and they are absent on the target. `artist` (id PK) uses `ON CONFLICT`; `artist_name_variation` is arbiter-less and parent-gated on the new-artist-id set. Only seed artists if the column-diff/enrichment step confirms they are needed.

## Rollback

There is nothing to roll back on a partial run — it is additive and re-runnable; a re-run computes a smaller new-parent-id set and completes. If seeded rows must be removed, delete by the exact seeded release_id list (never `verify_cache.py --prune`), after a `SELECT` with the same predicate to confirm scope.
