# discogs-etl#298 — empty track/artist index recovery — operator runbook

The prod discogs-cache can end up with `release` fully populated but the `release_*` child tables (chiefly `release_track` / `release_artist`) empty. This runbook explains how to detect that state, why it happens, and how to recover. It pairs with the code guardrail shipped for [discogs-etl#298](https://github.com/WXYC/discogs-etl/issues/298), which makes the state loud instead of silent.

## Symptom

LML track search (`search_releases_by_track` → `TRACK_ON_COMPILATION`, `SONG_AS_TRACK`, `resolve_release_for_track`) is broadly degraded: album-less `/lookup`s surface thin write-back-only matches (e.g. a remix) instead of the in-library original. The tell is a `release`-full / children-empty cache.

## Detection

```bash
psql "$DATABASE_URL_DISCOGS" -c "
  SELECT (SELECT count(*)                     FROM release)        AS releases,
         (SELECT count(DISTINCT release_id)   FROM release_track)  AS track_releases,
         (SELECT count(DISTINCT release_id)   FROM release_artist) AS artist_releases;
"
```

Healthy: `track_releases` and `artist_releases` are on the same order as `releases` (near 1:1 — every Discogs release carries ≥1 artist credit and essentially a tracklist). Degraded: the child counts are a tiny fraction (the [#298](https://github.com/WXYC/discogs-etl/issues/298) incident was 1,839 of 258,990 ≈ 0.7%, and those were only the rows LML's runtime `write_release` back-patched, all stamped in the current month).

The guardrail now surfaces this automatically (see "What the guardrail does" below), but this query is the direct check.

## Root cause

`release` is upsert-not-truncate by design ([#252](https://github.com/WXYC/discogs-etl/issues/252)) so a rebuild preserves LML's runtime artwork back-patches. But the base stage (`scripts/import_csv.py:import_release_via_upsert`) `TRUNCATE ... CASCADE`s every `release_*` child table in a committed transaction and only reloads them across several later steps (`--base-only` children, then `dedup`, then `--tracks-only`). A run that aborts anywhere in that window leaves `release` full and the children empty — a state a naive row-count check reads as healthy.

The end-of-run drift watchdog (`scripts/check_cache_drift.py`, invoked at `scripts/rebuild-cache.sh:333`) runs only after a *fully successful* pipeline, so a mid-run abort never triggers it. And no rebuild could self-heal while [#296](https://github.com/WXYC/discogs-etl/issues/296) (expired `GH_TOKEN`) blocked every monthly tick.

## Recovery — there is no shortcut

The child-table CSVs are converter-derived into an ephemeral `TemporaryDirectory` (deleted on exit), so there is no persisted artifact to re-import. The only safe, bounded path is: **rotate the expired PAT, then run one full rebuild.** The default rebuild path is idempotent and repopulates all children.

> **Do NOT** run `import_csv.py --truncate-existing` against the base tables to "just reload." That wipes LML's artwork back-patches ([#252](https://github.com/WXYC/discogs-etl/issues/252)). If you must do a targeted child-only reload with fresh CSVs, use `--base-only` then `--tracks-only` **without** `--truncate-existing`, which preserves `release`.

### 1. Rotate `GH_TOKEN` (unblocks [#296](https://github.com/WXYC/discogs-etl/issues/296))

Replace the SSM SecureString at `/wxyc/discogs-rebuild/GH_TOKEN` (account `503977661500`) with a fresh fine-grained PAT granting `contents: read` on `WXYC/library-metadata-lookup` and `WXYC/discogs-xml-converter` (both needed by the two `gh release download` calls). No code change. See [`ec2-rebuild-runbook.md` → "`gh release download` fails with `HTTP 401`"](ec2-rebuild-runbook.md).

```bash
aws ssm put-parameter \
  --name /wxyc/discogs-rebuild/GH_TOKEN \
  --type SecureString \
  --overwrite \
  --value '<fresh-fine-grained-PAT>'
```

Set a calendar reminder for the PAT's expiry — this failure is only visible via the `:warning:` Slack post and the S3 log.

### 2. Trigger one full rebuild

Dispatch the ephemeral rebuild (EventBridge normally fires it monthly at `cron(0 6 4 * ? *)`; trigger it manually now). Follow the dispatch + monitoring procedure in [`ec2-rebuild-runbook.md`](ec2-rebuild-runbook.md). The run uses the default idempotent path (no `--truncate-existing`) — `import_release_via_upsert` upserts `release` while the `--base-only` / `dedup` / `--tracks-only` steps rebuild the children.

Watch for the new preflight and post-reload log lines (`step=reload_invariant`) and the `:white_check_mark:` Slack success. A `:warning:` with `[#298] cache reload invariant violated` on the strict gate means the run finished with an empty index — investigate before retrying; do not paper over it by re-dispatching.

### 3. Verify recovery

Re-run the detection query — `track_releases` / `artist_releases` should be back on the order of `releases`.

Then confirm the acceptance repro. A `/lookup` for **"me and mr. jones by plug"** (album-less) should return the in-library original *Drum 'n' Bass for Papa* (Discogs release `3192` → WXYC library id `38167`), not the row-less non-library remix *Me & Mr Sutton* (Discogs release `1643641`). Replaying LML's `search_releases_by_track` against the cache should now surface release `3192` from its repopulated `release_track` row for "Me & Mr Jones".

## What the guardrail does (post-#298)

`scripts/run_pipeline.py` now brackets the reload with a `release`-vs-children coverage invariant (`MIN_CHILD_COVERAGE_RATIO`, `evaluate_reload_invariant`):

- **Preflight (warn):** before the truncating base step, if the DB is already in the `release`-full / children-empty state a prior abort left, it logs a loud `[#298] ... invariant violated on entry (recovering this run)` WARNING (`step=reload_invariant`). It does not raise — this run is about to repopulate — so operators see recovery is under way.
- **Post-reload gate (raise):** after `set_tables_logged`, before `report_sizes` and the `:white_check_mark:`, it re-checks the final state consumers will read. A violation (e.g. an empty/missing tracks CSV COPYed as zero rows) raises, so the run fails **before** reporting success.

Together these mean a partial-rebuild abort can no longer leave a `release`-full / children-empty state undetected.

## Related

- [#296](https://github.com/WXYC/discogs-etl/issues/296) — expired `GH_TOKEN`; must be rotated before any rebuild can complete.
- [#252](https://github.com/WXYC/discogs-etl/issues/252) — why `release` is upsert-not-truncate.
- [#226](https://github.com/WXYC/discogs-etl/issues/226) — the drift-watchdog gap (extends `check_cache_drift.py`); complementary to this guardrail.
