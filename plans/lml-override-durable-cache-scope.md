# Durable discogs-cache scope for LML override-pinned releases

## Problem

LML's `lml_cache.library_release_override` table pins WXYC library items (`library_id`) to specific Discogs `release_id`s. At lookup time the read-path resolves a pin by reading that release's metadata + tracklist from the shared discogs-cache PostgreSQL. But the monthly cache rebuild loads Discogs releases **scoped to WXYC library artists** — so any pinned release that is *not* by a library artist is absent from the cache and is re-dropped on every rebuild. Those pins then cold-fetch from the live Discogs API on the first lookup after each rebuild (LML#706 cold-tail latency) and fail entirely if Discogs is unavailable.

This is not hypothetical or small. Measured against prod on 2026-07-26:

```sql
SELECT o.source,
       count(DISTINCT o.discogs_release_id)                                 AS pinned,
       count(DISTINCT o.discogs_release_id) FILTER (WHERE r.id IS NULL)     AS missing_from_cache
FROM lml_cache.library_release_override o
LEFT JOIN release r ON r.id = o.discogs_release_id
GROUP BY o.source;
```

| source | pinned releases | **missing from cache** |
|---|---|---|
| `alex-l-2026` (Phase-1, release-typed links) | 12,040 | **4,821** |
| `alex-l-2026-masters` (Phase-2 Tier A/B, from cached versions) | 27,466 | 0 |
| `alex-l-2026-masters-api` (Phase-2 master `main_release` tail) | 12,972 | **12,033** |
| `alex-l-2026-masters-lowconf` (Phase-2 Tier C) | 5,824 | 0 |

**16,850 distinct pinned releases are missing from the cache today** — including 4,821 Phase-1 pins that have been silently cold-tailing since 2026-07-18. The Tier A/B/C `masters` pins have zero gap because they were resolved *from* cached versions (in-scope by construction); the gap is exactly the pins that point outside the library-artist scope.

Warming (proactively fetching these releases into the cache) does not solve it: the next monthly rebuild wipes any non-library release. We need a durable scope change so that **every release the override table pins is retained across rebuilds**.

## Desired end state

Every `discogs_release_id` in `lml_cache.library_release_override` (verified column name — the pin's Discogs release id) is durably present in the discogs-cache — the `release` row **and** its child rows (`release_track` etc., i.e. a real tracklist) — surviving every monthly rebuild, and automatically covering any pin added in the future. No LML read-path change (respects the post-launch hardening freeze on `lookup/artwork.py`, WXYC project #32).

> **Naming:** the LML-owned override table's column is `discogs_release_id`; the discogs-cache primary key is `release.id`. This plan uses `discogs_release_id` throughout when referring to the override set and `release.id` for the cache row.

## Why the naive "union one release-id set into the scope" is not enough

The library scope is enforced at **four independent seams**, each of which will drop an out-of-scope release. A durable fix must make all four honor the override set (or route the override releases through the pipeline as first-class rows). Seam map (file:line against `origin/main` @ 28903ff):

- **Seam A — Rust converter `(artist,title)` filter (external repo).** `scripts/run_pipeline.py::convert_and_filter` (~L721–763) forwards `--library-db` to the `WXYC/discogs-xml-converter` binary, which applies a pair-wise `(artist,title)` filter *inside its streaming scan* and decides what `release.csv` (and the child CSVs) contain at all. An out-of-scope pinned release never appears in the CSVs. Filter logic lives in the separate `discogs-xml-converter` repo.
- **Seam B — staging-prune + child truncate.** `scripts/import_csv.py::import_release_via_upsert` (~L691–781) TRUNCATEs the release child tables (~L707–711), COPYs `release.csv` into `release_staging`, upserts, then `DELETE FROM release r WHERE NOT EXISTS (SELECT 1 FROM release_staging s WHERE s.id = r.id)` (`PRUNE_STALE_RELEASES_SQL`, ~L686–688, exec ~L776). **Two kills here:** a pinned release not in this month's `release.csv` is deleted, *and* its children were truncated and never reloaded (they aren't in the child CSVs). Sparing the parent alone still loses the tracklist.
- **Seam C — per-master dedup.** `scripts/dedup_releases.py` (~L290–298) keeps one release per `(master_id, format)` via `ROW_NUMBER()`. A specific pinned pressing can be deduped away in favor of a higher-ranked sibling.
- **Seam D — fuzzy prune.** `scripts/verify_cache.py` `--prune` (run every rebuild, `run_pipeline.py:~1454`) fuzzy-matches every cached release's `(artist,title)` against a `LibraryIndex` built from `library.db` and deletes non-matches (`classify_all_releases` → `prune_ids` → `prune_releases`/`prune_releases_copy_swap`).

Masters need **no** change: `import_masters` derives its scope from `SELECT DISTINCT master_id FROM release` (`import_csv.py:~1162`), so once override releases are in `release` with their `master_id`, their masters are pulled automatically.

## Recommended approach — converter allowlist + prune/dedup exemptions

Route the override releases through the pipeline as first-class rows, so their tracklists come from the monthly dump (a durable, self-refreshing source) rather than a transient warm.

1. **Read the override set once** (`run_pipeline.py`, new helper; test home `tests/unit/test_run_pipeline.py`). First cross-schema read in the pipeline: `SELECT DISTINCT discogs_release_id FROM lml_cache.library_release_override` on the existing cache connection (read-only — allowed; only *truncating* `lml_cache.*` is guarded, and reads never touch the truncate lists). Write the ids to an allowlist file threaded to the seams below. If the table/schema is absent (fresh/test DB), degrade to empty set — but **scope the catch to `UndefinedTable`/`InvalidSchemaName`** (psycopg `errors.UndefinedTable`/`InvalidSchemaName`), never a blanket `except`: a column-name typo must fail loudly here, not silently return an empty set that only surfaces as a parity failure at the next rebuild.
2. **Seam A — converter release-id allowlist (`discogs-xml-converter`, Rust).** Add `--keep-release-ids <file>`: emit any release (and its child rows) whose id is in the set, in addition to the `(artist,title)` filter. The monthly full-dump re-scan makes this a self-maintaining durable source of the tracklists. **Cross-repo, and the rollout order is load-bearing:** `rebuild-cache.sh:~115-131` downloads the **latest** converter GitHub-release binary (`gh release download`, no `--tag`; fallback `cargo build` of `/opt/discogs-xml-converter`), so there is no tag pin to bump — the next monthly tick picks up whatever is newest. Therefore: (a) **publish the converter release that understands `--keep-release-ids` first**, and only then (b) teach `run_pipeline.py` to pass it. Guard the hand-off with a compatibility check (feature-detect via `--help`/version, or a converter capability probe) so `run_pipeline.py` passes the flag only to a converter that supports it — otherwise a mid-rollout monthly run feeds an unknown arg to the latest binary and the whole rebuild errors. **Fail-safe default:** if the probe itself fails or is ambiguous (e.g. `--help` output format drifts), do **not** pass the flag — degrade to the status-quo scope, consistent with the "a bug degrades to status quo, never over-deletion" principle below.
3. **Seam C — dedup exemption (`dedup_releases.py`).** Exclude override ids from `dedup_delete_ids` so the exact pinned pressing is never evicted by a sibling.
4. **Seam D — prune exemption (`verify_cache.py`).** The **required** mutation is *adding override ids to `keep_ids`*. On the monthly path `len(prune_ids) > 10000`, so `async_main` (~L2177) calls `prune_releases_copy_swap(db_url, report.keep_ids, report.review_ids)`, which keeps only `keep_ids ∪ review_ids` and never consults `prune_ids` — so "remove from `prune_ids`" is a **no-op on the monthly path** and only matters on the small-prune (`≤10000`) delete branch (~L2179). Inject at the `classify_all_releases` return. **Ordering dependency:** copy-swap can only *keep* rows that already exist in `release`, so this exemption protects override rows **only after** Seam A (converter allowlist) or the one-time backfill has populated them — it is inert on its own.
5. **Seam B — no change needed** once (2) lands: override releases are in `release.csv` (so the staging-prune keeps them) and their children are in the child CSVs (so the truncate+reload repopulates them).
6. **Masters — no change** (scope derived from `release.master_id`).

## One-time backfill (immediate durability, before the next monthly rebuild)

The pins are cold **now**; the recommended change only takes effect at the next rebuild. Bridge with a one-time additive seed of the 16,850 currently-missing releases (+ children) into prod, from a full-Discogs source.

- **Reuse the existing additive seeder, do not build a new one.** The bs1631 work already built exactly this tool: `scripts/seed_cache_from_clone.py` (`--ids-file`, `release`/`artist` via `ON CONFLICT (id) DO NOTHING`, child inserts gated on the new-parent set since child tables have no PK/UNIQUE, imports `COPY_TABLE_SPEC`). Feed it the 16,850 ids as `--ids-file`. **Do not** reuse `verify_cache.py::copy_releases_to_target` — it is destructive (`_create_target_schema` `DROP TABLE ... CASCADE`s every release table, `scripts/verify_cache.py:~1378-1393`) and cannot seed additively into prod.
- **Prerequisite / merge-order dependency:** `seed_cache_from_clone.py` currently lives only on the unmerged branch `feat/bs1631-seed-cache-from-clone`, **not** on `origin/main` (this plan's base). This plan is **blocked-by** that branch merging (or must rebase onto it). Declare the dependency in the ticket's Relationships; do not duplicate the seeder.
- **Source of the release rows:** a full (unscoped) Discogs cache. One is available locally (a full `discogs` dump on the dev box: 2.53M masters / 19M releases / 174M tracks — the July 2026 dump), from which all 16,850 were already confirmed present with non-blank tracklists during LML#858 resolution. Alternatively, single-file-convert the monthly `releases.xml.gz` for just the override ids → `import_csv.py`.
- **Hard constraint (from the bs1631 plan):** `verify_cache.py --prune` must **never** run against the backfilled ids — it would classify them PRUNE and delete exactly what was seeded. Which is precisely why Seam D's prune exemption (recommended change §4) is the durable counterpart that keeps the backfill alive across subsequent rebuilds.

## Alternatives considered

- **Load-strategy change at Seam B** (child tables: truncate+reload → upsert-with-selective-delete, exempting override ids). Single-repo (no converter change) but rewrites the hot, well-tested loader; child tables have no unique key, so "upsert" means delete-per-parent + insert, a real correctness/perf surface. Heavier than the allowlist for the same result.
- **LML-owned durable shadow cache** (`lml_cache.*` table of pinned-release metadata + `artwork.py` fallback). Rejected: duplicates Discogs metadata into a second store, needs a freshness sync, and edits the frozen `artwork.py` (#32). This is the "Option B" the LML#858 owner already declined in favor of the ETL-side root-cause fix.

## Ownership & safety

- discogs-etl **reads** `lml_cache.library_release_override` (first cross-schema read in the pipeline) but never migrates/truncates/manages it — consistent with the schema-ownership rule (`CLAUDE.md`: LML owns `lml_cache.*`). The truncate guard (`_validate_truncate_lists`) is unaffected; never add `lml_cache.*` to a truncate list.
- The change is additive to the *keep* set only — it can never cause more pruning, only less. A bug degrades to the status quo (releases missing), never to over-deletion of library data.

## Testing (TDD, per discogs-etl `CLAUDE.md`)

- **Seam D:** extend `tests/integration/test_prune.py::TestPruneClassification` — override ids always land in `report.keep_ids`, never `prune_ids`, even when their `(artist,title)` fails the fuzzy match.
- **Seam C:** dedup test — an override id is retained even when a sibling under the same `(master_id, format)` ranks higher.
- **Seam A:** Rust converter unit test — `--keep-release-ids` emits an off-scope release + its children.
- **Override read:** unit test the `run_pipeline.py` helper (empty set when the table/schema is absent; distinct ids otherwise).
- **Backfill:** integration test — additive + idempotent (re-run inserts nothing new), parent-gated children, 0 rows pruned by a subsequent `verify_cache` classification.
- **Post-rebuild parity (automatable half of acceptance #4):** a `pg`-marked integration assertion that after the keep-union pipeline runs, every `release_id` in `lml_cache.library_release_override` is present in `release` with ≥1 non-blank `release_track`. This is the in-repo counterpart to the cross-repo LML E2E and is what CI actually guards.

## Acceptance criteria

- [ ] After a full rebuild, **all** `discogs_release_id`s in `lml_cache.library_release_override` are present in `release` and each has ≥1 non-blank `release_track` (0 missing; parity query above returns `missing_from_cache = 0` for every source).
- [ ] The override union is additive-to-keep only; a rebuild never prunes an override id (assert via a post-prune classification check).
- [ ] One-time backfill loads the 16,850 currently-missing releases additively/idempotently; 0 trackless; Phase-1/Phase-2 override rows unchanged.
- [ ] **(Manual / cross-repo)** Post-change E2E: a sample of previously-cold pins resolves **hot** (served from cache, no live Discogs call) via LML `extended` lookup. This drives library-metadata-lookup and has no test home in this repo; the automatable half is the in-repo parity assertion above (Testing §). *(Baseline established 2026-07-26: the 14,409 API-tail pins already resolve correctly via cold-fetch — 20/20 sampled — so this criterion is specifically "hot, no live call".)*
- [ ] Docs updated: (a) `docs/architecture.md` — the converter allowlist (step 3), and the dedup/prune override exemptions (the seams are documented at `docs/architecture.md:7,11,13,65`); (b) `CLAUDE.md` schema-ownership section — a one-line carve-out for the pipeline's new **read-only** dependency on `lml_cache.library_release_override` (degrades to empty when the table/schema is absent; never migrates/truncates it); (c) the cross-repo `discogs-xml-converter` flag documented in that repo.

## Related

- **discogs-etl#308** ([Epic] warm the discogs-cache *within* library scope) — **related, not a duplicate.** #308 closes coverage gaps for *library* releases lost to artist-match false-negatives (#305 ANV/alias, #217 count audit). This issue is the inverse case: releases deliberately pinned *outside* library scope by the override table, which must be **retained** rather than recovered. Same "cache coverage" family, distinct mechanic.
- **Blocked-by:** the additive seeder `scripts/seed_cache_from_clone.py` on branch **`feat/bs1631-seed-cache-from-clone`** (no PR/issue open yet — must be PR'd + merged before the one-time backfill can run). See §"One-time backfill".
- **LML#858** — the Phase-2 master seeding that surfaced the API-tail gap; **LML#706** — cold-tail latency this makes durable.
- **`scripts/seed_cache_from_clone.py` on branch `feat/bs1631-seed-cache-from-clone`** (the additive `--ids-file` seeder this plan **reuses**; referenced by branch path because the bs1631 plan doc lives only in the sibling checkout, not on the branch). This work is **blocked-by** that branch landing on `origin/main` (or must rebase onto it) — wire as a native blocked-by dependency in the ticket.
- **discogs-etl#317 / #318** — masters import (no change needed; scope derives from `release.master_id`).
- `WXYC/discogs-xml-converter` — the Seam A allowlist change (cross-repo).
