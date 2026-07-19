# discogs-cache Postgres tuning — operator runbook

The production discogs-cache Postgres instance runs **non-default memory and I/O settings**, applied for [#313](https://github.com/WXYC/discogs-etl/issues/313) to fix an LML latency regression ([library-metadata-lookup#706](https://github.com/WXYC/library-metadata-lookup/issues/706)). This file is the durable, version-visible record of those values so a reader can (a) see the box is tuned, (b) know to what and why, and (c) restore the tuning after a Railway volume reprovision or service recreation, which would otherwise silently revert to stock defaults.

This is [#314](https://github.com/WXYC/discogs-etl/issues/314). The tuning is **not** baked into any image or migration — see "Why per-service, not the image" below.

## Applied values

The discogs-cache Postgres service runs with these settings. Stock is stock PG 16 defaults for reference.

| setting | stock | applied | scaling basis |
|---|---|---|---|
| `shared_buffers` | 128MB | **2GB** | 25% of the 8 GB instance (restart-only param) |
| `effective_cache_size` | 4GB | **6GB** | ~75% of 8 GB — planner hint, no allocation |
| `work_mem` | 4MB | **16MB** | low query concurrency on 8 vCPU |
| `maintenance_work_mem` | 64MB | **512MB** | index/rebuild maintenance (monthly rebuild) |
| `random_page_cost` | 4 | **1.1** | cloud SSD — size-independent, correct for any SSD instance |
| `effective_io_concurrency` | 1 | **200** | cloud SSD — size-independent |

Instance sizing this assumes: **8 GB RAM, ~935 MB on-disk DB**. The DB fits inside `shared_buffers` at 2GB, which is the point — the working set stays resident instead of churning through the buffer manager. If the instance is resized, revisit `shared_buffers` / `effective_cache_size` / `work_mem` (the top four rows scale with RAM; the bottom two do not).

`shared_buffers` sizing is under separate cost-driven review ([#313](https://github.com/WXYC/discogs-etl/issues/313), post-BS#1631 steady-state read) — 2GB vs 1GB vs 512MB. Update this table if it changes.

## Why this matters (the durability gap)

The values above were originally applied **live via `ALTER SYSTEM`** (2026-07-15), which writes `postgresql.auto.conf` on the Railway volume. That file:

- **survives** service restarts, redeploys, and the monthly data rebuild (none of those touch it);
- does **not** survive a **full volume recreation / fresh Postgres service reprovision** — that reverts the instance to stock (`shared_buffers=128MB`, `random_page_cost=4`, …), silently reintroducing the exact buffer-eviction / connection-pool-saturation regression #313 fixed, with no alert.

The durable fix is to carry the settings as **service configuration** (a per-service Custom Start Command), which Railway restores on reprovision, and to record them here.

## Why per-service, not the image

The instinct to bake `shared_buffers` into the Postgres image `CMD` is **wrong here**. `ghcr.io/wxyc/wxyc-postgres` is a **shared** base image: `discogs-cache`, `musicbrainz-cache`, and `wikidata-cache` all pull it (`:pg16` today), and its [runbook](https://github.com/WXYC/wxyc-etl/blob/main/docs/wxyc-postgres-image.md) defines it as a **pure overlay** — no config or behavior change beyond the `wxyc_unaccent.rules` text-search file. Baking discogs-cache's 2GB sizing (specific to its 8 GB instance / ~935 MB DB) into the shared `CMD` would mis-size the other two caches and break that invariant.

So tuning lives **per Railway service**, as a Custom Start Command. Two traps to respect:

1. **Start command overrides `CMD` entirely.** A Railway per-service Custom Start Command replaces the image `CMD`, it does not append to it. So the command must carry the **full** flag set (including the SSD flags that aren't size-dependent) or they get dropped.
2. **`-c` outranks `postgresql.auto.conf`.** Command-line `-c` flags win over `ALTER SYSTEM` values, so the start command cleanly supersedes the current live `ALTER SYSTEM` state. Once the start command is verified, the `ALTER SYSTEM` values can stay (harmless, shadowed) or be `ALTER SYSTEM RESET`-ed for cleanliness.

## The Custom Start Command

On the discogs-cache **Postgres** service (Railway → `request-o-matic` project → the Postgres service that backs `DATABASE_URL_DISCOGS`), set the service **Custom Start Command** to:

```
postgres -c shared_buffers=2GB -c effective_cache_size=6GB -c work_mem=16MB -c maintenance_work_mem=512MB -c random_page_cost=1.1 -c effective_io_concurrency=200
```

Keep it on one line in the Railway field. Every flag is present on purpose (trap 1 above).

### Applying it

Setting a Custom Start Command triggers a **redeploy + Postgres restart (~30–60 s downtime)**. LML, request-o-matic, and semantic-index all read this cache, so treat it like the #313 apply: pick a quiet window and avoid restarting while a bulk backfill (e.g. a `flowsheet-metadata-backfill-cron` / BS#1631-style drain) is hammering the cache. It is **data-safe / config-only** — no data is touched, and the effective config is already correct live via `ALTER SYSTEM`, so there is no correctness urgency; the restart is purely to make the config durable.

Apply via the Railway dashboard (Service → Settings → Deploy → Custom Start Command) or the Railway API/MCP `update_service` (`startCommand`).

### Post-restart verification (non-destructive)

```bash
psql "$DATABASE_URL_DISCOGS" -c "
  SELECT name, setting, unit, source
    FROM pg_settings
   WHERE name IN ('shared_buffers','effective_cache_size','work_mem',
                  'maintenance_work_mem','random_page_cost','effective_io_concurrency')
   ORDER BY name;
"
```

Expect `shared_buffers` = `262144` (8 kB units = 2GB), `random_page_cost` = `1.1`, `effective_io_concurrency` = `200`, etc. Once the start command is in effect, `source` reads `command line` for these (vs `configuration file` when they came from `postgresql.auto.conf`) — that `source` flip is the proof the durable lever, not the volume state, is now supplying the values.

A quick residency sanity check (should be ~99% after warmup):

```bash
psql "$DATABASE_URL_DISCOGS" -c "
  SELECT round(100.0 * sum(heap_blks_hit) / nullif(sum(heap_blks_hit)+sum(heap_blks_read),0), 2) AS cache_hit_pct
    FROM pg_statio_user_tables;
"
```

## musicbrainz-cache / wikidata-cache — separate question

The two SSD flags (`random_page_cost=1.1`, `effective_io_concurrency=200`) are correct for **any** cloud-SSD instance regardless of DB size, so `musicbrainz-cache` and `wikidata-cache` (same shared image, same Railway class) would plausibly benefit from their own per-service start commands carrying at least those two. That is **out of scope for #314** — it's a separate `wxyc-etl` / [Music Data Pipeline Hardening](https://github.com/orgs/WXYC/projects/19) evaluation, not something to fold in here (their memory sizing would need its own analysis, and they don't share discogs-cache's 935 MB working set). Captured so it isn't silently dropped; file separately if pursued.

## Related

- [#313](https://github.com/WXYC/discogs-etl/issues/313) — applied + verified the tuning live via `ALTER SYSTEM`; this runbook makes it survive reprovisioning.
- [#314](https://github.com/WXYC/discogs-etl/issues/314) — this durability work.
- [library-metadata-lookup#706](https://github.com/WXYC/library-metadata-lookup/issues/706) — the latency regression the tuning addresses.
- [wxyc-etl `docs/wxyc-postgres-image.md`](https://github.com/WXYC/wxyc-etl/blob/main/docs/wxyc-postgres-image.md) — shared-image runbook establishing the pure-overlay invariant.
