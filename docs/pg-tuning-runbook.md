# discogs-cache Postgres tuning — operator runbook

The production discogs-cache Postgres instance runs **non-default memory and I/O settings**, applied for [#313](https://github.com/WXYC/discogs-etl/issues/313) to fix an LML latency regression ([library-metadata-lookup#706](https://github.com/WXYC/library-metadata-lookup/issues/706)). This file is the version-visible record of those values — what they are, why, and how to restore them after a volume reprovision.

> **⚠️ Do NOT set a Railway Custom Start Command on this service to carry the tuning.** It was tried on 2026-07-18 and **took discogs-cache down** (crash-loop). See [The Custom Start Command trap](#the-custom-start-command-trap-do-not-use) below. The tuning currently lives in `postgresql.auto.conf` via `ALTER SYSTEM`; the durability gap is handled by the [recovery procedure](#recovery-after-a-volume-reprovision), not by a start command.

## Applied values

The discogs-cache Postgres service runs with these settings (stock = PG defaults, for reference).

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

## How the tuning is applied today

The values were applied **live via `ALTER SYSTEM`** (2026-07-15), which writes `postgresql.auto.conf` on the Railway volume. To reproduce or restore:

```sql
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET effective_cache_size = '6GB';
ALTER SYSTEM SET work_mem = '16MB';
ALTER SYSTEM SET maintenance_work_mem = '512MB';
ALTER SYSTEM SET random_page_cost = '1.1';
ALTER SYSTEM SET effective_io_concurrency = '200';
```

`shared_buffers` is a **restart-only** parameter, so after running these the service needs a restart for `shared_buffers` to take effect (the others reload with `SELECT pg_reload_conf();`). Restart via the Railway dashboard (Service → ⋯ → Restart) or `railway redeploy`.

`postgresql.auto.conf`:

- **survives** service restarts, redeploys, and the monthly data rebuild (none of those touch it);
- does **not** survive a **full volume recreation / fresh Postgres service reprovision** — that reverts the instance to stock (`shared_buffers=128MB`, `random_page_cost=4`, …), silently reintroducing the exact buffer-eviction / connection-pool-saturation regression #313 fixed, with no alert.

That last point is the **durability gap**. Because no clean automatic lever exists on this image (see below), the gap is closed by a **documented recovery step + a drift check**, not by baked config.

## Recovery after a volume reprovision

If the discogs-cache Postgres volume is ever recreated / the service reprovisioned, the tuning is gone. To restore:

1. Connect: `psql "$DATABASE_URL_DISCOGS"` (or the Railway public proxy URL).
2. Run the `ALTER SYSTEM` block above.
3. Restart the service (for `shared_buffers`).
4. Verify with the [verification query](#verification-non-destructive).

Consider wiring a drift check (e.g. a cache-health metric that alarms when `shared_buffers` reads `128MB`) so a silent reprovision-revert surfaces instead of degrading latency unnoticed.

## Why not the image (the shared-image constraint)

The instinct to bake `shared_buffers` into the Postgres image `CMD` is **wrong here**. `ghcr.io/wxyc/wxyc-postgres` is a **shared** base image: `discogs-cache`, `musicbrainz-cache`, and `wikidata-cache` all pull it, and its [runbook](https://github.com/WXYC/wxyc-etl/blob/main/docs/wxyc-postgres-image.md) defines it as a **pure overlay** — no config or behavior change beyond the `wxyc_unaccent.rules` text-search file. Baking discogs-cache's 2GB sizing (specific to its 8 GB instance / ~935 MB DB) into the shared `CMD` would mis-size the other two caches and break that invariant. (Prod discogs-cache runs `:pg17-v0.4.x`; the local compose in `docs/architecture.md` still pins `:pg16`.)

## The Custom Start Command trap (do NOT use)

A per-service Railway **Custom Start Command** looks like the ideal durable lever — it's per-service (doesn't touch the shared image) and Railway service config survives a volume reprovision. **It does not work on this image, and applying it causes an outage.** Verified live on 2026-07-18:

- Setting the start command to `postgres -p 5432 -c listen_addresses=* -c shared_buffers=2GB …` and redeploying **crash-looped** the container with:
  ```
  "root" execution of the PostgreSQL server is not permitted.
  The server must be started under an unprivileged user ID …
  ```
- **Root cause:** a custom start command bypasses the image's `wrapper.sh` / `docker-entrypoint.sh` entrypoint chain — the code that drops the container from `root` to the `postgres` user (via `gosu`, keyed on the first arg being literally `postgres`) **and** provisions SSL into `postgresql.conf`. Run the `postgres` binary "directly" and it executes as root, which Postgres refuses. So the start command breaks both the privilege drop and SSL setup.
- A second, independent trap even if the entrypoint issue were solved: the image's default `CMD` is `postgres -p 5432 -c listen_addresses=*`, and **`-c listen_addresses=*` is the only thing that binds TCP** (initdb leaves it listening on the unix socket only). A start command replaces `CMD` entirely, so omitting that flag makes the DB unreachable over the network.

**Recovery from that outage** (documented in case it recurs): clear the start command and redeploy the image default.

```bash
# via the CLI's token against the Railway GraphQL API (serviceId/environmentId for the discogs-cache Postgres)
# set startCommand to "" (empty string; null is treated as "no change" and does NOT clear it), then redeploy.
```

In the dashboard: Service → Settings → Deploy → clear the Custom Start Command field → Deploy. The image default restores the entrypoint, privilege drop, SSL, and `listen_addresses=*`; the `ALTER SYSTEM` tuning on the volume is untouched, so the DB returns to the correct tuned state after crash-recovery.

### Is there any durable-and-automatic lever?

Two candidate directions, **neither yet validated — do not try either on prod without an off-prod test first**:

1. **Entrypoint-preserving start command** — e.g. `wrapper.sh postgres -p 5432 -c listen_addresses=* -c shared_buffers=2GB …`, so the privilege-drop/SSL entrypoint still runs and the tuning flags ride along. Unverified; depends on exactly how Railway wraps the start command, and the 2026-07-18 attempt shows the failure mode is a live outage. Test against a throwaway service before ever pointing it at prod.
2. **Env-var hook in the shared image (preferred long-term)** — teach `wxyc-postgres`'s `wrapper.sh` to read an env var (e.g. `WXYC_PG_EXTRA_ARGS`) and append it as `-c` flags to the postgres invocation *from inside the entrypoint*. That would be per-service (each cache sets its own env), survive reprovision (Railway env vars do), and not bypass the privilege-drop/SSL logic. This is a `wxyc-etl` image change, tracked separately — the real fix if automatic durability is wanted.

## Verification (non-destructive)

```bash
psql "$DATABASE_URL_DISCOGS" -c "
  SELECT name, setting, unit, source
    FROM pg_settings
   WHERE name IN ('shared_buffers','effective_cache_size','work_mem',
                  'maintenance_work_mem','random_page_cost','effective_io_concurrency')
   ORDER BY name;
"
```

Expect (8 kB units where noted): `shared_buffers` = `262144` (2GB), `effective_cache_size` = `786432` (6GB), `work_mem` = `16384` kB, `maintenance_work_mem` = `524288` kB, `random_page_cost` = `1.1`, `effective_io_concurrency` = `200`. `source` reads `configuration file` (they come from `postgresql.auto.conf`).

Residency sanity check (≈99% once warm; **expect it low right after a restart** — the buffer cache is cold and re-warms with live traffic):

```bash
psql "$DATABASE_URL_DISCOGS" -c "
  SELECT round(100.0 * sum(heap_blks_hit) / nullif(sum(heap_blks_hit)+sum(heap_blks_read),0), 2) AS cache_hit_pct
    FROM pg_statio_user_tables;
"
```

## musicbrainz-cache / wikidata-cache — separate question

The two SSD flags (`random_page_cost=1.1`, `effective_io_concurrency=200`) are correct for **any** cloud-SSD instance regardless of DB size, so `musicbrainz-cache` and `wikidata-cache` (same shared image, same Railway class) would plausibly benefit from at least those two via their own `ALTER SYSTEM`. That is **out of scope here** — a separate `wxyc-etl` / [Music Data Pipeline Hardening](https://github.com/orgs/WXYC/projects/19) evaluation (their memory sizing needs its own analysis, and they don't share discogs-cache's 935 MB working set). Captured so it isn't silently dropped; file separately if pursued.

## Related

- [#313](https://github.com/WXYC/discogs-etl/issues/313) — applied + verified the tuning live via `ALTER SYSTEM`.
- [#314](https://github.com/WXYC/discogs-etl/issues/314) — durability work; the 2026-07-18 start-command outage and revised direction are recorded there.
- [library-metadata-lookup#706](https://github.com/WXYC/library-metadata-lookup/issues/706) — the latency regression the tuning addresses.
- [wxyc-etl `docs/wxyc-postgres-image.md`](https://github.com/WXYC/wxyc-etl/blob/main/docs/wxyc-postgres-image.md) — shared-image runbook establishing the pure-overlay invariant.
