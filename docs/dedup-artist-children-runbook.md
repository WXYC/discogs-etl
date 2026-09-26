# `artist_*` child dedupe — operator runbook

`scripts/dedup_artist_children.py` collapses each `(artist_id, <key>)` group in the four `artist_*` child tables down to one row. Every monthly rebuild appends the Discogs dump's rows without deduplicating against what is already there, so the cache accumulates roughly one extra copy per rebuild; as of 2026-09-25 it holds about 4.8 copies of each row. Tracked at [discogs-etl#433](https://github.com/WXYC/discogs-etl/issues/433).

This is the `B1p` step of the Railway memory-reduction sequence. The follow-up (`B2`) adds the UNIQUE constraints and an `ON CONFLICT` loader so the duplication stops recurring; until that lands, a rebuild re-introduces one copy and this script is simply re-run.

## When to run

Off-peak, and nowhere near the monthly rebuild tick (EventBridge `cron(0 6 4 * ? *)` — the 3rd of the month at 23:00 PDT). Both the DELETE and the `VACUUM FULL` hold locks that block live LML writes and reads; a multi-minute block on `artist_name_variation` sits well past Backend-Service's 35 s client timeout.

The script takes the rebuild advisory lock (key `354001`, the same key `scripts/run_pipeline.py` uses — same resource, same hazard) and exits `75` without touching anything if a rebuild already holds it. That is a backstop, not the plan: `scripts/rebuild-cache.sh` runs `alembic upgrade head` *before* `run_pipeline.py` takes the lock, so a rebuild starting mid-procedure is not prevented, only made to bow out later. Schedule away from the tick.

## Prereqs

| Variable | Required | Notes |
|---|---|---|
| `DATABASE_URL_DISCOGS` | yes | Cache PG URL. `--database-url` overrides; `DATABASE_URL` is the second fallback. |
| `SENTRY_DSN` | optional | The stderr JSON logger works without it. |

Before running against prod, confirm the three `#422` tables read `relpersistence = 'p'`:

```sql
SELECT relname, relpersistence FROM pg_class
WHERE relname IN ('release_track','release_track_artist','release_video');
```

A bulk DELETE plus `VACUUM FULL` on an 8 GB instance is the most plausible moment for memory pressure in this whole sequence, and running it while the track-search substrate is UNLOGGED means a PostgreSQL crash truncates it. Repaired 2026-09-26; re-check rather than assume, because an aborted rebuild can strand them again.

## Flags

```
--database-url URL   Cache PG URL (defaults to $DATABASE_URL_DISCOGS / $DATABASE_URL)
--execute            Delete the surplus rows. Without it the script only reports.
--vacuum-full        VACUUM (FULL, ANALYZE) each table so the space is returned
```

Exit codes: `0` success · `1` the surplus assertion failed and that table rolled back · `2` no database URL · `75` the rebuild lock is held.

## Which row survives, and why it is not just ctid

Each table deletes the row with the *greater* rank tuple, so the minimum survives. The rank is per-table, and the two tables that need more than ctid need it because the ETL and LML write different column sets:

| Table | Non-key columns | ETL loads | LML writes | Rank ahead of ctid |
|---|---|---|---|---|
| `artist_name_variation` | *none* | `artist_id, name` | same | — duplicates are byte-identical |
| `artist_url` | *none* | `artist_id, url` | same | — duplicates are byte-identical |
| `artist_alias` | `alias_id` | **omitted → NULL** | real `alias_id` | `alias_id IS NULL` |
| `artist_member` | `member_name`, `active` | `member_name` only; `active` → **`DEFAULT true`** | real `active` | `active IS NULL`, then `active IS NOT DISTINCT FROM true` |

`false < true` and the minimum survives, so each term names the property whose *absence* should lose: a NULL `alias_id` loses to LML's real one, and a defaulted `active = true` loses to LML's `active = false`. `artist_member` sorts NULL last because `NULL IS NOT DISTINCT FROM true` is false, which would otherwise let a NULL `active` outlive a real `true` — belt-and-braces, since neither writer produces a NULL `active` today.

This is not hypothetical. Measured on prod 2026-09-25: **12,905** `artist_alias` groups mix NULL and non-NULL `alias_id`, and **16,185** `artist_member` groups disagree on `active`. A raw-ctid rule would have discarded LML's value in those groups. Zero groups carry two *different* non-NULL `alias_id`s, so `(artist_id, alias_name)` is a sound key — the problem was the tie-break, not the key.

`alembic/versions/0009_cache_metadata_unique.py` is the in-repo precedent for the shape: a preference-ranked self-join DELETE (`api_fetch` wins) followed by the constraint.

## Expected counts (prod, measured 2026-09-25)

The dry-run must reproduce these within LML's hydration drift — single digits per hour. A larger mismatch means the tables moved in a way the measurement did not see: stop and re-measure rather than executing.

| Table | Rows | Distinct keys | Surplus to delete | Ratio |
|---|---:|---:|---:|---:|
| `artist_name_variation` | 3,988,085 | 809,820 | 3,178,265 | 4.92× |
| `artist_url` | 1,031,279 | 214,070 | 817,209 | 4.82× |
| `artist_member` | 779,125 | 162,499 | 616,626 | 4.79× |
| `artist_alias` | 324,432 | 67,245 | 257,187 | 4.82× |

The four tables were 393 MB (333 MB heap + 60 MB indexes) against a 1429 MB database; expect roughly 1429 MB → 1117 MB after the vacuum.

## Procedure

### 1. Dry run

```sh
python scripts/dedup_artist_children.py
```

One INFO line per table: `<table>: N rows, M distinct keys, surplus S, deleted 0`. Compare each surplus to the table above.

### 2. Execute

```sh
python scripts/dedup_artist_children.py --execute
```

Each table runs `count → DELETE → assert` inside one `REPEATABLE READ` transaction, so the surplus the DELETE is checked against is the one the count saw. Read Committed would take a fresh snapshot per statement and an LML insert landing in between would make the assertion flap. A disagreement rolls that table's transaction back and exits `1`; nothing partial is ever committed, so the run is resumable from step 1.

`40001` (serialization failure — reachable here: a concurrent LML `DELETE`+`INSERT` committing mid-DELETE aborts the attempt whole) and `40P01` (deadlock) retry up to five times, each attempt re-running the whole block under a new snapshot.

For the DELETE's duration it holds row locks on every row it is removing, so LML's `write_artist_details` blocks for any artist with duplicates.

### 3. Reclaim

```sh
python scripts/dedup_artist_children.py --vacuum-full
```

Plain `VACUUM` does not shrink the files, and `run_vacuum` in the pipeline only covers `PIPELINE_TABLES`, so without this step the space does not come back. `VACUUM FULL` takes ACCESS EXCLUSIVE per table; LML's reads of that table block for the seconds it takes. Lock waits are bounded at 5 s and retried.

### 4. Verify

```sql
SELECT 'artist_alias' AS t, count(*), count(DISTINCT (artist_id, alias_name)) FROM artist_alias
UNION ALL SELECT 'artist_name_variation', count(*), count(DISTINCT (artist_id, name)) FROM artist_name_variation
UNION ALL SELECT 'artist_member', count(*), count(DISTINCT (artist_id, member_id)) FROM artist_member
UNION ALL SELECT 'artist_url', count(*), count(DISTINCT (artist_id, url)) FROM artist_url;
```

Both counts must match per table. Then check the preservation invariant — every artist that held at least one child row still holds one. Measured floors from 2026-09-25: `artist_name_variation` 127,788 · `artist_url` 80,169 · `artist_alias` 32,740 · `artist_member` 23,648.

Do **not** use `artist.fetched_at` to separate LML-hydrated artists from imported ones. The column is `NOT NULL DEFAULT now()`, so the ETL stamps it too — 209,677 of 209,677 artists satisfy `fetched_at IS NOT NULL`, and any check built on it is measuring nothing.

Finally record `pg_database_size(current_database())` before and after on #433 and on [#432](https://github.com/WXYC/discogs-etl/issues/432), which sizes `shared_buffers` from that number.

## Rollback

**None, and here is the honest version of why.**

For `artist_name_variation` and `artist_url` the deleted rows are byte-identical to the survivor, so nothing is lost. For `artist_alias` and `artist_member` the rows are *not* identical — the tie-break exists precisely because they differ — and the guarantee is instead that the survivor carries the strictly greater information: a non-NULL `alias_id`, a non-default `active`. The rows discarded are the ETL's placeholder-valued copies. If you are relying on that guarantee, the disagreement counts above are what evidences it; re-run them, do not assume them.

If an `--execute` run aborted part-way, the per-table transaction rolled back and a fresh dry-run shows which tables are still duplicated.

## Failure modes

| Symptom | Meaning |
|---|---|
| exit `75` | A rebuild (or another operator session) holds advisory lock `354001`. Wait; do not force it. |
| exit `1`, "expected to delete N rows … but deleted M" | The DELETE and its count disagreed, or duplicates survived. That table rolled back. Re-run the dry-run; if the surplus has moved far from the table above, something else is writing. |
| `SQLSTATE 40001 … retrying` warnings | Normal under live LML traffic. Five attempts per table; exhausting them exits non-zero with nothing committed. |
| `VACUUM FULL` never gets its lock | A long LML read is holding the table. The 5 s timeout retries with backoff; if it keeps failing, run step 3 in a quieter window. |
