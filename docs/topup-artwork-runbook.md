# LML#221 artwork top-up drain — operator runbook

`scripts/topup_artwork.py` walks the never-asked tail of `release` and fills `artwork_url` from the live Discogs API. The drain is a manual, one-shot operator action — not on a recurring schedule. Re-running is idempotent because the candidate query reads only `artwork_url IS NULL AND artwork_checked_at IS NULL` (the predicate of `release_artwork_null_idx`, added by migration 0008).

## When to run

After a monthly cache rebuild, or whenever `WXYC/DiscogsCache.artwork_never_asked_count` (published by `scripts/cache_health_metrics.py`) sits high enough that the next run will be worth the Discogs spend. The CloudWatch alarm "never_asked_count above 30% for 7 days" is the typical trigger.

Do not run while a `flowsheet-metadata-backfill-cron`-style bulk job is hammering LML — both this drain and LML share the same Discogs token, and stacking them risks Discogs 429s that slow down live traffic. Check the [LML monopolization incident note](https://github.com/WXYC/library-metadata-lookup/issues/995) before kicking a drain during peak hours.

## Prereqs

Environment:

| Variable | Required | Notes |
|---|---|---|
| `DATABASE_URL_DISCOGS` | yes | Cache PG URL. `--database-url` overrides. |
| Discogs auth (one shape) | yes | See below — either personal token *or* OAuth consumer pair. |
| `SENTRY_DSN` | optional | Stderr logger works without it. |

Discogs auth accepts either shape (matches LML's `DiscogsService`):

| Variable(s) | Shape | Source |
|---|---|---|
| `DISCOGS_TOKEN` (or legacy `DISCOGS_API_TOKEN`) | Personal access token | LML's runtime env on Railway |
| `DISCOGS_API_KEY` + `DISCOGS_API_SECRET` | OAuth consumer pair | `WXYC/secrets/secrets.txt` (look for the `_V2_5` variant if present) |

If both are exported the token wins. The script fails fast at startup if neither is set.

The script depends only on `psycopg`, stdlib `urllib`, and `lib.observability` — installable from `pip install -e .`.

## Flags

```
--database-url URL    PG URL (defaults to $DATABASE_URL_DISCOGS / $DATABASE_URL)
--limit N             Max releases to process this invocation (default 1000)
--rate N              Discogs requests per minute (default 10)
--batch-size N        Rows per UPDATE commit (default 100)
--dry-run             Fetch + log counts, skip writes
```

## Procedure

### 1. Sanity-check the candidate count

```bash
psql "$DATABASE_URL_DISCOGS" -c "
  SELECT COUNT(*) AS never_asked
    FROM release
   WHERE artwork_url IS NULL
     AND artwork_checked_at IS NULL;
"
```

If the count is in the low thousands, a single `--limit 1000` pass is fine. Higher counts should be split across multiple invocations (see step 4).

### 2. Dry run

```bash
python -m scripts.topup_artwork --limit 50 --rate 10 --dry-run
```

Confirm the summary line shows `candidates=50` and the per-bucket counts (`with_artwork`, `without_artwork`, `deleted`) sum to `fetched + deleted`. `updated=0` is expected for `--dry-run`.

### 3. Bounded slice

```bash
python -m scripts.topup_artwork --limit 1000 --rate 10
```

At rate 10 a 1000-row slice takes ~100 minutes wall-clock. Watch the log for `discogs fetch failed` lines — those rows stay in the never-asked bucket and the next invocation retries them. A small failure rate (single-digit percent) is normal; sustained failures mean Discogs is throttling, raise `--rate` only after confirming LML isn't fighting for the token.

### 4. Repeat

Re-run step 3 until the candidate count from step 1 settles. The partial index makes each invocation's SELECT cheap even when the residual is small.

### 5. Post-run verification

```bash
psql "$DATABASE_URL_DISCOGS" -c "
  SELECT
    COUNT(*) FILTER (WHERE artwork_url IS NOT NULL) AS with_artwork,
    COUNT(*) FILTER (WHERE artwork_url IS NULL AND artwork_checked_at IS NOT NULL) AS asked_no_image,
    COUNT(*) FILTER (WHERE artwork_url IS NULL AND artwork_checked_at IS NULL) AS never_asked
    FROM release;
"
```

`with_artwork` should jump by the drain's `with_artwork` summary count. `asked_no_image` jumps by `without_artwork + deleted`. `never_asked` drops by `with_artwork + without_artwork + deleted` (i.e. `updated`).

## Off-hours operation

Late-night drains can raise `--rate` to 30 or so without risking live traffic — the LML token bucket still holds 50/min, and Discogs's 60/min ceiling is the hard limit. Don't exceed 50: that's the design point at which LML and the drain start fighting for the token.

## Failure modes

| Symptom | Likely cause | Action |
|---|---|---|
| `summary.failed` is most of the candidates | Discogs throttling — either LML is hot or the rate is too high | Lower `--rate`, check LML traffic |
| `summary.deleted` is unusually high | Bulk-loader dump contains releases that have since been withdrawn | No action; the rows are stamped imageless and won't recur |
| `summary.candidates == 0` | Drain converged | Nothing to do |
| `release.artwork_checked_at column does not exist` | Migration 0008 not applied to this DB | `alembic upgrade head` per `docs/migrations-runbook.md` |

## Why this lives in discogs-etl, not LML

The drain talks to the cache directly and writes the same `(artwork_url, artwork_checked_at)` shape that the monthly rebuild's bulk loader produces. Routing it through LML's `/lookup` would inherit LML's per-request semaphore + token bucket — fine for live traffic, wasteful for a bounded batch. The cross-cache-identity pivot (LML as sole composer of identity-bearing writes) does not apply: artwork is metadata, not identity, and the bulk loader has always written the same column without going through LML.
