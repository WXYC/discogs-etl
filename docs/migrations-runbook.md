# Discogs cache migrations — first-deploy runbook

This runbook covers the one-time operator step of stamping the production `discogs-cache` PostgreSQL database with the alembic baseline, so future deploys can run `alembic upgrade head` incrementally.

You only need this once per environment (prod, staging). After the stamp, the monthly rebuild workflow (`.github/workflows/rebuild-cache.yml`) handles every subsequent migration automatically.

## Why this is needed

`alembic/versions/0001_initial.py` is the recorded baseline. Its `upgrade()` replays `schema/create_functions.sql`, `schema/create_database.sql`, `schema/create_indexes.sql`, and `schema/create_track_indexes.sql` against an empty database. Those SQL files use bare `CREATE TABLE` (no `IF NOT EXISTS`), so re-applying them against an existing populated DB fails with `relation "release" already exists`.

Existing production already has the schema (the pipeline created it via `apply_schema()` long before alembic was introduced) but lacks the `alembic_version` table. We need to tell alembic "the current state is already 0001_initial; don't re-apply it" — that's what `alembic stamp head` does.

## Prerequisites

- Direct database access to prod `discogs-cache` (port 5433 in our infra). Confirm via `psql $DATABASE_URL_DISCOGS -c '\dt'` and look for `release`, `release_artist`, `entity.identity`, etc.
- A recent backup snapshot. Create one if there isn't one in the last 24 hours (the `alembic stamp` itself is non-destructive, but pre-state coverage protects you if a typo or wrong env var lands you in the wrong DB).
- A clone of `discogs-etl` with the dev extras installed in a venv:

  ```bash
  cd /path/to/discogs-etl
  python3 -m venv .venv
  source .venv/bin/activate          # important: activate before pip install so alembic lands inside .venv/bin
  pip install -e ".[dev]"
  which alembic                       # → /path/to/discogs-etl/.venv/bin/alembic
  ```

  All later `alembic` invocations assume the venv is activated (or you're calling `.venv/bin/alembic` explicitly).
- **Run against staging first** if a staging `discogs-cache` exists. Repeat the procedure against staging, verify the workflow's next dispatch run is clean against staging, and only then run against prod. The cost of a botched prod stamp is restoring from snapshot; the cost of a botched staging stamp is dropping `alembic_version` and re-running.

## Procedure

```bash
# 1. Take a backup snapshot of discogs-cache. RDS: console → Databases → wxyc-db
#    → Actions → Take snapshot. Self-hosted: `pg_dump $DATABASE_URL_DISCOGS > snap.sql`.
#    Don't skip this even though `alembic stamp` is non-destructive — the snapshot
#    is your reversal path if you discover later you stamped the wrong DB.

# 2. Activate the venv and point at the prod DB.
source .venv/bin/activate
export DATABASE_URL_DISCOGS="<prod url>"

# 3. Confirm the DB has the schema but no alembic_version table.
psql "$DATABASE_URL_DISCOGS" -c "\dt" | head -20
psql "$DATABASE_URL_DISCOGS" -c "SELECT to_regclass('public.alembic_version')"
# → should print NULL on the first run, indicating alembic has never been initialized.
# → if it prints `alembic_version`, the DB is already stamped — see "When NOT to run this" below.

# 4. Stamp the current state as 0001_initial. This creates `alembic_version`
#    and inserts the baseline revision without running upgrade().
alembic stamp head

# 5. Verify.
psql "$DATABASE_URL_DISCOGS" -c "SELECT * FROM alembic_version"
# → should print version_num = '0001_initial'

# 6. Confirm alembic agrees the DB is at head. Read-only; safe.
alembic current                # → 0001_initial (head)
```

Do **not** run `alembic upgrade head --sql` here as a "dry run". See the warning below — that command is destructive against this baseline and was the trigger for the 2026-04-28 prod-cache wipe. The `0001_initial.py` migration now refuses to run in offline mode and will raise loudly, but the runbook should not invite the user to test that path.

After this, the monthly rebuild workflow runs `alembic upgrade head` automatically before each rebuild, applying any new migrations between rebuilds.

## When NOT to run this

- **Empty databases** (no `release` table) — these go through the normal rebuild path; alembic upgrade applies the baseline correctly. Don't pre-stamp.
- **Already-stamped databases** — running `alembic stamp head` again is a no-op (the row already exists), but `alembic upgrade head` is the appropriate command.

## Reversibility

`alembic stamp head` is metadata-only — it inserts one row into `alembic_version` and does not touch any schema or data. Reversal is correspondingly cheap. Three possible "I want to undo this" cases:

### Case 1: stamped a populated DB, want to undo cleanly

The expected reversal path. The stamp wrote one row; remove it:

```sql
DROP TABLE alembic_version;
```

Schema, data, and the workflow's behavior all return to the pre-stamp state (the workflow's `Verify alembic baseline is stamped` guard will fail loudly on the next run, instead of silently applying migrations).

### Case 2: stamped an empty / fresh DB by mistake (footgun)

The dangerous case. After stamping a DB that has *no* schema yet, alembic believes the DB is at `0001_initial` and `alembic upgrade head` becomes a no-op forever. The rebuild pipeline's `apply_schema()` step would still create the schema correctly on the next run — so things appear to work — but you've lost the migration system's invariant ("`alembic_version` reflects what's actually in the DB"). Worse: any future migration `0002_*.py` would NOT be applied to that DB even though it isn't really at 0001 yet.

Recovery:

1. **Untangle in place** (works because the DB was empty anyway):
   ```sql
   DROP TABLE alembic_version;
   ```
   Re-run the rebuild pipeline normally; the next `alembic upgrade head` applies `0001_initial` against the (still empty) DB correctly and re-creates `alembic_version`.

2. **Restore from snapshot**: only useful if you mis-stamped a populated DB rather than an empty one and you want a known-good baseline. Step 1 of "Procedure" recommended a snapshot for this case.

### Case 3: ran `alembic upgrade head` on a populated DB *without* stamping first

This is what the `Verify alembic baseline is stamped` workflow guard exists to prevent. If it gets bypassed (manual run from a developer machine, etc.), `0001_initial.py.upgrade()`'s populated-DB short-circuit detects the existing `release` and `cache_metadata` tables, logs `0001_initial: discogs-cache schema already present; skipping schema apply`, and returns without re-executing `schema/*.sql`. Alembic then writes `alembic_version = '0001_initial'` itself, so the DB ends up in the same state as if it had been properly `alembic stamp head`-ed.

That outcome is safe — no data loss — but the audit trail is murky (no operator-issued stamp). If you discover this happened, run the verification block in [Verifying after a stamp](#verifying-after-a-stamp) to confirm the schema and row counts match what you expect, and add a note to the operator log.

The destructive path the populated-DB guard cuts off is: side-channel re-runs `schema/*.sql`, whose first statements are `DROP TABLE IF EXISTS release ... CASCADE`, dropping every release/artist/master table. The guard fails open by checking for `cache_metadata` (specific to this schema) alongside `release`, so an unrelated DB that happens to have a `release` table won't accidentally short-circuit.

## Verifying after a stamp

The procedure's step 4 covers the happy path. For a more thorough check (e.g., before letting the next monthly cron run), all read-only:

```bash
# Confirm the version row.
psql "$DATABASE_URL_DISCOGS" -c "SELECT version_num FROM alembic_version"
# → version_num = '0001_initial'

# Confirm the underlying schema looks like prod (sanity check that you didn't
# stamp a fresh staging clone by accident).
psql "$DATABASE_URL_DISCOGS" -c "SELECT count(*) FROM release"
# → expect a multi-million row count for prod, ~0 for empty staging.

# Confirm alembic agrees on current head. Read-only; safe.
DATABASE_URL_DISCOGS="$DATABASE_URL_DISCOGS" .venv/bin/alembic current
# → 0001_initial (head)
```

If `alembic current` reports `0001_initial (head)` and the `release` row count looks right, the stamp landed correctly.

> **Do not run `alembic upgrade head --sql` against this database.** See the next section.

### Why `alembic upgrade head --sql` is unsafe with this baseline

`--sql` is documented as "emit SQL instead of executing" — a dry run. That contract holds only when the migration's `upgrade()` uses alembic's wrapped connection (`op.execute`, `op.create_table`, etc.). `0001_initial.py.upgrade()` does not: it opens its own `psycopg.connect(..., autocommit=True)` and runs `schema/create_database.sql` (and friends) directly. That side-channel cannot be intercepted by `--sql`, so:

1. Alembic's stdout shows `BEGIN; CREATE TABLE alembic_version ...; INSERT INTO alembic_version ...; COMMIT;` — looks like a no-op dry run.
2. The side-channel quietly executes `schema/create_database.sql`, whose first statements are `DROP TABLE IF EXISTS release ... CASCADE`.
3. Every release/artist/master table is dropped and recreated empty.
4. The command exits 0.

This is what wiped the WXYC prod discogs-cache on 2026-04-28 (~14,667 release rows, recovered manually over ~6 hours).

**Defensive guards now in place:**

- `0001_initial.py.upgrade()` raises `RuntimeError` immediately if `context.is_offline_mode()` is true. `alembic upgrade head --sql` will fail fast with a clear message instead of silently dropping tables.
- A second guard short-circuits `upgrade()` if `release` and `cache_metadata` are both already present (the latter is specific to this schema, so the pair is unlikely to match an unrelated DB). The short-circuit logs a warning and returns; alembic itself records `version_num = '0001_initial'` afterward, leaving the DB equivalent to one that was `alembic stamp head`-ed. This catches both the stamped+populated and populated+unstamped accident cases.
- Two pg-marked integration tests pin both guards: `tests/integration/test_alembic_baseline.py::test_alembic_upgrade_head_sql_against_populated_db_is_safe` and `::test_alembic_upgrade_head_against_populated_unstamped_db_is_safe`.

If a future migration needs autocommit DDL (e.g., `CREATE INDEX CONCURRENTLY`, extension creation), keep the guards: either follow the same `is_offline_mode()` check, or write the migration with `op.execute(..., execution_options={"isolation_level": "AUTOCOMMIT"})` so alembic's offline mode can intercept it.
