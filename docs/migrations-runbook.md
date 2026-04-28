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

# 6. Dry-run upgrade to confirm there's nothing pending.
alembic current                # → 0001_initial (head)
alembic upgrade head           # → no upgrades executed (we're already at head)
```

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

This is what the `Verify alembic baseline is stamped` workflow guard exists to prevent. If it gets bypassed (manual run from a developer machine, etc.), `0001_initial.py` fails fast on the first `CREATE TABLE` against an existing relation:

```
psycopg.errors.DuplicateTable: relation "release" already exists
```

Because the file uses autocommit, partial application is technically possible but unlikely (the failing statement is the first DDL in the first SQL file). Inspect the schema after the failure: `\dt+` to see what's there, `\d+ <suspect>` to compare. If anything was created that wasn't there before, restore from the snapshot taken in step 1.

## Verifying after a stamp

The procedure's step 4 covers the happy path. For a more thorough check (e.g., before letting the next monthly cron run):

```bash
# Confirm the version row.
psql "$DATABASE_URL_DISCOGS" -c "SELECT version_num FROM alembic_version"
# → version_num = '0001_initial'

# Confirm the underlying schema looks like prod (sanity check that you didn't
# stamp a fresh staging clone by accident).
psql "$DATABASE_URL_DISCOGS" -c "SELECT count(*) FROM release"
# → expect a multi-million row count for prod, ~0 for empty staging.

# Dry-run what the next workflow run would do.
DATABASE_URL_DISCOGS="$DATABASE_URL_DISCOGS" .venv/bin/alembic current
# → 0001_initial (head)
DATABASE_URL_DISCOGS="$DATABASE_URL_DISCOGS" .venv/bin/alembic upgrade head --sql
# → "-- Running upgrade ..." for any pending migration, empty for none.
```

If `alembic current` reports `0001_initial (head)` and the `release` row count looks right, the stamp landed correctly.
