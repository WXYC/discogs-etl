# Discogs cache migrations — first-deploy runbook

This runbook covers the one-time operator step of stamping the production `discogs-cache` PostgreSQL database with the alembic baseline, so future deploys can run `alembic upgrade head` incrementally.

You only need this once per environment (prod, staging). After the stamp, the monthly rebuild workflow (`.github/workflows/rebuild-cache.yml`) handles every subsequent migration automatically.

## Why this is needed

`alembic/versions/0001_initial.py` is the recorded baseline. Its `upgrade()` replays `schema/create_functions.sql`, `schema/create_database.sql`, `schema/create_indexes.sql`, and `schema/create_track_indexes.sql` against an empty database. Those SQL files use bare `CREATE TABLE` (no `IF NOT EXISTS`), so re-applying them against an existing populated DB fails with `relation "release" already exists`.

Existing production already has the schema (the pipeline created it via `apply_schema()` long before alembic was introduced) but lacks the `alembic_version` table. We need to tell alembic "the current state is already 0001_initial; don't re-apply it" — that's what `alembic stamp head` does.

## Prerequisites

- Direct database access to prod `discogs-cache` (port 5433 in our infra). Confirm via `psql $DATABASE_URL_DISCOGS -c '\dt'` and look for `release`, `release_artist`, `entity.identity`, etc.
- A recent backup snapshot. Create one if there isn't one in the last 24 hours (the `alembic stamp` is non-destructive, but pre-state lets you recover if anything else surprises you).
- `alembic` available — clone discogs-etl, `python3 -m venv .venv`, `pip install -e ".[dev]"`.

## Procedure

```bash
# 1. Take a backup snapshot of discogs-cache (RDS console → Snapshots → Take snapshot, or pg_dump if self-hosted).

# 2. Confirm the DB has the schema but no alembic_version table.
psql "$DATABASE_URL_DISCOGS" -c "\dt" | head -20
psql "$DATABASE_URL_DISCOGS" -c "SELECT to_regclass('public.alembic_version')"
# → should print NULL the first time, indicating alembic has never been initialized.

# 3. Stamp the current state as 0001_initial. This creates the `alembic_version`
#    table and inserts the baseline revision without running upgrade().
DATABASE_URL_DISCOGS="$DATABASE_URL_DISCOGS" .venv/bin/alembic stamp head

# 4. Verify.
psql "$DATABASE_URL_DISCOGS" -c "SELECT * FROM alembic_version"
# → should print version_num = '0001_initial'

# 5. Test that future upgrades will be clean by running alembic upgrade head;
#    when there are no new migrations, this is a no-op.
DATABASE_URL_DISCOGS="$DATABASE_URL_DISCOGS" .venv/bin/alembic upgrade head
# → "Context impl PostgresqlImpl. Will assume transactional DDL."
# → No upgrades executed.
```

After this, the monthly rebuild workflow runs `alembic upgrade head` automatically before each rebuild, applying any new migrations between rebuilds.

## When NOT to run this

- **Empty databases** (no `release` table) — these go through the normal rebuild path; alembic upgrade applies the baseline correctly. Don't pre-stamp.
- **Already-stamped databases** — running `alembic stamp head` again is a no-op (the row already exists), but `alembic upgrade head` is the appropriate command.

## Rollback

`alembic stamp head` doesn't change schema. To "unstamp", drop the `alembic_version` table:

```sql
DROP TABLE alembic_version;
```

The migration system goes back to its pre-stamp state. Don't do this in prod unless you're starting fresh.
