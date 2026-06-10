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

## Applying a single migration out-of-band (between rebuilds)

The monthly EC2 rebuild (`scripts/rebuild-cache.sh`) runs `alembic upgrade head` against the prod cache at step 2b before any dump work. That's the canonical deploy path, and most migrations should ride it: schema and data changes coalesce inside one operator window. (The sibling `workflow_dispatch`-only path at `.github/workflows/rebuild-cache.yml` adds a `Verify alembic baseline is stamped` pre-guard before its own `alembic upgrade head`; the EC2 script does not, and assumes the stamp procedure earlier in this runbook has already run.)

Sometimes you want to apply a migration *now* — a consumer-side deploy is gated on the new schema being live (e.g. LML#530's `POST /api/v1/identity/resolve` returning 200 instead of 503 once `entity.release_identity` exists) and you don't want to schedule a multi-hour rebuild to ship pure DDL. This section is for that case.

### When to use this path

Use the direct invocation when **all four** are true:

1. The migration is **pure DDL** (no data backfill, no row mutation, no application-level coordination beyond the schema change itself). Skim the migration body — if it's `op.execute(...)` on `CREATE TABLE` / `CREATE INDEX` / `ALTER TABLE ADD COLUMN` and nothing else, you're in scope. If it backfills, do it via the rebuild so the data step rides the same operator window.
2. The migration is **idempotent** under re-application (DDL uses `IF NOT EXISTS` / `IF EXISTS`). Every revision since 0001 follows this convention; the integration tests in `tests/integration/test_alembic_*.py` pin it.
3. The migration is **reversible** in case you want to roll back without restoring from snapshot. The downgrade should be DDL-only too.
4. A **consumer is blocked** on the schema being live. If nothing's waiting, just let the next rebuild apply it on the natural cadence — fewer moving parts.

If any of those is false, trigger the EC2 rebuild instead. The direct path is surgical, not a substitute for the rebuild.

### Preconditions

- The PR that introduces the migration is **merged to `main`**. The deploy applies whatever's at the `main` checkout's `alembic upgrade head`; running this against an unmerged branch is how you ship something that didn't pass review.
- A recent backup snapshot. Cheap insurance — the migration is DDL only, but a typo on `DATABASE_URL_DISCOGS` can land you in the wrong DB.
- `discogs-cache` is **already alembic-stamped AND populated** — both, not either. Stamped means `alembic_version` exists and holds a recorded revision (the first-deploy stamp procedure earlier in this runbook produces that state). Populated means the `release` / `cache_metadata` tables have rows. Running the direct path against an *empty* DB stamps it as "at head" against a schema that isn't actually there — see [Reversibility Case 2](#case-2-stamped-an-empty--fresh-db-by-mistake-footgun) for the recovery. Running against an *unstamped + populated* DB is covered by the `0001_initial` populated-schema short-circuit but is the workflow's territory; this path assumes the baseline is already stamped.
- You know the migration's current head and target head. Read `alembic current` before, expect a specific revision id after.

### Procedure

```bash
# 1. Take a backup snapshot. RDS console → Take snapshot, or `pg_dump`. Same
#    discipline as the first-deploy stamp — pure DDL is reversible, but the
#    snapshot covers "I pointed at the wrong DB".

# 2. Pull the merged migration into a clean checkout. Don't run from a stale
#    branch — the deploy applies whatever `alembic upgrade head` finds in
#    your local `alembic/versions/`.
cd /path/to/discogs-etl
git fetch origin && git checkout main && git pull --ff-only origin main

# 3. Activate the venv and point at the prod DB. Use ".venv/bin/alembic"
#    explicitly if you're not sure activation is sticky.
source .venv/bin/activate
export DATABASE_URL_DISCOGS="<prod url>"

# 4. Confirm the pre-state. The current revision should match what you
#    expect to upgrade *from* (i.e. the previous head, not your new one).
alembic current
# → e.g. 0011_artist_not_found (head)
# If this prints your NEW revision, the migration already applied and you
# can skip step 6.
# If this prints an unexpected revision, STOP and figure out why before
# applying anything.

# 5. List what `upgrade head` will apply. Read-only; safe. No truncation —
#    if the chain grows past one terminal page, scroll. Hiding the tail is
#    how you ship a revision you didn't intend.
alembic history --indicate-current
# → confirm the only pending revision is the one you intend to apply.
# If there are multiple pending revisions and you only want one, name the
# target explicitly in step 6 instead of `head`.

# 6. Apply the migration.
alembic upgrade head
# → or: alembic upgrade <revision_id>   (single-step the chain)

# 7. Verify alembic agrees on the new head.
alembic current
# → e.g. 0012_entity_release_identity (head)
```

### Why `--sql` is still off-limits

Of revisions 0001 through 0012, only 0006 goes through alembic's wrapped connection (`op.execute` on idempotent `CREATE TABLE IF NOT EXISTS` DDL). The other eleven open their own `psycopg.connect(..., autocommit=True)` for DDL. Among those eleven, 0010 / 0011 / 0012 route the offline-mode refuse and URL resolution through `lib/alembic_helpers.py` (`refuse_offline`, `resolve_db_url`); 0001 through 0005 and 0007 through 0009 carry inline copies of the same two helpers (0001's are inlined directly into its `upgrade()` body; the rest are extracted to module-private `_refuse_offline` / `_resolve_db_url`). `alembic upgrade head --sql` cannot intercept the side-channel, so for any migration that opens its own connection the apparent "dry run" output is misleading: the DDL runs for real and the SQL emission shows only the `alembic_version` UPDATE. Every side-channel migration calls `refuse_offline` (or its inline equivalent) first and raises loudly if `context.is_offline_mode()` is true, so the command fails fast — but the lesson from the 2026-04-28 wipe (logged above under "Why `alembic upgrade head --sql` is unsafe with this baseline") is "don't try to dry-run alembic against this schema." If you want to preview the SQL, read the migration source.

### Post-deploy smoke

The migration owner is responsible for naming a smoke that proves the new surface is live. Examples:

- **0010 / 0011** (release / artist `not_found`): `\d release` / `\d artist` shows the column with `NOT NULL DEFAULT false`.
- **0012** (entity.release_identity): `POST /api/v1/identity/resolve` against the LML deploy that consumes the table returns 200 with the mint-then-remint shape locked in [WXYC/wxyc-shared#175](https://github.com/WXYC/wxyc-shared/pull/175) (1.13.0) — first call mints (`minted: true`), second call returns the same identity (`minted: false`). The pg-marked test `tests/integration/test_alembic_0012_entity_release_identity.py::test_mint_then_remint_smoke` exercises the underlying DB shape; the HTTP shape lives in the wxyc-shared spec and the LML#530 implementation.

If the migration is purely structural and no consumer is using it yet, the smoke is `alembic current` reporting the new head — that's the bar.

### Recovery

DDL migrations have working `downgrade()` paths. If you applied the wrong revision or the post-deploy smoke fails:

```bash
# Step back one revision.
alembic downgrade -1
alembic current
# → confirm you're back at the previous head.
```

The integration tests pin downgrade-idempotence (re-application is a no-op), so a downgrade + re-upgrade cycle is safe.

If the downgrade itself errors (rare, but possible if a consumer raced you and wrote rows that block the DROP), STOP. The wrong move is to force the drop via `psql -c "DROP TABLE ... CASCADE"` — that breaks the alembic state. The right move is to read the error, decide whether to roll forward (apply a follow-up migration) or restore from snapshot.

### What NOT to do

- **Don't run from a feature branch.** `alembic upgrade head` applies what's on the local checkout. Always `git checkout main && git pull` first.
- **Don't run `alembic upgrade head --sql`.** See above. Read the migration source if you want to preview.
- **Don't apply multiple unrelated migrations in one direct invocation** unless they're a tightly-coupled chain. Each migration deserves its own consumer smoke; chaining them muddles attribution if the smoke fails.
- **Don't substitute the direct path for a rebuild that includes data backfill.** If the migration body has anything past pure DDL (UPDATE, COPY, function bodies that rewrite rows), the rebuild is the right path so the data and schema operations share an operator window.
- **Don't skip the snapshot** even though the operation is reversible. The snapshot's job is to catch the "wrong DB" error class, not the "bad migration" one.

## One-time recovery: dev/EC2 systems on the dict-based 0004 (post-#223)

`alembic/versions/0004_wxyc_identity_match_fns.py` was rewritten in [#223](https://github.com/WXYC/discogs-etl/issues/223) to deploy `wxyc_unaccent` via a pure-SQL `wxyc_unaccent_text(text)` function instead of a `$SHAREDIR/tsearch_data/`-backed text-search dictionary. Railway-managed Postgres can't host the rules file (the path is root-owned and unwritable even with `pg_write_server_files`), and that's where the rebuild target lives going forward.

Systems where the *old* dict-based 0004 has already been applied (Jake's Homebrew dev cache, the EC2 legacy cache) still have a `wxyc_unaccent` text-search dictionary plus `wxyc_match_form` etc. bodies that call `unaccent('wxyc_unaccent', r)`. Alembic won't re-run 0004 on those because `version_num` already says `0004_wxyc_identity_match_fns`. Force-converge each system once:

```bash
source .venv/bin/activate
export DATABASE_URL_DISCOGS="<dev or ec2 url>"

# Confirm we're on 0004 with the dict-based bodies (i.e. pre-#223 deploy).
psql "$DATABASE_URL_DISCOGS" -c "SELECT version_num FROM alembic_version"
# → version_num = '0004_wxyc_identity_match_fns'
psql "$DATABASE_URL_DISCOGS" -c "SELECT dictname FROM pg_ts_dict WHERE dictname='wxyc_unaccent'"
# → 1 row means dict-based 0004 is deployed.

# Downgrade through 0004, then upgrade back. The downgrade drops the dict
# (DROP TEXT SEARCH DICTIONARY IF EXISTS wxyc_unaccent) along with the
# function family; the upgrade re-deploys the new function-based path.
alembic downgrade 0003_wxyc_library_v2
alembic upgrade head

# Verify the post-#223 surface.
psql "$DATABASE_URL_DISCOGS" -c "SELECT dictname FROM pg_ts_dict WHERE dictname='wxyc_unaccent'"
# → 0 rows
psql "$DATABASE_URL_DISCOGS" -c "SELECT proname FROM pg_proc WHERE proname='wxyc_unaccent_text'"
# → wxyc_unaccent_text  (1 row)
```

Production (the Railway destination) doesn't need this recipe — it sat at `0003_wxyc_library_v2` waiting for #223 to land, so the first `alembic upgrade head` against the new 0004 deploys the function-based path directly.
