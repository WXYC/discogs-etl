# Plan: discogs-etl#223 — make 0004's `wxyc_unaccent` Railway-compatible

Closes [WXYC/discogs-etl#223](https://github.com/WXYC/discogs-etl/issues/223).

## Problem

Alembic migration `0004_wxyc_identity_match_fns` creates a Postgres text-search dictionary `wxyc_unaccent` via `TEMPLATE = unaccent, RULES = 'wxyc_unaccent'`. The `unaccent` template loads the rules from `$SHAREDIR/tsearch_data/wxyc_unaccent.rules`. On the production rebuild target (Railway-managed Postgres) that file is never present, and `alembic upgrade head` aborts at the dictionary creation with `ConfigFileError: could not open unaccent file`.

The issue text proposes fixing this by embedding the rules in 0004 and writing the file to `$SHAREDIR/tsearch_data/` at upgrade time. **That approach does not work on Railway.** Direct probe of `request-o-matic/Postgres` (PG 17.7-3.pgdg13+1, postgres superuser, all `pg_*_server_files` roles granted):

```
/tmp                                            → COPY TO succeeds
/usr/share/postgresql/17/tsearch_data/probe.rules → ERROR: Permission denied
```

`$SHAREDIR` is root-owned and the postgres OS user has no write bit. Server-side filesystem access is unavailable regardless of role grants.

## Constraints

| # | Constraint | Source |
|---|---|---|
| 1 | Byte-equality with Rust's `to_match_form*` over the 252-row fixture | `tests/integration/test_wxyc_identity_match_parity.py::test_postgres_functions_match_fixture_row_for_row` — fixture row `Молчат Дома → молчат дома` is the canary |
| 2 | Vendored `wxyc_identity_match_functions.sql` SHA-pinned (wxyc-etl@v0.4.0) | `wxyc-etl-pin.txt`; `test_pin_file_sha256s_match_vendored_files` |
| 3 | Vendored `wxyc_unaccent.rules` SHA-pinned (wxyc-etl@v0.4.0) | same |
| 4 | "Don't bundle other alembic-related cleanup; keep the fix tight" | issue #223 |
| 5 | Performance acceptable for GIN-index builds over millions of release rows | implicit — `wxyc_match_form` is called per-row at index-build time |

Constraint 1 rules out switching the dictionary's backing file (e.g. `RULES = 'unaccent'` for the bundled rules) — the Cyrillic fixture row diverges. Constraint 2/3 means we cannot edit the vendored files on disk.

## Approach

Replace the text-search dictionary with a pure-SQL function `wxyc_unaccent_text(text)` that bakes the WXYC rules directly into Postgres. The function uses:

- `translate(s, src_chars, dst_chars)` for the 417 single-char → single-char rules (a single C-fast pass)
- a `REPLACE` chain for the 16 multi-char-destination rules (`æ → ae`, `ŀ → l·`, `ẚ → aʾ`, etc.)

Empirical pre-check on the vendored rules file shows **zero overlap** between the 13 characters produced by multi-char-dst rules (`1234adeijlnoz·ʼʾ⁄`) and the single-char-src set, so the two-pass implementation (REPLACE first, then translate) is byte-equivalent to the single-pass `unaccent` dictionary semantics.

The vendored SQL is **patched at apply time**, not on disk:

```python
canonical = canonical.replace(
    "r := unaccent('wxyc_unaccent', r);",
    "r := wxyc_unaccent_text(r);",
)
```

The substitution is a single hit (verified — exact-match grep returns 1 line). The on-disk file stays SHA-pinned per the vendoring contract; the parity test (which deploys via `alembic upgrade` and tests against the live function) still passes.

## Implementation steps

1. **New: `lib/unaccent_codegen.py`** (extracted per plan-review for testability)
   - Function `parse_rules_file(path) -> list[tuple[str, str]]`: parse the tab-separated rules, skip blank/comment lines, return `(src, dst)` pairs.
   - Function `partition_rules(rules) -> tuple[list, list]`: split into `single_char_dst` and `multi_char_dst` sets.
   - Function `assert_no_overlap_invariant(single, multi) -> None`: raise with a re-vendoring hint if any char produced by a multi-char-dst rule appears in the single-char-src set.
   - Function `build_unaccent_function_sql(rules_path) -> str`: emit the `CREATE OR REPLACE FUNCTION wxyc_unaccent_text(text)` SQL using `translate()` + the REPLACE chain. Uses dollar-quoted string literals (`$wxyc$...$wxyc$`) to keep generated SQL readable.

2. **`alembic/versions/0004_wxyc_identity_match_fns.py`**
   - Remove `_SETUP_SQL` (drop+create dictionary) entirely.
   - `upgrade()`:
     - `CREATE EXTENSION IF NOT EXISTS unaccent` (kept; downstream callers may still want the built-in).
     - Import `build_unaccent_function_sql` from `lib.unaccent_codegen`; apply the generated function.
     - Apply the canonical vendored SQL *after substitution* (`unaccent('wxyc_unaccent', r)` → `wxyc_unaccent_text(r)`); assert exactly 1 substitution occurred, else raise with a re-vendoring hint.
   - `downgrade()`:
     - `DROP FUNCTION` each entry in `_DOWNGRADE_FUNCTIONS` (existing list — already covers the match-fn family).
     - `DROP FUNCTION IF EXISTS wxyc_unaccent_text(text)` (new).
     - `DROP TEXT SEARCH DICTIONARY IF EXISTS wxyc_unaccent` (kept — old deployments that previously applied the dict-based 0004 still have it; the `IF EXISTS` makes it a no-op on fresh function-based deployments).
   - Update docstring: no more `$SHAREDIR/tsearch_data/` dependency; document the substitution and the no-overlap invariant.

3. **New: `tests/unit/test_wxyc_unaccent_function_codegen.py`**
   - Verifies parsing: every non-blank non-comment line yields a `(src, dst)` pair; no orphaned tabs.
   - Verifies partitioning: every rule lands in exactly one of {single, multi}; src is always 1 char.
   - Verifies the no-overlap invariant against the *current vendored file* — magic counts avoided; the test exercises the invariant logic itself, so if a future re-vendor adds a rule that breaks it, the test fails loudly with an actionable error.
   - Verifies emitted SQL: contains `CREATE OR REPLACE FUNCTION wxyc_unaccent_text`, contains exactly one `translate(` call, contains REPLACE calls for every multi-char-dst rule.
   - Verifies the canonical-SQL substitution sentinel `r := unaccent('wxyc_unaccent', r);` appears exactly once in the vendored functions SQL (pins the substitution target so a re-vendoring that moves or duplicates the line fails the test).

4. **`tests/integration/test_wxyc_identity_match_parity.py`** — no changes expected. The migration deploys the function family; the 252-row fixture is unchanged; byte-equality assertions still hold because the function applies the same 433 rules. If they do change, the fix is wrong.

5. **`tests/integration/test_alembic_baseline.py`** (existing) — confirm pass after the 0004 change. If the file has an explicit `wxyc_unaccent`-dictionary assertion, replace it with one against `wxyc_unaccent_text(text)`; check current structure first.

6. **`docs/migrations-runbook.md`** — remove the operator instruction about provisioning `wxyc_unaccent.rules` to `$SHAREDIR/tsearch_data/`. The fresh-clone story is now "no manual provisioning required". Add a one-time recovery step for Homebrew/EC2 dev systems that already applied the dict-based 0004 (`alembic downgrade 0003 && alembic upgrade head`); call out that prod (Railway) needs no recovery because it sits at 0003.

7. **`wxyc-etl-pin.txt`** — add a comment near the `unaccent_rules_sha256` line: `# When bumping: rerun tests/unit/test_wxyc_unaccent_function_codegen.py — it asserts the no-overlap invariant for the two-pass codegen in alembic/versions/0004.` This makes the dependency surface-visible at the SHA-bump site.

8. **`CLAUDE.md`** (discogs-etl) — update the 0004 entry to reflect the function-based implementation. Note the substitution as the divergence point from the vendored SQL, and the no-overlap invariant gate.

9. **Forward-migrate existing dict-based 0004 deployments (Option α).** Systems that have already applied the old 0004 (Jake's Homebrew dev; the EC2 cache) have a `wxyc_unaccent` text-search dictionary and function bodies that call `unaccent('wxyc_unaccent', r)`. Alembic won't re-run 0004 on those. Documented one-time operator command: `alembic downgrade 0003 && alembic upgrade head`. The dict will be dropped on downgrade (now safe via `IF EXISTS`), and the upgrade re-applies the function-based path. Two deployments, both mutable, both reachable via Jake's existing access.

   Option β (add 0006 as a forward-only re-apply) was considered and rejected: it adds a permanent migration whose only purpose is to fix systems that will only ever exist for ~2 weeks. The manual recipe is simpler.

## Risk + mitigation

| Risk | Mitigation |
|---|---|
| Codegen produces SQL that diverges from `unaccent` dict semantics for some input | No-overlap invariant test (#3 above) + the existing 252-row parity test. The Cyrillic fixture row is the canary. |
| Future wxyc-etl rules update introduces a rule that breaks the two-pass equivalence (e.g. adds a multi-char-dst rule whose output chars overlap with single-char-src) | Codegen invariant test fails loudly at re-vendoring time; the wxyc-etl-pin SHA bump won't merge without addressing it. |
| Performance regression on GIN index build | `translate(s, big_src, big_dst)` is C-fast; 16 REPLACE calls is a small bounded chain. Should be comparable to (or faster than) the dictionary lookup. Verify by re-running the index-build benchmark in `tests/integration/` if one exists; otherwise note as follow-up. |
| Vendored SQL substitution is brittle (string match could fail if the line drifts upstream) | Substitution is asserted: if the source string is not present exactly once in the canonical, fail with a re-vendoring hint. |
| Existing Homebrew/EC2 deployments keep using the old dict-based path until manually re-applied | Document the one-time operator command; the old path still works (the dict is provisioned there). |

## Acceptance criteria

- `gh workflow run rebuild-cache.yml` against the Railway destination gets past `Apply pending alembic migrations` without `ConfigFileError`. (Verified via either a real workflow_dispatch or a local `alembic upgrade head` against the same Railway PG.)
- `git clone discogs-etl && alembic upgrade head` against a fresh local PG (Homebrew, docker-compose, anything) works without any tsearch_data provisioning step.
- `pytest tests/integration/test_wxyc_identity_match_parity.py -v` passes — 252-row fixture still byte-equal.
- `pytest tests/unit/test_wxyc_unaccent_function_codegen.py -v` passes (new tests for the codegen helper).
- `ruff check` and `ruff format --check` pass over the new `lib/unaccent_codegen.py` and the modified `alembic/versions/0004_wxyc_identity_match_fns.py` — the project's CI gate already includes both checks.
- `docs/migrations-runbook.md` no longer references the `$SHAREDIR/tsearch_data/` provisioning step; includes the one-time recovery recipe for dev/EC2.
- `CLAUDE.md` reflects the new function-based path and the substitution invariant.
- `wxyc-etl-pin.txt` carries the codegen-invariant cross-reference comment.
- PR delta ≤ ~400 lines (migration + codegen helper + tests + doc updates).

## Out of scope

- Upstream wxyc-etl change to remove `wxyc_unaccent.rules` from the Rust crate. The vendored file is still the canonical rule source; the Postgres side simply consumes it via codegen rather than via the text-search dictionary mechanism. Cross-cache vendoring contract is unchanged.
- Changes to `musicbrainz-cache` / `wikidata-cache` — the function family is currently only deployed by discogs-etl. If/when other consumers adopt 0004's pattern, they inherit this fix.
- Index/cache-load expression flips from `lower(f_unaccent(col))` to `wxyc_identity_match_artist(col)` — that follow-on is tracked separately (see 0004's docstring: "ships outside the alembic migration itself").
