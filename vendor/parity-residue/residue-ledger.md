# Phase 3.5 catalog parity — expected-residue ledger

Measured 2026-08-11 against prod tubafrenzy MySQL (`wxycmusic`) and prod Backend PG (`wxyc_schema`). Supersedes the 2026-08-02 drift table in [WXYC/wiki#89](https://github.com/WXYC/wiki/issues/89) and pre-populates the ledger that [WXYC/discogs-etl#346](https://github.com/WXYC/discogs-etl/issues/346) step 2 requires ("clean = 0 unmatched **after the documented residue ledger**").

## Headline: the release drift is not backfillable, and it is not a defect

The issue's acceptance criterion "drift table at 0" is **unachievable as written**. A catch-up backfill run of `jobs/library-etl` will move none of these rows, because none of them is lagging — every one is either a row the ETL deliberately collapsed or a row it structurally cannot see.

| Set | Count | Backfillable? | Disposition |
|---|---:|---|---|
| MySQL-only, duplicate-collapse | 599 | **No — and shouldn't be** | Accept as permanent residue |
| MySQL-only, structurally excluded | 12 | Only by fixing the MySQL row | Accept, or clean 12 rows upstream |
| Backend-only, upstream-deleted orphans | 115 | No — needs a *delete*, not a backfill | **Undecided — needs a D-row** |
| **Total unmatched by legacy id** | **726** | | |

Matched on both sides: 64,116. MySQL `LIBRARY_RELEASE`: 64,727 (max ID 72,281). Backend `library`: 64,307, of which 64,231 carry a tubafrenzy-space `legacy_release_id` and 76 carry a minted id ≥ 1,000,000.

## Set 1 — 599 duplicate-collapse rows (`residue-collapsed-599.tsv`)

`library-etl` inserts a release only if `findExistingRelease` (`jobs/library-etl/job.ts`) finds no row matching `(artist_id, genre_id, album_title, code_number, code_volume_letters)`. Where tubafrenzy holds two `LIBRARY_RELEASE` rows with an identical tuple, Backend correctly holds **one**, keyed to whichever legacy id it saw first. The other legacy id then has no counterpart.

Verified: **599 of 599** have a twin sharing their exact identity tuple, and in every case that twin **is** present in Backend. The ids are overwhelmingly consecutive pairs — `11406`/`11407`, `51999`/`52000`, `7219`/`7220` — the same "one release entered twice" signature already recorded for call-number collisions. They span the entire id history (69 below id 10000), which is what rules out a lagging incremental watermark as the cause.

The `.tsv` maps each unmatched `mysql_id` to the `backend_kept_legacy_id` that absorbed it, so the parity harness can subtract them by id rather than by count.

**Disposition: accept permanently.** Zeroing them would mean re-introducing 599 duplicate catalog rows.

## Set 2 — 12 structurally excluded rows (`residue-absent-12.txt`)

These never reach the ETL's insert path at all. The extraction SELECT uses INNER JOINs on `LIBRARY_CODE`, `GENRE`, and `FORMAT`, so a dangling FK drops the row before any skip logic runs; `db_only` genre and empty title are explicit skips.

| Reason | ids |
|---|---|
| empty `TITLE` | 21107, 39290, 51871, 52374, 65301, 66329 |
| genre = `db_only` | 50881, 50949, 71987, 71988 |
| dangling `LIBRARY_CODE_ID` | 36676, 64658 |

**Disposition: accept, or fix the 12 rows in `/wxycdb` before it goes dark.** Note 71987/71988 are recent — `db_only` is still being used, so this set grows slowly while `/wxycdb` stays open.

## Set 3 — 115 upstream-deleted orphans (`residue-orphan-115.txt`)

Releases present in Backend with a tubafrenzy-space legacy id that **no longer exists** in MySQL. `library-etl` is insert-only, so deletions made in `/wxycdb` were never propagated. This set is live and growing: MySQL's release count fell 101 between 2026-08-02 (64,828) and 2026-08-11 (64,727) while max ID rose, i.e. librarians are still deleting in `/wxycdb` today.

This is the release-level analogue of decision **D6** (the 4,161 extra CTA rows, kept and documented) — but wiki#89's release row does not mention it, and no decision covers it. At cutover these 115 become live rows in the Backend-sourced `library.db`, and therefore searchable in LML and the request line, despite having been deliberately deleted from the catalog.

**Disposition (decided 2026-08-11): delete the 115 from Backend before the flip.** Not mirroring D6 — the catalog should match librarian intent rather than resurrect deleted releases into live search. This needs a scripted, reviewed delete (and a decision on what to do with dependent rows: flowsheet references, rotation entries, `library_release_override` pins, streaming links). Until it runs, these surface in the harness as `extra_in_backend`.

**Exclusion (decided 2026-08-12): orphans carrying `flowsheet` references are not deleted.** `flowsheet.album_id` is `onDelete: 'set null'`, so deleting them would blank the album on historical plays — 59 plays across 7 releases on the 2026-08-11 snapshot. Those rows stay as accepted residue: a librarian's `/wxycdb` delete should not reach back into the playlist archive. The exclusion is a live predicate, not a fixed set of 7 — re-derive it with the orphan set at run time, and report the excluded ids so the harness carries them as documented residue rather than drift.

Note the set is **live and growing** while `/wxycdb` stays open, so the delete has to be re-derived immediately before the flip rather than run off this snapshot.

## Artist-side drift — same story, also not backfillable

| Surface | MySQL | Backend | Gap |
|---|---:|---:|---:|
| Artist codes | 24,428 | 23,879 | 549 |
| Artist-code cross-refs | 119 | 78 | 41 |
| Release cross-refs | 35 | 22 | 13 |

Backend has no `legacy_*_id` on `artists`, so identity is the ETL's own key: `(fold_artist_name(artist_name), lower(code_letters))`, where `fold_artist_name` is NFD → strip U+0300–U+036F → lowercase, and `code_letters` comes from `normalizeCodeLetters` (`Z-[A-Z]` → `V/A`, 3 chars → upper, otherwise first 2 chars upper). Replicating that key over all 24,428 `LIBRARY_CODE` rows decomposes the gap into three structural classes:

| Class | Rows | Why Backend has no row |
|---|---:|---|
| Collapsed onto a shared key | 271 | Two codes fold to one artist — e.g. 15 `Various Artists` variants under `Z--` collapse to one, plus pairs like `molasses`/`MO` |
| Empty `CALL_LETTERS` | 69 | `normalizeCodeLetters` returns null; the row has no usable code |
| Has letters but **zero releases** | 246 | Artists are only created from the release stream, so a code with no `LIBRARY_RELEASE` is never encountered |

Those overlap slightly (some no-release codes also share a fold key), and Backend holds artists created by paths other than this ETL, which is why the arithmetic lands ~37 rows off rather than exactly. The mechanism is not in doubt: **all three classes are structural, and re-running `library-etl` creates none of them.**

The two cross-ref gaps follow from the release drift: `importReleaseCrossrefs` skips a row whenever its artist, genre, or album lookup misses, so dropped releases cascade into dropped cross-refs. Decision **D5** already freezes artist-code cross-refs at cutover, so the 41 sit inside that freeze.

## What this changes in wiki#89

1. **AC#3** ("`library-etl` stopped and final catch-up run; drift table at 0") — rewrite. The catch-up run should be described as *"run the final catch-up; confirm the unmatched set equals this ledger"*, not *"drift at 0"*.
2. **AC#4 / D8** ("7 consecutive clean parity days", clean = 0 unmatched) — the definition already says "after the documented residue ledger"; this file is that ledger, and the harness needs the three id files to subtract.
3. **New decision row (D10)** for Set 3: the 115 orphans are deleted from Backend before the flip. Needs its own ticket — the delete has dependent-row implications and must be re-derived at flip time.
4. The quantified-gap table in the issue body is stale (measured 2026-08-02, before BS#1963 landed) and reports a one-directional "net 602". The drift is two-directional: 611 missing + 115 orphaned.

## Reproducing

Read-only. MySQL via `ssh kattare` with credentials from Tomcat's `setenv.sh`; Backend PG via `ssh wxyc-ec2` running `psql` in a throwaway `postgres:16` container against `--env-file ~/.env`. Scratch SQL and the id-set diff (`comm`-based, plus a tuple-collapse check in Python) are reconstructable from this file's description; nothing was written to either database.
