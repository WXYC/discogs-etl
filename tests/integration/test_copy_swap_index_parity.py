"""Pin tests for index recreation across the copy-swap rebuild paths.

Parallel guard to ``tests/integration/test_verify_cache_columns.py``, which
covers *columns*. This file covers *indexes*.

Both ``scripts/verify_cache.py`` (``--prune``) and ``scripts/dedup_releases.py``
rebuild cache tables with ``CREATE TABLE new_X AS SELECT ...`` followed by a
RENAME swap. CTAS carries no indexes, so every index on a swapped table is
destroyed by the swap and has to be recreated explicitly afterwards. An index
that the schema declares -- in ``create_database.sql``, ``create_indexes.sql``,
or ``create_track_indexes.sql`` -- but the swapping script does not recreate
therefore disappears on every rebuild.

That failure is quieter than the dropped-column class these tests were
originally written for. A dropped column raises ``column "X" does not exist``
on the next query; a dropped index raises nothing at all -- the planner simply
picks a worse path and the query gets slower. There is no error to trace back
to the rebuild.

Both halves of the guard are scoped per script, because the two scripts do not
swap the same tables and do not run the same recreation code:

* The obligation is over the tables **that script** swaps, parsed from its own
  roster literal (``PRUNE_COPY_TABLES`` / ``DEDUP_TABLES``). Dedup does not
  swap the track tables, so it owes nothing for the track indexes.
* The discharge is the DDL on **that script's post-swap path**, parsed from the
  functions it actually calls after the swap. Matching the whole file instead
  would credit dedup with recreating the track indexes -- DDL that really is in
  ``dedup_releases.py``, in ``add_track_constraints_and_indexes``, which the
  post-swap path never calls. A guard satisfied by dead code would stay green
  through exactly the relocation it exists to catch.

Regression origin
-----------------

``release_artwork_null_idx`` hit exactly that. Migration 0008 creates it, and
the production cache has run well past that revision (head
``0013_adopt_entity_identity``), yet an audit of prod on 2026-08-19 found
``release`` carrying only ``release_pkey`` and ``idx_release_title_trgm``. The
index was created by 0008 as designed and then eaten by the next copy-swap,
which is also why the "dual-write convention keeps the fresh-rebuild and
alembic-upgrade paths in parity" claim in ``create_database.sql`` was false in
practice: the *third* path, the copy-swap rebuild, was never in parity with
either.

Its consumer is ``scripts/topup_artwork.py``, whose candidate query is exactly
the index predicate (``artwork_url IS NULL AND artwork_checked_at IS NULL``).
Without the index that drain seq-scans the full ``release`` table.

See WXYC/discogs-etl#239 for the index, and
``tests/integration/test_alembic_0008_artwork_checked_at.py`` for the migration
that creates it.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
SCHEMA_DIR = REPO_ROOT / "schema"
VERIFY_CACHE_PY = REPO_ROOT / "scripts" / "verify_cache.py"
DEDUP_RELEASES_PY = REPO_ROOT / "scripts" / "dedup_releases.py"

# Every schema file that declares indexes. ``create_database.sql`` is the
# cold-build schema; the other two are applied later in the pipeline, after the
# data they index has been imported (steps 6 and 8 in docs/architecture.md), and
# are the canonical home for the trigram indexes. All three declare indexes on
# copy-swapped tables, so all three have to feed the declared set -- parsing
# only create_database.sql would leave the trigram indexes unguarded, which is
# how a #409-shaped regression could recur with this file still green.
_SCHEMA_FILES = (
    SCHEMA_DIR / "create_database.sql",
    SCHEMA_DIR / "create_indexes.sql",
    SCHEMA_DIR / "create_track_indexes.sql",
)


@dataclass(frozen=True)
class _RebuildScript:
    """One copy-swap rebuild path, described well enough to check its parity.

    ``roster_literal`` names the module-level list of ``(old, new, cols, id)``
    tuples the script copy-swaps; it is parsed out of the source rather than
    imported because importing either script pulls in the compiled ``wxyc_etl``
    extension, which these pure-text parsers do not otherwise need.

    ``post_swap_functions`` names the functions that run *after* the swap and
    are therefore where recreation DDL has to live to count. The list is
    maintained by hand against the call sites in each script's ``main()``;
    :meth:`TestRecreationParsingIsScopedToThePostSwapPath.
    test_post_swap_functions_are_called_by_their_script` fails if one of them
    stops being called, and the parsers raise rather than return an empty set if
    one stops existing.
    """

    label: str
    path: Path
    roster_literal: str
    post_swap_functions: tuple[str, ...]


# verify_cache.main() runs _prune_copy_swap_tables() then
# _prune_add_base_constraints_and_indexes() -- nothing else touches indexes
# after the swap.
_PRUNE = _RebuildScript(
    label="scripts/verify_cache.py",
    path=VERIFY_CACHE_PY,
    roster_literal="PRUNE_COPY_TABLES",
    post_swap_functions=("_prune_add_base_constraints_and_indexes",),
)

# dedup_releases.main() swaps every DEDUP_TABLES entry then calls
# add_base_constraints_and_indexes(). It deliberately does NOT call
# add_track_constraints_and_indexes() -- DEDUP_TABLES has no track tables, so
# their indexes were never destroyed and there is nothing to put back.
_DEDUP = _RebuildScript(
    label="scripts/dedup_releases.py",
    path=DEDUP_RELEASES_PY,
    roster_literal="DEDUP_TABLES",
    post_swap_functions=("add_base_constraints_and_indexes",),
)

_REBUILD_SCRIPTS = (_PRUNE, _DEDUP)

# Indexes declared on a copy-swapped table that the rebuild paths deliberately
# do NOT recreate. Every entry needs a reason. An entry here is a decision on
# record, not a TODO -- if the decision is revisited, delete the entry and add
# the DDL to both scripts instead.
# Empty by design. The sole former entry, ``idx_release_master_id``, was exempted
# on the premise that its absence post-swap was deliberate (WXYC/discogs-etl#320).
# WXYC/discogs-etl#412 overturned that with a prod measurement -- a master_id
# filter on `release` was a 192ms / 133,637-buffer full scan of all 148,491 rows
# with the index absent, and 0.069ms / 5 buffers against the same predicate once
# it existed -- and this file's own rule for a revisited decision is to delete the
# entry and add the DDL to both scripts, which #412 did. Keeping the roster (and
# TestExemptionRosterIsCurrent) in place: it is the seam a future deliberate
# omission needs, and its guards are correct when empty.
_EXEMPT_FROM_RECREATION: dict[str, str] = {}

_DECLARED_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(\w+)\s+ON\s+(\w+)",
    re.IGNORECASE | re.DOTALL,
)

_RECREATED_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+IF\s+NOT\s+EXISTS\s+(\w+)",
    re.IGNORECASE,
)


def _parse_declared_indexes() -> dict[str, str]:
    """Map index name -> table name for every CREATE INDEX across _SCHEMA_FILES.

    A few names are declared in more than one file (e.g. the track FK indexes
    appear in both create_database.sql and create_track_indexes.sql). Those are
    the same index on the same table, so a plain union is correct;
    ``test_no_index_name_is_declared_on_two_different_tables`` fails loudly if a
    name ever resolves to two different tables, which would make the merged
    mapping arbitrary.
    """
    declared: dict[str, str] = {}
    for path in _SCHEMA_FILES:
        for name, table in _DECLARED_INDEX_RE.findall(path.read_text()):
            declared[name] = table
    return declared


def _parse_swapped_tables(script: _RebuildScript) -> set[str]:
    """Return the tables ``script`` copy-swaps, from its roster literal.

    Raises rather than returning an empty set when the literal cannot be found:
    an empty roster would silently make every coverage assertion below vacuous.
    """
    source = script.path.read_text()
    block = re.search(rf"^{script.roster_literal}\b[^=]*=\s*\[(.*?)^\]", source, re.S | re.M)
    if block is None:
        raise AssertionError(
            f"{script.roster_literal} literal not found in {script.label}. The "
            f"copy-swap roster moved or changed shape; the index-parity guards "
            f"in this file cannot see what the script swaps."
        )
    tables = set(re.findall(r'\(\s*"(\w+)",\s*"new_\w+"', block.group(1)))
    if not tables:
        raise AssertionError(
            f"Parsed no tables out of {script.label}:{script.roster_literal}. The "
            f'(old, "new_X", cols, id) tuple shape changed; every coverage '
            f"assertion in this file would pass vacuously."
        )
    return tables


def _parse_recreated_indexes(script: _RebuildScript) -> set[str]:
    """Return index names ``script`` recreates on its post-swap path.

    Scoped to ``script.post_swap_functions`` rather than the whole file, so DDL
    that exists in the module but is unreachable from the swap does not count as
    recreation. Raises if a named function is missing, since an unresolved name
    would quietly shrink the set to nothing.
    """
    source = script.path.read_text()
    tree = ast.parse(source)
    found: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            if node.name in script.post_swap_functions:
                found[node.name] = ast.get_source_segment(source, node) or ""
    missing = sorted(set(script.post_swap_functions) - set(found))
    if missing:
        raise AssertionError(
            f"{script.label} no longer defines {missing}, named in "
            f"_RebuildScript.post_swap_functions. Update the descriptor to the "
            f"functions that now run after the swap -- an unresolved name would "
            f"make this script's recreation set empty and the guard vacuous."
        )
    names: set[str] = set()
    for segment in found.values():
        names.update(_RECREATED_INDEX_RE.findall(segment))
    return names


def _declared_on(tables: set[str]) -> dict[str, str]:
    return {name: table for name, table in _parse_declared_indexes().items() if table in tables}


def _all_swapped_tables() -> set[str]:
    """Union of both scripts' swap rosters -- the full at-risk table set."""
    return set().union(*(_parse_swapped_tables(s) for s in _REBUILD_SCRIPTS))


class TestCopySwapRecreatesDeclaredIndexes:
    """Every index on a copy-swapped table must survive that script's rebuild."""

    def test_every_declared_index_on_a_swapped_table_is_recreated(self) -> None:
        for script in _REBUILD_SCRIPTS:
            declared = _declared_on(_parse_swapped_tables(script))
            recreated = _parse_recreated_indexes(script)
            missing = {
                name: table
                for name, table in declared.items()
                if name not in recreated and name not in _EXEMPT_FROM_RECREATION
            }
            assert not missing, (
                f"{script.label} does not recreate {sorted(missing)} after the copy-swap. "
                f"CTAS carries no indexes, so these are destroyed on every rebuild and "
                f"never come back -- silently, since a missing index degrades plans "
                f"rather than raising. Add the CREATE INDEX CONCURRENTLY DDL to "
                f"{sorted(script.post_swap_functions)} in {script.label}, or add an "
                f"entry with a reason to _EXEMPT_FROM_RECREATION."
            )

    def test_release_artwork_null_idx_is_recreated(self) -> None:
        """Regression pin: the index prod was found missing on 2026-08-19.

        Consumer is scripts/topup_artwork.py, whose candidate query is the
        index predicate verbatim. See WXYC/discogs-etl#239.
        """
        for script in _REBUILD_SCRIPTS:
            assert "release_artwork_null_idx" in _parse_recreated_indexes(script), (
                f"release_artwork_null_idx is not recreated on {script.label}'s "
                f"post-swap path -- topup_artwork.py will seq-scan the full release "
                f"table. See WXYC/discogs-etl#239."
            )

    def test_idx_release_master_id_is_recreated(self) -> None:
        """Regression pin: the index prod was found missing on 2026-08-20.

        Same class as release_artwork_null_idx above, found one day later by a
        different consumer -- WXYC/library-metadata-lookup#1241's sibling-pressing
        artwork lookup, which filters `release` by master_id on the artwork-miss
        path. Both rebuild paths CTAS `release` on the monthly rebuild's default
        route (dedup first, then verify_cache --prune), so recreating it on only
        one site does not survive a full rebuild: the later CTAS silently undoes
        the earlier recreation.

        See WXYC/discogs-etl#412, and
        tests/integration/test_copy_swap_preserves_master_id_index.py for the
        behavioral (live-Postgres) pin that the index lands with this exact
        partial predicate -- which a source-text parser cannot verify.
        """
        for script in _REBUILD_SCRIPTS:
            assert "idx_release_master_id" in _parse_recreated_indexes(script), (
                f"idx_release_master_id is not recreated on {script.label}'s post-swap "
                f"path -- a master_id filter on `release` will full-scan the table. "
                f"See WXYC/discogs-etl#412."
            )


class TestIndexParityGuardsAreNotVacuous:
    """The parsers above are regex/AST over source text; assert they actually parsed."""

    def test_schema_declares_indexes_on_swapped_tables(self) -> None:
        declared = _declared_on(_all_swapped_tables())
        assert len(declared) >= 12, (
            f"Only parsed {len(declared)} declared indexes on copy-swapped tables "
            f"({sorted(declared)}). The CREATE INDEX regex or one of the files in "
            f"_SCHEMA_FILES changed shape; the coverage test above is passing "
            f"vacuously."
        )

    def test_declared_indexes_include_the_trigram_schema_files(self) -> None:
        """The declared set must span every schema file, not just create_database.sql.

        The trigram indexes are declared in ``create_indexes.sql`` /
        ``create_track_indexes.sql``, not in ``create_database.sql`` -- several of
        them on copy-swapped tables. If the declared set is parsed from
        ``create_database.sql`` alone, an index added to one of those files on a
        swapped table and omitted from the rebuild scripts reproduces #409
        exactly with this guard still green.
        """
        declared = _declared_on(_all_swapped_tables())
        for name in (
            "idx_release_title_trgm",
            "idx_release_artist_name_trgm",
            "idx_release_track_title_trgm",
            "idx_release_track_artist_name_trgm",
        ):
            assert name in declared, (
                f"{name} is declared on a copy-swapped table but is not in the parsed "
                f"declared set. _SCHEMA_FILES is not covering every file that declares "
                f"indexes on swapped tables, so the coverage test above has a blind spot."
            )

    def test_no_index_name_is_declared_on_two_different_tables(self) -> None:
        """A name reused across schema files makes the merged mapping arbitrary."""
        seen: dict[str, tuple[Path, str]] = {}
        conflicts: list[str] = []
        for path in _SCHEMA_FILES:
            for name, table in _DECLARED_INDEX_RE.findall(path.read_text()):
                prior = seen.get(name)
                if prior is not None and prior[1] != table:
                    conflicts.append(
                        f"{name}: {prior[0].name} says {prior[1]}, {path.name} says {table}"
                    )
                seen[name] = (path, table)
        assert not conflicts, (
            f"Index names declared on conflicting tables: {conflicts}. "
            f"_parse_declared_indexes merges the schema files into one mapping, so "
            f"whichever file is parsed last silently wins and index coverage is "
            f"attributed to the wrong table."
        )

    def test_each_rebuild_script_recreates_indexes(self) -> None:
        for script in _REBUILD_SCRIPTS:
            recreated = _parse_recreated_indexes(script)
            assert len(recreated) >= 9, (
                f"Only parsed {len(recreated)} recreated indexes from "
                f"{sorted(script.post_swap_functions)} in {script.label} "
                f"({sorted(recreated)}). The CREATE INDEX CONCURRENTLY regex no longer "
                f"matches that script's DDL; the coverage test above cannot fail."
            )

    def test_each_rebuild_script_swaps_the_release_table(self) -> None:
        """Cheap sanity check that the roster parse produced the real swap set."""
        for script in _REBUILD_SCRIPTS:
            tables = _parse_swapped_tables(script)
            assert "release" in tables, (
                f"{script.label}:{script.roster_literal} parsed as {sorted(tables)}, "
                f"which does not include the release table. The roster literal changed "
                f"shape and the parse is picking up the wrong thing."
            )


class TestRecreationParsingIsScopedToThePostSwapPath:
    """Finding the DDL *somewhere* in the file is not the property we need."""

    def test_dedup_recreation_set_excludes_ddl_off_its_post_swap_path(self) -> None:
        """dedup's track-index DDL is real but unreachable from its swap.

        ``dedup_releases.main()`` swaps ``DEDUP_TABLES`` (which has no track
        tables) and then calls ``add_base_constraints_and_indexes`` only. The
        track DDL lives in ``add_track_constraints_and_indexes``, which that path
        never calls -- so a file-wide grep credits dedup with recreating indexes
        it does not recreate, and would keep the guard green if the DDL that
        *is* on the post-swap path were relocated into a dead branch.
        """
        recreated = _parse_recreated_indexes(_DEDUP)
        assert "idx_release_track_title_trgm" not in recreated, (
            "_parse_recreated_indexes credited dedup_releases.py with "
            "idx_release_track_title_trgm, which only appears in "
            "add_track_constraints_and_indexes -- a function main() does not call "
            "after the swap. The parser is matching the whole file instead of the "
            "post-swap path."
        )

    def test_post_swap_functions_are_called_by_their_script(self) -> None:
        """A named post-swap function that nothing calls is dead code, not a discharge."""
        for script in _REBUILD_SCRIPTS:
            tree = ast.parse(script.path.read_text())
            called = {
                node.func.id
                for node in ast.walk(tree)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            }
            uncalled = sorted(set(script.post_swap_functions) - called)
            assert not uncalled, (
                f"{script.label} never calls {uncalled}, which "
                f"_RebuildScript.post_swap_functions names as its post-swap "
                f"recreation path. Either the call site moved -- in which case the "
                f"indexes are no longer being recreated -- or the descriptor is stale."
            )


class TestExemptionRosterIsCurrent:
    """An exemption for an index that no longer exists hides a coverage hole."""

    def test_every_exemption_is_still_declared_on_a_swapped_table(self) -> None:
        declared = _declared_on(_all_swapped_tables())
        stale = sorted(set(_EXEMPT_FROM_RECREATION) - set(declared))
        assert not stale, (
            f"_EXEMPT_FROM_RECREATION lists {stale}, which the schema no longer "
            f"declares on a copy-swapped table. Remove the stale entry -- a dead "
            f"exemption silently widens the hole the next time that name is reused."
        )

    def test_no_exemption_is_also_recreated(self) -> None:
        """An index both exempted and recreated means the roster is lying."""
        for script in _REBUILD_SCRIPTS:
            recreated = _parse_recreated_indexes(script)
            contradictory = sorted(set(_EXEMPT_FROM_RECREATION) & recreated)
            assert not contradictory, (
                f"{contradictory} are exempted from recreation but {script.label} "
                f"recreates them anyway. Drop the _EXEMPT_FROM_RECREATION entry -- the "
                f"exemption documents a decision that is no longer in force."
            )
