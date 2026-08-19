"""Pin tests for index recreation across the copy-swap rebuild paths.

Parallel guard to ``tests/integration/test_verify_cache_columns.py``, which
covers *columns*. This file covers *indexes*.

Both ``scripts/verify_cache.py`` (``--prune``) and ``scripts/dedup_releases.py``
rebuild cache tables with ``CREATE TABLE new_X AS SELECT ...`` followed by a
RENAME swap. CTAS carries no indexes, so every index on a swapped table is
destroyed by the swap and has to be recreated explicitly afterwards. An index
that ``schema/create_database.sql`` declares but neither script recreates
therefore disappears on every rebuild.

That failure is quieter than the dropped-column class these tests were
originally written for. A dropped column raises ``column "X" does not exist``
on the next query; a dropped index raises nothing at all — the planner simply
picks a worse path and the query gets slower. There is no error to trace back
to the rebuild.

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

import re
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
SCHEMA_SQL = REPO_ROOT / "schema" / "create_database.sql"
VERIFY_CACHE_PY = REPO_ROOT / "scripts" / "verify_cache.py"
DEDUP_RELEASES_PY = REPO_ROOT / "scripts" / "dedup_releases.py"

# Tables rebuilt by the copy-swap, mirroring
# ``scripts/verify_cache.py:PRUNE_COPY_TABLES``. Hardcoded rather than imported
# because importing verify_cache pulls in the compiled ``wxyc_etl`` extension,
# which these pure-text parsers do not otherwise need. The roster is pinned
# against the real list by ``test_swapped_table_roster_matches_prune_copy_tables``
# below, so drift fails loudly instead of silently shrinking coverage.
_COPY_SWAPPED_TABLES = frozenset(
    {
        "release",
        "release_artist",
        "release_label",
        "release_genre",
        "release_style",
        "release_track",
        "release_track_artist",
        "cache_metadata",
    }
)

# Indexes declared on a copy-swapped table that the rebuild paths deliberately
# do NOT recreate. Every entry needs a reason. An entry here is a decision on
# record, not a TODO -- if the decision is revisited, delete the entry and add
# the DDL to both scripts instead.
_EXEMPT_FROM_RECREATION: dict[str, str] = {
    "idx_release_master_id": (
        "Deliberate and documented in schema/create_database.sql: the swap's "
        "CTAS carries no indexes and add_base_constraints_and_indexes does not "
        "recreate this one, so it is gone post-swap by design. The master_id "
        "COLUMN itself persists (it is in dedup_releases.DEDUP_TABLES). See "
        "WXYC/discogs-etl#320 for the contract history."
    ),
}


def _parse_declared_indexes() -> dict[str, str]:
    """Map index name -> table name for every CREATE INDEX in create_database.sql."""
    sql = SCHEMA_SQL.read_text()
    pattern = re.compile(
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(\w+)\s+ON\s+(\w+)",
        re.IGNORECASE | re.DOTALL,
    )
    return {name: table for name, table in pattern.findall(sql)}


def _parse_recreated_indexes(script: Path) -> set[str]:
    """Return index names a rebuild script recreates via CREATE INDEX CONCURRENTLY."""
    source = script.read_text()
    pattern = re.compile(
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+IF\s+NOT\s+EXISTS\s+(\w+)",
        re.IGNORECASE,
    )
    return set(pattern.findall(source))


def _declared_on_swapped_tables() -> dict[str, str]:
    return {
        name: table
        for name, table in _parse_declared_indexes().items()
        if table in _COPY_SWAPPED_TABLES
    }


_REBUILD_SCRIPTS = (
    ("scripts/verify_cache.py", VERIFY_CACHE_PY),
    ("scripts/dedup_releases.py", DEDUP_RELEASES_PY),
)


class TestCopySwapRecreatesDeclaredIndexes:
    """Every index on a copy-swapped table must survive the rebuild."""

    def test_every_declared_index_on_a_swapped_table_is_recreated(self) -> None:
        declared = _declared_on_swapped_tables()
        for label, script in _REBUILD_SCRIPTS:
            recreated = _parse_recreated_indexes(script)
            missing = {
                name: table
                for name, table in declared.items()
                if name not in recreated and name not in _EXEMPT_FROM_RECREATION
            }
            assert not missing, (
                f"{label} does not recreate {sorted(missing)} after the copy-swap. "
                f"CTAS carries no indexes, so these are destroyed on every rebuild and "
                f"never come back -- silently, since a missing index degrades plans "
                f"rather than raising. Add the CREATE INDEX CONCURRENTLY DDL to {label}, "
                f"or add an entry with a reason to _EXEMPT_FROM_RECREATION."
            )

    def test_release_artwork_null_idx_is_recreated(self) -> None:
        """Regression pin: the index prod was found missing on 2026-08-19.

        Consumer is scripts/topup_artwork.py, whose candidate query is the
        index predicate verbatim. See WXYC/discogs-etl#239.
        """
        for label, script in _REBUILD_SCRIPTS:
            assert "release_artwork_null_idx" in _parse_recreated_indexes(script), (
                f"release_artwork_null_idx is not recreated by {label} -- "
                f"topup_artwork.py will seq-scan the full release table. "
                f"See WXYC/discogs-etl#239."
            )


class TestIndexParityGuardsAreNotVacuous:
    """The parsers above are regex over source text; assert they actually parsed."""

    def test_schema_declares_indexes_on_swapped_tables(self) -> None:
        declared = _declared_on_swapped_tables()
        assert len(declared) >= 8, (
            f"Only parsed {len(declared)} declared indexes on copy-swapped tables "
            f"({sorted(declared)}). The CREATE INDEX regex or "
            f"schema/create_database.sql changed shape; the coverage test above is "
            f"passing vacuously."
        )

    def test_each_rebuild_script_recreates_indexes(self) -> None:
        for label, script in _REBUILD_SCRIPTS:
            recreated = _parse_recreated_indexes(script)
            assert len(recreated) >= 10, (
                f"Only parsed {len(recreated)} recreated indexes from {label} "
                f"({sorted(recreated)}). The CREATE INDEX CONCURRENTLY regex no longer "
                f"matches that script's DDL; the coverage test above cannot fail."
            )

    def test_swapped_table_roster_matches_prune_copy_tables(self) -> None:
        """_COPY_SWAPPED_TABLES is hardcoded; pin it against the real list.

        Parsed from source rather than imported, for the reason given at the
        roster's definition.
        """
        source = VERIFY_CACHE_PY.read_text()
        block = re.search(r"^PRUNE_COPY_TABLES\s*=\s*\[(.*?)^\]", source, re.S | re.M)
        assert block is not None, "PRUNE_COPY_TABLES literal not found in verify_cache.py"
        actual = set(re.findall(r'\(\s*"(\w+)",\s*"new_\w+"', block.group(1)))
        assert actual == set(_COPY_SWAPPED_TABLES), (
            f"_COPY_SWAPPED_TABLES is stale: PRUNE_COPY_TABLES now swaps {sorted(actual)}. "
            f"Update the roster in this file so index coverage tracks the real swap set."
        )


class TestExemptionRosterIsCurrent:
    """An exemption for an index that no longer exists hides a coverage hole."""

    def test_every_exemption_is_still_declared_on_a_swapped_table(self) -> None:
        declared = _declared_on_swapped_tables()
        stale = sorted(set(_EXEMPT_FROM_RECREATION) - set(declared))
        assert not stale, (
            f"_EXEMPT_FROM_RECREATION lists {stale}, which create_database.sql no longer "
            f"declares on a copy-swapped table. Remove the stale entry -- a dead "
            f"exemption silently widens the hole the next time that name is reused."
        )

    def test_no_exemption_is_also_recreated(self) -> None:
        """An index both exempted and recreated means the roster is lying."""
        for label, script in _REBUILD_SCRIPTS:
            recreated = _parse_recreated_indexes(script)
            contradictory = sorted(set(_EXEMPT_FROM_RECREATION) & recreated)
            assert not contradictory, (
                f"{contradictory} are exempted from recreation but {label} recreates "
                f"them anyway. Drop the _EXEMPT_FROM_RECREATION entry -- the exemption "
                f"documents a decision that is no longer in force."
            )
