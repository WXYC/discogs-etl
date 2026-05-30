"""Pin tests for the copy-swap column lists in scripts/verify_cache.py.

Regression coverage for the schema-drift class shipping a CTAS pattern with a
hardcoded column list. See ``tests/integration/test_dedup.py::TestDedupCopySwapPreservesMasterId``
for the parallel guard on ``dedup_releases.DEDUP_TABLES``.

Both lists in verify_cache.py rebuild the cache tables by
``CREATE TABLE new_X AS SELECT {columns} FROM X`` followed by a RENAME swap.
``CREATE TABLE AS SELECT`` inherits only the columns named in the SELECT —
any column omitted from the column list is silently dropped from the live
table after the swap. The dropped columns leave no relic in pg_attribute,
so the drift is invisible to ALTER-history audits.

These tests assert that every column declared in ``schema/create_database.sql``
for the affected tables is present in both copy-swap column lists, so any
future schema addition that forgets to update verify_cache.py fails CI rather
than disappearing on the next monthly rebuild.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
SCHEMA_DIR = REPO_ROOT / "schema"

_VERIFY_CACHE_PATH = REPO_ROOT / "scripts" / "verify_cache.py"
# Guarded so multiple test files share one module object — otherwise the
# second-loaded copy shadows the first and breaks ProcessPool pickling for
# any worker holding references to symbols from the original load (see #109).
# Must register in sys.modules BEFORE exec_module so that @dataclass can
# resolve cls.__module__ back to the module object during class construction.
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _spec = importlib.util.spec_from_file_location("verify_cache", _VERIFY_CACHE_PATH)
    assert _spec is not None and _spec.loader is not None
    _vc = importlib.util.module_from_spec(_spec)
    sys.modules["verify_cache"] = _vc
    _spec.loader.exec_module(_vc)

PRUNE_COPY_TABLES = _vc.PRUNE_COPY_TABLES
COPY_TABLE_SPEC = _vc.COPY_TABLE_SPEC


def _parse_create_table_columns(table_name: str) -> list[str]:
    """Return the column names declared in ``schema/create_database.sql`` for table_name.

    The schema file is the source of truth. Parsing it keeps these tests
    automatically in sync with future ADD COLUMN additions to create_database.sql.

    Limitation: the body extractor uses non-greedy ``.*?\\)\\s*;`` and is not
    paren-balanced — it works only as long as no column-level ``CHECK (...)``
    constraints appear inside the CREATE TABLE body. The defensive guard below
    catches that case as a loud failure instead of a silently-wrong column list.
    """
    sql = SCHEMA_DIR.joinpath("create_database.sql").read_text()
    # WXYC/discogs-etl#242 flipped these to ``CREATE TABLE IF NOT EXISTS``;
    # tolerate both forms so the regex doesn't silently miss the body.
    pattern = re.compile(
        r"CREATE TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        + re.escape(table_name)
        + r"\s*\((.*?)\)\s*;",
        re.DOTALL | re.IGNORECASE,
    )
    match = pattern.search(sql)
    assert match is not None, f"CREATE TABLE {table_name} not found in create_database.sql"
    body = match.group(1)
    # Guard against nested-paren constructs the non-greedy regex can't handle.
    # If the matched body has unbalanced parens, the regex stopped early.
    assert body.count("(") == body.count(")"), (
        f"CREATE TABLE {table_name} body has unbalanced parens — the body extractor "
        f"is not paren-balanced and likely terminated at a nested ``)``. "
        f"Rewrite _parse_create_table_columns with a depth-aware matcher."
    )
    columns: list[str] = []
    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        line = line.split("--", 1)[0].strip().rstrip(",")
        if not line:
            continue
        if line.upper().startswith(("CONSTRAINT", "PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK")):
            continue
        col = line.split()[0]
        columns.append(col)
    return columns


def _columns_in_copy_list(table_name: str, copy_list: list) -> list[str]:
    """Pull the column-string portion out of a copy-spec tuple, normalize to list."""
    for entry in copy_list:
        if entry[0] != table_name:
            continue
        cols_field = entry[2]
        if isinstance(cols_field, str):
            return [c.strip() for c in cols_field.split(",")]
        return list(cols_field)
    raise AssertionError(f"{table_name} not in copy list: {[e[0] for e in copy_list]}")


# Tables whose CTAS column list must stay in sync with create_database.sql.
# Includes every table where this PR's column-drop bug actually surfaced
# (release, release_artist, release_label, release_track_artist). release_genre
# and release_style are 2-col release_id + value tables with no scope for
# drift and are intentionally omitted.
_TABLES_UNDER_TEST = ("release", "release_artist", "release_label", "release_track_artist")


class TestPruneCopyTablesCoversSchema:
    """The --prune copy-swap (the path the monthly rebuild uses) must preserve every column.

    Before the fix, ``release`` was reduced to 6 columns (missing ``released``
    and ``master_id``), ``release_artist`` to 4 (missing ``role``), and
    ``release_label`` to 2 (missing ``label_id`` and ``catno``). LML's
    ``cache_service.get_release`` SELECT then failed with
    ``column "released" does not exist`` on every release lookup, forcing a
    100% Tier-2 cache miss and saturating the Discogs API rate limit.
    """

    def test_every_schema_column_is_copied(self) -> None:
        for table in _TABLES_UNDER_TEST:
            schema_cols = set(_parse_create_table_columns(table))
            copy_cols = set(_columns_in_copy_list(table, PRUNE_COPY_TABLES))
            missing = schema_cols - copy_cols
            assert not missing, (
                f"PRUNE_COPY_TABLES[{table!r}] is missing {sorted(missing)} — "
                f"these columns will be dropped from {table} on every rebuild. "
                f"Update scripts/verify_cache.py:PRUNE_COPY_TABLES."
            )

    def test_release_keeps_released_and_master_id(self) -> None:
        """Regression pin for the 2026-05 outage."""
        cols = set(_columns_in_copy_list("release", PRUNE_COPY_TABLES))
        assert "released" in cols, "release.released dropped — see WXYC/discogs-etl outage 2026-05"
        assert "master_id" in cols, "release.master_id dropped — see WXYC/discogs-etl#129"

    def test_release_keeps_artwork_checked_at(self) -> None:
        """Regression pin for WXYC/discogs-etl#239.

        ``artwork_checked_at`` is set by LML's live-API path on every cache
        write_release. Dropping it from the prune copy-swap re-introduces
        the never-asked / asked-but-empty ambiguity that LML#414's runtime
        repair and LML#423's predicate update were designed to resolve.
        """
        cols = set(_columns_in_copy_list("release", PRUNE_COPY_TABLES))
        assert "artwork_checked_at" in cols, (
            "release.artwork_checked_at dropped from PRUNE_COPY_TABLES — "
            "see WXYC/discogs-etl#239 / WXYC/library-metadata-lookup#423"
        )

    def test_release_artist_keeps_role(self) -> None:
        cols = set(_columns_in_copy_list("release_artist", PRUNE_COPY_TABLES))
        assert "role" in cols, "release_artist.role dropped — see WXYC/discogs-etl#218"

    def test_release_label_keeps_label_id_and_catno(self) -> None:
        cols = set(_columns_in_copy_list("release_label", PRUNE_COPY_TABLES))
        assert "label_id" in cols, "release_label.label_id dropped from copy-swap"
        assert "catno" in cols, "release_label.catno dropped from copy-swap"

    def test_release_track_artist_keeps_extra_and_role(self) -> None:
        """Regression pin for #218: extra + role on release_track_artist."""
        cols = set(_columns_in_copy_list("release_track_artist", PRUNE_COPY_TABLES))
        assert "extra" in cols, "release_track_artist.extra dropped — see WXYC/discogs-etl#218"
        assert "role" in cols, "release_track_artist.role dropped — see WXYC/discogs-etl#218"


class TestCopyToTargetSpecCoversSchema:
    """The --copy-to target-db path's column spec must also preserve every column.

    Parallel guard to TestPruneCopyTablesCoversSchema. The --copy-to flag is
    deprecated per the cache-builder CLI convention but still functional, so
    the contract still matters — any operator who passes --copy-to/--target-db-url
    must get a complete schema in the destination.
    """

    def test_every_schema_column_is_copied(self) -> None:
        for table in _TABLES_UNDER_TEST:
            schema_cols = set(_parse_create_table_columns(table))
            copy_cols = set(_columns_in_copy_list(table, COPY_TABLE_SPEC))
            missing = schema_cols - copy_cols
            assert not missing, (
                f"COPY_TABLE_SPEC[{table!r}] is missing {sorted(missing)} — "
                f"--copy-to target DB will land an incomplete schema. "
                f"Update scripts/verify_cache.py:COPY_TABLE_SPEC."
            )
