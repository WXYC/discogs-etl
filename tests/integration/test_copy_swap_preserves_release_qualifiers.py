"""Pin: release.status / notes / data_quality survive both copy-swap paths.

Sibling to ``tests/integration/test_copy_swap_preserves_master_id_index.py``
(index survival) and ``tests/integration/test_verify_cache_columns.py``
(source-level column parity for ``verify_cache.py``'s two lists). This file
covers the three columns added by WXYC/discogs-etl#428.

Both rebuild paths rebuild ``release`` with ``CREATE TABLE new_release AS
SELECT {columns} FROM release`` and then RENAME. A CTAS inherits only the
columns its SELECT names, and the swap carries that absence onto the live
table — leaving no relic in ``pg_attribute``, so the loss is invisible to any
ALTER-history audit and only shows up as a "column does not exist" at read
time, a month later. That is exactly how ``release.master_id`` disappeared in
[#129](https://github.com/WXYC/discogs-etl/issues/129) and five more columns in
[#232](https://github.com/WXYC/discogs-etl/issues/232).

Two independent CTAS sites run on the monthly rebuild's default path:
``dedup_releases.py`` first, then ``verify_cache.py --prune``, which does its
own fresh CTAS of ``release``. A fix in only one does not survive a full
rebuild, because the later site's CTAS silently undoes the earlier one. Both
are pinned here behaviorally (do the values still exist on a live table after
the swap?), alongside a source-level parity guard that generalizes the pin to
every column ``schema/create_database.sql`` declares for a ``DEDUP_TABLES``
table — the check ``verify_cache.py``'s lists have had since #232 and
``DEDUP_TABLES`` has not.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
SCHEMA_DIR = REPO_ROOT / "schema"

# Load production modules using the sys.modules guard convention shared by the
# sibling copy-swap tests: registering in sys.modules before exec_module lets
# dataclass/typing introspection inside the loaded module resolve its own
# __module__, and sharing one module object across test files avoids a
# second-loaded copy shadowing the first (see #109).
_DEDUP_PATH = REPO_ROOT / "scripts" / "dedup_releases.py"
if "dedup_releases" in sys.modules:
    _dd = sys.modules["dedup_releases"]
else:
    _dspec = importlib.util.spec_from_file_location("dedup_releases", _DEDUP_PATH)
    assert _dspec is not None and _dspec.loader is not None
    _dd = importlib.util.module_from_spec(_dspec)
    sys.modules["dedup_releases"] = _dd
    _dspec.loader.exec_module(_dd)

_VERIFY_CACHE_PATH = REPO_ROOT / "scripts" / "verify_cache.py"
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _vcspec = importlib.util.spec_from_file_location("verify_cache", _VERIFY_CACHE_PATH)
    assert _vcspec is not None and _vcspec.loader is not None
    _vc = importlib.util.module_from_spec(_vcspec)
    sys.modules["verify_cache"] = _vc
    _vcspec.loader.exec_module(_vc)

QUALIFIER_COLUMNS = ("status", "notes", "data_quality")

# Releases 1 and 2 collide on (master_id=100, format='LP') so dedup drops one;
# release 3 is the singleton survivor. Compositions from WXYC's canonical
# example data per docs/test-fixtures.md.
_SEED_ROWS = [
    (1, "DOGA", 100, "LP", "AR", "Accepted", "Reissue of the 2013 Sonamos LP.", "Correct"),
    (2, "DOGA (reissue)", 100, "LP", "US", "Draft", None, "Needs Vote"),
    (3, "Edits", 200, "CD", "US", "Deleted", None, "Entirely Incorrect"),
]
_SURVIVOR_QUALIFIERS = {
    1: ("Accepted", "Reissue of the 2013 Sonamos LP.", "Correct"),
    3: ("Deleted", None, "Entirely Incorrect"),
}


def _parse_create_table_columns(table_name: str) -> list[str]:
    """Column names declared for *table_name* in ``schema/create_database.sql``.

    Deliberately a thin re-implementation of the helper in
    ``test_verify_cache_columns.py`` rather than an import of it: importing
    across test modules couples two pin files that exist to fail independently.
    """
    import re

    sql = SCHEMA_DIR.joinpath("create_database.sql").read_text()
    pattern = re.compile(
        r"CREATE TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?" + re.escape(table_name) + r"\s*\((.*?)\)\s*;",
        re.DOTALL | re.IGNORECASE,
    )
    match = pattern.search(sql)
    assert match is not None, f"CREATE TABLE {table_name} not found in create_database.sql"
    body = match.group(1)
    assert body.count("(") == body.count(")"), (
        f"CREATE TABLE {table_name} body has unbalanced parens — the non-greedy "
        f"body extractor terminated early. Rewrite with a depth-aware matcher."
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
        columns.append(line.split()[0])
    return columns


def _copied_columns(table_name: str, copy_list: list) -> set[str]:
    for entry in copy_list:
        if entry[0] != table_name:
            continue
        cols_field = entry[2]
        if isinstance(cols_field, str):
            return {c.strip() for c in cols_field.split(",")}
        return set(cols_field)
    raise AssertionError(f"{table_name} not in copy list: {[e[0] for e in copy_list]}")


class TestDedupTablesCoverSchema:
    """``DEDUP_TABLES`` must name every column the schema declares.

    ``verify_cache.py``'s two lists have had this guard since #232;
    ``DEDUP_TABLES`` — the list that actually lost ``master_id`` in #129 — has
    not, which is why #232 could drop five more columns three revisions later.
    """

    def test_every_schema_column_is_copied(self) -> None:
        for old_table, _new_table, columns, _id_col in _dd.DEDUP_TABLES:
            schema_cols = set(_parse_create_table_columns(old_table))
            missing = schema_cols - {c.strip() for c in columns.split(",")}
            assert not missing, (
                f"DEDUP_TABLES[{old_table!r}] is missing {sorted(missing)} — the "
                f"dedup copy-swap will drop these from {old_table} on the next "
                f"rebuild, silently. Update scripts/dedup_releases.py:DEDUP_TABLES."
            )


class TestQualifiersInEveryCopyList:
    """Explicit three-column pin across all three hardcoded copy lists."""

    @pytest.mark.parametrize(
        ("list_name", "copy_list"),
        [
            ("dedup_releases.DEDUP_TABLES", _dd.DEDUP_TABLES),
            ("verify_cache.PRUNE_COPY_TABLES", _vc.PRUNE_COPY_TABLES),
            ("verify_cache.COPY_TABLE_SPEC", _vc.COPY_TABLE_SPEC),
        ],
    )
    def test_release_keeps_the_qualifier_columns(self, list_name, copy_list) -> None:
        cols = _copied_columns("release", copy_list)
        missing = [c for c in QUALIFIER_COLUMNS if c not in cols]
        assert not missing, (
            f"{list_name}['release'] is missing {missing} — see WXYC/discogs-etl#428. "
            f"The columns would survive the import and disappear at the copy-swap, "
            f"exactly as master_id did in #129."
        )


def _drop_all_tables(conn) -> None:
    """Clear pipeline tables and any leftover ``new_`` copy-swap artifacts."""
    base = (
        "cache_metadata",
        "release_track_artist",
        "release_track",
        "release_video",
        "release_style",
        "release_genre",
        "release_label",
        "release_artist",
        "release",
    )
    with conn.cursor() as cur:
        for t in base:
            cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS new_{t} CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS {t}_old CASCADE")
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids CASCADE")
        cur.execute("DROP TABLE IF EXISTS _keep_ids CASCADE")


def _seed_minimal_fixture(db_url: str) -> None:
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.executemany(
                "INSERT INTO release "
                "(id, title, master_id, format, country, status, notes, data_quality) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                _SEED_ROWS,
            )
            cur.executemany(
                "INSERT INTO release_artist (release_id, artist_name) VALUES (%s, %s)",
                [(1, "Juana Molina"), (2, "Juana Molina"), (3, "Chuquimamani-Condori")],
            )
            cur.executemany(
                "INSERT INTO cache_metadata (release_id, source) VALUES (%s, %s)",
                [(1, "bulk_import"), (2, "bulk_import"), (3, "bulk_import")],
            )
    finally:
        conn.close()


def _run_dedup_swap(db_url: str) -> None:
    """Exercise dedup's copy-swap path end-to-end (drops release 2)."""
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE UNLOGGED TABLE dedup_delete_ids (release_id integer PRIMARY KEY)")
            cur.execute("INSERT INTO dedup_delete_ids VALUES (2)")

        for old, new, cols, id_col in _dd.DEDUP_TABLES:
            _dd.copy_table(conn, old, new, cols, id_col)

        with conn.cursor() as cur:
            for stmt in (
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE release_genre DROP CONSTRAINT IF EXISTS fk_release_genre_release",
                "ALTER TABLE release_style DROP CONSTRAINT IF EXISTS fk_release_style_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ):
                cur.execute(stmt)

        for old, new, _, _ in _dd.DEDUP_TABLES:
            _dd.swap_tables(conn, old, new)

        _dd.add_base_constraints_and_indexes(conn, db_url=db_url)
    finally:
        conn.close()


def _surviving_qualifiers(db_url: str) -> dict[int, tuple]:
    """Read the three columns back off the post-swap ``release`` table.

    An ``UndefinedColumn`` here is the failure this file exists to catch, so it
    is re-raised as a named assertion rather than a bare psycopg error.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT id, status, notes, data_quality FROM release ORDER BY id")
            except psycopg.errors.UndefinedColumn as exc:
                raise AssertionError(
                    f"A qualifier column is gone from `release` after the copy-swap: {exc}. "
                    f"CTAS inherits only the columns its SELECT names — add them to the "
                    f"copied-column string (WXYC/discogs-etl#428, precedent #129/#232)."
                ) from exc
            return {row[0]: row[1:] for row in cur.fetchall()}
    finally:
        conn.close()


@pytest.mark.pg
class TestDedupCopySwapPreservesQualifiers:
    """dedup_releases.py's CTAS keeps the three columns AND their values."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_dedup(self, db_url):
        self.__class__._db_url = db_url
        _seed_minimal_fixture(db_url)
        _run_dedup_swap(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_qualifiers_survive_the_dedup_swap(self) -> None:
        assert _surviving_qualifiers(self.db_url) == _SURVIVOR_QUALIFIERS


@pytest.mark.pg
class TestPruneCopySwapPreservesQualifiers:
    """verify_cache.py --prune runs a second, independent CTAS of ``release``.

    Not redundant with the dedup pin above: it runs *after* dedup on every
    rebuild that supplies library.db, so a fix landing only in
    dedup_releases.py is undone here immediately afterwards.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_prune(self, db_url):
        self.__class__._db_url = db_url
        _seed_minimal_fixture(db_url)
        _vc.prune_releases_copy_swap(db_url, keep_ids={1, 3}, review_ids=set())

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_qualifiers_survive_the_prune_swap(self) -> None:
        assert _surviving_qualifiers(self.db_url) == _SURVIVOR_QUALIFIERS
