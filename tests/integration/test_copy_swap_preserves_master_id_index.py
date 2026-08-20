"""Pin: idx_release_master_id survives both copy-swap rebuild paths.

Sibling regression to ``tests/integration/test_copy_swap_preserves_not_null.py``
(WXYC/discogs-etl#254/#256, NOT NULL / DEFAULT survival) and
``tests/integration/test_verify_cache_columns.py`` (column survival). This
file covers *one specific index*: ``idx_release_master_id``.

``schema/create_database.sql`` declares
``idx_release_master_id ON release(master_id) WHERE master_id IS NOT NULL``,
but neither copy-swap rebuild path recreated it after the swap:
``CREATE TABLE new_release AS SELECT ...`` (CTAS) carries no indexes, and
this index was simply absent from both
``dedup_releases.add_base_constraints_and_indexes``'s and
``verify_cache._prune_add_base_constraints_and_indexes``'s CONCURRENTLY
index lists. Verified read-only against prod on 2026-08-20: ``release``
carried exactly ``release_pkey`` and ``idx_release_title_trgm``. A
``master_id`` filter was a 192ms / 133,637-buffer full scan of all 148,491
rows with the index absent; an index scan against the same predicate later
that day took 0.069ms / 5 buffers.

Two independent copy-swap sites both CTAS ``release`` on the monthly
rebuild's default path (library.db supplied): ``dedup_releases.py`` runs
first, ``verify_cache.py --prune`` runs second and does its own fresh CTAS
of ``release`` — so a fix that only touches one site does not survive a
full rebuild; the later site's CTAS silently undoes the earlier site's
recreation. Both are pinned here.

See WXYC/discogs-etl#412. The general "every declared index is recreated by
every rebuild path" audit lives in
``tests/integration/test_copy_swap_index_parity.py``; this file is the
behavioral regression pin for this specific index (does it actually land on
a live Postgres table with the exact predicate), which a pure source-text
parser cannot verify on its own.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import psycopg
import pytest

pytestmark = pytest.mark.pg

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

# Load production modules using the sys.modules guard convention shared by
# test_copy_swap_preserves_not_null.py / test_verify_cache_columns.py:
# registering in sys.modules before exec_module lets dataclass/typing
# introspection inside the loaded module resolve its own __module__ name,
# and sharing one module object across test files avoids a second-loaded
# copy shadowing the first (see #109).
_DEDUP_PATH = Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py"
if "dedup_releases" in sys.modules:
    _dd = sys.modules["dedup_releases"]
else:
    _dspec = importlib.util.spec_from_file_location("dedup_releases", _DEDUP_PATH)
    assert _dspec is not None and _dspec.loader is not None
    _dd = importlib.util.module_from_spec(_dspec)
    sys.modules["dedup_releases"] = _dd
    _dspec.loader.exec_module(_dd)

_VERIFY_CACHE_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _vcspec = importlib.util.spec_from_file_location("verify_cache", _VERIFY_CACHE_PATH)
    assert _vcspec is not None and _vcspec.loader is not None
    _vc = importlib.util.module_from_spec(_vcspec)
    sys.modules["verify_cache"] = _vc
    _vcspec.loader.exec_module(_vc)

_INDEX_NAME = "idx_release_master_id"
_EXPECTED_PREDICATE = "master_id IS NOT NULL"


def _drop_all_tables(conn) -> None:
    """Clear pipeline tables and any leftover ``new_`` copy-swap artifacts."""
    base = (
        "cache_metadata",
        "release_track_artist",
        "release_track",
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
    """Apply schema + insert a handful of releases, some sharing a master_id.

    Three releases: two share master_id=100 (one of which dedup/prune will
    drop), one has NULL master_id (exercises the partial predicate — it must
    never appear in the index). Compositions from WXYC's canonical example
    data per docs/test-fixtures.md.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())

            cur.executemany(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (%s, %s, %s, %s, %s)",
                [
                    (1, "DOGA", 100, "LP", "AR"),
                    (2, "DOGA (reissue)", 100, "LP", "US"),
                    (3, "Edits", None, "CD", "US"),
                ],
            )
            cur.executemany(
                "INSERT INTO release_artist (release_id, artist_name) VALUES (%s, %s)",
                [
                    (1, "Juana Molina"),
                    (2, "Juana Molina"),
                    (3, "Chuquimamani-Condori"),
                ],
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


def _index_def(conn, table: str, index_name: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexdef FROM pg_indexes WHERE tablename = %s AND indexname = %s",
            (table, index_name),
        )
        row = cur.fetchone()
        return row[0] if row else None


class TestDedupCopySwapPreservesMasterIdIndex:
    """dedup_releases.add_base_constraints_and_indexes recreates idx_release_master_id."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_dedup(self, db_url):
        self.__class__._db_url = db_url
        _seed_minimal_fixture(db_url)
        _run_dedup_swap(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_index_exists_after_dedup_swap(self):
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            indexdef = _index_def(conn, "release", _INDEX_NAME)
        finally:
            conn.close()
        assert indexdef is not None, (
            f"{_INDEX_NAME} is missing from `release` after the dedup copy-swap. "
            f"CTAS carries no indexes; scripts/dedup_releases.py:"
            f"add_base_constraints_and_indexes must recreate it alongside the "
            f"other CONCURRENTLY indexes."
        )

    def test_index_predicate_matches_schema(self):
        """The recreated index must match create_database.sql's partial predicate
        exactly, or the two definitions can silently diverge."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            indexdef = _index_def(conn, "release", _INDEX_NAME)
        finally:
            conn.close()
        assert indexdef is not None
        assert _EXPECTED_PREDICATE in indexdef, (
            f"{_INDEX_NAME} exists but its predicate ({indexdef!r}) does not match "
            f"schema/create_database.sql's `WHERE {_EXPECTED_PREDICATE}`. The "
            f"post-swap recreation DDL in dedup_releases.py must match the schema "
            f"declaration verbatim."
        )


class TestPruneCopySwapPreservesMasterIdIndex:
    """verify_cache.prune_releases_copy_swap recreates idx_release_master_id.

    The prune step runs after dedup on every rebuild that supplies
    library.db and performs its own independent CTAS of `release` — so this
    is not redundant with the dedup-side pin above: a fix that lands only in
    dedup_releases.py does not survive a full pipeline run, because prune's
    CTAS drops the index right back out immediately afterward.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_prune(self, db_url):
        self.__class__._db_url = db_url
        _seed_minimal_fixture(db_url)
        _vc.prune_releases_copy_swap(db_url, keep_ids={1, 3}, review_ids=set())

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_index_exists_after_prune_swap(self):
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            indexdef = _index_def(conn, "release", _INDEX_NAME)
        finally:
            conn.close()
        assert indexdef is not None, (
            f"{_INDEX_NAME} is missing from `release` after the prune copy-swap. "
            f"CTAS carries no indexes; scripts/verify_cache.py:"
            f"_prune_add_base_constraints_and_indexes must recreate it alongside "
            f"the other CONCURRENTLY indexes."
        )

    def test_index_predicate_matches_schema(self):
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            indexdef = _index_def(conn, "release", _INDEX_NAME)
        finally:
            conn.close()
        assert indexdef is not None
        assert _EXPECTED_PREDICATE in indexdef, (
            f"{_INDEX_NAME} exists but its predicate ({indexdef!r}) does not match "
            f"schema/create_database.sql's `WHERE {_EXPECTED_PREDICATE}`. The "
            f"post-swap recreation DDL in verify_cache.py must match the schema "
            f"declaration verbatim."
        )
