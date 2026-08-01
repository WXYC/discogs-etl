"""Integration tests for ``scripts/derive_va_release.py`` (#344).

``va_release`` is the derived VA-compilation lookup table consumed by LML's
compilation-matching scripts (``match_compilations.py``,
``canonicalize_compilations.py``, and through them the recall-index build
``build_compilation_track_location.py``). These tests exercise the derivation
predicate, the index shape, the atomic re-derivation swap, and the row-count
floor guard against a real PostgreSQL.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = REPO_ROOT / "schema"

# Load by file path (the repo idiom for scripts/ modules) rather than
# sys.path-inserting the scripts directory, which would leave every script
# importable by bare name for the rest of the pytest session.
_spec = importlib.util.spec_from_file_location(
    "derive_va_release", REPO_ROOT / "scripts" / "derive_va_release.py"
)
derive_module = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("derive_va_release", derive_module)
_spec.loader.exec_module(derive_module)

FloorViolation = derive_module.FloorViolation
derive_va_release = derive_module.derive_va_release

pytestmark = pytest.mark.pg


def _set_up_cache_schema(db_url: str) -> None:
    """Apply the cache schema (functions first — see run_pipeline #104 note)."""
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            cur.execute((SCHEMA_DIR / "create_functions.sql").read_text())
            cur.execute((SCHEMA_DIR / "create_database.sql").read_text())
    finally:
        conn.close()


def _insert_release(cur, release_id: int, title: str) -> None:
    cur.execute(
        "INSERT INTO release (id, title) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
        (release_id, title),
    )


def _insert_credit(
    cur,
    release_id: int,
    artist_name: str,
    artist_id: int | None = None,
    extra: int = 0,
) -> None:
    cur.execute(
        "INSERT INTO release_artist (release_id, artist_id, artist_name, extra)"
        " VALUES (%s, %s, %s, %s)",
        (release_id, artist_id, artist_name, extra),
    )


def _seed_fixture_releases(db_url: str) -> None:
    """Insert the predicate-coverage fixture set.

    Titles follow the repo's WXYC-representative fixture guidance; the
    Stereolab row reuses the canonical docs/test-fixtures.md ids (5002/102).
    """
    conn = psycopg.connect(db_url)
    try:
        with conn.cursor() as cur:
            # (a) plain main Various credit with the canonical artist id
            _insert_release(cur, 9001, "Lost In Translation (Music From The Motion Picture)")
            _insert_credit(cur, 9001, "Various", artist_id=194)
            # (b) ANV credit — name drifts, artist id is authoritative
            _insert_release(cur, 9002, "Tobacco A Go Go: Vintage Songs Of Smoke")
            _insert_credit(cur, 9002, "Various Artists", artist_id=194)
            # (c) API-fetched credit — LML runtime inserts leave artist_id NULL
            _insert_release(cur, 9003, "Sonidos Nuevos: Música Experimental")
            _insert_credit(cur, 9003, "Various", artist_id=None)
            # (d) Various only as an extra credit — not a VA compilation
            _insert_release(cur, 9004, "Pequena Vertigem De Amor")
            _insert_credit(cur, 9004, "Sessa", artist_id=51001)
            _insert_credit(cur, 9004, "Various", artist_id=194, extra=1)
            # (e) ordinary single-artist release (canonical fixture ids)
            _insert_release(cur, 5002, "Aluminum Tunes")
            _insert_credit(cur, 5002, "Stereolab", artist_id=102)
            # (f) two qualifying Various credits on one release — DISTINCT
            _insert_release(cur, 9006, "Broadcast Traffic: Airchecks 1994")
            _insert_credit(cur, 9006, "Various", artist_id=194)
            _insert_credit(cur, 9006, "Various", artist_id=None)
        conn.commit()
    finally:
        conn.close()


def _derive(db_url: str, **kwargs) -> int:
    conn = psycopg.connect(db_url)
    try:
        return derive_va_release(conn, **kwargs)
    finally:
        conn.close()


def _va_rows(db_url: str) -> dict[int, tuple[str, str]]:
    conn = psycopg.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, title, norm_title FROM va_release ORDER BY id")
            return {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    finally:
        conn.close()


def _va_indexes(db_url: str) -> set[str]:
    conn = psycopg.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT indexname FROM pg_indexes"
                " WHERE schemaname = 'public' AND tablename = 'va_release'"
            )
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


@pytest.fixture()
def cache_db(fresh_db_url: str) -> str:
    _set_up_cache_schema(fresh_db_url)
    _seed_fixture_releases(fresh_db_url)
    return fresh_db_url


class TestDerivationPredicate:
    def test_included_and_excluded_rows(self, cache_db: str) -> None:
        count = _derive(cache_db)

        rows = _va_rows(cache_db)
        # (a) canonical id, (b) ANV via id, (c) NULL-id name match, (f) DISTINCT
        assert set(rows) == {9001, 9002, 9003, 9006}
        assert count == 4

    def test_norm_title_is_pg_lower_title(self, cache_db: str) -> None:
        _derive(cache_db)

        rows = _va_rows(cache_db)
        # norm_title is PostgreSQL lower(title). For these fixtures that
        # coincides with Python str.lower(); the two diverge on titles like
        # Turkish dotted İ or Greek final sigma, where PG semantics are the
        # contract (both consumers compare norm_title against PG-side
        # lower($1) in SQL) — see the module docstring.
        for title, norm_title in rows.values():
            assert norm_title == title.lower()
        # The diacritic fixture keeps its accents — lower() only, no unaccent.
        assert rows[9003][1] == "sonidos nuevos: música experimental"

    def test_only_gin_trigram_index_is_created(self, cache_db: str) -> None:
        _derive(cache_db)

        indexes = _va_indexes(cache_db)
        assert "idx_va_release_norm_trgm" in indexes
        # The captured local DDL had a btree norm_title index no consumer
        # queries; it must NOT be recreated (dead weight on every derivation).
        assert "idx_va_release_norm_exact" not in indexes


class TestRederivation:
    def test_rederivation_replaces_stale_ids(self, cache_db: str) -> None:
        _derive(cache_db)

        # Simulate a rebuild/prune cycle: one VA release vanishes, one appears.
        conn = psycopg.connect(cache_db)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM release_artist WHERE release_id = 9001")
                cur.execute("DELETE FROM release WHERE id = 9001")
                _insert_release(cur, 9007, "Comemos Flores: Canciones De Primavera")
                _insert_credit(cur, 9007, "Various", artist_id=194)
            conn.commit()
            count = derive_va_release(conn)
        finally:
            conn.close()

        rows = _va_rows(cache_db)
        assert 9001 not in rows
        assert 9007 in rows
        assert count == 4

    def test_autocommit_connection_is_rejected(self, cache_db: str) -> None:
        """The atomic swap and the floor rollback both depend on one
        transaction; on an autocommit connection the DROP would already be
        durable when the floor guard runs, so the API refuses it outright."""
        conn = psycopg.connect(cache_db, autocommit=True)
        try:
            with pytest.raises(ValueError, match="autocommit"):
                derive_va_release(conn)
        finally:
            conn.close()


class TestFloorGuard:
    def _delete_all_credits(self, db_url: str) -> None:
        conn = psycopg.connect(db_url)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM release_artist")
            conn.commit()
        finally:
            conn.close()

    def test_below_floor_rolls_back_and_preserves_populated_table(self, cache_db: str) -> None:
        _derive(cache_db)
        before = _va_rows(cache_db)
        assert before

        self._delete_all_credits(cache_db)
        with pytest.raises(FloorViolation):
            _derive(cache_db, floor=1)

        # The pre-existing derivation must survive the rolled-back swap.
        assert _va_rows(cache_db) == before

    def test_first_run_commits_even_when_empty(self, fresh_db_url: str) -> None:
        # No pre-existing va_release and zero qualifying credits: the floor
        # has nothing to protect, so the backfill always produces a table (an
        # empty table beats the UndefinedTableError crash #344 fixes).
        _set_up_cache_schema(fresh_db_url)
        count = _derive(fresh_db_url, floor=1)

        assert count == 0
        assert _va_rows(fresh_db_url) == {}

    def test_empty_preexisting_table_recommits_empty_without_violation(self, cache_db: str) -> None:
        """The guard is keyed on pre-existing ROWS, not table existence: after
        an honest empty publish (the pipeline's --floor 0 path), a still-empty
        re-derivation must commit cleanly — not raise FloorViolation daily
        while 'protecting' zero rows."""
        self._delete_all_credits(cache_db)
        assert _derive(cache_db, floor=0) == 0  # honest empty publish
        assert _va_rows(cache_db) == {}

        count = _derive(cache_db, floor=1)  # daily default floor
        assert count == 0
        assert _va_rows(cache_db) == {}

    def test_floor_zero_opts_out_even_with_populated_table(self, cache_db: str) -> None:
        _derive(cache_db)

        self._delete_all_credits(cache_db)
        count = _derive(cache_db, floor=0)

        assert count == 0
        assert _va_rows(cache_db) == {}
