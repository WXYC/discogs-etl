"""Integration tests for ``scripts/derive_va_release.py`` (#344).

``va_release`` is the derived VA-compilation lookup table consumed by LML's
compilation-matching scripts (``match_compilations.py``,
``canonicalize_compilations.py``, and through them the recall-index build
``build_compilation_track_location.py``). These tests exercise the derivation
predicate, the index shape, the atomic re-derivation swap, and the row-count
floor guard against a real PostgreSQL.
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = REPO_ROOT / "schema"

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from derive_va_release import (  # noqa: E402
    FloorViolation,
    derive_va_release,
)

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
    """Insert the canonical predicate-coverage fixture set.

    Titles follow the repo's WXYC-representative fixture guidance (comp-shelf
    style titles rather than mainstream catalog).
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
            # (e) ordinary single-artist release
            _insert_release(cur, 9005, "Aluminum Tunes")
            _insert_credit(cur, 9005, "Stereolab", artist_id=51002)
            # (f) two qualifying Various credits on one release — DISTINCT
            _insert_release(cur, 9006, "Broadcast Traffic: Airchecks 1994")
            _insert_credit(cur, 9006, "Various", artist_id=194)
            _insert_credit(cur, 9006, "Various", artist_id=None)
        conn.commit()
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
        conn = psycopg.connect(cache_db)
        try:
            count = derive_va_release(conn)
        finally:
            conn.close()

        rows = _va_rows(cache_db)
        # (a) canonical id, (b) ANV via id, (c) NULL-id name match, (f) DISTINCT
        assert set(rows) == {9001, 9002, 9003, 9006}
        assert count == 4

    def test_norm_title_is_lower_title_including_diacritics(self, cache_db: str) -> None:
        conn = psycopg.connect(cache_db)
        try:
            derive_va_release(conn)
        finally:
            conn.close()

        rows = _va_rows(cache_db)
        for title, norm_title in rows.values():
            assert norm_title == title.lower()
        # The diacritic fixture keeps its accents — lower() only, no unaccent.
        assert rows[9003][1] == "sonidos nuevos: música experimental"

    def test_only_gin_trigram_index_is_created(self, cache_db: str) -> None:
        conn = psycopg.connect(cache_db)
        try:
            derive_va_release(conn)
        finally:
            conn.close()

        indexes = _va_indexes(cache_db)
        assert "idx_va_release_norm_trgm" in indexes
        # The captured local DDL had a btree norm_title index no consumer
        # queries; it must NOT be recreated (dead weight on every derivation).
        assert "idx_va_release_norm_exact" not in indexes


class TestRederivation:
    def test_rederivation_replaces_stale_ids(self, cache_db: str) -> None:
        conn = psycopg.connect(cache_db)
        try:
            derive_va_release(conn)
        finally:
            conn.close()

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


class TestFloorGuard:
    def _delete_all_credits(self, db_url: str) -> None:
        conn = psycopg.connect(db_url)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM release_artist")
            conn.commit()
        finally:
            conn.close()

    def test_below_floor_rolls_back_and_preserves_existing_table(self, cache_db: str) -> None:
        conn = psycopg.connect(cache_db)
        try:
            derive_va_release(conn)
        finally:
            conn.close()
        before = _va_rows(cache_db)
        assert before

        self._delete_all_credits(cache_db)
        conn = psycopg.connect(cache_db)
        try:
            with pytest.raises(FloorViolation):
                derive_va_release(conn, floor=1)
        finally:
            conn.close()

        # The pre-existing derivation must survive the rolled-back swap.
        assert _va_rows(cache_db) == before

    def test_first_run_commits_even_when_empty(self, fresh_db_url: str) -> None:
        # No pre-existing va_release and zero qualifying credits: the floor is
        # skipped so the backfill always produces a table (an empty table beats
        # the UndefinedTableError crash #344 fixes).
        _set_up_cache_schema(fresh_db_url)
        conn = psycopg.connect(fresh_db_url)
        try:
            count = derive_va_release(conn, floor=1)
        finally:
            conn.close()

        assert count == 0
        assert _va_rows(fresh_db_url) == {}

    def test_floor_zero_opts_out_even_with_existing_table(self, cache_db: str) -> None:
        conn = psycopg.connect(cache_db)
        try:
            derive_va_release(conn)
        finally:
            conn.close()

        self._delete_all_credits(cache_db)
        conn = psycopg.connect(cache_db)
        try:
            count = derive_va_release(conn, floor=0)
        finally:
            conn.close()

        assert count == 0
        assert _va_rows(cache_db) == {}
