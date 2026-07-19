"""Integration tests for scripts/seed_cache_from_clone.py against real PostgreSQL.

Exercises the additive, artist-scoped clone -> prod seed (BS#1631 tail cache).
The core properties under test:

  (a) only genuinely-new release_ids (and their children) are inserted;
  (b) pre-existing prod rows are left byte-identical (never overwritten);
  (c) a second run is a no-op -- no duplicate child rows (the arbiter-less
      parent-gated idempotency property);
  (d) --dry-run writes nothing;
  (e) prod-only columns absent in the clone (release.artwork_checked_at,
      release.not_found) default on the target rather than failing the COPY;
  (f) a table absent in the clone (release_video) is skipped, not fatal.

Two-DB source->target harness modeled on tests/integration/test_copy_to_target.py
(uuid-named scratch databases), but the source is populated by direct INSERTs so
the test controls exactly which release_ids pre-exist on the target and can
simulate the real clone's missing prod-only columns.
"""

from __future__ import annotations

import importlib.util
import os
import sys as _sys
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

ADMIN_URL = os.environ.get("DATABASE_URL_TEST", "postgresql://localhost:5433/postgres")

# Load the module under test from scripts/ (not on sys.path).
_SEED_PATH = Path(__file__).parent.parent.parent / "scripts" / "seed_cache_from_clone.py"
if "seed_cache_from_clone" in _sys.modules:
    _seed = _sys.modules["seed_cache_from_clone"]
else:
    _spec = importlib.util.spec_from_file_location("seed_cache_from_clone", _SEED_PATH)
    assert _spec is not None and _spec.loader is not None
    _seed = importlib.util.module_from_spec(_spec)
    _sys.modules["seed_cache_from_clone"] = _seed
    _spec.loader.exec_module(_seed)

seed_releases_additive = _seed.seed_releases_additive
seed_artists_additive = _seed.seed_artists_additive
compute_new_release_ids = _seed.compute_new_release_ids

pytestmark = [pytest.mark.pg]


# ---------------------------------------------------------------------------
# Scratch database helpers (mirror test_copy_to_target.py)
# ---------------------------------------------------------------------------


def _create_temp_database() -> tuple[str, str]:
    db_name = f"discogs_seed_test_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    admin_conn.close()
    base = ADMIN_URL.rsplit("/", 1)[0]
    return f"{base}/{db_name}", db_name


def _drop_database(db_name: str) -> None:
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = {} AND pid <> pg_backend_pid()"
            ).format(sql.Literal(db_name))
        )
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
    admin_conn.close()


def _apply_schema(db_url: str) -> None:
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    conn.close()


def _make_clone_source(db_url: str) -> None:
    """Prod-shaped schema, then mutated to mirror the real clone:

    the clone predates release.artwork_checked_at / release.not_found and has no
    release_video table. Dropping them here forces the seed's column-intersection
    path (a bare COPY_TABLE_SPEC COPY would fail with 'column does not exist').
    """
    _apply_schema(db_url)
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE release DROP COLUMN artwork_checked_at")
        cur.execute("ALTER TABLE release DROP COLUMN not_found")
        cur.execute("DROP TABLE IF EXISTS release_video CASCADE")
        cur.execute("DROP TABLE IF EXISTS cache_metadata CASCADE")
    conn.close()


def _insert_release(
    cur: psycopg.Cursor,
    rid: int,
    artist: str,
    title: str,
    *,
    artwork_url: str | None = None,
    include_prod_cols: bool = False,
) -> None:
    """Insert a release + one main artist + two tracks + a genre.

    include_prod_cols=False matches the clone (no artwork_checked_at/not_found).
    """
    if include_prod_cols:
        cur.execute(
            "INSERT INTO release (id, title, artwork_url, not_found) VALUES (%s, %s, %s, FALSE)",
            (rid, title, artwork_url),
        )
    else:
        cur.execute(
            "INSERT INTO release (id, title, artwork_url) VALUES (%s, %s, %s)",
            (rid, title, artwork_url),
        )
    cur.execute(
        "INSERT INTO release_artist (release_id, artist_id, artist_name, extra) "
        "VALUES (%s, %s, %s, 0)",
        (rid, rid * 10, artist),
    )
    cur.execute(
        "INSERT INTO release_track (release_id, sequence, position, title) VALUES (%s, 1, 'A1', %s)",
        (rid, f"{title} - track one"),
    )
    cur.execute(
        "INSERT INTO release_track (release_id, sequence, position, title) VALUES (%s, 2, 'A2', %s)",
        (rid, f"{title} - track two"),
    )
    cur.execute("INSERT INTO release_genre (release_id, genre) VALUES (%s, 'Rock')", (rid,))


# Source (clone) release ids and their tail artists.
CLONE_RELEASES = [
    (100, "Juana Molina", "Segundo"),
    (101, "Juana Molina", "Halo"),
    (102, "Sessa", "Grandeza"),
    (200, "Chuquimamani-Condori", "DJ E"),
]
# The one release already present on the prod target before seeding.
PREEXISTING_ID = 100
PROD_TITLE_100 = "PROD-SIDE TITLE (must survive)"


class TestSeedCacheFromClone:
    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self):
        source_url, source_name = _create_temp_database()
        target_url, target_name = _create_temp_database()
        self.__class__._source_url = source_url
        self.__class__._target_url = target_url

        # Source = mini clone (missing prod-only columns).
        _make_clone_source(source_url)
        conn = psycopg.connect(source_url)
        with conn.cursor() as cur:
            for rid, artist, title in CLONE_RELEASES:
                _insert_release(cur, rid, artist, title, artwork_url=f"http://art/{rid}.jpg")
        conn.commit()
        conn.close()

        # Target = prod-shaped, with release 100 already present under a
        # DIFFERENT title so any overwrite is detectable.
        _apply_schema(target_url)
        conn = psycopg.connect(target_url)
        with conn.cursor() as cur:
            _insert_release(
                cur,
                PREEXISTING_ID,
                "Juana Molina",
                PROD_TITLE_100,
                artwork_url="http://prod/100.jpg",
                include_prod_cols=True,
            )
        conn.commit()
        conn.close()

        yield

        _drop_database(source_name)
        _drop_database(target_name)

    @pytest.fixture(autouse=True)
    def _attrs(self):
        self.source_url = self.__class__._source_url
        self.target_url = self.__class__._target_url

    def _child_count(self, url: str, table: str, rid: int) -> int:
        conn = psycopg.connect(url)
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table} WHERE release_id = %s", (rid,))
            n = cur.fetchone()[0]
        conn.close()
        return n

    def _release_ids(self, url: str) -> set[int]:
        conn = psycopg.connect(url)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release")
            ids = {r[0] for r in cur.fetchall()}
        conn.close()
        return ids

    # --- (d) dry-run writes nothing -------------------------------------

    def test_dry_run_writes_nothing(self) -> None:
        before = self._release_ids(self.target_url)
        planned = seed_releases_additive(
            self.source_url,
            self.target_url,
            [rid for rid, _, _ in CLONE_RELEASES],
            dry_run=True,
        )
        after = self._release_ids(self.target_url)
        assert after == before, "dry-run must not insert any releases"
        # Planned counts should reflect the 3 genuinely-new releases.
        assert planned["release"] == 3

    def test_compute_new_release_ids_excludes_existing(self) -> None:
        new_ids = compute_new_release_ids(self.target_url, [rid for rid, _, _ in CLONE_RELEASES])
        assert new_ids == {101, 102, 200}

    # --- (a) inserts only the new releases + children -------------------

    def test_seed_inserts_only_new(self) -> None:
        counts = seed_releases_additive(
            self.source_url,
            self.target_url,
            [rid for rid, _, _ in CLONE_RELEASES],
        )
        assert self._release_ids(self.target_url) == {100, 101, 102, 200}
        # 3 new releases inserted (100 already existed -> skipped).
        assert counts["release"] == 3
        # Children present for the new releases.
        assert self._child_count(self.target_url, "release_track", 101) == 2
        assert self._child_count(self.target_url, "release_artist", 200) == 1

    # --- (e) prod-only columns default ----------------------------------

    def test_prod_only_columns_default(self) -> None:
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_checked_at, not_found FROM release WHERE id = 101")
            checked_at, not_found = cur.fetchone()
        conn.close()
        assert checked_at is None
        assert not_found is False

    # --- (b) pre-existing rows byte-identical ---------------------------

    def test_preexisting_release_not_overwritten(self) -> None:
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT title, artwork_url FROM release WHERE id = %s", (PREEXISTING_ID,))
            title, artwork = cur.fetchone()
        conn.close()
        assert title == PROD_TITLE_100, "existing prod release must not be overwritten"
        assert artwork == "http://prod/100.jpg"
        # And its children were NOT duplicated from the source copy of 100.
        assert self._child_count(self.target_url, "release_artist", PREEXISTING_ID) == 1
        assert self._child_count(self.target_url, "release_track", PREEXISTING_ID) == 2

    # --- (c) second run is a no-op --------------------------------------

    def test_second_run_is_noop(self) -> None:
        counts = seed_releases_additive(
            self.source_url,
            self.target_url,
            [rid for rid, _, _ in CLONE_RELEASES],
        )
        assert counts["release"] == 0, "re-run must insert no new releases"
        # No duplicate child rows for an already-seeded release.
        assert self._child_count(self.target_url, "release_track", 101) == 2
        assert self._child_count(self.target_url, "release_artist", 200) == 1


def _insert_artist(
    cur: psycopg.Cursor,
    aid: int,
    name: str,
    variations: list[str],
    *,
    include_prod_cols: bool = False,
) -> None:
    if include_prod_cols:
        cur.execute("INSERT INTO artist (id, name, not_found) VALUES (%s, %s, FALSE)", (aid, name))
    else:
        cur.execute("INSERT INTO artist (id, name) VALUES (%s, %s)", (aid, name))
    for v in variations:
        cur.execute("INSERT INTO artist_name_variation (artist_id, name) VALUES (%s, %s)", (aid, v))


# (artist_id, name, [name_variations])
CLONE_ARTISTS = [
    (500, "Juana Molina", ["Juana Molina y Los Hermanos", "J. Molina"]),
    (501, "Sessa", ["Sérgio Sessa"]),
]
PREEXISTING_ARTIST = 500
PROD_ARTIST_500 = "PROD-SIDE ARTIST (must survive)"


class TestSeedArtistsFromClone:
    """artist / artist_name_variation seeding: id-PK ON CONFLICT for artist,
    parent-gated (no-arbiter) inserts for artist_name_variation."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self):
        source_url, source_name = _create_temp_database()
        target_url, target_name = _create_temp_database()
        self.__class__._source_url = source_url
        self.__class__._target_url = target_url

        # Source clone: artist lacks the prod-only not_found column.
        _apply_schema(source_url)
        conn = psycopg.connect(source_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE artist DROP COLUMN not_found")
        conn.close()
        conn = psycopg.connect(source_url)
        with conn.cursor() as cur:
            for aid, name, variations in CLONE_ARTISTS:
                _insert_artist(cur, aid, name, variations)
        conn.commit()
        conn.close()

        # Target: artist 500 already present under a different name + 1 variation.
        _apply_schema(target_url)
        conn = psycopg.connect(target_url)
        with conn.cursor() as cur:
            _insert_artist(
                cur,
                PREEXISTING_ARTIST,
                PROD_ARTIST_500,
                ["prod-only variation"],
                include_prod_cols=True,
            )
        conn.commit()
        conn.close()

        yield

        _drop_database(source_name)
        _drop_database(target_name)

    @pytest.fixture(autouse=True)
    def _attrs(self):
        self.source_url = self.__class__._source_url
        self.target_url = self.__class__._target_url

    def _variation_count(self, aid: int) -> int:
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM artist_name_variation WHERE artist_id = %s", (aid,))
            n = cur.fetchone()[0]
        conn.close()
        return n

    def _artist_ids(self) -> set[int]:
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM artist")
            ids = {r[0] for r in cur.fetchall()}
        conn.close()
        return ids

    def test_dry_run_writes_nothing(self) -> None:
        before = self._artist_ids()
        planned = seed_artists_additive(
            self.source_url, self.target_url, [a for a, _, _ in CLONE_ARTISTS], dry_run=True
        )
        assert self._artist_ids() == before
        assert planned["artist"] == 1  # only 501 is new

    def test_seed_inserts_only_new_artist(self) -> None:
        counts = seed_artists_additive(
            self.source_url, self.target_url, [a for a, _, _ in CLONE_ARTISTS]
        )
        assert self._artist_ids() == {500, 501}
        assert counts["artist"] == 1
        assert self._variation_count(501) == 1

    def test_prod_only_column_defaults(self) -> None:
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT not_found FROM artist WHERE id = 501")
            not_found = cur.fetchone()[0]
        conn.close()
        assert not_found is False

    def test_preexisting_artist_not_overwritten(self) -> None:
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM artist WHERE id = %s", (PREEXISTING_ARTIST,))
            name = cur.fetchone()[0]
        conn.close()
        assert name == PROD_ARTIST_500
        # The prod artist's single variation was NOT joined by the source's two.
        assert self._variation_count(PREEXISTING_ARTIST) == 1

    def test_second_run_no_duplicate_variations(self) -> None:
        counts = seed_artists_additive(
            self.source_url, self.target_url, [a for a, _, _ in CLONE_ARTISTS]
        )
        assert counts["artist"] == 0
        assert self._variation_count(501) == 1
