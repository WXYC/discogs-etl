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
import logging
import os
import sys as _sys
import threading
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

    * the clone predates release.artwork_checked_at / release.not_found -> forces
      the seed's column-intersection path (a bare COPY_TABLE_SPEC COPY would fail
      with 'column does not exist');
    * the clone has no release_video table -> forces the absent-table skip path;
    * the clone's cache_metadata.cached_at is NULLABLE and carries NULLs (the real
      clone does) -> a straight column copy into prod's NOT NULL cached_at would
      fail, so the seed must NOT copy the clone's freshness timestamps.
    """
    _apply_schema(db_url)
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE release DROP COLUMN artwork_checked_at")
        cur.execute("ALTER TABLE release DROP COLUMN not_found")
        cur.execute("DROP TABLE IF EXISTS release_video CASCADE")
        cur.execute("ALTER TABLE cache_metadata ALTER COLUMN cached_at DROP NOT NULL")
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

        # Source = mini clone (missing prod-only columns; nullable cache_metadata).
        _make_clone_source(source_url)
        conn = psycopg.connect(source_url)
        with conn.cursor() as cur:
            for rid, artist, title in CLONE_RELEASES:
                _insert_release(cur, rid, artist, title, artwork_url=f"http://art/{rid}.jpg")
                # Clone freshness rows carry NULL cached_at (the real clone does);
                # copying these into prod's NOT NULL column must not happen.
                cur.execute(
                    "INSERT INTO cache_metadata (release_id, source, cached_at) "
                    "VALUES (%s, 'bulk_import', NULL)",
                    (rid,),
                )
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

    @pytest.fixture(autouse=True)
    def _reset_target(self):
        """Reset the shared target to its pristine pre-seed baseline before each
        test, so tests are order-independent: every test starts with only the
        preexisting prod release (100) present, and an assertion-only test that
        needs seeded rows performs the (idempotent) seed itself rather than
        relying on an earlier test having run."""
        conn = psycopg.connect(self.__class__._target_url, autocommit=True)
        with conn.cursor() as cur:
            # CASCADE empties every table that FK-references release(id)
            # (release_artist/label/genre/style/track/track_artist/video,
            # cache_metadata) in one shot.
            cur.execute("TRUNCATE release CASCADE")
            _insert_release(
                cur,
                PREEXISTING_ID,
                "Juana Molina",
                PROD_TITLE_100,
                artwork_url="http://prod/100.jpg",
                include_prod_cols=True,
            )
        conn.close()

    def _seed_all(self) -> dict[str, int]:
        """Run the (idempotent) release seed for every CLONE_RELEASES id."""
        return seed_releases_additive(
            self.source_url, self.target_url, [rid for rid, _, _ in CLONE_RELEASES]
        )

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

    # --- cache_metadata seeded fresh, not copied ------------------------

    def test_cache_metadata_seeded_fresh(self) -> None:
        """The clone's NULL cached_at must never reach prod's NOT NULL column;
        each new release gets a fresh bulk_import row with cached_at defaulted."""
        self._seed_all()
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT release_id, source, cached_at FROM cache_metadata "
                "WHERE release_id = ANY(%s::integer[])",
                ([101, 102, 200],),
            )
            rows = cur.fetchall()
        conn.close()
        assert len(rows) == 3, "each new release should get one fresh cache_metadata row"
        for _rid, source, cached_at in rows:
            assert source == "bulk_import"
            assert cached_at is not None, "cached_at must default (not copy the clone's NULL)"

    # --- (e) prod-only columns default ----------------------------------

    def test_prod_only_columns_default(self) -> None:
        self._seed_all()
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_checked_at, not_found FROM release WHERE id = 101")
            checked_at, not_found = cur.fetchone()
        conn.close()
        assert checked_at is None
        assert not_found is False

    # --- (b) pre-existing rows byte-identical ---------------------------

    def test_preexisting_release_not_overwritten(self) -> None:
        self._seed_all()
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
        self._seed_all()  # first run populates the new releases + children
        counts = self._seed_all()  # second run must be a pure no-op
        assert counts["release"] == 0, "re-run must insert no new releases"
        # No duplicate child rows for an already-seeded release.
        assert self._child_count(self.target_url, "release_track", 101) == 2
        assert self._child_count(self.target_url, "release_artist", 200) == 1

    # --- candidate_ids materialization (finding #7) ----------------------

    def test_generator_candidate_ids_not_exhausted_prematurely(self, caplog) -> None:
        """A one-shot iterable (e.g. a generator) must not be silently exhausted
        by internal reuse. Seed once up front so all CLONE_RELEASES are already
        present, then re-seed passing a generator: this exercises the "no new
        ids" logging branch, which re-reads candidate_ids after _compute_new_ids
        has already consumed it."""
        self._seed_all()
        ids_gen = (rid for rid, _, _ in CLONE_RELEASES)
        with caplog.at_level(logging.INFO, logger="seed_cache_from_clone"):
            seed_releases_additive(self.source_url, self.target_url, ids_gen)
        assert any(
            f"all {len(CLONE_RELEASES)} candidates already present" in r.message
            for r in caplog.records
        ), (
            "candidate count in the log must reflect the materialized list, not an exhausted generator"
        )

    # --- dry-run cache_metadata mirrors the real release count (finding #6) -

    def test_dry_run_cache_metadata_mirrors_release_source_count(self) -> None:
        """cache_metadata's dry-run count must mirror release's real source-side
        count for the same id set, not echo len(new_ids) -- a candidate id absent
        from source (e.g. a stale/bogus id) must not inflate the fresh-seed
        count."""
        bogus_id = 999999  # not present on either database
        planned = seed_releases_additive(self.source_url, self.target_url, [bogus_id], dry_run=True)
        assert planned["release"] == 0
        assert planned["cache_metadata"] == 0, (
            "cache_metadata dry-run count must mirror the real (zero) release count, "
            "not len(new_ids)=1"
        )

    # --- --source needs no write access (finding #5) ----------------------

    def test_source_connection_never_writes(self) -> None:
        """The seed must work against a --source held under
        default_transaction_read_only -- proving it never CREATEs or COPYs INTO
        anything on the source, matching the runbook's read-only contract."""
        new_id = 901
        prep_conn = psycopg.connect(self.source_url)
        with prep_conn.cursor() as cur:
            _insert_release(
                cur,
                new_id,
                "Chuquimamani-Condori",
                "Nunca Estuve Sola",
                artwork_url=f"http://art/{new_id}.jpg",
            )
            cur.execute(
                "INSERT INTO cache_metadata (release_id, source, cached_at) "
                "VALUES (%s, 'bulk_import', NULL)",
                (new_id,),
            )
        prep_conn.commit()
        prep_conn.close()

        source_db_name = self.source_url.rsplit("/", 1)[-1]
        admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)
        with admin_conn.cursor() as cur:
            cur.execute(
                sql.SQL("ALTER DATABASE {} SET default_transaction_read_only = on").format(
                    sql.Identifier(source_db_name)
                )
            )
        admin_conn.close()
        try:
            counts = seed_releases_additive(self.source_url, self.target_url, [new_id])
        finally:
            admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)
            with admin_conn.cursor() as cur:
                cur.execute(
                    sql.SQL("ALTER DATABASE {} RESET default_transaction_read_only").format(
                        sql.Identifier(source_db_name)
                    )
                )
            admin_conn.close()

        assert counts["release"] == 1

    # --- concurrent-run serialization via advisory lock (finding #2) ------

    def test_advisory_lock_serializes_concurrent_seed_writes(self) -> None:
        """A holder of pg_advisory_xact_lock(330001) on the target must block a
        concurrent seed_releases_additive real run until the holder releases --
        otherwise two overlapping invocations could both compute the same
        release as "new" and double-insert its arbiter-less child rows."""
        new_id = 902
        prep_conn = psycopg.connect(self.source_url)
        with prep_conn.cursor() as cur:
            _insert_release(
                cur, new_id, "Cat Power", "Moon Pix", artwork_url=f"http://art/{new_id}.jpg"
            )
            cur.execute(
                "INSERT INTO cache_metadata (release_id, source, cached_at) "
                "VALUES (%s, 'bulk_import', NULL)",
                (new_id,),
            )
        prep_conn.commit()
        prep_conn.close()

        holder = psycopg.connect(self.target_url)
        with holder.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(330001)")

        result_box: dict = {}

        def run_seed() -> None:
            result_box["counts"] = seed_releases_additive(
                self.source_url, self.target_url, [new_id]
            )

        t = threading.Thread(target=run_seed)
        t.start()
        try:
            t.join(timeout=1.0)
            assert t.is_alive(), "seed must block while the advisory lock is held"
        finally:
            holder.rollback()  # releases pg_advisory_xact_lock
            holder.close()

        t.join(timeout=5.0)
        assert not t.is_alive(), "seed must proceed once the lock is released"
        assert result_box["counts"]["release"] == 1


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

# A clone-side artist row with a NULL fetched_at (e.g. legacy/backfilled data).
# artist.fetched_at is NOT NULL DEFAULT now() on prod; a straight copy of a NULL
# value must not raise -- it must default on the target instead.
NULL_FETCHED_AT_ARTIST_ID = 502
NULL_FETCHED_AT_ARTIST_NAME = "Hermanos Gutiérrez"


class TestSeedArtistsFromClone:
    """artist / artist_name_variation seeding: id-PK ON CONFLICT for artist,
    parent-gated (no-arbiter) inserts for artist_name_variation."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self):
        source_url, source_name = _create_temp_database()
        target_url, target_name = _create_temp_database()
        self.__class__._source_url = source_url
        self.__class__._target_url = target_url

        # Source clone: artist lacks the prod-only not_found column, and (like
        # the real clone) can carry a NULL fetched_at.
        _apply_schema(source_url)
        conn = psycopg.connect(source_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE artist DROP COLUMN not_found")
            cur.execute("ALTER TABLE artist ALTER COLUMN fetched_at DROP NOT NULL")
        conn.close()
        conn = psycopg.connect(source_url)
        with conn.cursor() as cur:
            for aid, name, variations in CLONE_ARTISTS:
                _insert_artist(cur, aid, name, variations)
            cur.execute(
                "INSERT INTO artist (id, name, fetched_at) VALUES (%s, %s, NULL)",
                (NULL_FETCHED_AT_ARTIST_ID, NULL_FETCHED_AT_ARTIST_NAME),
            )
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

    @pytest.fixture(autouse=True)
    def _reset_target(self):
        """Reset the shared target to its pristine pre-seed baseline before each
        test (only preexisting artist 500 present), so tests are
        order-independent; an assertion-only test seeds itself first."""
        conn = psycopg.connect(self.__class__._target_url, autocommit=True)
        with conn.cursor() as cur:
            # CASCADE empties artist_name_variation (and any other artist child)
            # alongside artist in one shot.
            cur.execute("TRUNCATE artist CASCADE")
            _insert_artist(
                cur,
                PREEXISTING_ARTIST,
                PROD_ARTIST_500,
                ["prod-only variation"],
                include_prod_cols=True,
            )
        conn.close()

    def _seed_all(self) -> dict[str, int]:
        """Run the (idempotent) artist seed for every CLONE_ARTISTS id."""
        return seed_artists_additive(
            self.source_url, self.target_url, [a for a, _, _ in CLONE_ARTISTS]
        )

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
        self._seed_all()
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT not_found FROM artist WHERE id = 501")
            not_found = cur.fetchone()[0]
        conn.close()
        assert not_found is False

    def test_preexisting_artist_not_overwritten(self) -> None:
        self._seed_all()
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM artist WHERE id = %s", (PREEXISTING_ARTIST,))
            name = cur.fetchone()[0]
        conn.close()
        assert name == PROD_ARTIST_500
        # The prod artist's single variation was NOT joined by the source's two.
        assert self._variation_count(PREEXISTING_ARTIST) == 1

    def test_second_run_no_duplicate_variations(self) -> None:
        self._seed_all()  # first run populates the new artist + variations
        counts = self._seed_all()  # second run must be a pure no-op
        assert counts["artist"] == 0
        assert self._variation_count(501) == 1

    # --- artist.fetched_at NOT NULL guard (finding #3) --------------------

    def test_null_fetched_at_defaults_on_target(self) -> None:
        """The clone can carry a NULL artist.fetched_at; the target's NOT NULL
        column must default (COALESCE to now()) rather than raising."""
        counts = seed_artists_additive(
            self.source_url, self.target_url, [NULL_FETCHED_AT_ARTIST_ID]
        )
        assert counts["artist"] == 1
        conn = psycopg.connect(self.target_url)
        with conn.cursor() as cur:
            cur.execute("SELECT fetched_at FROM artist WHERE id = %s", (NULL_FETCHED_AT_ARTIST_ID,))
            fetched_at = cur.fetchone()[0]
        conn.close()
        assert fetched_at is not None
