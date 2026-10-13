"""Integration tests for #450: a rebuild preserves LML's ``api_fetch`` releases.

The rebuild destroys ``cache_metadata.source = 'api_fetch'`` releases at three
seams (import-stage prune, base-import TRUNCATE, verify-prune's copy-swap).
``lib.api_fetch_preservation`` snapshots them into scratch tables before the
first destructive statement and restores, after prune, every snapshot release
the dump did not carry (the dump wins on an id collision). These tests drive
the real copy-swap prune between snapshot and restore so the restored rows must
satisfy the FK and NOT NULL constraints the swap re-applies.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path

import psycopg
import pytest

from lib.api_fetch_preservation import (
    SNAPSHOT_SUFFIX,
    ApiFetchRestoreError,
    restore_api_fetch_releases,
    snapshot_api_fetch_releases,
)
from lib.scratch_namespace import scratch_name

pytestmark = pytest.mark.pg

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

_VC_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _spec = importlib.util.spec_from_file_location("verify_cache", _VC_PATH)
    assert _spec is not None and _spec.loader is not None
    _vc = importlib.util.module_from_spec(_spec)
    sys.modules["verify_cache"] = _vc
    _spec.loader.exec_module(_vc)

_IC_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_ic_spec = importlib.util.spec_from_file_location("import_csv_api_fetch_pg", _IC_PATH)
assert _ic_spec is not None and _ic_spec.loader is not None
_ic = importlib.util.module_from_spec(_ic_spec)
_ic_spec.loader.exec_module(_ic)

CHILD_TABLES = (
    "release_artist",
    "release_label",
    "release_genre",
    "release_style",
    "release_track",
    "release_track_artist",
    "release_video",
)
ALL_TABLES = ("cache_metadata", *CHILD_TABLES, "release")

# Dump-resident release (survives prune), an api_fetch-only release (the one to
# preserve), and an api_fetch release the dump also carries (the dump wins).
DUMP_ID, API_ONLY_ID, API_AND_DUMP_ID = 100, 200, 300
# A WXYC-pinned api_fetch release the dump does not carry (#424 keep set).
PINNED_API_ID = 400


def _scratch(base: str) -> str:
    return scratch_name(base, SNAPSHOT_SUFFIX)


def _insert_release(cur, release_id: int, title: str, artist: str, source: str, cached_at: str):
    cur.execute("INSERT INTO release (id, title) VALUES (%s, %s)", (release_id, title))
    cur.execute(
        "INSERT INTO release_artist (release_id, artist_id, artist_name, extra) "
        "VALUES (%s, 1, %s, 0)",
        (release_id, artist),
    )
    cur.execute(
        "INSERT INTO release_label (release_id, label_id, label_name, catno) "
        "VALUES (%s, 1, 'Sonamos', 'SON-1')",
        (release_id,),
    )
    cur.execute("INSERT INTO release_genre VALUES (%s, 'Rock')", (release_id,))
    cur.execute("INSERT INTO release_style VALUES (%s, 'Folk')", (release_id,))
    cur.execute(
        "INSERT INTO release_track VALUES (%s, 1, 'A1', 'la paradoja', '3:00')", (release_id,)
    )
    cur.execute(
        "INSERT INTO release_track_artist (release_id, track_sequence, artist_name) "
        "VALUES (%s, 1, %s)",
        (release_id, artist),
    )
    cur.execute(
        "INSERT INTO release_video (release_id, sequence, src, title) "
        "VALUES (%s, 1, 'https://example.test/v', 'v')",
        (release_id,),
    )
    cur.execute(
        "INSERT INTO cache_metadata (release_id, cached_at, source) VALUES (%s, %s, %s)",
        (release_id, cached_at, source),
    )


def _rows(db_url: str, query: str) -> list[tuple]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


@pytest.fixture()
def seeded_db(db_url):
    """A pre-rebuild cache: one dump release, two api_fetch releases."""
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS {_scratch(table)} CASCADE")
        cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        _insert_release(cur, DUMP_ID, "Doga", "Juana Molina", "bulk_import", "2026-01-01")
        _insert_release(
            cur, API_ONLY_ID, "On Your Own Love Again", "Jessica Pratt", "api_fetch", "2025-01-15"
        )
        _insert_release(
            cur, API_AND_DUMP_ID, "Edits", "Chuquimamani-Condori", "api_fetch", "2025-06-01"
        )
    return db_url


def _simulate_dump_rebuild(db_url: str) -> None:
    """Destroy everything, reload the dump's copy of two releases, then prune."""
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE release CASCADE")
        _insert_release(cur, DUMP_ID, "Doga", "Juana Molina", "bulk_import", "2026-10-01")
        _insert_release(
            cur,
            API_AND_DUMP_ID,
            "Edits (dump)",
            "Chuquimamani-Condori",
            "bulk_import",
            "2026-10-01",
        )
    _vc.prune_releases_copy_swap(db_url, keep_ids={DUMP_ID, API_AND_DUMP_ID}, review_ids=set())


class TestSnapshotRestoreRoundTrip:
    def test_api_fetch_release_absent_from_dump_survives_with_children_and_source(
        self, seeded_db
    ) -> None:
        assert snapshot_api_fetch_releases(seeded_db) == 2
        _simulate_dump_rebuild(seeded_db)
        assert _rows(seeded_db, f"SELECT 1 FROM release WHERE id = {API_ONLY_ID}") == []

        summary = restore_api_fetch_releases(seeded_db)

        assert (summary.preserved, summary.rehydrated, summary.superseded) == (1, 0, 1)
        for table in CHILD_TABLES:
            assert _rows(
                seeded_db, f"SELECT count(*) FROM {table} WHERE release_id = {API_ONLY_ID}"
            ) == [(1,)], table
        assert _rows(
            seeded_db,
            "SELECT source, cached_at::date::text FROM cache_metadata "
            f"WHERE release_id = {API_ONLY_ID}",
        ) == [("api_fetch", "2025-01-15")]
        # Scratch tables are dropped after a successful restore.
        assert _rows(seeded_db, f"SELECT to_regclass('{_scratch('release')}')") == [(None,)]

    def test_api_fetch_release_present_in_dump_keeps_the_dump_copy(self, seeded_db) -> None:
        snapshot_api_fetch_releases(seeded_db)
        _simulate_dump_rebuild(seeded_db)

        restore_api_fetch_releases(seeded_db)

        assert _rows(seeded_db, f"SELECT title FROM release WHERE id = {API_AND_DUMP_ID}") == [
            ("Edits (dump)",)
        ]
        assert _rows(
            seeded_db, f"SELECT source FROM cache_metadata WHERE release_id = {API_AND_DUMP_ID}"
        ) == [("bulk_import",)]
        # Parent-gated: the arbiter-less child tables never see a duplicate.
        for table in CHILD_TABLES:
            assert _rows(
                seeded_db, f"SELECT count(*) FROM {table} WHERE release_id = {API_AND_DUMP_ID}"
            ) == [(1,)], table

    def test_restored_rows_satisfy_constraints_the_swap_reapplies(self, seeded_db) -> None:
        snapshot_api_fetch_releases(seeded_db)
        _simulate_dump_rebuild(seeded_db)
        restore_api_fetch_releases(seeded_db)
        with psycopg.connect(seeded_db) as conn, conn.cursor() as cur:
            # Cascading delete proves the FK on cache_metadata is live for the restored row.
            cur.execute(f"DELETE FROM release WHERE id = {API_ONLY_ID}")
            cur.execute(f"SELECT count(*) FROM cache_metadata WHERE release_id = {API_ONLY_ID}")
            assert cur.fetchone() == (0,)

    def test_logs_summary(self, seeded_db, caplog) -> None:
        snapshot_api_fetch_releases(seeded_db)
        _simulate_dump_rebuild(seeded_db)
        with caplog.at_level(logging.INFO, logger="lib.api_fetch_preservation"):
            restore_api_fetch_releases(seeded_db)
        assert "preserved=1" in caplog.text
        assert "superseded_by_dump=1" in caplog.text


class TestSnapshotSafety:
    def test_no_tables_is_a_noop(self, db_url) -> None:
        with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
            for table in ALL_TABLES:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        assert snapshot_api_fetch_releases(db_url) == 0
        assert restore_api_fetch_releases(db_url) is None

    def test_resumed_run_reuses_its_own_snapshot(self, seeded_db) -> None:
        snapshot_api_fetch_releases(seeded_db)
        _simulate_dump_rebuild(seeded_db)
        # A resumed run's snapshot predates the destruction, so it is reused as is.
        assert snapshot_api_fetch_releases(seeded_db, reuse_existing=True) == 2
        assert restore_api_fetch_releases(seeded_db).preserved == 1

    def test_previous_run_left_a_snapshot_behind(self, seeded_db) -> None:
        """A run that died between snapshot and restore must not block the next one."""
        snapshot_api_fetch_releases(seeded_db)
        # The run died after the base import's child TRUNCATE and its prune: the
        # api_fetch-only release is gone and every survivor is children-less.
        with psycopg.connect(seeded_db, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f"DELETE FROM release WHERE id = {API_ONLY_ID}")
            for table in (*CHILD_TABLES, "cache_metadata"):
                cur.execute(f"TRUNCATE {table}")

        # The next run restores the left-behind snapshot, then takes a fresh one.
        assert snapshot_api_fetch_releases(seeded_db) == 2
        for release_id in (API_ONLY_ID, API_AND_DUMP_ID):
            assert _rows(
                seeded_db, f"SELECT source FROM cache_metadata WHERE release_id = {release_id}"
            ) == [("api_fetch",)]
            assert _rows(
                seeded_db, f"SELECT count(*) FROM release_track WHERE release_id = {release_id}"
            ) == [(1,)]

        _simulate_dump_rebuild(seeded_db)
        assert restore_api_fetch_releases(seeded_db).preserved == 1
        assert _rows(seeded_db, f"SELECT 1 FROM release WHERE id = {API_ONLY_ID}") == [(1,)]


class TestPinnedReleaseAbsentFromDump:
    """A pinned api_fetch release the dump lacks keeps its ``release`` row through
    the real import (#424) but loses its children and provenance; restore re-hydrates it."""

    def test_rehydrates_children_and_provenance(self, seeded_db, tmp_path, monkeypatch) -> None:
        with psycopg.connect(seeded_db, autocommit=True) as conn, conn.cursor() as cur:
            _insert_release(cur, PINNED_API_ID, "Moon Pix", "Cat Power", "api_fetch", "2025-03-03")
            cur.execute(
                "UPDATE release SET artwork_checked_at = '2025-03-03' WHERE id = %s",
                (PINNED_API_ID,),
            )
        assert snapshot_api_fetch_releases(seeded_db) == 3

        # The real base import: the dump carries DUMP_ID and API_AND_DUMP_ID only.
        (tmp_path / "release.csv").write_text(
            "id,title,country,released,format,master_id\n"
            f"{DUMP_ID},Doga,AR,2008,LP,\n{API_AND_DUMP_ID},Edits (dump),US,2023,LP,\n"
        )
        monkeypatch.setenv(_ic.MAX_PIN_SHORTFALL_RATIO_ENV, "0.5")
        with psycopg.connect(seeded_db) as conn:
            _ic.import_release_via_upsert(
                conn, tmp_path, keep_release_ids={PINNED_API_ID, DUMP_ID, API_AND_DUMP_ID}
            )
            with conn.cursor() as cur:
                # The child import: the dump's copies carry only an artist row.
                for release_id in (DUMP_ID, API_AND_DUMP_ID):
                    cur.execute(
                        "INSERT INTO release_artist (release_id, artist_id, artist_name, extra) "
                        "VALUES (%s, 1, 'dump artist', 0)",
                        (release_id,),
                    )
            conn.commit()
            _ic.populate_cache_metadata(conn)
        assert _rows(
            seeded_db, f"SELECT source FROM cache_metadata WHERE release_id = {PINNED_API_ID}"
        ) == [("bulk_import",)]

        summary = restore_api_fetch_releases(seeded_db)

        assert (summary.preserved, summary.rehydrated, summary.superseded) == (1, 1, 1)
        for table in CHILD_TABLES:
            assert _rows(
                seeded_db, f"SELECT count(*) FROM {table} WHERE release_id = {PINNED_API_ID}"
            ) == [(1,)], table
        assert _rows(
            seeded_db,
            "SELECT source, cached_at::date::text FROM cache_metadata "
            f"WHERE release_id = {PINNED_API_ID}",
        ) == [("api_fetch", "2025-03-03")]
        assert _rows(
            seeded_db,
            f"SELECT artwork_checked_at::date::text FROM release WHERE id = {PINNED_API_ID}",
        ) == [("2025-03-03",)]
        # The dump still wins for the release it carried: no snapshot child is merged in.
        assert _rows(
            seeded_db, f"SELECT count(*) FROM release_label WHERE release_id = {API_AND_DUMP_ID}"
        ) == [(0,)]


class TestRestoreFailure:
    def test_failure_retains_scratch_tables_and_raises(self, seeded_db) -> None:
        snapshot_api_fetch_releases(seeded_db)
        _simulate_dump_rebuild(seeded_db)
        with psycopg.connect(seeded_db, autocommit=True) as conn, conn.cursor() as cur:
            # A NOT NULL column the snapshot cannot supply makes the re-insert fail.
            cur.execute("ALTER TABLE release ADD COLUMN extra_col text NOT NULL DEFAULT 'x'")
            cur.execute("ALTER TABLE release ALTER COLUMN extra_col DROP DEFAULT")

        with pytest.raises(ApiFetchRestoreError):
            restore_api_fetch_releases(seeded_db)

        assert _rows(seeded_db, f"SELECT id FROM {_scratch('release')} ORDER BY id") == [
            (API_ONLY_ID,),
            (API_AND_DUMP_ID,),
        ]
        # The failed restore is one transaction: nothing was half-inserted.
        assert _rows(seeded_db, f"SELECT 1 FROM release WHERE id = {API_ONLY_ID}") == []
