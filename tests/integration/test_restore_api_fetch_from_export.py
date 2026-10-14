"""Integration tests for #462: restore an ``api_fetch`` export through the pipeline's own restore.

``scripts/restore_api_fetch_from_export.py`` stages the ``--source`` export's
``api_fetch`` release family into the ``<table>_api_fetch_snap`` scratch tables on
``--target`` (``lib.api_fetch_preservation.stage_snapshot_from``) and then runs
``restore_api_fetch_releases``, so a manual restore follows the same rules as the
monthly rebuild's: the dump wins, hollow pins are rehydrated, the whole
``cache_metadata`` row comes back. Two scratch databases stand in for the export
and the cache.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from lib.api_fetch_preservation import (
    SNAPSHOT_SUFFIX,
    ApiFetchRestoreError,
    restore_api_fetch_releases,
    snapshot_api_fetch_releases,
    stage_snapshot_from,
)
from lib.scratch_namespace import scratch_name
from tests.conftest import ADMIN_URL, _ephemeral_database

pytestmark = pytest.mark.pg

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"
_CLI_PATH = Path(__file__).parent.parent.parent / "scripts" / "restore_api_fetch_from_export.py"
_spec = importlib.util.spec_from_file_location("restore_api_fetch_from_export", _CLI_PATH)
assert _spec is not None and _spec.loader is not None
_cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cli)

REBUILD_LOCK_KEY = 354001  # lib.rebuild_lock.REBUILD_LOCK_KEY; pinned here on purpose

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

ABSENT_ID, HOLLOW_ID, POPULATED_ID, BULK_ID = 200, 300, 400, 500


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
        "INSERT INTO cache_metadata (release_id, cached_at, source, last_validated) "
        "VALUES (%s, %s, %s, %s)",
        (release_id, cached_at, source, "2026-02-02" if source == "api_fetch" else None),
    )


def _rows(db_url: str, query: str) -> list[tuple]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def _apply_schema(db_url: str) -> None:
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())


def _execute(db_url: str, *statements: str) -> None:
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)


def _make_restore_fail(target: str) -> None:
    """Give ``release`` a NOT NULL column the export lacks, so restoring an absent id fails."""
    _execute(
        target,
        "ALTER TABLE release ADD COLUMN extra_col text NOT NULL DEFAULT 'x'",
        "ALTER TABLE release ALTER COLUMN extra_col DROP DEFAULT",
    )


def _set_default_read_only(db_url: str, on: bool) -> None:
    name = sql.Identifier(db_url.rsplit("/", 1)[-1])
    if on:
        stmt = sql.SQL("ALTER DATABASE {} SET default_transaction_read_only = on").format(name)
    else:
        stmt = sql.SQL("ALTER DATABASE {} RESET default_transaction_read_only").format(name)
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute(stmt)


def _rebuild_lock_is_free(db_url: str) -> bool:
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (REBUILD_LOCK_KEY,))
        return cur.fetchone()[0]  # closing the probe's session frees its own lock


@pytest.fixture()
def lock_conns(monkeypatch):
    """Keep a live reference to every rebuild-lock connection the CLI opens.

    Without it the CLI's lock connection is a local that CPython finalizes when
    ``main`` returns, which frees the lock even if the explicit release were
    missing; holding a reference makes "the lock is free afterwards" prove the
    release ran.
    """
    kept: list[psycopg.Connection] = []
    real = _cli.try_acquire_rebuild_lock

    def spy(url: str, *, holder: str) -> psycopg.Connection | None:
        conn = real(url, holder=holder)
        if conn is not None:
            kept.append(conn)
        return conn

    monkeypatch.setattr(_cli, "try_acquire_rebuild_lock", spy)
    yield kept
    for conn in kept:
        conn.close()


@pytest.fixture()
def source_url():
    """The export: three api_fetch releases and one bulk_import release."""
    yield from _ephemeral_database()


@pytest.fixture()
def export_and_cache(source_url, fresh_db_url):
    """``(source, target)``: the target holds only what a rebuild left behind."""
    _apply_schema(source_url)
    _apply_schema(fresh_db_url)
    with psycopg.connect(source_url, autocommit=True) as conn, conn.cursor() as cur:
        _insert_release(cur, ABSENT_ID, "Doga", "Juana Molina", "api_fetch", "2025-01-15")
        _insert_release(cur, HOLLOW_ID, "Moon Pix", "Cat Power", "api_fetch", "2025-03-03")
        cur.execute(
            "UPDATE release SET artwork_checked_at = '2025-03-03' WHERE id = %s", (HOLLOW_ID,)
        )
        _insert_release(
            cur, POPULATED_ID, "Edits", "Chuquimamani-Condori", "api_fetch", "2025-06-01"
        )
        _insert_release(cur, BULK_ID, "Aluminum Tunes", "Stereolab", "bulk_import", "2025-01-01")
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        # A pin the dump lacked: its release row survived, children and provenance did not.
        cur.execute("INSERT INTO release (id, title) VALUES (%s, 'Moon Pix')", (HOLLOW_ID,))
        cur.execute(
            "INSERT INTO cache_metadata (release_id, source) VALUES (%s, 'bulk_import')",
            (HOLLOW_ID,),
        )
        # The dump's own copy, with its own (single) child rows.
        cur.execute("INSERT INTO release (id, title) VALUES (%s, 'Edits (dump)')", (POPULATED_ID,))
        cur.execute(
            "INSERT INTO release_artist (release_id, artist_id, artist_name, extra) "
            "VALUES (%s, 1, 'dump artist', 0)",
            (POPULATED_ID,),
        )
        cur.execute(
            "INSERT INTO cache_metadata (release_id, source) VALUES (%s, 'bulk_import')",
            (POPULATED_ID,),
        )
    return source_url, fresh_db_url


def _run(source: str, target: str, *extra: str) -> int:
    return _cli.main(["--source", source, "--target", target, *extra])


class TestRestoreFromExport:
    def test_absent_release_returns_with_all_nine_tables_and_full_cache_metadata(
        self, export_and_cache
    ) -> None:
        source, target = export_and_cache

        assert _run(source, target) == 0

        for table in CHILD_TABLES:
            assert _rows(
                target, f"SELECT count(*) FROM {table} WHERE release_id = {ABSENT_ID}"
            ) == [(1,)], table
        assert _rows(target, f"SELECT title FROM release WHERE id = {ABSENT_ID}") == [("Doga",)]
        assert _rows(
            target,
            "SELECT source, cached_at::date::text, last_validated::date::text "
            f"FROM cache_metadata WHERE release_id = {ABSENT_ID}",
        ) == [("api_fetch", "2025-01-15", "2026-02-02")]

    def test_hollow_pin_is_rehydrated(self, export_and_cache) -> None:
        source, target = export_and_cache

        assert _run(source, target) == 0

        for table in CHILD_TABLES:
            assert _rows(
                target, f"SELECT count(*) FROM {table} WHERE release_id = {HOLLOW_ID}"
            ) == [(1,)], table
        assert _rows(
            target,
            "SELECT source, last_validated::date::text FROM cache_metadata "
            f"WHERE release_id = {HOLLOW_ID}",
        ) == [("api_fetch", "2026-02-02")]
        assert _rows(
            target, f"SELECT artwork_checked_at::date::text FROM release WHERE id = {HOLLOW_ID}"
        ) == [("2025-03-03",)]

    def test_populated_release_is_left_alone_and_gains_no_duplicates(
        self, export_and_cache
    ) -> None:
        source, target = export_and_cache

        assert _run(source, target) == 0

        assert _rows(target, f"SELECT title FROM release WHERE id = {POPULATED_ID}") == [
            ("Edits (dump)",)
        ]
        assert _rows(
            target, f"SELECT source FROM cache_metadata WHERE release_id = {POPULATED_ID}"
        ) == [("bulk_import",)]
        counts = {
            table: _rows(target, f"SELECT count(*) FROM {table} WHERE release_id = {POPULATED_ID}")
            for table in CHILD_TABLES
        }
        assert counts["release_artist"] == [(1,)]
        assert all(n == [(0,)] for t, n in counts.items() if t != "release_artist"), counts

    def test_non_api_fetch_release_is_not_staged_or_restored(self, export_and_cache) -> None:
        source, target = export_and_cache

        assert _run(source, target) == 0

        assert _rows(target, f"SELECT 1 FROM release WHERE id = {BULK_ID}") == []

    def test_scratch_tables_are_dropped_and_the_source_is_untouched(self, export_and_cache) -> None:
        source, target = export_and_cache
        before = _rows(source, "SELECT count(*) FROM release")

        assert _run(source, target) == 0

        assert _rows(target, f"SELECT to_regclass('{_scratch('release')}')") == [(None,)]
        assert _rows(source, "SELECT count(*) FROM release") == before
        assert _rows(source, f"SELECT to_regclass('{_scratch('release')}')") == [(None,)]

    def test_logs_summary(self, export_and_cache, caplog) -> None:
        source, target = export_and_cache
        with caplog.at_level(logging.INFO):
            assert _run(source, target) == 0
        assert "snapshotted=3 preserved=1 rehydrated=1 superseded_by_dump=1" in caplog.text

    def test_a_source_without_a_table_the_target_has_still_restores(
        self, export_and_cache, caplog
    ) -> None:
        """A clone predating release_video: that family is skipped with a warning, not fatal."""
        source, target = export_and_cache
        with psycopg.connect(source, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE release_video")

        assert _run(source, target) == 0

        assert _rows(
            target, f"SELECT count(*) FROM release_video WHERE release_id = {ABSENT_ID}"
        ) == [(0,)]
        assert _rows(target, f"SELECT 1 FROM release_track WHERE release_id = {ABSENT_ID}") == [
            (1,)
        ]
        assert "release_video: the export has no such table; restoring without it" in caplog.text

    def test_a_source_column_the_target_lacks_is_left_out_with_a_warning(
        self, export_and_cache, caplog
    ) -> None:
        """An export taken before the cache dropped a column: only shared columns are copied."""
        source, target = export_and_cache
        _execute(source, "ALTER TABLE release ADD COLUMN retired_col text DEFAULT 'old'")

        assert _run(source, target) == 0

        assert _rows(target, f"SELECT title FROM release WHERE id = {ABSENT_ID}") == [("Doga",)]
        assert "release: not restoring export columns missing from --target: retired_col" in (
            caplog.text
        )


class TestSourceIsOnlyRead:
    def test_restores_from_a_source_held_read_only(self, export_and_cache) -> None:
        """A read-only role or a hot standby works as --source: nothing is written there."""
        source, target = export_and_cache
        _set_default_read_only(source, True)
        try:
            assert _run(source, target) == 0
        finally:
            _set_default_read_only(source, False)

        assert _rows(target, f"SELECT title FROM release WHERE id = {ABSENT_ID}") == [("Doga",)]

    def test_reads_the_export_in_a_read_only_transaction(self, export_and_cache) -> None:
        """The export's cache_metadata raises when it is read from a read-write transaction."""
        source, target = export_and_cache
        _execute(
            source,
            "ALTER TABLE cache_metadata RENAME TO cache_metadata_rows",
            "CREATE FUNCTION assert_read_only() RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN "
            "IF NOT current_setting('transaction_read_only')::boolean THEN "
            "RAISE EXCEPTION 'the export was read in a read-write transaction'; END IF; "
            "RETURN true; END $$",
            "CREATE VIEW cache_metadata AS "
            "SELECT * FROM cache_metadata_rows WHERE assert_read_only()",
        )

        assert _run(source, target) == 0

        assert _rows(target, f"SELECT title FROM release WHERE id = {ABSENT_ID}") == [("Doga",)]


class TestWrongDatabase:
    @pytest.mark.parametrize("mode", [(), ("--dry-run",)], ids=["real", "dry-run"])
    @pytest.mark.parametrize(
        ("side", "breakage", "reason"),
        [
            ("source", "DROP TABLE cache_metadata", "--source has no cache_metadata table"),
            (
                "source",
                "UPDATE cache_metadata SET source = 'bulk_import'",
                "--source holds no api_fetch releases",
            ),
            ("target", "DROP TABLE release_video", "--target has no release_video table"),
        ],
        ids=["source-without-cache_metadata", "source-without-api_fetch", "target-missing-table"],
    )
    def test_exits_2_and_writes_nothing(
        self, export_and_cache, caplog, side, breakage, reason, mode
    ) -> None:
        """Nothing to restore, or nowhere to restore it, is the wrong database, not a no-op."""
        source, target = export_and_cache
        _execute(source if side == "source" else target, breakage)

        assert _run(source, target, *mode) == 2

        assert reason in caplog.text
        assert _rows(target, f"SELECT to_regclass('{_scratch('release')}')") == [(None,)]
        assert _rows(target, f"SELECT 1 FROM release WHERE id = {ABSENT_ID}") == []


class TestStageSnapshotFrom:
    def test_stages_the_api_fetch_family_and_counts_releases(self, export_and_cache) -> None:
        source, target = export_and_cache

        assert stage_snapshot_from(source, target) == 3

        assert _rows(target, f"SELECT id FROM {_scratch('release')} ORDER BY id") == [
            (ABSENT_ID,),
            (HOLLOW_ID,),
            (POPULATED_ID,),
        ]
        # Staged only: nothing reached the live tables.
        assert _rows(target, f"SELECT 1 FROM release WHERE id = {ABSENT_ID}") == []

    def test_a_snapshot_left_behind_is_restored_before_staging(self, export_and_cache) -> None:
        """Never merge into an existing snapshot: restore it first, as the pipeline does."""
        source, target = export_and_cache
        # A run that died between snapshot and restore left a snapshot of 999 behind.
        with psycopg.connect(target, autocommit=True) as conn, conn.cursor() as cur:
            _insert_release(cur, 999, "Back, Baby", "Jessica Pratt", "api_fetch", "2025-04-04")
        assert snapshot_api_fetch_releases(target) == 1
        with psycopg.connect(target, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM release WHERE id = 999")

        assert stage_snapshot_from(source, target) == 3

        assert _rows(target, "SELECT 1 FROM release WHERE id = 999") == [(1,)]
        assert _rows(target, f"SELECT count(*) FROM {_scratch('release')}") == [(3,)]

    def test_failed_restore_keeps_the_staged_tables(self, export_and_cache) -> None:
        source, target = export_and_cache
        _make_restore_fail(target)
        stage_snapshot_from(source, target)

        with pytest.raises(ApiFetchRestoreError):
            restore_api_fetch_releases(target)

        assert _rows(target, f"SELECT count(*) FROM {_scratch('release')}") == [(3,)]


class TestContention:
    def test_exits_75_and_writes_nothing_while_354001_is_held(self, export_and_cache) -> None:
        source, target = export_and_cache
        holder = psycopg.connect(target, autocommit=True)
        try:
            with holder.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (REBUILD_LOCK_KEY,))

            assert _run(source, target) == 75
        finally:
            holder.close()

        assert _rows(target, f"SELECT to_regclass('{_scratch('release')}')") == [(None,)]
        assert _rows(target, f"SELECT 1 FROM release WHERE id = {ABSENT_ID}") == []

    def test_releases_354001_when_done(self, export_and_cache, lock_conns) -> None:
        source, target = export_and_cache

        assert _run(source, target) == 0

        assert len(lock_conns) == 1
        assert _rebuild_lock_is_free(target)

    def test_takes_354001_under_its_own_name(self, export_and_cache, monkeypatch) -> None:
        """rebuild-cache.sh names the holder from ``application_name``; an unnamed
        restore would read as an operator session in the Slack post."""
        source, target = export_and_cache
        names: list[str] = []
        real = _cli.try_acquire_rebuild_lock

        def spy(url: str, **kwargs: object) -> psycopg.Connection | None:
            conn = real(url, **kwargs)
            if conn is not None:
                names.append(conn.execute("SHOW application_name").fetchone()[0])
            return conn

        monkeypatch.setattr(_cli, "try_acquire_rebuild_lock", spy)

        assert _run(source, target) == 0
        assert names == ["discogs-etl-restore-api-fetch"]

    def test_a_failed_restore_exits_1_keeps_the_staged_tables_and_releases_354001(
        self, export_and_cache, lock_conns
    ) -> None:
        source, target = export_and_cache
        _make_restore_fail(target)

        assert _run(source, target) == 1

        assert _rows(target, f"SELECT count(*) FROM {_scratch('release')}") == [(3,)]
        assert _rows(target, f"SELECT 1 FROM release WHERE id = {ABSENT_ID}") == []
        assert len(lock_conns) == 1
        assert _rebuild_lock_is_free(target)

    def test_a_failed_left_behind_restore_exits_1_and_says_the_export_was_not_staged(
        self, export_and_cache, lock_conns, caplog
    ) -> None:
        source, target = export_and_cache
        _execute(target, "INSERT INTO release (id, title) VALUES (999, 'Back, Baby')")
        _execute(
            target, "INSERT INTO cache_metadata (release_id, source) VALUES (999, 'api_fetch')"
        )
        assert snapshot_api_fetch_releases(target) == 1
        _execute(target, "DELETE FROM cache_metadata WHERE release_id = 999")
        _execute(target, "DELETE FROM release WHERE id = 999")
        _make_restore_fail(target)

        assert _run(source, target) == 1

        # Only the earlier snapshot is retained; the export was never staged.
        assert _rows(target, f"SELECT id FROM {_scratch('release')}") == [(999,)]
        assert "the export was not staged" in caplog.text
        assert _rebuild_lock_is_free(target)


class TestDryRun:
    def test_writes_nothing_and_reports_counts(self, export_and_cache, caplog) -> None:
        source, target = export_and_cache
        with caplog.at_level(logging.INFO):
            assert _run(source, target, "--dry-run") == 0

        assert "snapshotted=3 preserved=1 rehydrated=1 superseded_by_dump=1" in caplog.text
        assert _rows(target, f"SELECT to_regclass('{_scratch('release')}')") == [(None,)]
        assert _rows(target, f"SELECT 1 FROM release WHERE id = {ABSENT_ID}") == []
        assert _rows(
            target, f"SELECT count(*) FROM release_artist WHERE release_id = {HOLLOW_ID}"
        ) == [(0,)]

    def test_does_not_take_the_lock(self, export_and_cache) -> None:
        source, target = export_and_cache
        holder = psycopg.connect(target, autocommit=True)
        try:
            with holder.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (REBUILD_LOCK_KEY,))
            assert _run(source, target, "--dry-run") == 0
        finally:
            holder.close()

    def test_identical_dsns_are_refused(self, export_and_cache) -> None:
        _source, target = export_and_cache
        with pytest.raises(SystemExit) as exc:
            _run(target, target)
        assert exc.value.code == 2
