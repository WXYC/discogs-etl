"""Unit tests for ``scripts/dedup_artist_children.py`` (discogs-etl#433).

Pure coverage: the key table derived from ``import_csv.ARTIST_TABLES``, the
SQL builders, the per-table transaction's isolation level and surplus
assertion, the bounded serialization retry, and the advisory-lock bow-out.
The Postgres paths (including the two tie-breaks that keep LML's data) live
in ``tests/integration/test_dedup_artist_children.py``.
"""

from __future__ import annotations

import contextlib

import psycopg
import pytest

import scripts.dedup_artist_children as dedup
from scripts.dedup_artist_children import (
    ARTIST_CHILD_KEYS,
    DEDUPE_ATTEMPTS,
    REPEATABLE_READ_SQL,
    RETRYABLE_SQLSTATES,
    SurplusMismatchError,
    build_delete_sql,
    build_surplus_sql,
    dedupe_table,
    main,
)
from scripts.import_csv import ARTIST_TABLES


class _FakeCursor:
    """Records statements, serves canned count rows, fakes DELETE rowcounts."""

    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn
        self.rowcount = -1

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def execute(self, statement: str, params: object = None) -> None:
        self._conn.statements.append(statement)
        if statement.startswith("DELETE"):
            error = self._conn.delete_errors.pop(0) if self._conn.delete_errors else None
            if error is not None:
                raise error
            self.rowcount = self._conn.deleted.pop(0)

    def fetchone(self) -> tuple[int, int]:
        return self._conn.rows.pop(0)


class _FakeConn:
    """Minimal stand-in for a psycopg connection used by ``dedupe_table``."""

    def __init__(
        self,
        rows: list[tuple[int, int]],
        deleted: list[int] | None = None,
        delete_errors: list[BaseException | None] | None = None,
    ) -> None:
        self.rows = list(rows)
        self.deleted = list(deleted or [])
        self.delete_errors = list(delete_errors or [])
        self.statements: list[str] = []
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    @contextlib.contextmanager
    def transaction(self):
        try:
            yield self
        except BaseException:
            self.rollbacks += 1
            raise


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry backoff must not make the unit suite wall-clock-bound."""
    monkeypatch.setattr(dedup.time, "sleep", lambda _seconds: None)


class TestKeyDerivation:
    """One constant, two consumers: B2's constraint reads the same mapping."""

    def test_covers_every_artist_child_table(self) -> None:
        assert set(ARTIST_CHILD_KEYS) == {config["table"] for config in ARTIST_TABLES}

    @pytest.mark.parametrize("config", ARTIST_TABLES, ids=lambda c: str(c["table"]))
    def test_key_is_the_csv_unique_key_mapped_to_db_columns(self, config: dict) -> None:
        csv_to_db = dict(zip(config["csv_columns"], config["db_columns"]))
        expected = tuple(csv_to_db[column] for column in config["unique_key"])
        assert ARTIST_CHILD_KEYS[config["table"]] == expected

    def test_artist_member_key_is_translated_rather_than_copied(self) -> None:
        # The CSV names are group_artist_id/member_artist_id; the table's are
        # artist_id/member_id. Copying the CSV key verbatim would build SQL
        # against columns that do not exist.
        assert ARTIST_CHILD_KEYS["artist_member"] == ("artist_id", "member_id")


class TestSurplusSql:
    def test_counts_distinct_over_the_whole_key(self) -> None:
        assert build_surplus_sql("artist_member") == (
            "SELECT count(*), count(DISTINCT (artist_id, member_id)) FROM artist_member"
        )


class TestDeleteSql:
    @pytest.mark.parametrize("table", ["artist_name_variation", "artist_url"])
    def test_tables_without_non_key_columns_rank_on_ctid_alone(self, table: str) -> None:
        statement = build_delete_sql(table)
        assert "(a.ctid) > (b.ctid)" in statement
        assert "IS NULL" not in statement

    def test_alias_keeps_the_populated_alias_id(self) -> None:
        # false sorts before true, the minimum rank survives, so a row whose
        # alias_id IS NULL loses to one carrying LML's real value.
        assert "(a.alias_id IS NULL, a.ctid) > (b.alias_id IS NULL, b.ctid)" in build_delete_sql(
            "artist_alias"
        )

    def test_member_is_null_last_then_keeps_the_non_default_active(self) -> None:
        assert (
            "(a.active IS NULL, a.active IS NOT DISTINCT FROM true, a.ctid) > "
            "(b.active IS NULL, b.active IS NOT DISTINCT FROM true, b.ctid)"
        ) in build_delete_sql("artist_member")

    @pytest.mark.parametrize("table", sorted(ARTIST_CHILD_KEYS))
    def test_self_join_matches_on_every_key_column(self, table: str) -> None:
        statement = build_delete_sql(table)
        assert statement.startswith(f"DELETE FROM {table} a USING {table} b")
        for column in ARTIST_CHILD_KEYS[table]:
            assert f"a.{column} = b.{column}" in statement


class TestDedupeTable:
    def test_opens_the_transaction_in_repeatable_read(self) -> None:
        # READ COMMITTED takes a fresh snapshot per statement, so an LML
        # insert landing between the count and the DELETE would make the
        # surplus assertion flap.
        conn = _FakeConn(rows=[(5, 3), (3, 3)], deleted=[2])
        dedupe_table(conn, "artist_url", execute=True)
        assert conn.statements[0] == REPEATABLE_READ_SQL

    def test_dry_run_reports_the_surplus_and_issues_no_delete(self) -> None:
        conn = _FakeConn(rows=[(5, 3)])
        result = dedupe_table(conn, "artist_url", execute=False)
        assert (result.total, result.distinct, result.surplus, result.deleted) == (5, 3, 2, 0)
        assert not any(s.startswith("DELETE") for s in conn.statements)

    def test_rolls_back_when_the_delete_row_count_disagrees(self) -> None:
        conn = _FakeConn(rows=[(5, 3), (4, 3)], deleted=[1])
        with pytest.raises(SurplusMismatchError):
            dedupe_table(conn, "artist_url", execute=True)
        assert conn.rollbacks == 1

    def test_rolls_back_when_duplicates_remain_after_the_delete(self) -> None:
        conn = _FakeConn(rows=[(5, 3), (3, 2)], deleted=[2])
        with pytest.raises(SurplusMismatchError):
            dedupe_table(conn, "artist_url", execute=True)
        assert conn.rollbacks == 1

    @pytest.mark.parametrize("sqlstate", RETRYABLE_SQLSTATES)
    def test_retries_the_whole_block_after_a_retryable_sqlstate(self, sqlstate: str) -> None:
        error = psycopg.errors.lookup(sqlstate)()
        conn = _FakeConn(rows=[(5, 3), (5, 3), (3, 3)], delete_errors=[error, None], deleted=[2])
        result = dedupe_table(conn, "artist_url", execute=True)
        assert result.deleted == 2
        # The surplus is recomputed under the new snapshot, not reused.
        assert conn.statements.count(REPEATABLE_READ_SQL) == 2

    def test_gives_up_after_the_bounded_number_of_attempts(self) -> None:
        errors = [psycopg.errors.SerializationFailure() for _ in range(DEDUPE_ATTEMPTS)]
        conn = _FakeConn(rows=[(5, 3)] * DEDUPE_ATTEMPTS, delete_errors=errors)
        with pytest.raises(psycopg.errors.SerializationFailure):
            dedupe_table(conn, "artist_url", execute=True)
        assert conn.statements.count(REPEATABLE_READ_SQL) == DEDUPE_ATTEMPTS

    def test_does_not_retry_an_unrelated_sqlstate(self) -> None:
        conn = _FakeConn(rows=[(5, 3)], delete_errors=[psycopg.errors.UndefinedTable()])
        with pytest.raises(psycopg.errors.UndefinedTable):
            dedupe_table(conn, "artist_url", execute=True)
        assert conn.statements.count(REPEATABLE_READ_SQL) == 1


class TestMainBowOut:
    def test_exits_75_without_touching_the_database_when_the_lock_is_held(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(dedup, "try_acquire_rebuild_lock", lambda _url: None)

        def _explode(**_kwargs: object) -> int:
            raise AssertionError("run() must not be reached when the lock is held")

        monkeypatch.setattr(dedup, "run", _explode)
        exit_code = main(["--database-url", "postgresql://example/discogs", "--execute"])
        assert exit_code == dedup.REBUILD_LOCK_BOWED_OUT_EXIT_CODE == 75

    def test_releases_the_lock_after_a_successful_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        released: list[object] = []
        sentinel = object()
        monkeypatch.setattr(dedup, "try_acquire_rebuild_lock", lambda _url: sentinel)
        monkeypatch.setattr(dedup, "release_rebuild_lock", released.append)
        monkeypatch.setattr(dedup, "run", lambda **_kwargs: 0)
        assert main(["--database-url", "postgresql://example/discogs"]) == 0
        assert released == [sentinel]

    def test_reports_a_usage_error_when_no_database_url_is_resolvable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for name in ("DATABASE_URL_DISCOGS", "DATABASE_URL"):
            monkeypatch.delenv(name, raising=False)
        assert main([]) == 2
