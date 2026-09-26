"""Postgres coverage for ``scripts/dedup_artist_children.py`` (discogs-etl#433).

The prod cache carries ~4.8 copies of every ``artist_*`` child row: the
monthly rebuild appends the dump's rows without deduplicating against what
is already there. This suite pins the DELETE's three load-bearing
properties against a real database:

* it removes exactly the surplus and leaves ``count(*) = count(DISTINCT key)``;
* it never shrinks the set of artists holding at least one child row, which
  is what protects rows LML hydrated that appear in no CSV;
* **the per-table tie-break keeps the informative row.** ``artist_alias``
  and ``artist_member`` carry non-key columns the ETL does not load
  (``alias_id`` arrives NULL, ``active`` takes ``DEFAULT true``), so a raw
  ctid rule can delete LML's real value in favour of an ETL placeholder.
  12,905 ``artist_alias`` groups and 16,185 ``artist_member`` groups on prod
  are in exactly that state. The two ``TestTieBreak`` classes below fail
  under a raw-ctid rule; that is the point of them.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from lib.rebuild_lock import REBUILD_LOCK_BOWED_OUT_EXIT_CODE, REBUILD_LOCK_KEY
from scripts.dedup_artist_children import ARTIST_CHILD_KEYS, dedupe_table, main

SCHEMA_SQL = Path(__file__).resolve().parents[2] / "schema" / "create_database.sql"

# Two artists with deliberately different provenance. The rebuild stub stands
# for an artist in the Discogs dump, imported three times by three monthly
# rebuilds; the hydrated artist stands for one LML wrote at runtime and that
# appears in no CSV. `fetched_at` cannot tell them apart (it is NOT NULL
# DEFAULT now(), so the ETL stamps it too — measured 209,677 of 209,677 rows
# on prod), which is why the distinction here is made by construction.
REBUILD_ARTIST = 1001
HYDRATED_ARTIST = 2002
COPIES = 3

_CHILD_ROWS: dict[str, tuple[str, tuple[object, ...]]] = {
    "artist_alias": ("(artist_id, alias_id, alias_name)", (None, "Cavern of Anti-Matter")),
    "artist_name_variation": ("(artist_id, name)", ("Stereolab",)),
    "artist_member": ("(artist_id, member_id, member_name)", (5001, "Lætitia Sadier")),
    "artist_url": ("(artist_id, url)", ("https://example.invalid/stereolab",)),
}


def _insert_child(cur: psycopg.Cursor, table: str, artist_id: int) -> None:
    columns, values = _CHILD_ROWS[table]
    placeholders = ", ".join(["%s"] * (len(values) + 1))
    cur.execute(f"INSERT INTO {table} {columns} VALUES ({placeholders})", (artist_id, *values))


def _counts(db_url: str, table: str) -> tuple[int, int]:
    key = ", ".join(ARTIST_CHILD_KEYS[table])
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*), count(DISTINCT ({key})) FROM {table}")
        row = cur.fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


def _artists_with_children(db_url: str, table: str) -> set[int]:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT artist_id FROM {table}")
        return {int(row[0]) for row in cur.fetchall()}


@pytest.fixture()
def cache_db(fresh_db_url: str) -> str:
    """An empty cache database built from ``schema/create_database.sql``."""
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    return fresh_db_url


@pytest.fixture()
def duplicated_db(cache_db: str) -> str:
    """Three copies of every child row for one artist, one copy for another."""
    with psycopg.connect(cache_db, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO artist (id, name) VALUES (%s, %s), (%s, %s)",
            (REBUILD_ARTIST, "Stereolab", HYDRATED_ARTIST, "Juana Molina"),
        )
        for table in _CHILD_ROWS:
            for _ in range(COPIES):
                _insert_child(cur, table, REBUILD_ARTIST)
            _insert_child(cur, table, HYDRATED_ARTIST)
    return cache_db


@pytest.mark.pg
class TestDryRun:
    @pytest.mark.parametrize("table", sorted(_CHILD_ROWS))
    def test_reports_the_surplus_without_deleting(self, duplicated_db: str, table: str) -> None:
        with psycopg.connect(duplicated_db) as conn:
            result = dedupe_table(conn, table, execute=False)
        assert (result.total, result.distinct, result.surplus) == (COPIES + 1, 2, COPIES - 1)
        assert result.deleted == 0
        assert _counts(duplicated_db, table) == (COPIES + 1, 2)

    def test_main_default_mode_changes_no_row(self, duplicated_db: str) -> None:
        assert main(["--database-url", duplicated_db]) == 0
        for table in _CHILD_ROWS:
            assert _counts(duplicated_db, table) == (COPIES + 1, 2)


@pytest.mark.pg
class TestExecute:
    def test_removes_exactly_the_surplus(self, duplicated_db: str) -> None:
        assert main(["--database-url", duplicated_db, "--execute"]) == 0
        for table in _CHILD_ROWS:
            total, distinct = _counts(duplicated_db, table)
            assert (total, distinct) == (2, 2), table

    def test_is_idempotent(self, duplicated_db: str) -> None:
        assert main(["--database-url", duplicated_db, "--execute"]) == 0
        assert main(["--database-url", duplicated_db, "--execute"]) == 0
        for table in _CHILD_ROWS:
            assert _counts(duplicated_db, table) == (2, 2), table

    def test_every_artist_holding_a_child_row_still_holds_one(self, duplicated_db: str) -> None:
        before = {t: _artists_with_children(duplicated_db, t) for t in _CHILD_ROWS}
        assert main(["--database-url", duplicated_db, "--execute"]) == 0
        for table in _CHILD_ROWS:
            assert _artists_with_children(duplicated_db, table) == before[table]
            assert HYDRATED_ARTIST in before[table]

    def test_vacuum_full_leaves_the_rows_alone(self, duplicated_db: str) -> None:
        assert main(["--database-url", duplicated_db, "--execute", "--vacuum-full"]) == 0
        for table in _CHILD_ROWS:
            assert _counts(duplicated_db, table) == (2, 2), table


@pytest.mark.pg
class TestRebuildLock:
    def test_bows_out_with_75_and_deletes_nothing(self, duplicated_db: str) -> None:
        holder = psycopg.connect(duplicated_db, autocommit=True)
        try:
            with holder.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (REBUILD_LOCK_KEY,))
                assert cur.fetchone() == (True,)
            exit_code = main(["--database-url", duplicated_db, "--execute"])
        finally:
            holder.close()
        assert exit_code == REBUILD_LOCK_BOWED_OUT_EXIT_CODE
        for table in _CHILD_ROWS:
            assert _counts(duplicated_db, table) == (COPIES + 1, 2), table


@pytest.mark.pg
@pytest.mark.parametrize("lml_row_first", [False, True], ids=["lml-row-last", "lml-row-first"])
class TestTieBreak:
    """Both cases fail under a raw-ctid rule in the ``lml-row-last`` ordering.

    ctid ascends with insertion order on a freshly-filled page, and the rule
    keeps the minimum rank, so a rank of ctid alone always keeps the
    first-inserted row. Seeding the ETL-shaped row first therefore makes a
    raw-ctid rule discard LML's value — which is the regression these two
    tests exist to catch. The reverse ordering is parameterized alongside so
    the ranking cannot pass by accidentally inverting.
    """

    def test_alias_keeps_the_populated_alias_id(self, cache_db: str, lml_row_first: bool) -> None:
        etl_row = (REBUILD_ARTIST, None, "Cavern of Anti-Matter")
        lml_row = (REBUILD_ARTIST, 77_001, "Cavern of Anti-Matter")
        order = (lml_row, etl_row) if lml_row_first else (etl_row, lml_row)
        with psycopg.connect(cache_db, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO artist (id, name) VALUES (%s, %s)", (REBUILD_ARTIST, "Stereolab")
            )
            cur.executemany(
                "INSERT INTO artist_alias (artist_id, alias_id, alias_name) VALUES (%s, %s, %s)",
                order,
            )

        assert main(["--database-url", cache_db, "--execute"]) == 0

        with psycopg.connect(cache_db) as conn, conn.cursor() as cur:
            cur.execute("SELECT alias_id FROM artist_alias")
            assert cur.fetchall() == [(77_001,)]

    def test_member_keeps_the_non_default_active(self, cache_db: str, lml_row_first: bool) -> None:
        # The ETL's COPY omits `active`, so the column takes DEFAULT true;
        # LML writes what Discogs reported, here a former member.
        etl_row = (
            "INSERT INTO artist_member (artist_id, member_id, member_name) VALUES (%s, %s, %s)"
        )
        lml_row = (
            "INSERT INTO artist_member (artist_id, member_id, member_name, active) "
            "VALUES (%s, %s, %s, false)"
        )
        values = (REBUILD_ARTIST, 5001, "Lætitia Sadier")
        order = (lml_row, etl_row) if lml_row_first else (etl_row, lml_row)
        with psycopg.connect(cache_db, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO artist (id, name) VALUES (%s, %s)", (REBUILD_ARTIST, "Stereolab")
            )
            for statement in order:
                cur.execute(statement, values)

        assert main(["--database-url", cache_db, "--execute"]) == 0

        with psycopg.connect(cache_db) as conn, conn.cursor() as cur:
            cur.execute("SELECT active FROM artist_member")
            assert cur.fetchall() == [(False,)]
