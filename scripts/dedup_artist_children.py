"""Remove duplicate rows from the four ``artist_*`` child tables (#433).

Every monthly rebuild appends the dump's ``artist_alias`` /
``artist_name_variation`` / ``artist_member`` / ``artist_url`` rows without
deduplicating against what is already there, so the cache carries ~4.8
copies of each. This collapses every key group to one row. Dry-run by
default; ``--execute`` deletes, ``--vacuum-full`` returns the space to the
filesystem afterwards (a plain ``VACUUM`` would not). Holds the rebuild
advisory lock throughout and bows out with exit 75 if a rebuild has it.

Which row survives is a per-table decision -- see :data:`TIE_BREAK_TERMS`.
``alembic/versions/0009_cache_metadata_unique.py`` is the in-repo precedent
for the shape. Operator procedure and expected prod counts:
``docs/dedup-artist-children-runbook.md``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.dsn import redact_dsn  # noqa: E402
from lib.observability import init_logger  # noqa: E402
from lib.pg_concurrent_ddl import (  # noqa: E402
    DEFAULT_LOCK_TIMEOUT,
    SQLSTATE_LOCK_NOT_AVAILABLE,
    SWAP_PATH_ATTEMPTS,
    SWAP_PATH_BACKOFF_SECONDS,
)
from lib.rebuild_lock import (  # noqa: E402
    REBUILD_LOCK_BOWED_OUT_EXIT_CODE,
    REBUILD_LOCK_KEY,
    release_rebuild_lock,
    try_acquire_rebuild_lock,
)
from scripts.import_csv import ARTIST_TABLES  # noqa: E402

logger = logging.getLogger(__name__)
_STEP = {"step": "dedup_artist_children"}

T = TypeVar("T")

#: Read Committed takes a fresh snapshot per statement, so an LML insert
#: committing between the count and the DELETE would move the surplus and
#: make the assertion flap. One snapshot makes ``rowcount == surplus`` real.
REPEATABLE_READ_SQL = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"

#: 40001 is the reachable one: a concurrent LML DELETE+INSERT committing
#: mid-DELETE aborts the attempt whole, leaving nothing partially deleted.
#: A retry re-runs the whole count -> DELETE -> assert block, since the
#: surplus must be recomputed under the new snapshot anyway.
RETRYABLE_SQLSTATES = ("40001", "40P01")
DEDUPE_ATTEMPTS = 5
DEDUPE_BACKOFF_SECONDS: tuple[float, ...] = (5.0, 15.0, 45.0, 90.0)


def _artist_child_keys() -> dict[str, tuple[str, ...]]:
    """Map each ``artist_*`` child table to its key, in database columns.

    Derived from ``import_csv.ARTIST_TABLES``, not restated, so this script
    and the UNIQUE constraint that follows cannot drift apart. CSV names are
    not always column names (``artist_member``'s key is ``group_artist_id,
    member_artist_id`` there, ``artist_id, member_id`` here), so the mapping
    goes through ``csv_columns -> db_columns``.
    """
    return {
        config["table"]: tuple(
            dict(zip(config["csv_columns"], config["db_columns"]))[column]
            for column in config["unique_key"]
        )
        for config in ARTIST_TABLES
    }


ARTIST_CHILD_KEYS: dict[str, tuple[str, ...]] = _artist_child_keys()

#: Rank terms ahead of ctid, per table; ``{t}`` is the row alias. The
#: minimum rank survives and ``false < true``, so each term names the
#: property whose *absence* should lose.
#:
#: Load-bearing, not stylistic. ``artist_name_variation`` and ``artist_url``
#: have no non-key columns, so their duplicates are byte-identical and plain
#: ctid is information-preserving. The other two carry non-key columns only
#: LML populates -- the ETL's COPY omits ``alias_id`` (lands NULL) and omits
#: ``active`` (takes ``DEFAULT true``) -- so raw ctid can delete LML's real
#: value in favour of an ETL placeholder: 12,905 and 16,185 groups
#: respectively were in that state on prod on 2026-09-25. NULL sorts last on
#: ``artist_member`` because ``NULL IS NOT DISTINCT FROM true`` is false,
#: which would otherwise let a NULL ``active`` outlive a real ``true``.
TIE_BREAK_TERMS: dict[str, tuple[str, ...]] = {
    "artist_alias": ("{t}.alias_id IS NULL",),
    "artist_member": ("{t}.active IS NULL", "{t}.active IS NOT DISTINCT FROM true"),
}


class SurplusMismatchError(RuntimeError):
    """The DELETE did not remove exactly the surplus counted alongside it."""


@dataclass(frozen=True)
class TableResult:
    """One table's counts, as seen inside the single snapshot that acted."""

    table: str
    total: int
    distinct: int
    deleted: int

    @property
    def surplus(self) -> int:
        return self.total - self.distinct

    def __str__(self) -> str:
        return (
            f"{self.table}: {self.total} rows, {self.distinct} distinct keys, "
            f"surplus {self.surplus}, deleted {self.deleted}"
        )


# Table and column names below come from the in-repo ARTIST_TABLES constant,
# never from input, so they are interpolated directly.
def build_surplus_sql(table: str) -> str:
    """Count rows and distinct keys in one round trip."""
    key = ", ".join(ARTIST_CHILD_KEYS[table])
    return f"SELECT count(*), count(DISTINCT ({key})) FROM {table}"


def build_delete_sql(table: str) -> str:
    """Self-join DELETE keeping the minimum-rank row of each key group."""
    terms = (*TIE_BREAK_TERMS.get(table, ()), "{t}.ctid")
    ranks = tuple(", ".join(term.format(t=alias) for term in terms) for alias in ("a", "b"))
    join = " AND ".join(f"a.{column} = b.{column}" for column in ARTIST_CHILD_KEYS[table])
    return f"DELETE FROM {table} a USING {table} b WHERE {join} AND ({ranks[0]}) > ({ranks[1]})"


def _with_sqlstate_retry(
    what: str,
    operation: Callable[[], T],
    *,
    sqlstates: tuple[str, ...],
    attempts: int,
    backoff: tuple[float, ...],
) -> T:
    """Run *operation*, retrying only *sqlstates*, at most *attempts* times."""
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except psycopg.Error as exc:
            if exc.sqlstate not in sqlstates or attempt == attempts:
                raise
            delay = backoff[min(attempt, len(backoff)) - 1]
            msg = f"{what}: SQLSTATE {exc.sqlstate} on attempt {attempt}/{attempts}"
            logger.warning("%s; retrying in %.1fs", msg, delay, extra=_STEP)
            time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


def dedupe_table(conn: psycopg.Connection, table: str, *, execute: bool) -> TableResult:
    """Count *table*'s surplus and, when *execute*, delete exactly that many.

    Count, DELETE and post-condition share one Repeatable Read transaction.
    A disagreement between the counted surplus and the DELETE's rowcount, or
    any duplicate left behind, raises :class:`SurplusMismatchError` and rolls
    that table back -- nothing partial commits, so the run stays resumable.
    """
    surplus_sql = build_surplus_sql(table)

    def _attempt() -> TableResult:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(REPEATABLE_READ_SQL)
            cur.execute(surplus_sql)
            total, distinct = cur.fetchone()  # type: ignore[misc]
            if not execute:
                return TableResult(table, total, distinct, 0)
            cur.execute(build_delete_sql(table))
            deleted = cur.rowcount
            cur.execute(surplus_sql)
            after_total, after_distinct = cur.fetchone()  # type: ignore[misc]
            if deleted != total - distinct or after_total != after_distinct:
                raise SurplusMismatchError(
                    f"{table}: expected to delete {total - distinct} rows ({total} rows, "
                    f"{distinct} distinct keys) but deleted {deleted}, leaving {after_total} "
                    f"rows / {after_distinct} distinct keys. Rolled back; re-run the dry-run."
                )
            return TableResult(table, total, distinct, deleted)

    return _with_sqlstate_retry(
        f"dedupe {table}",
        _attempt,
        sqlstates=RETRYABLE_SQLSTATES,
        attempts=DEDUPE_ATTEMPTS,
        backoff=DEDUPE_BACKOFF_SECONDS,
    )


def vacuum_table(conn: psycopg.Connection, table: str) -> None:
    """``VACUUM (FULL, ANALYZE)`` *table* under a bounded lock wait.

    FULL rewrites the heap, the only way the deleted rows become free
    filesystem space. ACCESS EXCLUSIVE, so LML's reads of the table block
    for its duration -- run off-peak.
    """

    def _attempt() -> None:
        with conn.cursor() as cur:
            cur.execute(f"SET lock_timeout = '{DEFAULT_LOCK_TIMEOUT}'")
            cur.execute(f"VACUUM (FULL, ANALYZE) {table}")

    _with_sqlstate_retry(
        f"vacuum {table}",
        _attempt,
        sqlstates=(SQLSTATE_LOCK_NOT_AVAILABLE,),
        attempts=SWAP_PATH_ATTEMPTS,
        backoff=tuple(SWAP_PATH_BACKOFF_SECONDS),
    )


def run(*, database_url: str, execute: bool, vacuum_full: bool) -> int:
    """Dedupe every ``artist_*`` child table, then optionally reclaim."""
    target = redact_dsn(database_url)
    logger.info("target=%s execute=%s vacuum_full=%s", target, execute, vacuum_full, extra=_STEP)
    with psycopg.connect(database_url) as conn:
        for table in ARTIST_CHILD_KEYS:
            logger.info("%s", dedupe_table(conn, table, execute=execute), extra=_STEP)
    if vacuum_full:
        # VACUUM cannot run inside a transaction block, hence a second,
        # autocommit connection rather than reusing the one above.
        with psycopg.connect(database_url, autocommit=True) as conn:
            for table in ARTIST_CHILD_KEYS:
                vacuum_table(conn, table)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "--database-url",
        help="Cache PG URL. Falls back to DATABASE_URL_DISCOGS, then DATABASE_URL.",
    )
    parser.add_argument(
        "--execute", action="store_true", help="Delete the surplus rows rather than only report."
    )
    parser.add_argument(
        "--vacuum-full",
        action="store_true",
        help="VACUUM (FULL, ANALYZE) each table afterwards so the space is returned.",
    )
    args = parser.parse_args(argv)

    init_logger(repo="discogs-etl", tool="discogs-etl dedup_artist_children")

    env = os.environ
    database_url = args.database_url or env.get("DATABASE_URL_DISCOGS") or env.get("DATABASE_URL")
    if not database_url:
        print("error: no --database-url, DATABASE_URL_DISCOGS or DATABASE_URL.", file=sys.stderr)
        return 2

    # Key 354001, the rebuild's own: same resource, same hazard -- a rebuild
    # importing artist CSVs mid-DELETE is what must not overlap. Returned
    # rather than run_pipeline's os._exit: nothing downstream of this script
    # can mistake a bow-out for success.
    lock_conn = try_acquire_rebuild_lock(database_url)
    if lock_conn is None:
        held = f"Rebuild lock {REBUILD_LOCK_KEY} is held elsewhere; nothing touched."
        logger.warning(
            "%s Bowing out with %d.", held, REBUILD_LOCK_BOWED_OUT_EXIT_CODE, extra=_STEP
        )
        return REBUILD_LOCK_BOWED_OUT_EXIT_CODE
    try:
        return run(database_url=database_url, execute=args.execute, vacuum_full=args.vacuum_full)
    except SurplusMismatchError as exc:
        logger.error("%s", exc, extra=_STEP)
        return 1
    finally:
        release_rebuild_lock(lock_conn)


if __name__ == "__main__":
    sys.exit(main())
