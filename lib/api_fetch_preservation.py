"""Preserve LML's ``api_fetch`` releases across a cache rebuild (discogs-etl#450).

A rebuild drops every release whose ``cache_metadata.source = 'api_fetch'`` --
LML's own Discogs API fetches -- at three seams: the import-stage prune, the
base import's TRUNCATE, and verify-prune's copy-swap. LML then re-fetches them
from the rate-limited Discogs API. This module keeps them, indefinitely:

1. :func:`snapshot_api_fetch_releases` copies every ``api_fetch`` release, with
   its rows from all nine pipeline tables, into ``<table>_api_fetch_snap``
   scratch tables *before* the first destructive statement.
2. :func:`restore_api_fetch_releases` runs after prune and restores the
   snapshot releases the dump did not carry: ids absent from ``release``, and
   pinned ids (#424) whose ``release`` row survived with no children. Any other
   id the rebuilt cache holds is the dump's copy, and the dump wins. Gating on
   the parent id also keeps the arbiter-less child tables free of duplicates.

The snapshot is one REPEATABLE READ transaction. A snapshot left behind by a
run that died before its restore is restored and then replaced, never
overwritten blindly, because a second snapshot taken after the rebuild began
would capture a cache that already lost the data. A failed restore is one
rolled-back transaction that leaves the scratch tables in place for forensics
and raises, the same asymmetry as ``_keep_ids_<suffix>`` (#357). Releases LML
writes after the snapshot are not covered; that window is accepted.

For a run that took no snapshot, :func:`stage_snapshot_from` builds the same
scratch tables from a pre-run export, so ``scripts/restore_api_fetch_from_export.py``
restores it through :func:`restore_api_fetch_releases` under the same rules (#462).
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass
from datetime import timedelta

import psycopg

from lib.scratch_namespace import scratch_name

logger = logging.getLogger(__name__)

SNAPSHOT_SUFFIX = "api_fetch_snap"

# Parent first: restore inserts in this order, so every child lands after its release.
SNAPSHOT_TABLES = (
    "release",
    "release_artist",
    "release_label",
    "release_genre",
    "release_style",
    "release_track",
    "release_track_artist",
    "release_video",
    "cache_metadata",
)

CHILD_TABLES = SNAPSHOT_TABLES[1:-1]

SIX_MONTHS = timedelta(days=183)

# Bounds the restore's wait for LML's open write transactions on ``release``;
# while queued, the lock request blocks every later LML write behind it.
RESTORE_LOCK_TIMEOUT = "2min"

# The release-family selector shared by the in-pipeline snapshot and the export staging.
_API_FETCH_IDS = "IN (SELECT release_id FROM cache_metadata WHERE source = 'api_fetch')"


class ApiFetchRestoreError(RuntimeError):
    """The restore failed and was rolled back; the scratch tables are retained."""


@dataclass(frozen=True)
class PreservationSummary:
    """Counts and age distribution for one restore, logged at the end of the run."""

    snapshotted: int
    preserved: int
    rehydrated: int
    superseded: int
    oldest_age: timedelta | None = None
    median_age: timedelta | None = None
    older_than_six_months: int = 0

    def format(self) -> str:
        line = (
            f"api_fetch preservation: snapshotted={self.snapshotted} "
            f"preserved={self.preserved} rehydrated={self.rehydrated} "
            f"superseded_by_dump={self.superseded}"
        )
        if self.oldest_age is not None and self.median_age is not None:
            line += (
                f" oldest={self.oldest_age.days}d median={self.median_age.days}d "
                f"older_than_6mo={self.older_than_six_months}"
            )
        return line


def summarize_ages(
    ages: list[timedelta],
) -> tuple[timedelta | None, timedelta | None, int]:
    """Return ``(oldest, median, count older than six months)`` for preserved copies."""
    if not ages:
        return None, None, 0
    return max(ages), statistics.median(ages), sum(1 for a in ages if a > SIX_MONTHS)


def _snap(table: str) -> str:
    return scratch_name(table, SNAPSHOT_SUFFIX)


def _key(table: str) -> str:
    return "id" if table == "release" else "release_id"


def _exists(cur, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (table,))
    return cur.fetchone()[0] is not None


def _columns(cur, table: str) -> list[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = %s ORDER BY ordinal_position",
        (table,),
    )
    return [row[0] for row in cur.fetchall()]


def snapshot_api_fetch_releases(db_url: str, *, reuse_existing: bool = False) -> int:
    """Snapshot every ``api_fetch`` release into scratch tables; return the release count.

    A database without ``release`` / ``cache_metadata`` has nothing to preserve
    and returns 0. An existing snapshot is reused when *reuse_existing* (a
    resumed run, whose snapshot predates the destruction) is set. Otherwise it
    is a run that died between snapshot and restore: it is restored first --
    the cache it left behind lacks those releases -- and then replaced by a
    fresh snapshot, so one failed month never blocks the next. Any failure
    propagates, so the caller aborts before touching the cache.
    """
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        if not (_exists(cur, "release") and _exists(cur, "cache_metadata")):
            logger.info("No release/cache_metadata tables yet; nothing to snapshot (#450)")
            return 0
        left_behind = _exists(cur, _snap("release"))
        if left_behind and reuse_existing:
            cur.execute(f"SELECT count(*) FROM {_snap('release')}")
            count = cur.fetchone()[0]
            logger.warning("Reusing existing api_fetch snapshot of %d releases (#450)", count)
            return count
    if left_behind:
        logger.warning(
            "An earlier run left its api_fetch snapshot behind; restoring it before "
            "taking a fresh one (#450)"
        )
        restore_api_fetch_releases(db_url)
    with psycopg.connect(db_url) as conn:
        # One consistent cut across all nine tables while LML keeps writing.
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.cursor() as cur:
            for table in SNAPSHOT_TABLES:
                if not _exists(cur, table):
                    continue
                cur.execute(
                    f"CREATE TABLE {_snap(table)} AS SELECT t.* FROM {table} t "
                    f"WHERE t.{_key(table)} {_API_FETCH_IDS}"
                )
            cur.execute(f"SELECT count(*) FROM {_snap('release')}")
            count = cur.fetchone()[0]
    logger.info("Snapshotted %d api_fetch releases into *_%s tables (#450)", count, SNAPSHOT_SUFFIX)
    return count


def stage_snapshot_from(source_url: str, target_url: str) -> int:
    """Stage *source_url*'s ``api_fetch`` release family as scratch tables on *target_url*.

    The manual counterpart of :func:`snapshot_api_fetch_releases` (#462) for an
    export taken before a run that kept no snapshot: it builds the same
    ``<table>_api_fetch_snap`` tables, so :func:`restore_api_fetch_releases`
    then applies the pipeline's own rules to them. Returns the staged release
    count; a source without ``release`` / ``cache_metadata`` stages nothing and
    returns 0. Columns are the ones both databases have, and a table the source
    lacks is skipped. The source is only read (a read-only REPEATABLE READ
    transaction, ``COPY ... TO STDOUT``); the target gets one transaction, so a
    failure leaves no half-staged family. A snapshot already on the target (a
    run that died before its restore) is restored first, never merged into.
    """
    with psycopg.connect(target_url) as conn, conn.cursor() as cur:
        left_behind = _exists(cur, _snap("release"))
    if left_behind:
        logger.warning(
            "The target holds an api_fetch snapshot from an earlier run; restoring it "
            "before staging the export (#462)"
        )
        restore_api_fetch_releases(target_url)
    with psycopg.connect(source_url) as src, psycopg.connect(target_url) as tgt:
        src.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        src.read_only = True
        with src.cursor() as s, tgt.cursor() as t:
            if not (_exists(s, "release") and _exists(s, "cache_metadata")):
                logger.info("The export has no release/cache_metadata tables; nothing to stage")
                return 0
            for table in SNAPSHOT_TABLES:
                if not _exists(s, table):
                    continue
                target_cols = set(_columns(t, table))
                cols = ", ".join(c for c in _columns(s, table) if c in target_cols)
                t.execute(f"CREATE TABLE {_snap(table)} AS SELECT {cols} FROM {table} WHERE false")
                select = f"SELECT {cols} FROM {table} WHERE {_key(table)} {_API_FETCH_IDS}"
                with s.copy(f"COPY ({select}) TO STDOUT") as out:
                    with t.copy(f"COPY {_snap(table)} ({cols}) FROM STDIN") as into:
                        for data in out:
                            into.write(data)
            t.execute(f"SELECT count(*) FROM {_snap('release')}")
            count = t.fetchone()[0]
    logger.info("Staged %d api_fetch releases from the export (#462)", count)
    return count


def _hollow(cur, alias: str) -> str:
    """SQL predicate: release *alias*.id has no row in any live child table."""
    checks = [
        f"NOT EXISTS (SELECT 1 FROM {t} c WHERE c.release_id = {alias}.id)"
        for t in CHILD_TABLES
        if _exists(cur, t)
    ]
    return " AND ".join(checks) or "FALSE"


def restore_api_fetch_releases(db_url: str) -> PreservationSummary | None:
    """Restore the snapshot releases the rebuilt dump did not carry, then drop the scratch tables.

    Returns ``None`` when no snapshot exists. Two kinds are restored:

    * **preserved** -- the id is absent from ``release``: every row is re-inserted.
    * **rehydrated** -- the id is in ``release`` with no row in any child table.
      That is a pinned release (#424) the dump did not carry: the prune kept its
      ``release`` row but the child TRUNCATE emptied it, ``artwork_checked_at``
      was NULLed and ``populate_cache_metadata`` re-labelled it ``bulk_import``.
      Its children are re-inserted, ``artwork_checked_at`` and ``cache_metadata``
      are set back to the snapshot. Every release the dump carries has at least
      an artist row, so a dump copy is never mistaken for one of these.

    Any other id is the dump's copy, and the dump wins. Runs as one transaction
    holding a SHARE ROW EXCLUSIVE lock on ``release`` (bounded by
    ``lock_timeout``) so LML cannot write one of these ids between the check and
    the insert. On any failure the transaction rolls back, the scratch tables
    stay, and :class:`ApiFetchRestoreError` is raised.
    """
    try:
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            if not _exists(cur, _snap("release")):
                logger.info("No api_fetch snapshot to restore (#450)")
                return None
            if not _exists(cur, "release"):
                logger.warning("No release table to restore into; api_fetch snapshot retained")
                return None
            cur.execute(f"SET LOCAL lock_timeout = '{RESTORE_LOCK_TIMEOUT}'")
            cur.execute("LOCK TABLE release IN SHARE ROW EXCLUSIVE MODE")
            cur.execute(f"SELECT count(*) FROM {_snap('release')}")
            snapshotted = cur.fetchone()[0]
            cur.execute(
                "CREATE TEMP TABLE _api_fetch_restore ON COMMIT DROP AS "
                "SELECT s.id, r.id IS NULL AS absent "
                f"FROM {_snap('release')} s LEFT JOIN release r ON r.id = s.id "
                f"WHERE r.id IS NULL OR ({_hollow(cur, 's')})"
            )
            cur.execute("SELECT count(*) FILTER (WHERE absent), count(*) FROM _api_fetch_restore")
            preserved, restoring = cur.fetchone()
            for table in SNAPSHOT_TABLES:
                if not _exists(cur, _snap(table)):
                    continue
                snap_cols = set(_columns(cur, _snap(table)))
                live_cols = [c for c in _columns(cur, table) if c in snap_cols]
                cols = ", ".join(live_cols)
                ids = "SELECT id FROM _api_fetch_restore"
                if table == "release":
                    ids += " WHERE absent"
                    cur.execute(
                        f"UPDATE release r SET artwork_checked_at = s.artwork_checked_at "
                        f"FROM {_snap(table)} s, _api_fetch_restore x "
                        "WHERE r.id = s.id AND x.id = s.id AND NOT x.absent"
                    )
                conflict = ""
                if table == "cache_metadata":
                    updates = ", ".join(
                        f"{c} = EXCLUDED.{c}" for c in live_cols if c != "release_id"
                    )
                    conflict = f" ON CONFLICT (release_id) DO UPDATE SET {updates}"
                cur.execute(
                    f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {_snap(table)} "
                    f"WHERE {_key(table)} IN ({ids}){conflict}"
                )
            cur.execute(
                "SELECT now() - cached_at FROM cache_metadata "
                "WHERE release_id IN (SELECT id FROM _api_fetch_restore)"
            )
            oldest, median, old_count = summarize_ages([row[0] for row in cur.fetchall()])
            summary = PreservationSummary(
                snapshotted=snapshotted,
                preserved=preserved,
                rehydrated=restoring - preserved,
                superseded=snapshotted - restoring,
                oldest_age=oldest,
                median_age=median,
                older_than_six_months=old_count,
            )
            for table in SNAPSHOT_TABLES:
                cur.execute(f"DROP TABLE IF EXISTS {_snap(table)}")
    except Exception as exc:
        logger.error(
            "api_fetch restore failed and was rolled back; *_%s scratch tables are retained "
            "for forensics (#450): %s",
            SNAPSHOT_SUFFIX,
            exc,
        )
        raise ApiFetchRestoreError(str(exc)) from exc
    logger.info(summary.format())
    return summary


def plan_restore_from(source_url: str, target_url: str) -> PreservationSummary:
    """Count what restoring *source_url*'s ``api_fetch`` releases would do, writing nothing (#462).

    Applies :func:`restore_api_fetch_releases`' rule to the target as it stands:
    absent ids are preserved, hollow ones rehydrated, the rest superseded by the
    dump. Both databases are only read.
    """
    ids: list[int] = []
    with psycopg.connect(source_url) as conn, conn.cursor() as cur:
        if _exists(cur, "release") and _exists(cur, "cache_metadata"):
            cur.execute(f"SELECT id FROM release WHERE id {_API_FETCH_IDS}")
            ids = [row[0] for row in cur.fetchall()]
    preserved = restoring = 0
    with psycopg.connect(target_url) as conn, conn.cursor() as cur:
        if _exists(cur, _snap("release")):
            logger.warning(
                "The target holds an api_fetch snapshot from an earlier run; a real run "
                "restores it first, so these counts are against the cache as it stands"
            )
        if ids and _exists(cur, "release"):
            cur.execute(
                "SELECT count(*) FILTER (WHERE r.id IS NULL), count(*) "
                "FROM unnest(%s::integer[]) AS x(id) LEFT JOIN release r ON r.id = x.id "
                f"WHERE r.id IS NULL OR ({_hollow(cur, 'x')})",
                (ids,),
            )
            preserved, restoring = cur.fetchone()
    return PreservationSummary(
        snapshotted=len(ids),
        preserved=preserved,
        rehydrated=restoring - preserved,
        superseded=len(ids) - restoring,
    )
