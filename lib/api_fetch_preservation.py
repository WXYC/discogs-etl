"""Preserve LML's ``api_fetch`` releases across a cache rebuild (discogs-etl#450).

A rebuild drops every release whose ``cache_metadata.source = 'api_fetch'`` --
LML's own Discogs API fetches -- at three seams: the import-stage prune, the
base import's TRUNCATE, and verify-prune's copy-swap. LML then re-fetches them
from the rate-limited Discogs API. This module keeps them, indefinitely:

1. :func:`snapshot_api_fetch_releases` copies every ``api_fetch`` release, with
   its rows from all nine pipeline tables, into ``<table>_api_fetch_snap``
   scratch tables *before* the first destructive statement.
2. :func:`restore_api_fetch_releases` runs after prune and re-inserts only the
   snapshot releases whose ids are absent from ``release``. An id the rebuilt
   cache already holds is the dump's copy, and the dump wins. Gating on the
   parent id also keeps the arbiter-less child tables free of duplicates.

The snapshot is atomic and refuses to overwrite an existing one, because a
second snapshot taken after the rebuild began would capture a cache that
already lost the data. A failed restore is one rolled-back transaction that
leaves the scratch tables in place for forensics and raises, the same
asymmetry as ``_keep_ids_<suffix>`` (#357). Releases LML writes after the
snapshot are not covered; that window is accepted.
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

SIX_MONTHS = timedelta(days=183)


class ApiFetchRestoreError(RuntimeError):
    """The restore failed and was rolled back; the scratch tables are retained."""


@dataclass(frozen=True)
class PreservationSummary:
    """Counts and age distribution for one restore, logged at the end of the run."""

    snapshotted: int
    preserved: int
    superseded: int
    dropped: int
    oldest_age: timedelta | None = None
    median_age: timedelta | None = None
    older_than_six_months: int = 0

    def format(self) -> str:
        line = (
            f"api_fetch preservation: snapshotted={self.snapshotted} "
            f"preserved={self.preserved} superseded_by_dump={self.superseded} "
            f"dropped={self.dropped}"
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
    and returns 0. An existing snapshot raises unless *reuse_existing* (a resumed
    run, whose snapshot predates the destruction) is set. Any failure rolls back
    and propagates, so the caller aborts before touching the cache.
    """
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        if not (_exists(cur, "release") and _exists(cur, "cache_metadata")):
            logger.info("No release/cache_metadata tables yet; nothing to snapshot (#450)")
            return 0
        if _exists(cur, _snap("release")):
            if not reuse_existing:
                raise RuntimeError(
                    f"{_snap('release')} already exists: an earlier run's api_fetch snapshot "
                    "is still retained. Restore or drop the *_api_fetch_snap tables first "
                    "(docs/ec2-rebuild-runbook.md)."
                )
            cur.execute(f"SELECT count(*) FROM {_snap('release')}")
            count = cur.fetchone()[0]
            logger.warning("Reusing existing api_fetch snapshot of %d releases (#450)", count)
            return count
        for table in SNAPSHOT_TABLES:
            if not _exists(cur, table):
                continue
            cur.execute(
                f"CREATE TABLE {_snap(table)} AS SELECT t.* FROM {table} t "
                f"WHERE t.{_key(table)} IN "
                "(SELECT release_id FROM cache_metadata WHERE source = 'api_fetch')"
            )
        cur.execute(f"SELECT count(*) FROM {_snap('release')}")
        count = cur.fetchone()[0]
    logger.info("Snapshotted %d api_fetch releases into *_%s tables (#450)", count, SNAPSHOT_SUFFIX)
    return count


def restore_api_fetch_releases(db_url: str) -> PreservationSummary | None:
    """Re-insert snapshot releases the rebuilt cache lacks, then drop the scratch tables.

    Returns ``None`` when no snapshot exists. Runs as one transaction holding a
    SHARE ROW EXCLUSIVE lock on ``release`` so LML cannot write one of these ids
    between the absence check and the insert. On any failure the transaction
    rolls back, the scratch tables stay, and :class:`ApiFetchRestoreError` is raised.
    """
    try:
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            if not _exists(cur, _snap("release")):
                logger.info("No api_fetch snapshot to restore (#450)")
                return None
            cur.execute("LOCK TABLE release IN SHARE ROW EXCLUSIVE MODE")
            cur.execute(f"SELECT count(*) FROM {_snap('release')}")
            snapshotted = cur.fetchone()[0]
            cur.execute(
                "CREATE TEMP TABLE _api_fetch_restore ON COMMIT DROP AS "
                f"SELECT s.id FROM {_snap('release')} s "
                "WHERE NOT EXISTS (SELECT 1 FROM release r WHERE r.id = s.id)"
            )
            cur.execute("SELECT count(*) FROM _api_fetch_restore")
            to_restore = cur.fetchone()[0]
            for table in SNAPSHOT_TABLES:
                if not _exists(cur, _snap(table)):
                    continue
                snap_cols = set(_columns(cur, _snap(table)))
                cols = ", ".join(c for c in _columns(cur, table) if c in snap_cols)
                cur.execute(
                    f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {_snap(table)} "
                    f"WHERE {_key(table)} IN (SELECT id FROM _api_fetch_restore)"
                )
            cur.execute(
                "SELECT count(*) FROM release WHERE id IN (SELECT id FROM _api_fetch_restore)"
            )
            preserved = cur.fetchone()[0]
            cur.execute(
                "SELECT now() - cached_at FROM cache_metadata "
                "WHERE release_id IN (SELECT id FROM _api_fetch_restore)"
            )
            oldest, median, old_count = summarize_ages([row[0] for row in cur.fetchall()])
            summary = PreservationSummary(
                snapshotted=snapshotted,
                preserved=preserved,
                superseded=snapshotted - to_restore,
                dropped=to_restore - preserved,
                oldest_age=oldest,
                median_age=median,
                older_than_six_months=old_count,
            )
            if summary.dropped:
                raise RuntimeError(f"{summary.dropped} snapshot releases missing after restore")
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
