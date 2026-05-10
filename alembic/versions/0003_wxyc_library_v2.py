"""wxyc_library v2 hook (consolidated cross-cache identity schema)

Lands E1 §4.1.1 of the cross-cache-identity plan:
https://github.com/WXYC/wiki/blob/main/plans/library-hook-canonicalization.md#411-docker-discogs-port-5433-dev-subset

Creates the new consolidated ``wxyc_library`` table that replaces the historical
per-cache hooks. Per the canonical schema in §3.1:

- One row per library release; library_id is the Backend wxyc_schema.library.id
- Carries normalized artist / title / label for cache-side joins
- B-tree indexes inline (small-table dev subset)
- GIN trigram indexes on norm_artist + norm_title built CONCURRENTLY after the
  table itself lands, mirroring the autocommit-side-channel pattern from
  0001_initial / 0002_backfill_trigram_indexes (CREATE INDEX CONCURRENTLY can
  not run inside alembic's wrapping transaction).

``artist_id`` / ``label_id`` / ``format_id`` / ``release_year`` are nullable,
matching §3.1 (per-cache loaders populate what their source exposes; not all
do).

Naming note: the spec called this revision ``0002_wxyc_library_v2`` but the
slot was taken by ``0002_backfill_trigram_indexes`` (which landed in the
interim, see PR #173 / commit 197009d). This revision chains as 0003 to
preserve the linear history alembic expects.

Revision ID: 0003_wxyc_library_v2
Revises: 0002_backfill_trigram_indexes
Create Date: 2026-05-10

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

import psycopg
from psycopg import sql

from alembic import context

# revision identifiers, used by Alembic.
revision: str = "0003_wxyc_library_v2"
down_revision: str | Sequence[str] | None = "0002_backfill_trigram_indexes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

# Per §3.1. Comments are deliberately verbose — this table is the cross-cache
# contract and consumers (LML, semantic-index, audits) read pg_description.
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS wxyc_library (
    library_id      INTEGER PRIMARY KEY,
    artist_id       INTEGER,
    artist_name     TEXT NOT NULL,
    album_title     TEXT NOT NULL,
    label_id        INTEGER,
    label_name      TEXT,
    format_id       INTEGER,
    format_name     TEXT,
    wxyc_genre      TEXT,
    call_letters    TEXT,
    call_numbers    INTEGER,
    release_year    SMALLINT,
    norm_artist     TEXT NOT NULL,
    norm_title      TEXT NOT NULL,
    norm_label      TEXT,
    snapshot_at     TIMESTAMPTZ NOT NULL,
    snapshot_source TEXT NOT NULL
        CHECK (snapshot_source IN ('backend', 'tubafrenzy', 'llm'))
);

COMMENT ON TABLE wxyc_library IS
    'Consolidated WXYC library hook. Per §3.1 of '
    'plans/library-hook-canonicalization.md. One row per library release; '
    'library_id mirrors Backend wxyc_schema.library.id.';
COMMENT ON COLUMN wxyc_library.library_id IS 'Backend wxyc_schema.library.id';
COMMENT ON COLUMN wxyc_library.artist_id IS
    'Backend wxyc_schema.artists.id. Nullable: this cache reads from '
    'library.db (a SQLite catalog export) which does not carry it.';
COMMENT ON COLUMN wxyc_library.format_id IS
    'Backend wxyc_schema.format.id. Nullable for the same reason as artist_id.';
COMMENT ON COLUMN wxyc_library.release_year IS
    'Sourced from flowsheet.release_year aggregated per library_id, with a '
    'fallback to a matched Discogs/MB release year. NULL for rows that '
    'pre-date metadata enrichment.';
COMMENT ON COLUMN wxyc_library.snapshot_at IS
    'When this row was last written. Cross-cache freshness is observable '
    'via the spread of snapshot_at across caches.';
COMMENT ON COLUMN wxyc_library.snapshot_source IS
    'Origin of the snapshot: backend | tubafrenzy | llm. Loaders set this '
    'based on which CatalogSource produced the row.';
"""

# B-tree indexes — built inline with the table because this is the small-
# subset dev cache (≤64K rows after Option-B drop-the-filter; tens-to-low-
# hundreds of rows in fixture tests). On the larger Homebrew caches §4.1.4
# warns that even these inline B-trees should run CONCURRENTLY; on this
# Docker cache the cost is sub-second.
_BTREE_INDEXES = """
CREATE INDEX IF NOT EXISTS wxyc_library_norm_artist_idx
    ON wxyc_library (norm_artist);
CREATE INDEX IF NOT EXISTS wxyc_library_norm_title_idx
    ON wxyc_library (norm_title);
CREATE INDEX IF NOT EXISTS wxyc_library_artist_id_idx
    ON wxyc_library (artist_id);
CREATE INDEX IF NOT EXISTS wxyc_library_format_id_idx
    ON wxyc_library (format_id);
CREATE INDEX IF NOT EXISTS wxyc_library_release_year_idx
    ON wxyc_library (release_year);
"""

# GIN trigram indexes — same CONCURRENTLY pattern as
# 0002_backfill_trigram_indexes. Built after the table + B-tree indexes are in
# place. ``gin_trgm_ops`` requires the pg_trgm extension, which the baseline
# (0001_initial) already creates.
_TRIGRAM_INDEXES: tuple[tuple[str, str], ...] = (
    ("wxyc_library_norm_artist_trgm_idx", "norm_artist"),
    ("wxyc_library_norm_title_trgm_idx", "norm_title"),
)

_CREATE_TRGM_INDEX = sql.SQL(
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} "
    "ON wxyc_library USING GIN ({column} gin_trgm_ops)"
)
_DROP_TRGM_INDEX = sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {index_name}")


# ---------------------------------------------------------------------------
# Helpers (mirror 0002_backfill_trigram_indexes)
# ---------------------------------------------------------------------------


def _resolve_db_url() -> str:
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply "
            "0003_wxyc_library_v2."
        )
    return db_url


def _refuse_offline(direction: str) -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            f"0003_wxyc_library_v2 does not support --sql / offline mode "
            f"({direction}): CREATE/DROP INDEX CONCURRENTLY cannot run inside "
            "a transaction, so this revision opens its own autocommit psycopg "
            "connection that bypasses alembic's offline SQL emission. Run "
            "`alembic upgrade head` (or `downgrade`) against a live DB instead."
        )


# ---------------------------------------------------------------------------
# upgrade / downgrade
# ---------------------------------------------------------------------------


def upgrade() -> None:
    _refuse_offline("upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    db_url = _resolve_db_url()

    # Phase 1: create the table + B-tree indexes inside a regular (autocommit)
    # connection. Splitting these from the CONCURRENTLY block below makes the
    # migration cheaply restartable: if the trgm-index step fails, the table
    # is already in place and re-running the migration is a no-op for it.
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        log.info("0003: creating wxyc_library table")
        cur.execute(_CREATE_TABLE)
        log.info("0003: creating wxyc_library b-tree indexes")
        cur.execute(_BTREE_INDEXES)

    # Phase 2: GIN trigram indexes via CREATE INDEX CONCURRENTLY. Each runs in
    # its own autocommit statement.
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for index_name, column in _TRIGRAM_INDEXES:
            log.info("0003: building %s on wxyc_library(%s)", index_name, column)
            cur.execute(
                _CREATE_TRGM_INDEX.format(
                    index_name=sql.Identifier(index_name),
                    column=sql.Identifier(column),
                )
            )


def downgrade() -> None:
    _refuse_offline("downgrade")

    db_url = _resolve_db_url()

    # Drop the trigram indexes CONCURRENTLY first (cheap if they don't exist),
    # then the table itself (which cascades the remaining b-tree indexes).
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for index_name, _ in _TRIGRAM_INDEXES:
            cur.execute(
                _DROP_TRGM_INDEX.format(index_name=sql.Identifier(index_name))
            )
        cur.execute("DROP TABLE IF EXISTS wxyc_library")
