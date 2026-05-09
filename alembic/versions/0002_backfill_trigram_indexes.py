"""backfill missing trigram indexes

Production (and any DB stamped at 0001_initial via the baseline's
populated-DB short-circuit) predates the moment the four GIN trigram indexes
on release/release_track/release_artist/release_track_artist landed in
``schema/*.sql``. Without them, the ``%`` operator falls back to a parallel
seq scan over ``release_track`` (~1.3M rows) and ``/api/v1/lookup`` p99
balloons to multiple seconds.

This revision applies the same four ``CREATE INDEX CONCURRENTLY IF NOT EXISTS``
statements that already live in ``schema/create_indexes.sql`` and
``schema/create_track_indexes.sql``. ``IF NOT EXISTS`` makes the apply a no-op
on DBs that already have them (e.g. one rebuilt from the baseline schema files
end-to-end).

Like 0001_initial, this revision opens a side-channel ``psycopg.connect(...,
autocommit=True)`` because ``CREATE INDEX CONCURRENTLY`` cannot run inside a
transaction, and alembic's ``run_migrations_online`` wraps the migration in
``context.begin_transaction()``.

Revision ID: 0002_backfill_trigram_indexes
Revises: 0001_initial
Create Date: 2026-05-09

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

import psycopg

from alembic import context

# revision identifiers, used by Alembic.
revision: str = "0002_backfill_trigram_indexes"
down_revision: str | Sequence[str] | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# (index_name, table, column) — column is fed verbatim into the index expression
# ``lower(f_unaccent(<column>))``. f_unaccent is the IMMUTABLE wrapper defined in
# schema/create_functions.sql; the baseline guarantees it exists.
_TRIGRAM_INDEXES: tuple[tuple[str, str, str], ...] = (
    ("idx_release_track_title_trgm", "release_track", "title"),
    ("idx_release_track_artist_name_trgm", "release_track_artist", "artist_name"),
    ("idx_release_artist_name_trgm", "release_artist", "artist_name"),
    ("idx_release_title_trgm", "release", "title"),
)


def _resolve_db_url() -> str:
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply "
            "0002_backfill_trigram_indexes."
        )
    return db_url


def _refuse_offline(direction: str) -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            f"0002_backfill_trigram_indexes does not support --sql / offline mode "
            f"({direction}): CREATE/DROP INDEX CONCURRENTLY cannot run inside a "
            "transaction, so this revision opens its own autocommit psycopg "
            "connection that bypasses alembic's offline SQL emission. Run "
            "`alembic upgrade head` (or `downgrade`) against a live DB instead."
        )


def upgrade() -> None:
    _refuse_offline("upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        for index_name, table, column in _TRIGRAM_INDEXES:
            log.info("0002: building %s on %s(%s)", index_name, table, column)
            cur.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} "
                f"ON {table} USING GIN (lower(f_unaccent({column})) gin_trgm_ops)"
            )


def downgrade() -> None:
    _refuse_offline("downgrade")

    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        for index_name, _, _ in _TRIGRAM_INDEXES:
            cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
