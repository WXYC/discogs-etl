"""initial baseline

Reproduces the canonical schema in ``schema/*.sql`` as the alembic baseline.

The SQL files remain the single source of truth — this revision just executes
them in pipeline order so alembic has a recorded starting point. Existing
production databases will be ``alembic stamp head``-ed on first post-migration
deploy (see WXYC/wxyc-etl#56); the runtime path that drives the pipeline still
applies the SQL files directly.

Revision ID: 0001_initial
Revises:
Create Date: 2026-04-27

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from pathlib import Path

import psycopg

from alembic import context

# revision identifiers, used by Alembic.
revision: str = "0001_initial"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# alembic/versions/0001_initial.py -> repo root is two parents up
_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schema"

# Order matches scripts/run_pipeline.py: functions precede create_database,
# which inlines the same f_unaccent (see issue #104), and the trigram indexes
# that follow once the tables exist need f_unaccent to exist.
_SCHEMA_FILES: tuple[str, ...] = (
    "create_functions.sql",
    "create_database.sql",
    "create_indexes.sql",
    "create_track_indexes.sql",
)


def upgrade() -> None:
    # Guard 1: refuse offline mode. This baseline applies schema/*.sql via a
    # side-channel ``psycopg.connect(..., autocommit=True)`` rather than
    # ``op.execute(...)``, so alembic's ``--sql`` (offline mode) cannot
    # intercept the SQL. Running ``alembic upgrade head --sql`` against a
    # populated DB used to silently DROP every release table here while the
    # console emitted innocuous "would execute" output. Refuse loudly.
    if context.is_offline_mode():
        raise RuntimeError(
            "0001_initial does not support --sql / offline mode: upgrade() opens "
            "its own psycopg connection that bypasses alembic's offline-mode SQL "
            "emission, so --sql cannot honestly dry-run it. Run "
            "`alembic upgrade head` against a live DB instead, or use "
            "`alembic current` for a read-only verification."
        )

    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply 0001_initial."
        )

    # Open a side-channel psycopg connection in autocommit mode and apply the
    # canonical schema/*.sql files. We bypass alembic's wrapped transaction so
    # the multi-statement files (some with DO $$ ... $$ blocks) execute the
    # same way scripts/run_pipeline.py applies them.
    #
    # CREATE INDEX CONCURRENTLY is stripped: this baseline only ever applies
    # to an empty database, so the online-DDL safety isn't relevant -- mirrors
    # the ``strip_concurrently=True`` path in scripts/run_pipeline.py.
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        # Guard 2: short-circuit if the discogs-cache schema is already in
        # place. Detected via ``release`` + ``cache_metadata`` (the latter is
        # specific to this schema, so the pair is unlikely to collide with an
        # unrelated DB that happens to have a ``release`` table).
        #
        # This catches both:
        #   - stamped + populated: a corrupt revision pointer or manual
        #     ``alembic downgrade base; alembic upgrade head`` against a still-
        #     populated DB.
        #   - populated + unstamped: a manual ``alembic upgrade head`` that
        #     bypassed the rebuild-cache.yml ``Verify alembic baseline is
        #     stamped`` workflow guard. Without this guard the side-channel
        #     would re-run schema/*.sql whose first lines drop every release
        #     table.
        # In either case alembic itself records ``version_num = '0001_initial'``
        # after this returns, leaving the DB equivalent to one that was
        # ``alembic stamp head``-ed.
        cur.execute(
            "SELECT to_regclass('public.release') IS NOT NULL "
            "AND to_regclass('public.cache_metadata') IS NOT NULL"
        )
        if cur.fetchone()[0]:
            logging.getLogger("alembic.runtime.migration").warning(
                "0001_initial: discogs-cache schema already present; skipping "
                "schema apply. alembic will record version_num = '0001_initial' "
                "on this database. Drop it first if you need to re-baseline."
            )
            return

        for name in _SCHEMA_FILES:
            sql = (_SCHEMA_DIR / name).read_text().replace(" CONCURRENTLY", "")
            cur.execute(sql)


def downgrade() -> None:
    # Baseline migration; no downgrade path. Drop the database to start over.
    raise NotImplementedError("0001_initial is the baseline migration; downgrade is not supported.")
