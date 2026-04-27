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

import os
from collections.abc import Sequence
from pathlib import Path

import psycopg

# revision identifiers, used by Alembic.
revision: str = "0001_initial"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# alembic/versions/0001_initial.py -> repo root is two parents up
_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schema"

# Order matches scripts/run_pipeline.py: functions must precede create_database
# because idx_master_title_trgm references f_unaccent (see issue #104).
# Indexes follow once the tables exist.
_SCHEMA_FILES: tuple[str, ...] = (
    "create_functions.sql",
    "create_database.sql",
    "create_indexes.sql",
    "create_track_indexes.sql",
)


def upgrade() -> None:
    # Open a side-channel psycopg connection in autocommit mode and apply the
    # canonical schema/*.sql files. We bypass alembic's wrapped transaction so
    # the multi-statement files (some with DO $$ ... $$ blocks) execute the
    # same way scripts/run_pipeline.py applies them.
    #
    # CREATE INDEX CONCURRENTLY is stripped: this baseline only ever applies
    # to an empty database, so the online-DDL safety isn't relevant -- mirrors
    # the ``strip_concurrently=True`` path in scripts/run_pipeline.py.
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply 0001_initial."
        )
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for name in _SCHEMA_FILES:
            sql = (_SCHEMA_DIR / name).read_text().replace(" CONCURRENTLY", "")
            cur.execute(sql)


def downgrade() -> None:
    # Baseline migration; no downgrade path. Drop the database to start over.
    raise NotImplementedError("0001_initial is the baseline migration; downgrade is not supported.")
