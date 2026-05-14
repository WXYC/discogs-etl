"""release_track_artist: add ``extra`` + ``role`` columns

Adds main-vs-extra credit distinction to ``release_track_artist`` so
downstream consumers can filter to main-artist credits and inspect the
source-side `<role>` string for extra credits. See WXYC/discogs-etl#218
and the consumer-side write-ups WXYC/library-metadata-lookup#327 / #328.

| Column   | Type    | Default | Notes |
|----------|---------|---------|-------|
| ``extra``| integer | ``0``   | ``0`` = main artist (`<artists>`); ``1`` = extra artist (`<extraartists>`). Mirrors ``release_artist.extra``. |
| ``role`` | text    | NULL    | Source `<role>` text on extra entries (``Producer``, ``Mixed By``, ``Written-By``, …). Always NULL for main credits. |

Both columns are additive and NULL-tolerant so the migration is
deploy-safe in either order relative to the producer (the discogs-xml-converter
change in WXYC/discogs-xml-converter#55):

* If the migration ships first, the discogs-etl loader reads
  3-column CSVs (legacy converter output) and PG defaults populate
  ``extra=0`` / ``role=NULL`` — which is the correct legacy-equivalent
  interpretation under which existing consumers were already operating.
* If the producer ships first, the converter still writes 3-column
  output until this migration applies; once applied, subsequent re-ETL
  runs populate the new columns with no further code changes.

Re-ETL of the three discogs-cache deployments (Docker, Homebrew full,
Railway) is required to populate the new columns against existing rows
and is tracked as a deploy follow-up.

Revision ID: 0005_release_track_artist_role
Revises: 0004_wxyc_identity_match_fns
Create Date: 2026-05-14

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

import psycopg

from alembic import context

# revision identifiers, used by Alembic.
revision: str = "0005_release_track_artist_role"
down_revision: str | Sequence[str] | None = "0004_wxyc_identity_match_fns"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_UPGRADE_SQL = """
ALTER TABLE release_track_artist
    ADD COLUMN IF NOT EXISTS extra integer DEFAULT 0;
ALTER TABLE release_track_artist
    ADD COLUMN IF NOT EXISTS role text;
"""

_DOWNGRADE_SQL = """
ALTER TABLE release_track_artist DROP COLUMN IF EXISTS role;
ALTER TABLE release_track_artist DROP COLUMN IF EXISTS extra;
"""


def _resolve_db_url() -> str:
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply "
            "0005_release_track_artist_role."
        )
    return db_url


def _refuse_offline(direction: str) -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            f"0005_release_track_artist_role does not support --sql / offline "
            f"mode ({direction}): the migration opens its own autocommit "
            "psycopg connection to apply DDL, mirroring 0001-0004. Run "
            "`alembic upgrade head` (or `downgrade`) against a live DB "
            "instead."
        )


def upgrade() -> None:
    _refuse_offline("upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        log.info("0005: ALTER release_track_artist ADD COLUMN extra, role")
        cur.execute(_UPGRADE_SQL)


def downgrade() -> None:
    _refuse_offline("downgrade")

    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_DOWNGRADE_SQL)
