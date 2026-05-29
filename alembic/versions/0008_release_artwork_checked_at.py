"""release.artwork_checked_at: distinguish never-asked from genuinely-imageless

Adds a nullable ``timestamptz`` column + a partial index supporting LML's
top-up drain query.

Background
----------

``release.artwork_url`` can be NULL for two distinct reasons today:

1. **Never asked.** The bulk loader (``scripts/import_csv.py:import_artwork``)
   didn't populate it — the row predates the artwork-loading step, the
   converter skipped it, or the dump had no image.
2. **Asked, genuinely no image.** LML hit the Discogs API at lookup time and
   the release legitimately has no cover (``images=[]`` in the API response).

These two states are indistinguishable today, which forces LML's cache-hit
predicate into one of two bad behaviors:

* Treat NULL as a hit → never repair the 48% bulk-loader gap
  (pre-WXYC/library-metadata-lookup#414 / PR #415 behavior).
* Treat NULL as a miss → re-fetch genuinely-imageless releases forever, on
  every lookup, burning Discogs rate limit
  (post-PR #415 behavior; structural follow-up is
  WXYC/library-metadata-lookup#423).

``artwork_checked_at`` resolves the ambiguity at the schema level:

* Bulk loader leaves it NULL (the row was never asked).
* LML's live-API path stamps ``now()`` whenever ``write_release`` runs.
* LML#221's top-up drain stamps it on every row it touches.

The companion LML predicate becomes "``artwork_url IS NOT NULL`` OR
``artwork_checked_at IS NOT NULL``" — both states are full hits; only the
never-asked state falls through.

Partial index
-------------

``release_artwork_null_idx ON release (id) WHERE artwork_url IS NULL AND
artwork_checked_at IS NULL`` covers the LML#221 drain scan precisely. A
wider predicate would index the genuinely-imageless tail (post-API-check
rows); a narrower one wouldn't capture the never-asked set. The drain
query is the only consumer of this index.

Dual-write convention
---------------------

The matching change to ``schema/create_database.sql`` (fresh-rebuild path)
adds the column + index alongside the existing ``release`` DDL so a
ground-up rebuild produces the same end-state as the alembic chain.
``ADD COLUMN IF NOT EXISTS`` + ``CREATE INDEX IF NOT EXISTS`` keep this
migration idempotent against destinations that landed on the column via
the legacy schema path before alembic was wired up.

Companion tickets
-----------------

* WXYC/discogs-etl#239 — this migration.
* WXYC/discogs-etl#242 — rebuild ``COALESCE`` upsert; preserves LML's
  back-patched ``artwork_url`` (and this column) across rebuilds.
* WXYC/discogs-etl#240 — ``import_artwork`` regression test; the bulk
  loader stays NULL-correct.
* WXYC/discogs-etl#241 — null-share monitoring; the schema split this
  migration introduces lets the metric distinguish the two NULL states.
* WXYC/library-metadata-lookup#423 — LML consumer of the column.
* WXYC/library-metadata-lookup#414, #221 — the LML problems this column
  closes out.

Revision ID: 0008_release_artwork_checked_at
Revises: 0007_wxyc_postgres_image_gate
Create Date: 2026-05-29

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

import psycopg

from alembic import context

revision: str = "0008_release_artwork_checked_at"
down_revision: str | Sequence[str] | None = "0007_wxyc_postgres_image_gate"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_UPGRADE_SQL = """
ALTER TABLE release
    ADD COLUMN IF NOT EXISTS artwork_checked_at timestamptz;

CREATE INDEX IF NOT EXISTS release_artwork_null_idx
    ON release (id)
    WHERE artwork_url IS NULL AND artwork_checked_at IS NULL;
"""

_DOWNGRADE_SQL = """
DROP INDEX IF EXISTS release_artwork_null_idx;
ALTER TABLE release DROP COLUMN IF EXISTS artwork_checked_at;
"""


def _resolve_db_url() -> str:
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply "
            "0008_release_artwork_checked_at."
        )
    return db_url


def _refuse_offline(direction: str) -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            f"0008_release_artwork_checked_at does not support --sql / "
            f"offline mode ({direction}): the migration opens its own "
            "autocommit psycopg connection to apply DDL, mirroring 0005. "
            "Run `alembic upgrade head` (or `downgrade`) against a live DB "
            "instead."
        )


def upgrade() -> None:
    _refuse_offline("upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        log.info(
            "0008: ALTER release ADD COLUMN artwork_checked_at + "
            "CREATE INDEX release_artwork_null_idx"
        )
        cur.execute(_UPGRADE_SQL)


def downgrade() -> None:
    _refuse_offline("downgrade")

    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_DOWNGRADE_SQL)
