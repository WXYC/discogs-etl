"""artist.not_found: tombstone marker for Discogs 404s on get_artist_details.

Parallel to 0010 but for the ``artist`` table. Same rationale: LML's
``DiscogsService.get_artist_details`` collapses 404 to ``None`` today,
the fallthrough seam refuses to write ``None``, and every caller
re-burns the rate-limit budget on the same 404. ``not_found = TRUE``
becomes the tombstone the existing ``fetched_at``-based ``is_pg_hit``
predicate (#503) already serves; this column just gives the read side
something explicit to short-circuit on.

Sibling discriminators on ``artist``:

* ``fetched_at`` (LML#503) — distinguishes ETL stubs (``fetched_at``
  defaulted at row creation) from rows hydrated by LML's live-API path
  (``fetched_at`` set by the LML write).
* ``not_found`` (this migration) — distinguishes hydrated 200-response
  rows from hydrated 404-response tombstones.

These compose: ``fetched_at IS NOT NULL`` means the row was touched by
*some* LML write; ``not_found`` then says which side of the 200/404
split that write came from.

LML's tombstone-write UPSERT preserves a hydrated parent's identifier
columns (``name`` / ``profile`` / ``image_url``) by intentionally
omitting them from the ``ON CONFLICT DO UPDATE SET`` clause. The
non-tombstone branch adds ``not_found = FALSE`` to its update set so a
recovered 200 clears any prior tombstone in one statement.

Empty-name write precondition probed at upgrade time, matching 0010's
pattern.

Revision ID: 0011_artist_not_found
Revises: 0010_release_not_found
Create Date: 2026-06-08

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

import psycopg

from alembic import context

revision: str = "0011_artist_not_found"
down_revision: str | Sequence[str] | None = "0010_release_not_found"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_UPGRADE_SQL = """
ALTER TABLE artist
    ADD COLUMN IF NOT EXISTS not_found boolean NOT NULL DEFAULT FALSE;
"""

_DOWNGRADE_SQL = """
ALTER TABLE artist DROP COLUMN IF EXISTS not_found;
"""


_PROBE_ID = -2147483648


def _resolve_db_url() -> str:
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply 0011_artist_not_found."
        )
    return db_url


def _refuse_offline(direction: str) -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            f"0011_artist_not_found does not support --sql / offline mode "
            f"({direction}): the migration opens its own psycopg connection "
            "to apply DDL and run a precondition probe. Run `alembic "
            "upgrade head` (or `downgrade`) against a live DB instead."
        )


def _probe_empty_name_write() -> None:
    """Verify ``artist.name = ''`` can be written.

    LML's tombstone INSERT writes ``name = ''`` + ``not_found = TRUE``.
    Probing surfaces a future-added CHECK at migration time rather than
    on the first prod 404.
    """
    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("BEGIN")
        try:
            cur.execute(
                "INSERT INTO artist (id, name) VALUES (%s, '')",
                (_PROBE_ID,),
            )
        except Exception as e:
            cur.execute("ROLLBACK")
            raise RuntimeError(
                "0011 precondition failed: artist.name cannot accept the "
                "empty-string sentinel that LML#510 writes for tombstone "
                "rows. A CHECK constraint, trigger, or DEFAULT clause has "
                "been added that rejects ''. Either drop the constraint or "
                f"change the tombstone sentinel in LML. Original error: {e}"
            ) from e
        else:
            cur.execute("ROLLBACK")


def upgrade() -> None:
    _refuse_offline("upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    _probe_empty_name_write()

    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        log.info("0011: ALTER artist ADD COLUMN not_found")
        cur.execute(_UPGRADE_SQL)


def downgrade() -> None:
    _refuse_offline("downgrade")

    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_DOWNGRADE_SQL)
