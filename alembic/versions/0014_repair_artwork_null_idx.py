"""repair release_artwork_null_idx on DBs where the copy-swap ate it

Migration 0008 creates ``release_artwork_null_idx`` and every production
``discogs-cache`` has run well past that revision. The index is nonetheless
absent from prod: an audit on 2026-08-19 found ``release`` carrying only
``release_pkey`` and ``idx_release_title_trgm``.

Cause
-----

The rebuild's copy-swap destroys it. ``scripts/verify_cache.py`` (``--prune``)
and ``scripts/dedup_releases.py`` rebuild ``release`` with ``CREATE TABLE
new_release AS SELECT ...`` followed by a RENAME. CTAS carries no indexes, so
the swap drops every index on the table, and only the ones each script
explicitly rebuilds afterwards come back. ``release_artwork_null_idx`` was not
in either list, so it was created by 0008 and then eaten by the next rebuild —
silently, because a missing index degrades query plans rather than raising.

This is why ``schema/create_database.sql``'s "the dual-write convention keeps
the fresh-rebuild and alembic-upgrade paths in parity" claim did not hold in
practice: there is a *third* path — the copy-swap rebuild — that was in parity
with neither.

The recurrence is fixed in the same change that adds this revision (both
scripts now recreate the index; pinned by
``tests/integration/test_copy_swap_index_parity.py``). This revision exists
only to repair databases that already drifted, since 0008 is already stamped
and will not re-run.

Why it matters
--------------

``scripts/topup_artwork.py``'s candidate query is this index's predicate
verbatim (``artwork_url IS NULL AND artwork_checked_at IS NULL``). Without the
index that drain seq-scans the whole ``release`` table.

Idempotency and downgrade
-------------------------

``CREATE INDEX CONCURRENTLY IF NOT EXISTS`` makes the apply a no-op on any DB
that still has the index (e.g. one built end-to-end from
``schema/create_database.sql`` and never pruned).

``downgrade`` is deliberately a **no-op**. This revision repairs drift rather
than introducing schema, so there is nothing to reverse: dropping the index
here would undo 0008, leaving a DB at 0013 in a state 0008 says it should not
be in. See WXYC/discogs-etl#239 for the index and its original rationale.

Like 0002 and 0008, this opens its own ``psycopg.connect(..., autocommit=True)``
side channel because ``CREATE INDEX CONCURRENTLY`` cannot run inside a
transaction and alembic wraps migrations in ``context.begin_transaction()``.

Revision ID: 0014_repair_artwork_null_idx
Revises: 0013_adopt_entity_identity
Create Date: 2026-08-19

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

import psycopg

from alembic import context

# revision identifiers, used by Alembic.
revision: str = "0014_repair_artwork_null_idx"
down_revision: str | Sequence[str] | None = "0013_adopt_entity_identity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_CREATE_INDEX = (
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS release_artwork_null_idx "
    "ON release (id) "
    "WHERE artwork_url IS NULL AND artwork_checked_at IS NULL"
)


def _resolve_db_url() -> str:
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply "
            "0014_repair_artwork_null_idx."
        )
    return db_url


def _refuse_offline(direction: str) -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            f"0014_repair_artwork_null_idx does not support --sql / offline "
            f"mode ({direction}): CREATE INDEX CONCURRENTLY cannot run inside a "
            "transaction, so this revision opens its own autocommit psycopg "
            "connection that bypasses alembic's offline SQL emission. Run "
            "`alembic upgrade head` against a live DB instead."
        )


def upgrade() -> None:
    _refuse_offline("upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(_resolve_db_url(), autocommit=True) as conn, conn.cursor() as cur:
        log.info(
            "0014: CREATE INDEX CONCURRENTLY release_artwork_null_idx "
            "(no-op if the copy-swap has not eaten it)"
        )
        cur.execute(_CREATE_INDEX)


def downgrade() -> None:
    """No-op by design — see the module docstring.

    This revision repairs a drifted DB back to what 0008 already specifies.
    Dropping the index on downgrade would undo 0008 rather than this revision.
    """
    _refuse_offline("downgrade")
