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

``IF NOT EXISTS`` is necessary but **not sufficient** for a safe re-run, which
is why the DDL goes through
:func:`lib.pg_concurrent_ddl.add_index_concurrently_safely` rather than being
executed raw. A ``CREATE INDEX CONCURRENTLY`` that gets interrupted — SIGTERM
on the rebuild instance, a job timeout, a cancelled statement — leaves the
index in ``pg_class`` with ``pg_index.indisvalid = false``. PostgreSQL counts
that invalid index as existing, so a second ``alembic upgrade head`` would
no-op on ``IF NOT EXISTS``, stamp 0014, and leave the operator with an index
the planner will never use: the exact silent state this revision exists to
repair. The helper drops the invalid leftover first, so a retried upgrade
converges on a valid index. Pinned by
``test_upgrade_replaces_an_invalid_leftover_index``.

``downgrade`` is deliberately a **no-op**. This revision repairs drift rather
than introducing schema, so there is nothing to reverse: dropping the index
here would undo 0008, leaving a DB at 0013 in a state 0008 says it should not
be in. See WXYC/discogs-etl#239 for the index and its original rationale.

Like 0002 and 0008, this opens its own ``psycopg.connect(..., autocommit=True)``
side channel because ``CREATE INDEX CONCURRENTLY`` cannot run inside a
transaction and alembic wraps migrations in ``context.begin_transaction()``.
URL resolution and the offline-mode refusal come from
``lib/alembic_helpers.py``, the same shared plumbing 0010-0013 use.

Revision ID: 0014_repair_artwork_null_idx
Revises: 0013_adopt_entity_identity
Create Date: 2026-08-19

"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import psycopg

from lib.alembic_helpers import refuse_offline, resolve_db_url
from lib.pg_concurrent_ddl import add_index_concurrently_safely

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


def upgrade() -> None:
    refuse_offline(revision, "upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn:
        log.info(
            "0014: CREATE INDEX CONCURRENTLY release_artwork_null_idx "
            "(no-op if the copy-swap has not eaten it; drops an INVALID "
            "leftover from an interrupted build first)"
        )
        add_index_concurrently_safely(conn, _CREATE_INDEX)


def downgrade() -> None:
    """No-op by design — see the module docstring.

    This revision repairs a drifted DB back to what 0008 already specifies.
    Dropping the index on downgrade would undo 0008 rather than this revision.
    """
    refuse_offline(revision, "downgrade")
