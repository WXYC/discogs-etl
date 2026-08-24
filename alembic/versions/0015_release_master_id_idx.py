"""create idx_release_master_id on DBs that only ever ran alembic

``schema/create_database.sql`` declares::

    CREATE INDEX IF NOT EXISTS idx_release_master_id
        ON release(master_id) WHERE master_id IS NOT NULL;

and **no alembic revision has ever created it**. That is the difference between
this revision and 0014, which they otherwise resemble: 0014 *repairs* an index
0008 already creates, on databases where the copy-swap ate it. Here there is no
prior revision to repair from — a database built by ``alembic upgrade head``
rather than cold-built from ``create_database.sql`` has simply never had this
index at any point in its history.

Two independent paths therefore leave a database without it:

1. **The cold-build/alembic split.** ``create_database.sql`` is the only
   declaration, so the alembic-upgrade path never produced it.
2. **The copy-swap.** ``CREATE TABLE new_release AS SELECT ...`` carries no
   indexes, and neither ``scripts/dedup_releases.py`` nor
   ``scripts/verify_cache.py --prune`` recreated it afterwards — so even a
   cold-built database lost it at the first rebuild.

Path 2's recurrence is fixed in the same change that adds this revision (both
scripts now recreate it; pinned by
``tests/integration/test_copy_swap_index_parity.py`` and behaviorally by
``tests/integration/test_copy_swap_preserves_master_id_index.py``). This
revision closes path 1, and repairs any database already drifted by either.

Why it matters
--------------

WXYC/library-metadata-lookup#1241's sibling-pressing artwork lookup filters
``release`` by ``master_id`` on the artwork-miss path, fanned out by
``asyncio.gather``. Measured read-only against prod on 2026-08-20 with the
index absent, that filter was a 192ms / 133,637-buffer full scan of all 148,491
rows; an index scan against the same predicate took 0.069ms / 5 buffers.

Idempotency and downgrade
-------------------------

``CREATE INDEX CONCURRENTLY IF NOT EXISTS`` makes the apply a no-op on any
database that already has the index — a freshly cold-built one, or prod, where
it was created by hand on 2026-08-20 ahead of this revision.

``IF NOT EXISTS`` is necessary but **not sufficient** for a safe re-run, which
is why the DDL goes through
:func:`lib.pg_concurrent_ddl.add_index_concurrently_safely` rather than being
executed raw. An interrupted ``CREATE INDEX CONCURRENTLY`` leaves the index in
``pg_class`` with ``pg_index.indisvalid = false``; PostgreSQL counts that as
existing, so a bare retry would no-op on ``IF NOT EXISTS``, stamp the revision,
and leave an index the planner will never use. The helper drops the invalid
leftover first. Pinned by ``test_upgrade_replaces_an_invalid_leftover_index``.

``downgrade`` is deliberately a **no-op**, but for a different reason than
0014's. 0014 declines to drop because doing so would undo 0008. Here there is
no earlier revision to fall back to — dropping on downgrade would instead put
the database *out* of parity with ``create_database.sql``, which declares the
index unconditionally, and would silently re-open the full-scan path for any
consumer already relying on it. A downgrade should return the schema to what
the prior revision specifies, and the prior revision specifies nothing about
this index either way.

Like 0002, 0008 and 0014, this opens its own
``psycopg.connect(..., autocommit=True)`` side channel because ``CREATE INDEX
CONCURRENTLY`` cannot run inside a transaction and alembic wraps migrations in
``context.begin_transaction()``. URL resolution and the offline-mode refusal
come from ``lib/alembic_helpers.py``.

See WXYC/discogs-etl#412 for the drift, and WXYC/discogs-etl#320 for the
superseded "its absence post-swap is deliberate" framing this change overturns.

Revision ID: 0015_release_master_id_idx
Revises: 0014_repair_artwork_null_idx
Create Date: 2026-08-23

"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import psycopg

from lib.alembic_helpers import refuse_offline, resolve_db_url
from lib.pg_concurrent_ddl import add_index_concurrently_safely

# revision identifiers, used by Alembic.
revision: str = "0015_release_master_id_idx"
down_revision: str | Sequence[str] | None = "0014_repair_artwork_null_idx"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_CREATE_INDEX = (
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_master_id "
    "ON release(master_id) "
    "WHERE master_id IS NOT NULL"
)


def upgrade() -> None:
    refuse_offline(revision, "upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn:
        log.info(
            "0015: CREATE INDEX CONCURRENTLY idx_release_master_id "
            "(no-op if the DB already has it; drops an INVALID leftover from "
            "an interrupted build first)"
        )
        add_index_concurrently_safely(conn, _CREATE_INDEX)


def downgrade() -> None:
    """No-op by design — see the module docstring.

    Dropping the index here would put the database out of parity with
    schema/create_database.sql, which declares it unconditionally. The prior
    revision says nothing about this index, so there is nothing to return to.
    """
    refuse_offline(revision, "downgrade")
