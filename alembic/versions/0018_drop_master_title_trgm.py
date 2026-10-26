"""drop the unused idx_master_title_trgm

``schema/create_database.sql`` used to declare::

    CREATE INDEX IF NOT EXISTS idx_master_title_trgm
        ON master USING GIN (lower(f_unaccent(title)) gin_trgm_ops);

Nothing queries it. Measured read-only against prod on 2026-09-25 it had
``idx_scan = 0`` and occupied ~5 MB, 2.52x the size of the table it indexes.
The declaration is deleted in the same change, because the monthly rebuild
applies that file and would otherwise recreate the index a month after this
revision dropped it (the same reason 0017 deleted the ``idx_artist_*_artist_id``
declarations).

``DROP INDEX CONCURRENTLY IF EXISTS`` cannot run inside a transaction, so this
opens its own ``psycopg.connect(..., autocommit=True)`` side channel like
0002/0008/0014/0015. ``IF EXISTS`` makes it a no-op on any database built from
the current schema files, which never had the index.

``downgrade`` recreates the index with the definition that was removed from the
schema file. Rollback order: ``alembic downgrade`` to 0017 from a checkout that
still has this revision, *then* revert the PR, both before the next rebuild.
Reverting first leaves ``alembic_version`` naming a revision the tree no longer
has, and the rebuild's ``alembic upgrade head`` fails on it.

See WXYC/discogs-etl#437 and #432.

Revision ID: 0018_drop_master_title_trgm
Revises: 0017_artist_child_unique
Create Date: 2026-10-25

"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import psycopg

from lib.alembic_helpers import refuse_offline, resolve_db_url

# revision identifiers, used by Alembic.
revision: str = "0018_drop_master_title_trgm"
down_revision: str | Sequence[str] | None = "0017_artist_child_unique"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    refuse_offline(revision, "upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn:
        log.info("0018: DROP INDEX CONCURRENTLY idx_master_title_trgm (no-op if absent)")
        conn.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_master_title_trgm")


def downgrade() -> None:
    refuse_offline(revision, "downgrade")

    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn:
        conn.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_master_title_trgm "
            "ON master USING GIN (lower(f_unaccent(title)) gin_trgm_ops)"
        )
