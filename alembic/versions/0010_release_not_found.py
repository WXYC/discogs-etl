"""release.not_found: tombstone marker for Discogs 404s on get_release.

Adds a ``boolean NOT NULL DEFAULT FALSE`` column. The default makes the
migration backward-compatible: prod LML running pre-510 code reads
``not_found = FALSE`` on every existing row and behaves exactly as it
did before.

Background
----------

LML's ``DiscogsService.get_release`` re-hits the Discogs API on every
call for a release id that 404s. The ``_api_fetch`` inner function
collapses 404 to ``None`` and the fallthrough seam refuses to write
``None`` to PG, so no row records "we asked Discogs, the answer was
nothing". Every subsequent call burns rate-limit budget on the same 404.

The principled fix composes on the existing
``release.artwork_checked_at`` discriminator (this repo's #239 / LML's
PR #415) rather than introducing a parallel negative-cache surface:
LML's ``_api_fetch`` returns a tombstone-shaped ``ReleaseMetadataResponse``
with ``not_found = TRUE``, ``title = ''`` (identifier sentinel), and
``artwork_checked_at = now()``. The fallthrough seam already writes
that via the existing ``pg_write`` path; subsequent reads short-circuit
on ``not_found = TRUE`` before fetching child tables.

LML's tombstone-write UPSERT preserves a hydrated parent row's
identifier columns (the ``ON CONFLICT DO UPDATE SET`` clause omits
``title`` / ``release_year`` / etc.) so a 404 after a 200 doesn't lose
the title. Symmetric: the non-tombstone branch adds
``not_found = FALSE`` to ``ON CONFLICT DO UPDATE SET`` so a recovered
200 clears any prior tombstone in one statement.

Rebuild-path symmetry
---------------------

``scripts/import_csv.py::import_release_via_upsert`` includes
``not_found = FALSE`` in both the INSERT and the ``ON CONFLICT DO
UPDATE SET`` so a pipeline refresh clears any prior tombstone. Without
this, a tombstoned id would survive a rebuild and stay unreachable
until LML's admin recovery endpoint deletes it.

Empty-string write precondition
-------------------------------

``release.title`` is ``text NOT NULL`` with no
``CHECK (title <> '')``. LML's tombstone INSERT writes ``title = ''``
as a sentinel — the ``not_found = TRUE`` flag is the authoritative
discriminator, and the empty string is a "this column is intentionally
unpopulated" marker on the row. The migration probes the write so a
future-added CHECK trips this migration rather than the prod write.

Dual-write convention
---------------------

``schema/create_database.sql`` (fresh-rebuild path) adds the column
alongside the existing ``release`` DDL so a ground-up rebuild produces
the same end-state as the alembic chain.

Companion tickets
-----------------

* WXYC/library-metadata-lookup#510 — the consumer.
* WXYC/library-metadata-lookup#503 — sibling ``artist.fetched_at``
  discriminator (the prior application of the same pattern).
* WXYC/discogs-etl#239 / WXYC/library-metadata-lookup#414 — sibling
  ``release.artwork_checked_at`` discriminator.

Revision ID: 0010_release_not_found
Revises: 0009_cache_metadata_unique
Create Date: 2026-06-08

"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import psycopg

from lib.alembic_helpers import refuse_offline, resolve_db_url

revision: str = "0010_release_not_found"
down_revision: str | Sequence[str] | None = "0009_cache_metadata_unique"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_UPGRADE_SQL = """
ALTER TABLE release
    ADD COLUMN IF NOT EXISTS not_found boolean NOT NULL DEFAULT FALSE;
"""

_DOWNGRADE_SQL = """
ALTER TABLE release DROP COLUMN IF EXISTS not_found;
"""


# Sentinel id used by the upgrade probe. Out-of-range for any real Discogs
# release id (negatives never appear in the dump), and rolled back in the
# same transaction so no row ever lands.
_PROBE_ID = -2147483648


def _probe_empty_title_write() -> None:
    """Verify ``release.title = ''`` can be written.

    LML's tombstone INSERT writes ``title = ''``. If a future CHECK or
    trigger is added that blocks this, the prod write would fail at
    runtime with a confused-looking error. Probing here surfaces the
    incompatibility at migration time, in a message that names the
    consumer (LML#510). Probe runs in a transaction that is always
    rolled back, so nothing is persisted.
    """
    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("BEGIN")
        try:
            cur.execute(
                "INSERT INTO release (id, title) VALUES (%s, '')",
                (_PROBE_ID,),
            )
        except Exception as e:
            cur.execute("ROLLBACK")
            raise RuntimeError(
                "0010 precondition failed: release.title cannot accept the "
                "empty-string sentinel that LML#510 writes for tombstone "
                "rows. A CHECK constraint, trigger, or DEFAULT clause has "
                "been added that rejects ''. Either drop the constraint or "
                f"change the tombstone sentinel in LML. Original error: {e}"
            ) from e
        else:
            cur.execute("ROLLBACK")


def upgrade() -> None:
    refuse_offline(revision, "upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    _probe_empty_title_write()

    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn, conn.cursor() as cur:
        log.info("0010: ALTER release ADD COLUMN not_found")
        cur.execute(_UPGRADE_SQL)


def downgrade() -> None:
    refuse_offline(revision, "downgrade")

    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_DOWNGRADE_SQL)
