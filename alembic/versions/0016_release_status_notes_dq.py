"""release.status / notes / data_quality: keep the three Discogs qualifiers.

``discogs-xml-converter``'s ``release.csv`` writer emits ``id, status, title,
country, released, notes, data_quality, master_id, format`` on every run. The
``release`` table config in ``scripts/import_csv.py`` named six of those nine,
so three columns the converter already produced were discarded at the COPY
seam. This revision adds the destination for them.

What they are
-------------

* ``status`` — Discogs's editorial state for the record: ``Accepted``,
  ``Draft``, ``Deleted``. A quality signal for any later ranking or filtering
  decision.
* ``notes`` — Discogs's freeform release note. Often records reissue
  provenance in prose ("Reissue of…", "Originally released in 1981"). Weak and
  unstructured — never a discriminator on its own — but free, and the only
  reissue hint available until format descriptions land
  (WXYC/discogs-etl#429).
* ``data_quality`` — Discogs's own confidence rating for the record:
  ``Correct``, ``Needs Vote``, ``Entirely Incorrect``, … Directly relevant to
  which pressing to trust.

Nothing reads them yet. This revision makes the later decisions possible; the
dedup ranking that would consume them is WXYC/discogs-etl#430 and is
deliberately out of scope here.

Nullable with no default
------------------------

All three are ``text`` with no ``NOT NULL`` and no ``DEFAULT``, which is what
lets a ``release.csv`` produced by a converter predating them still import:
``scripts/import_csv.py`` registers them as ``optional_csv_columns``, so an
absent header degrades to NULL instead of tripping the "Missing columns" bail
that writes zero rows (WXYC/discogs-etl#204). Pinned by
``tests/integration/test_import_release_qualifiers.py``.

No backfill runs here. Existing rows stay NULL until the next rebuild carries
values in; a NULL therefore means "this row predates #428", not "Discogs had
no value".

Dual-write convention
---------------------

``schema/create_database.sql`` (fresh-rebuild path) declares the same three
columns alongside the existing ``release`` DDL, so a ground-up rebuild
produces the same end-state as the alembic chain.

Copy-swap parity
----------------

Adding a column to ``release`` is only half the job. Both rebuild paths
recreate the table with ``CREATE TABLE new_release AS SELECT {columns} FROM
release``, and a CTAS inherits only the columns its SELECT names — so a column
missing from either hardcoded list is silently dropped on the next monthly
rebuild, leaving no relic in ``pg_attribute``. ``release.master_id`` went that
way in WXYC/discogs-etl#129 and five more columns in #232. All three columns
are therefore added to ``dedup_releases.DEDUP_TABLES``,
``verify_cache.PRUNE_COPY_TABLES`` and ``verify_cache.COPY_TABLE_SPEC``,
pinned by ``tests/integration/test_copy_swap_preserves_release_qualifiers.py``.

Revision ID: 0016_release_status_notes_dq
Revises: 0015_release_master_id_idx
Create Date: 2026-09-23

"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import psycopg

from lib.alembic_helpers import refuse_offline, resolve_db_url

revision: str = "0016_release_status_notes_dq"
down_revision: str | Sequence[str] | None = "0015_release_master_id_idx"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_UPGRADE_SQL = """
ALTER TABLE release
    ADD COLUMN IF NOT EXISTS status text,
    ADD COLUMN IF NOT EXISTS notes text,
    ADD COLUMN IF NOT EXISTS data_quality text;
"""

_DOWNGRADE_SQL = """
ALTER TABLE release
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS notes,
    DROP COLUMN IF EXISTS data_quality;
"""


def upgrade() -> None:
    refuse_offline(revision, "upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn, conn.cursor() as cur:
        log.info("0016: ALTER release ADD COLUMN status, notes, data_quality")
        # One ALTER, three subcommands: a single table rewrite-free catalog
        # update rather than three separate ACCESS EXCLUSIVE lock acquisitions
        # against a ~19M-row table.
        cur.execute(_UPGRADE_SQL)


def downgrade() -> None:
    refuse_offline(revision, "downgrade")

    with psycopg.connect(resolve_db_url(revision), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_DOWNGRADE_SQL)
