"""wxyc-postgres image gate: F0000 precheck for the wxyc_unaccent dictionary

Background — discogs-etl#223 (Option B, post-PR #230)
-----------------------------------------------------

PR #230 / migration 0004 made the runtime ``wxyc_unaccent`` invocation
filesystem-independent by deploying a pure-SQL ``wxyc_unaccent_text(text)``
function and patching the vendored canonical SQL's one call site to it. That
unblocked Railway (and any managed Postgres where ``$SHAREDIR/tsearch_data/``
is unwritable) without needing a custom image.

This migration is the Option B layer: it brings *deployed* state to "the
wxyc-postgres image is in place" by creating the canonical text-search
dictionary ``wxyc_unaccent`` from ``$SHAREDIR/tsearch_data/wxyc_unaccent.rules``
— which the ``ghcr.io/wxyc/wxyc-postgres:pg{16,17}`` image bakes into the
base. On a destination that *doesn't* run the wxyc-postgres image, the
``CREATE TEXT SEARCH DICTIONARY`` call lands an ``SQLSTATE F0000``
(``config_file_error``) and this migration re-raises with an actionable
runbook URL so the operator knows to swap the PG service image.

Defense-in-depth alongside 0004
-------------------------------

The function-based path in 0004 already handles all runtime calls — this
migration's dict is *not* required for ``wxyc_identity_match_*`` correctness
(the canonical SQL's ``unaccent('wxyc_unaccent', r)`` call was patched to
``wxyc_unaccent_text(r)`` at 0004's apply time). The dict becomes useful for
*future* migrations that can now assume the image is in place — and the
F0000 precheck gives a deploy-time signal that the image swap happened.

Sequencing
----------

Apply order on a Railway destination:

1. Operator swaps Railway PG service to ``ghcr.io/wxyc/wxyc-postgres:pg17``
   (one-time, tracked in the
   `wxyc-postgres operator runbook <https://github.com/WXYC/wxyc-etl/blob/main/docs/wxyc-postgres-image.md>`_).
2. ``alembic upgrade head`` applies this migration. Dict is created.

If step 1 was skipped or rolled back, step 2 aborts with the runbook URL and
the migration chain stays at 0006. Railway stays usable (runtime path still
works via 0004's function) — only the dict-creation gate fails.

Revision ID: 0007_wxyc_postgres_image_gate
Revises: 0006_lookup_negative
Create Date: 2026-05-20

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

import psycopg

from alembic import context

# revision identifiers, used by Alembic.
revision: str = "0007_wxyc_postgres_image_gate"
down_revision: str | Sequence[str] | None = "0006_lookup_negative"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# Operator runbook URL — also asserted by the unit + integration tests so any
# rename surfaces as a test failure before it ships.
# ---------------------------------------------------------------------------
RUNBOOK_URL = "https://github.com/WXYC/wxyc-etl/blob/main/docs/wxyc-postgres-image.md"


# Plpgsql wrapper around CREATE TEXT SEARCH DICTIONARY that catches F0000
# (config_file_error) when the rules file is absent from $SHAREDIR/tsearch_data
# and re-raises with the runbook URL embedded in the message.
#
# CREATE EXTENSION IF NOT EXISTS unaccent first — defensive, mirroring 0004's
# _PREP_SQL. The baseline migration creates it as part of schema/*.sql, but
# tests that stamp forward without applying schema (e.g. per-test fresh DBs)
# never run that step, so we re-assert it here. Harmless on already-loaded
# instances.
#
# Idempotency on the dict itself: DROP IF EXISTS first so re-running this
# migration after an image swap (or after a downgrade/upgrade cycle) doesn't
# trip the "object already exists" (42710) path. The DROP is unconditionally
# safe — old dict-based deploys had this dict too; the new function-based
# path doesn't depend on it; either way, dropping + recreating is a no-op
# for anything downstream.
_UPGRADE_SQL = f"""
CREATE EXTENSION IF NOT EXISTS unaccent;
DROP TEXT SEARCH DICTIONARY IF EXISTS wxyc_unaccent;
DO $$
BEGIN
    CREATE TEXT SEARCH DICTIONARY wxyc_unaccent (
        TEMPLATE = unaccent,
        RULES = 'wxyc_unaccent'
    );
EXCEPTION
    WHEN SQLSTATE 'F0000' THEN
        RAISE EXCEPTION USING
            MESSAGE = 'wxyc_unaccent.rules is missing from $SHAREDIR/tsearch_data/. '
                      'The destination PG must run the wxyc-postgres image '
                      '(ghcr.io/wxyc/wxyc-postgres:pg17 or :pg16). Runbook: '
                      '{RUNBOOK_URL}',
            ERRCODE = 'F0000';
END;
$$;
"""

_DOWNGRADE_SQL = "DROP TEXT SEARCH DICTIONARY IF EXISTS wxyc_unaccent;\n"


def _resolve_db_url() -> str:
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply "
            "0007_wxyc_postgres_image_gate."
        )
    return db_url


def _refuse_offline(direction: str) -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            f"0007_wxyc_postgres_image_gate does not support --sql / offline mode "
            f"({direction}): the migration opens its own autocommit psycopg "
            "connection so the F0000 precheck can surface as a real exception "
            "instead of an emitted SQL string. Run `alembic upgrade head` (or "
            "`downgrade`) against a live DB instead."
        )


def upgrade() -> None:
    _refuse_offline("upgrade")
    log = logging.getLogger("alembic.runtime.migration")
    db_url = _resolve_db_url()

    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        log.info("0007: probing wxyc_unaccent.rules availability + creating dictionary")
        cur.execute(_UPGRADE_SQL)


def downgrade() -> None:
    _refuse_offline("downgrade")
    db_url = _resolve_db_url()
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_DOWNGRADE_SQL)
