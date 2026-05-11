"""wxyc_identity_match_* plpgsql function family (wiki §3.3.5)

Deploys the four cross-cache-identity match functions onto the discogs-cache
PG instance. Vendored byte-for-byte from WXYC/wxyc-etl@v0.4.0
(`data/wxyc_identity_match_functions.sql`); SHA-pinned at repo root in
`wxyc-etl-pin.txt`. The wxyc_unaccent text-search dictionary is created from
the rules file at `vendor/wxyc-etl/wxyc_unaccent.rules`, which the deploy
target must have available at `$SHAREDIR/tsearch_data/`.

| Postgres function | Rust counterpart |
|---|---|
| wxyc_identity_match_artist                       | to_identity_match_form |
| wxyc_identity_match_title                        | to_identity_match_form_title |
| wxyc_identity_match_with_punctuation             | to_identity_match_form_with_punctuation |
| wxyc_identity_match_with_disambiguator_strip     | to_identity_match_form_with_disambiguator_strip |

Defensive pattern: this revision uses the same `is_offline_mode()` refuse
and `psycopg.connect(..., autocommit=True)` side-channel as
0001_initial / 0002_backfill_trigram_indexes / 0003_wxyc_library_v2 so that
`alembic upgrade --sql` (offline mode) fails fast rather than silently
emitting no-op SQL. The functions use `CREATE OR REPLACE FUNCTION`, so
re-running is a no-op. The text-search dictionary is dropped+recreated each
upgrade so a rules-file refresh picks up cleanly.

Column flip on indexes + cache-load expressions ships in this PR but lands
outside the alembic migration itself — see `schema/create_indexes.sql` and
`scripts/run_pipeline.py` changes. The migration alone is restartable; the
index flip is also idempotent (CREATE INDEX IF NOT EXISTS / DROP INDEX
CONCURRENTLY IF EXISTS).

Revision ID: 0004_wxyc_identity_match_fns
Revises: 0003_wxyc_library_v2
Create Date: 2026-05-11

"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from pathlib import Path

import psycopg

from alembic import context

# revision identifiers, used by Alembic.
revision: str = "0004_wxyc_identity_match_fns"
down_revision: str | Sequence[str] | None = "0003_wxyc_library_v2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# Path to the vendored canonical SQL. Loaded at migration time rather than
# embedded in this module so the byte-for-byte parity check in
# `tests/integration/test_wxyc_identity_match_parity.py` can read the same
# file and assert the deploy matches upstream.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_FUNCTIONS_SQL_PATH = _REPO_ROOT / "vendor" / "wxyc-etl" / "wxyc_identity_match_functions.sql"

_SETUP_SQL = """
CREATE EXTENSION IF NOT EXISTS unaccent;

DROP TEXT SEARCH DICTIONARY IF EXISTS wxyc_unaccent;
CREATE TEXT SEARCH DICTIONARY wxyc_unaccent (
  TEMPLATE = unaccent,
  RULES = 'wxyc_unaccent'
);
"""

_DOWNGRADE_FUNCTIONS = [
    "wxyc_identity_match_with_disambiguator_strip(text)",
    "wxyc_identity_match_with_punctuation(text)",
    "wxyc_identity_match_title(text)",
    "wxyc_identity_match_artist(text)",
    "wxyc_identity_baseline(text)",
    "wxyc_drop_articles(text)",
    "wxyc_strip_trailing_parens(text)",
    "wxyc_match_form(text)",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_db_url() -> str:
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply 0004_wxyc_identity_match_fns."
        )
    return db_url


def _refuse_offline(direction: str) -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            f"0004_wxyc_identity_match_fns does not support --sql / offline mode "
            f"({direction}): the migration opens its own autocommit psycopg connection "
            "to apply DDL (CREATE TEXT SEARCH DICTIONARY + CREATE OR REPLACE FUNCTION), "
            "so alembic's offline SQL emission cannot intercept it. Run "
            "`alembic upgrade head` (or `downgrade`) against a live DB instead."
        )


def _read_canonical_sql() -> str:
    if not _FUNCTIONS_SQL_PATH.is_file():
        raise RuntimeError(
            f"Canonical SQL not found at {_FUNCTIONS_SQL_PATH}. Vendor it from "
            "WXYC/wxyc-etl@v0.4.0 (data/wxyc_identity_match_functions.sql) and "
            "update wxyc-etl-pin.txt."
        )
    return _FUNCTIONS_SQL_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# upgrade / downgrade
# ---------------------------------------------------------------------------


def upgrade() -> None:
    _refuse_offline("upgrade")

    log = logging.getLogger("alembic.runtime.migration")
    db_url = _resolve_db_url()
    canonical = _read_canonical_sql()

    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        log.info("0004: creating unaccent extension + wxyc_unaccent dictionary")
        cur.execute(_SETUP_SQL)
        log.info("0004: applying canonical wxyc_identity_match_* function family")
        cur.execute(canonical)


def downgrade() -> None:
    _refuse_offline("downgrade")

    db_url = _resolve_db_url()
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for fn_sig in _DOWNGRADE_FUNCTIONS:
            cur.execute(f"DROP FUNCTION IF EXISTS {fn_sig}")
        cur.execute("DROP TEXT SEARCH DICTIONARY IF EXISTS wxyc_unaccent")
        # The unaccent extension is left in place; the baseline migration
        # (0001_initial) created it for f_unaccent() and we don't own its
        # lifecycle.
