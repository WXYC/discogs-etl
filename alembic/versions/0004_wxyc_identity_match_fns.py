"""wxyc_identity_match_* plpgsql function family (wiki §3.3.5)

Deploys the four cross-cache-identity match functions onto the discogs-cache
PG instance. Vendored byte-for-byte from WXYC/wxyc-etl@v0.4.0
(``data/wxyc_identity_match_functions.sql``); SHA-pinned at repo root in
``wxyc-etl-pin.txt``.

| Postgres function | Rust counterpart |
|---|---|
| wxyc_identity_match_artist                       | to_identity_match_form |
| wxyc_identity_match_title                        | to_identity_match_form_title |
| wxyc_identity_match_with_punctuation             | to_identity_match_form_with_punctuation |
| wxyc_identity_match_with_disambiguator_strip     | to_identity_match_form_with_disambiguator_strip |

Unaccent provisioning — function-based (discogs-etl#223)
--------------------------------------------------------

The vendored ``wxyc_match_form`` pipeline originally invoked
``unaccent('wxyc_unaccent', r)``, which required a Postgres text-search
dictionary backed by ``$SHAREDIR/tsearch_data/wxyc_unaccent.rules``. On
Railway-managed Postgres ``$SHAREDIR`` is root-owned and the postgres OS
user has no write permission, so the dictionary create step failed with
``ConfigFileError`` regardless of role grants — see #223 for the probe.

This migration now deploys a pure-SQL function ``wxyc_unaccent_text(text)``
that bakes the rules from ``vendor/wxyc-etl/wxyc_unaccent.rules`` directly
into Postgres (via ``translate()`` for 1-char rules and a small ``REPLACE``
chain for the 16 multi-char-destination rules). The vendored canonical SQL
is patched at apply time to call ``wxyc_unaccent_text(r)`` instead — the
file on disk stays SHA-pinned for the parity test; only the deployed
function body diverges. Codegen + substitution helpers live at
``lib/unaccent_codegen.py``; the two-pass equivalence invariant is asserted
there (unit tests at ``tests/unit/test_wxyc_unaccent_function_codegen.py``).

Defensive pattern: this revision uses the same ``is_offline_mode()`` refuse
and ``psycopg.connect(..., autocommit=True)`` side-channel as
0001_initial / 0002_backfill_trigram_indexes / 0003_wxyc_library_v2 so that
``alembic upgrade --sql`` (offline mode) fails fast rather than silently
emitting no-op SQL. The functions use ``CREATE OR REPLACE FUNCTION``, so
re-running is a no-op. Downgrade defensively drops both the new
``wxyc_unaccent_text`` function *and* the old ``wxyc_unaccent`` text-search
dictionary (``IF EXISTS``) so dev/EC2 systems that previously applied the
dict-based 0004 downgrade cleanly via ``alembic downgrade 0003``.

Column flip on indexes + cache-load expressions ships in this PR but lands
outside the alembic migration itself — see ``schema/create_indexes.sql`` and
``scripts/run_pipeline.py`` changes. The migration alone is restartable; the
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
from lib import unaccent_codegen

# revision identifiers, used by Alembic.
revision: str = "0004_wxyc_identity_match_fns"
down_revision: str | Sequence[str] | None = "0003_wxyc_library_v2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# Vendored canonical paths
# ---------------------------------------------------------------------------

# Loaded at migration time rather than embedded in this module so the
# byte-for-byte parity check in
# ``tests/integration/test_wxyc_identity_match_parity.py`` can read the same
# files and assert the deploy matches upstream.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_FUNCTIONS_SQL_PATH = _REPO_ROOT / "vendor" / "wxyc-etl" / "wxyc_identity_match_functions.sql"
_RULES_PATH = _REPO_ROOT / "vendor" / "wxyc-etl" / "wxyc_unaccent.rules"

# The ``unaccent`` extension is left as a hard prerequisite — the baseline
# migration (0001_initial) creates it for ``f_unaccent()``. We just make sure
# it's present so a downstream caller that still uses the extension's
# bare ``unaccent(text)`` form keeps working.
_PREP_SQL = "CREATE EXTENSION IF NOT EXISTS unaccent;\n"

_DOWNGRADE_FUNCTIONS = [
    "wxyc_identity_match_with_disambiguator_strip(text)",
    "wxyc_identity_match_with_punctuation(text)",
    "wxyc_identity_match_title(text)",
    "wxyc_identity_match_artist(text)",
    "wxyc_identity_baseline(text)",
    "wxyc_drop_articles(text)",
    "wxyc_strip_trailing_parens(text)",
    "wxyc_match_form(text)",
    "wxyc_unaccent_text(text)",
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
            "to apply DDL (CREATE OR REPLACE FUNCTION), so alembic's offline SQL "
            "emission cannot intercept it. Run `alembic upgrade head` (or "
            "`downgrade`) against a live DB instead."
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
    function_sql = unaccent_codegen.build_unaccent_function_sql(_RULES_PATH)
    patched_canonical = unaccent_codegen.patch_canonical_sql(canonical)

    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        log.info("0004: ensuring unaccent extension")
        cur.execute(_PREP_SQL)
        log.info("0004: applying wxyc_unaccent_text(text) function (codegen from vendored rules)")
        cur.execute(function_sql)
        log.info("0004: applying canonical wxyc_identity_match_* function family (patched)")
        cur.execute(patched_canonical)


def downgrade() -> None:
    _refuse_offline("downgrade")

    db_url = _resolve_db_url()
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for fn_sig in _DOWNGRADE_FUNCTIONS:
            cur.execute(f"DROP FUNCTION IF EXISTS {fn_sig}")
        # Defensive: dev/EC2 systems that previously applied the dict-based
        # 0004 have a ``wxyc_unaccent`` text-search dictionary that fresh
        # function-based deploys never create. ``IF EXISTS`` makes this a
        # no-op on those fresh deploys.
        cur.execute("DROP TEXT SEARCH DICTIONARY IF EXISTS wxyc_unaccent")
        # The unaccent extension is left in place; the baseline migration
        # (0001_initial) created it for f_unaccent() and we don't own its
        # lifecycle.
