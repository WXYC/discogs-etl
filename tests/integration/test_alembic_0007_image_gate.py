"""Integration tests for alembic migration 0007_wxyc_postgres_image_gate.

Covers:

1. **Positive**: against a Postgres with ``wxyc_unaccent.rules`` present at
   ``$SHAREDIR/tsearch_data/`` (the wxyc-postgres image's contract), 0007
   applies cleanly and the ``wxyc_unaccent`` dictionary works.
2. **F0000 catch-pattern**: the SQLSTATE the migration relies on is what
   Postgres actually emits when the rules file is missing. Exercised
   directly via a deliberately-nonexistent rules file name — independent
   of the actual migration's body.
3. **Static**: the migration body contains the runbook URL.
4. **Idempotence**: re-running 0007 doesn't trip "object already exists".
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = REPO_ROOT / "alembic" / "versions" / "0007_wxyc_postgres_image_gate.py"
RUNBOOK_URL = "https://github.com/WXYC/wxyc-etl/blob/main/docs/wxyc-postgres-image.md"


# ---------------------------------------------------------------------------
# Static (no DB) checks — keep these in this file alongside the integration
# tests so a future migration body change surfaces both sets of assertions
# from the same `pytest` invocation.
# ---------------------------------------------------------------------------


def test_migration_body_contains_runbook_url() -> None:
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert RUNBOOK_URL in body, (
        f"runbook URL {RUNBOOK_URL!r} missing from 0007 migration — operator "
        "error message would lose its actionable pointer. Update both the "
        "module constant and this test if the URL ever moves."
    )


def test_migration_targets_f0000_sqlstate() -> None:
    body = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "WHEN SQLSTATE 'F0000'" in body, (
        "0007 must catch SQLSTATE 'F0000' (config_file_error); the test that "
        "exercises this pattern against a real PG depends on the migration "
        "using the same SQLSTATE Postgres actually emits."
    )


# ---------------------------------------------------------------------------
# Live-PG assertions
# ---------------------------------------------------------------------------


def _run_alembic(args: list[str], db_url: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "DATABASE_URL_DISCOGS": db_url}
    env.pop("DATABASE_URL", None)
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.mark.pg
def test_f0000_sqlstate_emitted_by_missing_rules_file(db_url: str) -> None:
    """Independent of the migration: prove Postgres emits SQLSTATE F0000 when
    a text-search dictionary references a rules file that doesn't exist. If
    Postgres ever changes the SQLSTATE for this case, 0007's catch arm
    silently stops working and the migration would mask the misconfiguration.
    This test pins the upstream contract.
    """
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        # Per-test DBs in conftest don't run the baseline schema, so the
        # unaccent template isn't auto-loaded — provision it here so the
        # CREATE TEXT SEARCH DICTIONARY below can reach the F0000 path.
        cur.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
        with pytest.raises(psycopg.errors.ConfigFileError) as exc_info:
            cur.execute(
                "CREATE TEXT SEARCH DICTIONARY wxyc_nonexistent_probe ("
                "TEMPLATE = unaccent, RULES = 'deliberately_missing_for_f0000_test')"
            )
        assert exc_info.value.sqlstate == "F0000", (
            f"expected SQLSTATE F0000 (config_file_error); got {exc_info.value.sqlstate}. "
            "Update 0007's catch arm and this test together if upstream changes the code."
        )


@pytest.mark.pg
def test_0007_applies_cleanly_against_wxyc_postgres_image(db_url: str) -> None:
    """Positive: against a destination running ``ghcr.io/wxyc/wxyc-postgres``
    (the rules file is present at ``$SHAREDIR/tsearch_data/``), alembic
    upgrade through 0007 succeeds and the dictionary works end-to-end.
    """
    stamp = _run_alembic(["stamp", "0006_lookup_negative"], db_url)
    assert stamp.returncode == 0, (
        f"alembic stamp failed:\nstdout: {stamp.stdout}\nstderr: {stamp.stderr}"
    )

    result = _run_alembic(["upgrade", "0007_wxyc_postgres_image_gate"], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade 0007 failed (does the destination run wxyc-postgres "
        f"image with wxyc_unaccent.rules in $SHAREDIR/tsearch_data/?):\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_ts_dict WHERE dictname = 'wxyc_unaccent'")
        assert cur.fetchone() is not None, "wxyc_unaccent dictionary missing after 0007"
        # End-to-end: the dict actually unaccents.
        cur.execute("SELECT ts_lexize('wxyc_unaccent', 'café')")
        row = cur.fetchone()
        assert row is not None and row[0] == ["cafe"], (
            f"wxyc_unaccent('café') → {row[0]!r}, expected ['cafe']"
        )


@pytest.mark.pg
def test_0007_idempotent(fresh_db_url: str) -> None:
    """Re-running the upgrade pathway must not throw — the DROP IF EXISTS +
    CREATE pair in 0007 is the explicit guard. Verifies the alembic re-stamp
    / downgrade-then-upgrade flow stays clean.

    Uses ``fresh_db_url`` (function-scoped) so the stamp + downgrade + upgrade
    cycle exercises against a clean DB regardless of pytest's chosen test
    order. The module-scoped ``db_url`` would couple state across the other
    pg tests in this file in a way that's fragile under future reordering.
    """
    db_url = fresh_db_url
    _run_alembic(["stamp", "0006_lookup_negative"], db_url)
    first = _run_alembic(["upgrade", "0007_wxyc_postgres_image_gate"], db_url)
    assert first.returncode == 0

    # Downgrade + upgrade again — should drop the dict, recreate cleanly.
    down = _run_alembic(["downgrade", "0006_lookup_negative"], db_url)
    assert down.returncode == 0, (
        f"alembic downgrade failed:\nstdout: {down.stdout}\nstderr: {down.stderr}"
    )
    second = _run_alembic(["upgrade", "0007_wxyc_postgres_image_gate"], db_url)
    assert second.returncode == 0, (
        f"alembic upgrade (second pass) failed:\nstdout: {second.stdout}\nstderr: {second.stderr}"
    )

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_ts_dict WHERE dictname = 'wxyc_unaccent'")
        row = cur.fetchone()
        assert row is not None and row[0] == 1, (
            f"expected exactly one wxyc_unaccent dict after the cycle; got {row[0] if row else None}"
        )
