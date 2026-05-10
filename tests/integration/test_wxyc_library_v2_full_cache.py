"""Integration tests for the §4.1.4 (full Homebrew Discogs cache) helpers.

Two operator helpers ship with E1 §4.1.4:

- ``scripts/wxyc_library_parity_check.py`` — extended parity query over
  ``wxyc_release_match`` vs ``wxyc_library``, with auto-fallback when
  ``wxyc_norm_artist()`` is not yet deployed.
- ``scripts/wxyc_library_explain_analyze.py`` — pre-cutover plan
  verification harness that runs the top-5 LML query patterns.

Both work against any cache configured via ``DATABASE_URL_DISCOGS`` (the
Docker dev cache or the full Homebrew cache; the loader and migration are
identical between the two — see PR #185 / E1 §4.1.1). These tests verify
the helpers behave correctly against a fixture-loaded DB.

Marked ``pg`` because every test connects to PostgreSQL via the shared
``fresh_db_url`` fixture.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from loaders.wxyc import populate_wxyc_library_v2  # noqa: E402
from scripts.wxyc_library_explain_analyze import (  # noqa: E402
    QUERY_PATTERNS,
    run_explain,
)
from scripts.wxyc_library_parity_check import run_parity_check  # noqa: E402


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


@pytest.fixture()
def migrated_db(fresh_db_url: str) -> Iterator[str]:
    result = _run_alembic(["upgrade", "head"], fresh_db_url)
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    yield fresh_db_url


@pytest.fixture()
def populated_db(migrated_db: str, tmp_path: Path) -> str:
    """Migrated DB with a small fixture loaded into ``wxyc_library``."""
    library_db = tmp_path / "library.db"
    with sqlite3.connect(library_db) as conn:
        conn.execute(
            "CREATE TABLE library (id INTEGER PRIMARY KEY, artist TEXT, title TEXT, format TEXT)"
        )
        conn.executemany(
            "INSERT INTO library (id, artist, title, format) VALUES (?, ?, ?, ?)",
            [
                (1, "Juana Molina", "DOGA", "LP"),
                (2, "Jessica Pratt", "On Your Own Love Again", "LP"),
                (3, "Stereolab", "Aluminum Tunes", "CD"),
            ],
        )
        conn.commit()
    with psycopg.connect(migrated_db) as conn:
        populate_wxyc_library_v2(conn, library_db)
    return migrated_db


# ---------------------------------------------------------------------------
# Parity-check helper
# ---------------------------------------------------------------------------


@pytest.mark.pg
def test_parity_check_reports_unavailable_without_legacy_table(populated_db: str) -> None:
    """Without ``wxyc_release_match``, parity check returns mode=unavailable.

    The Docker dev cache and any test fixture lack the legacy two-table
    model (it's loaded out-of-band on the prod full cache only). The
    helper must not crash.
    """
    result = run_parity_check(populated_db)
    assert result.mode == "unavailable"
    assert result.unmatched_legacy_rows is None
    assert "wxyc_release_match" in (result.note or "")


@pytest.mark.pg
def test_parity_check_legacy_text_fallback(populated_db: str) -> None:
    """When ``wxyc_release_match`` exists but ``wxyc_norm_artist()`` does not,
    the helper falls back to legacy-text comparison per §4.1.4 sequencing."""
    with psycopg.connect(populated_db) as conn, conn.cursor() as cur:
        # Synthesize a minimal wxyc_release_match table so the parity query
        # has something to LEFT JOIN against. Real prod table has ~4.3 M
        # rows; we only need a handful to exercise the join shape.
        cur.execute(
            """
            CREATE TABLE wxyc_release_match (
                release_id     INTEGER,
                discogs_artist TEXT,
                norm_title     TEXT,
                title          TEXT
            )
            """
        )
        cur.executemany(
            "INSERT INTO wxyc_release_match (release_id, discogs_artist, norm_title, title) "
            "VALUES (%s, %s, %s, %s)",
            [
                (101, "Juana Molina", "doga", "DOGA"),
                (102, "Jessica Pratt", "on your own love again", "On Your Own Love Again"),
                # An artist whose stored row name doesn't exactly match the
                # ``wxyc_library.artist_name`` value — exercises the
                # unmatched-rows count.
                (103, "Some Other Artist", "something", "Something"),
            ],
        )
        conn.commit()

    result = run_parity_check(populated_db)
    assert result.mode == "legacy_text", (
        f"expected legacy_text fallback (no wxyc_norm_artist), got {result.mode}"
    )
    assert result.legacy_artists == 3
    assert result.new_artists is not None and result.new_artists >= 3
    # Two of the three legacy rows match by exact text; "Some Other Artist"
    # has no corresponding ``wxyc_library`` row, so unmatched == 1.
    assert result.unmatched_legacy_rows == 1


# ---------------------------------------------------------------------------
# EXPLAIN ANALYZE harness
# ---------------------------------------------------------------------------


@pytest.mark.pg
def test_explain_harness_runs_all_query_patterns(populated_db: str) -> None:
    """Every pattern executes without error; plan is captured.

    We don't pin specific node types (Postgres versions / row counts may
    legitimately change them) — only that each pattern produced a plan
    object and a wall time. The §4.1.4 gate (no seq scan, p95 within 1.5×)
    is operator-evaluated against the prod cache, not asserted in CI.
    """
    results = run_explain(populated_db)

    assert len(results) == len(QUERY_PATTERNS), (
        f"expected {len(QUERY_PATTERNS)} results, got {len(results)}"
    )

    for r in results:
        assert r.error is None, f"{r.name} raised: {r.error}"
        assert r.plan is not None, f"{r.name} produced no plan"
        assert r.elapsed_ms >= 0
        # Summary should at least carry a Node Type for the root.
        assert r.summary.get("node_type"), f"{r.name} summary missing node_type: {r.summary}"


@pytest.mark.pg
def test_explain_harness_query_patterns_cover_documented_origins() -> None:
    """The hard-coded query inventory documents an ``origin`` per query.

    Catches the case where someone adds a query without filling in the
    ``origin`` field — §4.1.4 wants traceability back to the LML code path
    that motivates the query.
    """
    expected_names = {
        "exact_norm_artist",
        "trgm_norm_artist",
        "exact_norm_title",
        "trgm_norm_title",
        "composite_artist_title",
    }
    actual_names = {p.name for p in QUERY_PATTERNS}
    assert actual_names == expected_names

    for p in QUERY_PATTERNS:
        assert p.origin and len(p.origin) > 5, f"{p.name}: origin missing"
        assert p.expected_index, f"{p.name}: expected_index missing"


# ---------------------------------------------------------------------------
# Loader parity for the full-cache deploy path
# ---------------------------------------------------------------------------


@pytest.mark.pg
def test_loader_works_against_arbitrary_database_url(migrated_db: str, tmp_path: Path) -> None:
    """``populate_wxyc_library_v2`` is database-agnostic: the same loader code
    works against the Docker dev cache (port 5433) and the full Homebrew cache
    (port 5432). The integration test in ``test_wxyc_library_v2.py`` already
    exercises the dev path; this test pins the contract that the function is
    parameterized purely by its ``pg_conn`` argument and carries no port- or
    DB-name-specific behaviour.

    The proof is structural: the function takes a ``psycopg.Connection`` and
    a ``Path`` to library.db. Whatever URL produced ``pg_conn`` is opaque.
    A successful run against ``migrated_db`` here — combined with the same
    function being driven against ``DATABASE_URL_DISCOGS`` in production
    rebuild-cache.sh — is the parity assertion §4.1.4 wants.
    """
    library_db = tmp_path / "library.db"
    with sqlite3.connect(library_db) as conn:
        conn.execute("CREATE TABLE library (id INTEGER PRIMARY KEY, artist TEXT, title TEXT)")
        # Diacritic-bearing canonical name — exercises the same identity-fold
        # path the prod-shape data will exercise, from wxycCanonicalArtistNames.
        conn.execute("INSERT INTO library VALUES (?, ?, ?)", (1, "Csillagrablók", "Eszter"))
        conn.commit()

    with psycopg.connect(migrated_db) as conn:
        written = populate_wxyc_library_v2(conn, library_db)
        assert written == 1
        with conn.cursor() as cur:
            cur.execute("SELECT norm_artist FROM wxyc_library WHERE library_id = 1")
            (norm_artist,) = cur.fetchone()
        # Diacritic must fold; this is the prod-relevant invariant.
        assert "ó" not in norm_artist
        assert norm_artist == norm_artist.lower()
