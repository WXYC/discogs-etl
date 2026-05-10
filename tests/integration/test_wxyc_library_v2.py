"""Integration tests for the v2 wxyc_library hook (E1 §4.1.1).

Validates the alembic migration ``0003_wxyc_library_v2`` and the matching
``populate_wxyc_library_v2()`` loader from ``loaders/wxyc.py``. Per the wiki
§4.1.1 amendment, this cache is schema-validation only — there is no in-repo
legacy predecessor to compare against, so the loader is verified against the
input fixture's row count rather than a parity comparator.

Marked ``pg`` because every test connects to PostgreSQL via the shared
``fresh_db_url`` / ``db_url`` fixtures from ``tests/conftest.py``.
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

from loaders.wxyc import (  # noqa: E402  (sys.path side-effect above)
    NORMALIZER_NAME,
    populate_wxyc_library_v2,
)

# ---------------------------------------------------------------------------
# Fixtures
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


@pytest.fixture()
def migrated_db(fresh_db_url: str) -> Iterator[str]:
    """Apply alembic upgrade head to a fresh DB; yield its URL."""
    result = _run_alembic(["upgrade", "head"], fresh_db_url)
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    yield fresh_db_url


_FIXTURE_ROWS = [
    (1, "Juana Molina", "DOGA", "LP", "Sonamos", "Rock"),
    (2, "Jessica Pratt", "On Your Own Love Again", "LP", "Drag City", "Rock"),
    (3, "Chuquimamani-Condori", "Edits", "CD", "self-released", "Electronic"),
    (
        4,
        "Duke Ellington & John Coltrane",
        "Duke Ellington & John Coltrane",
        "LP",
        "Impulse Records",
        "Jazz",
    ),
    (5, "Stereolab", "Aluminum Tunes", "CD", "Duophonic", "Rock"),
]


@pytest.fixture()
def library_db(tmp_path: Path) -> Path:
    """Build a tiny library.db with WXYC-representative artists.

    Uses the canonical fixture rows from the org-level CLAUDE.md.
    """
    db_path = tmp_path / "library.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE library (
                id INTEGER PRIMARY KEY,
                artist TEXT NOT NULL,
                title TEXT NOT NULL,
                format TEXT,
                label TEXT,
                genre TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO library (id, artist, title, format, label, genre) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            _FIXTURE_ROWS,
        )
        conn.commit()
    return db_path


# ---------------------------------------------------------------------------
# Migration + loader tests
# ---------------------------------------------------------------------------


@pytest.mark.pg
def test_migration_creates_wxyc_library_with_indexes(migrated_db: str) -> None:
    """Migration lands the table + b-tree + GIN trgm indexes per §3.1."""
    expected_indexes = {
        "wxyc_library_pkey",
        "wxyc_library_norm_artist_idx",
        "wxyc_library_norm_title_idx",
        "wxyc_library_artist_id_idx",
        "wxyc_library_format_id_idx",
        "wxyc_library_release_year_idx",
        "wxyc_library_norm_artist_trgm_idx",
        "wxyc_library_norm_title_trgm_idx",
    }
    with psycopg.connect(migrated_db) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = 'wxyc_library'"
        )
        present = {row[0] for row in cur.fetchall()}
    missing = expected_indexes - present
    assert not missing, f"missing indexes after migration: {missing}; present: {present}"


@pytest.mark.pg
def test_v2_loader_writes_every_fixture_row(migrated_db: str, library_db: Path) -> None:
    """Option B: every library.db row lands in wxyc_library, fully populated.

    This is the §4.1.1 verifier — fixture-based, not legacy-parity, since this
    cache is schema-validation only and has no in-repo legacy predecessor.
    Asserts:
      - row count matches the fixture
      - every fixture library_id is present
      - every row has populated norm_artist / norm_title
      - every row has snapshot_source = 'backend'
    """
    expected_ids = {row[0] for row in _FIXTURE_ROWS}

    with psycopg.connect(migrated_db) as conn:
        written = populate_wxyc_library_v2(conn, library_db, snapshot_source="backend")
        assert written == len(_FIXTURE_ROWS)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT library_id, artist_name, album_title, "
                "norm_artist, norm_title, snapshot_source "
                "FROM wxyc_library ORDER BY library_id"
            )
            rows = cur.fetchall()

    assert {r[0] for r in rows} == expected_ids
    for library_id, artist_name, album_title, norm_artist, norm_title, source in rows:
        assert artist_name, f"artist_name empty for library_id={library_id}"
        assert album_title, f"album_title empty for library_id={library_id}"
        assert norm_artist, f"norm_artist empty for library_id={library_id}"
        assert norm_title, f"norm_title empty for library_id={library_id}"
        assert source == "backend", (
            f"snapshot_source for library_id={library_id} was {source!r}, expected 'backend'"
        )


@pytest.mark.pg
def test_v2_loader_is_idempotent(migrated_db: str, library_db: Path) -> None:
    """Re-running the loader on the same library.db is a no-op (ON CONFLICT)."""
    with psycopg.connect(migrated_db) as conn:
        first = populate_wxyc_library_v2(conn, library_db)
        second = populate_wxyc_library_v2(conn, library_db)
        # Both calls report rows-attempted, not rows-inserted; idempotency is
        # observable in COUNT(*).
        assert first == second == len(_FIXTURE_ROWS)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wxyc_library")
            (count,) = cur.fetchone()
        assert count == len(_FIXTURE_ROWS)


@pytest.mark.pg
def test_v2_loader_rejects_invalid_snapshot_source(migrated_db: str, library_db: Path) -> None:
    """The CHECK constraint in §3.1 is mirrored in the loader's argument check."""
    with psycopg.connect(migrated_db) as conn:  # noqa: F841 (unused; ValueError raises before write)
        with pytest.raises(ValueError, match="snapshot_source"):
            populate_wxyc_library_v2(conn, library_db, snapshot_source="bogus")


@pytest.mark.pg
def test_normalizer_is_to_identity_match_form(migrated_db: str, library_db: Path) -> None:
    """The loader is locked onto ``wxyc_etl.text.to_identity_match_form``.

    Per plans/library-hook-canonicalization.md §3.3 / E3 step 4 — the
    cross-cache-identity hook runs on the locked-on baseline, not on any
    opt-in variant. ``NORMALIZER_NAME`` is the audit string the loader emits
    in INFO logs; this test pins both the constant and the actual normalized
    output so a future API rename is caught even if the constant drifts.
    """
    with psycopg.connect(migrated_db) as conn:
        populate_wxyc_library_v2(conn, library_db)

        # The audit string names the locked-on baseline.
        assert NORMALIZER_NAME == "wxyc_etl.text.to_identity_match_form"

        # The actual normalized values match what the canonical functions
        # produce — diacritic-fold + case-fold + collapse — so any future
        # algorithm drift in wxyc-etl is caught here.
        from wxyc_etl.text import (
            to_identity_match_form,
            to_identity_match_form_title,
        )

        with conn.cursor() as cur:
            cur.execute(
                "SELECT artist_name, album_title, label_name, "
                "norm_artist, norm_title, norm_label "
                "FROM wxyc_library WHERE library_id = 1"
            )
            artist, title, label, norm_a, norm_t, norm_l = cur.fetchone()
        assert norm_a == to_identity_match_form(artist)
        assert norm_t == to_identity_match_form_title(title)
        assert norm_l == to_identity_match_form(label)
