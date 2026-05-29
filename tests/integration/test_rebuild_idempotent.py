"""Pipeline-level rebuild semantics for WXYC/discogs-etl#242.

Tests 6 and 7 from the Option-B plan: confirm that ``--truncate-existing``
and ``--fresh-rebuild`` retain their explicit-wipe semantics when invoked
through the pipeline-level seams. Companion to
``TestImportArtworkPreservation`` in ``test_import.py``, which covers the
default-incremental path.

These are heavier than the test_import.py tests (they exercise the schema
DDL + run subprocesses or apply SQL files), so they live in their own file
with a slightly higher per-test cost budget.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

pytestmark = pytest.mark.pg

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = REPO_ROOT / "schema"
SCRIPT_DIR = REPO_ROOT / "scripts"

# Load run_pipeline so we can call its private helpers without spawning a
# subprocess.  _run_database_build is the seam --fresh-rebuild affects.
_RP_SPEC = importlib.util.spec_from_file_location("run_pipeline", SCRIPT_DIR / "run_pipeline.py")
assert _RP_SPEC is not None and _RP_SPEC.loader is not None
run_pipeline = importlib.util.module_from_spec(_RP_SPEC)
_RP_SPEC.loader.exec_module(run_pipeline)


def _apply_schema(db_url: str) -> None:
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    conn.close()


def _seed_back_patched_release(db_url: str, release_id: int) -> None:
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO release (id, title, artwork_url, artwork_checked_at) "
            "VALUES (%s, 'Seed', 'lml-backpatched', '2026-04-01 00:00:00+00')",
            (release_id,),
        )
    conn.commit()
    conn.close()


def _read_artwork(db_url: str, release_id: int) -> tuple:
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT artwork_url, artwork_checked_at FROM release WHERE id = %s",
            (release_id,),
        )
        row = cur.fetchone()
    conn.close()
    return row


class TestTruncateExistingWipesArtwork:
    """--truncate-existing is the operator-visible escape hatch for "wipe
    the cache data". After Option B the default incremental path
    preserves artwork; --truncate-existing must NOT — otherwise the
    flag's contract changes and operators using it for stale-data
    recovery get unexpected behavior."""

    def test_truncate_existing_wipes_back_patched_artwork(self, fresh_db_url, tmp_path) -> None:
        _apply_schema(fresh_db_url)
        _seed_back_patched_release(fresh_db_url, release_id=601)

        # Fixture: release.csv contains 601 with no row in release_image.csv.
        (tmp_path / "release.csv").write_text(
            "id,title,country,released,format,master_id\n601,Seed,US,2024,LP,\n"
        )
        (tmp_path / "release_image.csv").write_text("release_id,type,width,height,uri\n")
        # Empty children CSVs so --base-only's child COPY succeeds.
        for child in (
            "release_artist",
            "release_label",
            "release_genre",
            "release_style",
        ):
            (tmp_path / f"{child}.csv").write_text("")

        env = {
            **os.environ,
            "DATABASE_URL_DISCOGS": fresh_db_url,
            "DATABASE_URL_TEST": fresh_db_url,
        }
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "import_csv.py"),
                "--base-only",
                "--truncate-existing",
                str(tmp_path),
                fresh_db_url,
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

        url, checked_at = _read_artwork(fresh_db_url, 601)
        # --truncate-existing wiped the row; the re-COPY put 601 back with
        # NULL artwork (no row in release_image.csv → import_artwork did
        # not stamp it).
        assert url is None, (
            "--truncate-existing must wipe artwork to preserve its 'reset "
            "everything' contract. Preserving back-patches here would "
            "break operators using the flag to recover from stale rows."
        )
        assert checked_at is None


class TestFreshRebuildDropsAndRecreates:
    """--fresh-rebuild applies schema/drop_core_tables.sql before
    create_database.sql, restoring today's drop+recreate semantics.
    Anchors operator intent: "I want a from-scratch rebuild" stays
    available as an explicit flag, even though the default path now
    preserves artwork."""

    def test_fresh_rebuild_drops_release_table_and_artwork(self, fresh_db_url) -> None:
        _apply_schema(fresh_db_url)
        _seed_back_patched_release(fresh_db_url, release_id=701)

        # Apply drop_core_tables.sql + create_database.sql the way
        # _run_database_build does when fresh_rebuild=True.
        run_pipeline.run_sql_file(fresh_db_url, SCHEMA_DIR / "drop_core_tables.sql")
        run_pipeline.run_sql_file(fresh_db_url, SCHEMA_DIR / "create_functions.sql")
        run_pipeline.run_sql_file(fresh_db_url, SCHEMA_DIR / "create_database.sql")

        # release table now empty (recreated). 701's back-patch is gone.
        conn = psycopg.connect(fresh_db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_fresh_rebuild_preserves_alembic_version(self, fresh_db_url) -> None:
        """drop_core_tables.sql intentionally does NOT touch
        alembic_version — migration history must survive a fresh
        rebuild. Pin so a future edit to drop_core_tables.sql doesn't
        silently drop the table and leave the cache un-stamped on the
        next workflow run."""
        _apply_schema(fresh_db_url)
        # Stand in for the alembic_version table the workflow's stamping
        # step creates. drop_core_tables.sql must not touch it.
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE alembic_version (version_num varchar(32) NOT NULL PRIMARY KEY)"
            )
            cur.execute("INSERT INTO alembic_version (version_num) VALUES ('0008')")
        conn.close()

        run_pipeline.run_sql_file(fresh_db_url, SCHEMA_DIR / "drop_core_tables.sql")
        run_pipeline.run_sql_file(fresh_db_url, SCHEMA_DIR / "create_functions.sql")
        run_pipeline.run_sql_file(fresh_db_url, SCHEMA_DIR / "create_database.sql")

        conn = psycopg.connect(fresh_db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT version_num FROM alembic_version")
            version = cur.fetchone()[0]
        conn.close()
        assert version == "0008"
