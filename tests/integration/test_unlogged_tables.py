"""Integration tests for UNLOGGED/LOGGED table conversion during pipeline.

Verifies that set_tables_unlogged() and set_tables_logged() correctly change
table persistence via pg_class.relpersistence ('p' = LOGGED, 'u' = UNLOGGED).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

# Load run_pipeline as a module (it's a script, not a package).
_spec = importlib.util.spec_from_file_location(
    "run_pipeline",
    Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py",
)
run_pipeline = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_pipeline)

pytestmark = pytest.mark.postgres


def _get_table_persistence(db_url: str, table_name: str) -> str:
    """Return relpersistence for a table ('p' = LOGGED, 'u' = UNLOGGED, 't' = temp)."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT relpersistence FROM pg_class WHERE relname = %s",
            (table_name,),
        )
        result = cur.fetchone()
    conn.close()
    assert result is not None, f"Table {table_name} not found"
    return result[0]


class TestSetTablesUnlogged:
    """set_tables_unlogged() converts LOGGED tables to UNLOGGED."""

    @pytest.fixture(autouse=True)
    def _apply_schema(self, db_url):
        self.db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

    def test_tables_start_logged_then_become_unlogged(self) -> None:
        """Schema creates LOGGED tables; set_tables_unlogged converts to UNLOGGED."""
        for table in run_pipeline.PIPELINE_TABLES:
            assert _get_table_persistence(self.db_url, table) == "p", (
                f"Table {table} should start as LOGGED"
            )
        run_pipeline.set_tables_unlogged(self.db_url)
        for table in run_pipeline.PIPELINE_TABLES:
            assert _get_table_persistence(self.db_url, table) == "u", (
                f"Table {table} should be UNLOGGED"
            )


class TestSetTablesLogged:
    """set_tables_logged() converts UNLOGGED tables back to LOGGED."""

    @pytest.fixture(autouse=True)
    def _apply_schema_and_set_unlogged(self, db_url):
        self.db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()
        run_pipeline.set_tables_unlogged(db_url)

    def test_tables_become_logged(self) -> None:
        """All pipeline tables have relpersistence = 'p' after set_tables_logged."""
        run_pipeline.set_tables_logged(self.db_url)
        for table in run_pipeline.PIPELINE_TABLES:
            assert _get_table_persistence(self.db_url, table) == "p", (
                f"Table {table} should be LOGGED"
            )

    def test_idempotent_on_already_logged(self) -> None:
        """set_tables_logged on already-LOGGED tables does not error."""
        run_pipeline.set_tables_logged(self.db_url)
        # Call again on already-LOGGED tables
        run_pipeline.set_tables_logged(self.db_url)
        for table in run_pipeline.PIPELINE_TABLES:
            assert _get_table_persistence(self.db_url, table) == "p"
