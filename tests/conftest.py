"""Shared Postgres fixtures for integration and E2E tests.

The fixtures create a temporary test database, run the schema, and clean up
on teardown.  Tests that need Postgres should use the ``db_url`` or ``db_conn``
fixtures and be marked with ``@pytest.mark.pg``.

Connection target:
    DATABASE_URL_TEST env var, default ``postgresql://localhost:5433/postgres``.

The default port 5433 matches the docker-compose.yml mapping so tests can
run against the containerised database out of the box.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest

# Try importing psycopg; if missing the fixtures will skip gracefully.
try:
    import psycopg
    from psycopg import sql

    HAS_PSYCOPG = True
except ImportError:
    HAS_PSYCOPG = False


ADMIN_URL = os.environ.get("DATABASE_URL_TEST", "postgresql://localhost:5433/postgres")


def _postgres_available() -> bool:
    """Return True if we can connect to the test Postgres instance."""
    if not HAS_PSYCOPG:
        return False
    try:
        conn = psycopg.connect(ADMIN_URL, connect_timeout=3, autocommit=True)
        conn.close()
        return True
    except Exception:
        return False


def _ephemeral_database() -> Iterator[str]:
    """Generator body for a temp-DB fixture.

    Wrap with ``@pytest.fixture(scope=...)`` to get module- or function-scoped
    isolation. Skips the test if Postgres is unreachable.
    """
    if not _postgres_available():
        pytest.skip("PostgreSQL not available (set DATABASE_URL_TEST)")

    db_name = f"discogs_test_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)

    with admin_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    # Build the test database URL by replacing the database name in the admin URL.
    # Handle both postgresql://host/db and postgresql://user:pass@host:port/db forms.
    base = ADMIN_URL.rsplit("/", 1)[0]
    test_url = f"{base}/{db_name}"

    try:
        yield test_url
    finally:
        # Teardown: drop the test database. Force-disconnect any remaining
        # connections first.
        with admin_conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = {} AND pid <> pg_backend_pid()"
                ).format(sql.Literal(db_name))
            )
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
        admin_conn.close()


@pytest.fixture(scope="module")
def db_url() -> Iterator[str]:
    """Module-scoped temp DB shared across all tests in a module.

    Cheap when the tests don't write to the DB (or all do equivalent setup),
    but tests that mutate state can leak across each other in undefined
    pytest execution order. Use ``fresh_db_url`` instead when each test
    needs a guaranteed-clean DB.
    """
    yield from _ephemeral_database()


@pytest.fixture()
def fresh_db_url() -> Iterator[str]:
    """Function-scoped temp DB — one fresh database per test.

    Use this for tests that mutate schema/version state in ways that would
    leak across the module (e.g. alembic migrations applied in different
    sequences). Slower than ``db_url`` because each test pays the create+drop
    cost, but worth it when correctness depends on a clean slate.
    """
    yield from _ephemeral_database()


@pytest.fixture()
def db_conn(db_url):
    """Provide a connection to the test database with rollback on teardown."""
    conn = psycopg.connect(db_url)
    yield conn
    conn.rollback()
    conn.close()
