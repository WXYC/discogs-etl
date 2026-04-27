"""Cross-repo E2E test: entity resolution chain across LML components.

Tests the entity resolution pipeline:
1. Run discogs-cache pipeline to populate release data
2. Create entity.identity schema and seed with WXYC library artists
3. Run Discogs reconciliation against the populated cache
4. Verify identity records are updated with Discogs artist IDs
5. Verify the identity endpoint returns correct data (when available)

When sibling repo code is not available, the test falls back to direct SQL
verification of the entity schema and reconciliation logic.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"
FIXTURE_LIBRARY_DB = FIXTURES_DIR / "library.db"
RUN_PIPELINE = Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py"

ADMIN_URL = os.environ.get("DATABASE_URL_TEST", "postgresql://localhost:5433/postgres")

# Sibling repo paths
REPO_ROOT = Path(__file__).parent.parent.parent
ORG_ROOT = REPO_ROOT.parent
LML_REPO = ORG_ROOT / "library-metadata-lookup"

# Try importing entity resolution modules from LML worktree
# The entity resolution code lives in LML's entity-store worktree
HAS_ENTITY_RESOLUTION = False
_entity_store_path = LML_REPO / ".claude" / "worktrees" / "entity-store"
_identity_endpoints_path = LML_REPO / ".claude" / "worktrees" / "5c-identity-endpoints"
_entity_path = None

for candidate in [_identity_endpoints_path, _entity_store_path]:
    if (candidate / "scripts" / "entity_resolution" / "discogs.py").exists():
        _entity_path = candidate
        break

if _entity_path is not None:
    try:
        sys.path.insert(0, str(_entity_path))
        from scripts.entity_resolution.discogs import DiscogsReconciler  # noqa: F401

        HAS_ENTITY_RESOLUTION = True
        sys.path.pop(0)
    except (ImportError, ModuleNotFoundError):
        HAS_ENTITY_RESOLUTION = False

try:
    import asyncio

    import asyncpg

    HAS_ASYNCPG = True
except ImportError:
    HAS_ASYNCPG = False

# WXYC canonical example artists for entity resolution testing
WXYC_ARTISTS = [
    "Stereolab",
    "Cat Power",
    "Juana Molina",
    "Jessica Pratt",
    "Chuquimamani-Condori",
    "Duke Ellington",
    "Sessa",
    "Father John Misty",
]

# Artists that exist in the fixture CSV data and can be reconciled
FIXTURE_ARTISTS_WITH_DISCOGS = {
    "Autechre": 1,
    "Stereolab": 2,
}

# Artists that should NOT match anything in the fixture data
UNKNOWN_ARTISTS = [
    "Nonexistent Band ZZZZZ",
    "Fake Artist 12345",
]

pytestmark = pytest.mark.pg

# -- Entity schema DDL (matches wxyc-etl schema/entity.rs) --

ENTITY_SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS entity;

CREATE TABLE IF NOT EXISTS entity.identity (
    id SERIAL PRIMARY KEY,
    library_name TEXT NOT NULL UNIQUE,
    discogs_artist_id INTEGER,
    wikidata_qid TEXT,
    musicbrainz_artist_id TEXT,
    spotify_artist_id TEXT,
    apple_music_artist_id TEXT,
    bandcamp_id TEXT,
    reconciliation_status TEXT NOT NULL DEFAULT 'unreconciled',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entity.reconciliation_log (
    id SERIAL PRIMARY KEY,
    identity_id INTEGER NOT NULL REFERENCES entity.identity(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence REAL,
    method TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


@pytest.fixture(scope="class")
def e2e_db_url():
    """Create a fresh database for entity resolution E2E tests."""
    db_name = f"discogs_entity_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)

    with admin_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    base = ADMIN_URL.rsplit("/", 1)[0]
    test_url = f"{base}/{db_name}"

    yield test_url

    with admin_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = {} AND pid <> pg_backend_pid()"
            ).format(sql.Literal(db_name))
        )
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
    admin_conn.close()


class TestEntitySchemaSetup:
    """Verify entity.identity schema can be created alongside the discogs-cache schema."""

    @pytest.fixture(autouse=True, scope="class")
    def _setup_schemas(self, e2e_db_url):
        """Run pipeline then create entity schema."""
        self.__class__._db_url = e2e_db_url

        # Step 1: Run pipeline to populate discogs data
        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                e2e_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

        # Step 2: Create entity schema
        conn = psycopg.connect(e2e_db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(ENTITY_SCHEMA_DDL)
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_entity_schema_exists(self) -> None:
        """entity schema was created successfully."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name = 'entity'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None, "entity schema does not exist"

    def test_identity_table_exists(self) -> None:
        """entity.identity table was created successfully."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'entity' AND table_name = 'identity'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None, "entity.identity table does not exist"

    def test_reconciliation_log_table_exists(self) -> None:
        """entity.reconciliation_log table was created successfully."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'entity' AND table_name = 'reconciliation_log'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None, "entity.reconciliation_log table does not exist"

    def test_discogs_data_present_alongside_entity_schema(self) -> None:
        """Discogs release data coexists with entity schema."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count > 0, "release table empty after pipeline + entity schema setup"


class TestEntityReconciliationDirect:
    """Test entity reconciliation using direct SQL (no sibling repo required).

    Seeds entity.identity with test artists, then resolves them against
    the release_artist table using the same SQL cascade that the LML
    DiscogsReconciler uses.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _setup(self, e2e_db_url):
        """Run pipeline, create entity schema, seed artists."""
        self.__class__._db_url = e2e_db_url

        # Run pipeline
        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                e2e_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

        # Create entity schema
        conn = psycopg.connect(e2e_db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(ENTITY_SCHEMA_DDL)

        # Seed entity.identity with test artists
        all_artists = list(FIXTURE_ARTISTS_WITH_DISCOGS.keys()) + UNKNOWN_ARTISTS
        with conn.cursor() as cur:
            for artist in all_artists:
                cur.execute(
                    "INSERT INTO entity.identity (library_name) VALUES (%s) "
                    "ON CONFLICT (library_name) DO NOTHING",
                    (artist,),
                )

        # Run direct SQL reconciliation (exact match against release_artist)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE entity.identity ei
                SET discogs_artist_id = sub.artist_id,
                    reconciliation_status = 'reconciled',
                    updated_at = now()
                FROM (
                    SELECT DISTINCT ON (lower(ra.artist_name))
                        lower(ra.artist_name) AS artist_name_lower,
                        ra.artist_id
                    FROM release_artist ra
                    WHERE ra.extra = 0
                      AND ra.artist_id IS NOT NULL
                ) sub
                WHERE lower(ei.library_name) = sub.artist_name_lower
                  AND ei.discogs_artist_id IS NULL
            """)

            # Log the reconciliations
            cur.execute("""
                INSERT INTO entity.reconciliation_log
                    (identity_id, source, external_id, confidence, method)
                SELECT ei.id, 'discogs', ei.discogs_artist_id::text, 1.0, 'exact_match'
                FROM entity.identity ei
                WHERE ei.reconciliation_status = 'reconciled'
            """)

        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_known_artists_resolved(self) -> None:
        """Artists in fixture data resolve to Discogs artist IDs."""
        conn = self._connect()
        for artist_name, expected_id in FIXTURE_ARTISTS_WITH_DISCOGS.items():
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT discogs_artist_id, reconciliation_status "
                    "FROM entity.identity WHERE library_name = %s",
                    (artist_name,),
                )
                row = cur.fetchone()
            assert row is not None, f"No identity record for {artist_name}"
            assert row[0] == expected_id, (
                f"{artist_name}: expected discogs_artist_id={expected_id}, got {row[0]}"
            )
            assert row[1] == "reconciled", (
                f"{artist_name}: expected status='reconciled', got '{row[1]}'"
            )
        conn.close()

    def test_unknown_artists_not_resolved(self) -> None:
        """Artists not in fixture data remain unreconciled."""
        conn = self._connect()
        for artist_name in UNKNOWN_ARTISTS:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT discogs_artist_id, reconciliation_status "
                    "FROM entity.identity WHERE library_name = %s",
                    (artist_name,),
                )
                row = cur.fetchone()
            assert row is not None, f"No identity record for {artist_name}"
            assert row[0] is None, f"{artist_name}: should have no discogs_artist_id, got {row[0]}"
            assert row[1] == "unreconciled", (
                f"{artist_name}: expected status='unreconciled', got '{row[1]}'"
            )
        conn.close()

    def test_reconciliation_log_populated(self) -> None:
        """Reconciliation log has entries for resolved artists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*) FROM entity.reconciliation_log
                WHERE source = 'discogs' AND method = 'exact_match'
            """)
            count = cur.fetchone()[0]
        conn.close()
        assert count == len(FIXTURE_ARTISTS_WITH_DISCOGS), (
            f"Expected {len(FIXTURE_ARTISTS_WITH_DISCOGS)} reconciliation log entries, got {count}"
        )

    def test_identity_unique_constraint(self) -> None:
        """entity.identity enforces unique library_name."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.UniqueViolation):
                cur.execute(
                    "INSERT INTO entity.identity (library_name) VALUES (%s)",
                    ("Autechre",),
                )
        conn.close()


class TestEntityReconciliationCrossRepo:
    """Test entity reconciliation using LML's DiscogsReconciler (cross-repo).

    Skips when the entity resolution code is not available in the LML repo.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _setup(self, e2e_db_url):
        if not HAS_ENTITY_RESOLUTION:
            pytest.skip("Entity resolution code not available in library-metadata-lookup")
        if not HAS_ASYNCPG:
            pytest.skip("asyncpg not available")

        self.__class__._db_url = e2e_db_url

        # Run pipeline
        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                "--csv-dir",
                str(CSV_DIR),
                "--library-db",
                str(FIXTURE_LIBRARY_DB),
                "--database-url",
                e2e_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

        # Create entity schema
        conn = psycopg.connect(e2e_db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(ENTITY_SCHEMA_DDL)
        conn.close()

        # Run DiscogsReconciler via asyncpg
        async def _reconcile():
            pool = await asyncpg.create_pool(e2e_db_url)
            try:
                reconciler = DiscogsReconciler(pool)
                test_names = list(FIXTURE_ARTISTS_WITH_DISCOGS.keys()) + UNKNOWN_ARTISTS
                results = await reconciler.reconcile_batch(test_names)
                self.__class__._results = results
            finally:
                await pool.close()

        asyncio.get_event_loop().run_until_complete(_reconcile())

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_known_artists_matched(self) -> None:
        """DiscogsReconciler resolves known fixture artists."""
        results = self.__class__._results
        for artist_name, expected_id in FIXTURE_ARTISTS_WITH_DISCOGS.items():
            lower_name = artist_name.lower()
            assert lower_name in results, f"{artist_name} not in reconciliation results"
            assert results[lower_name].discogs_artist_id == expected_id, (
                f"{artist_name}: expected ID {expected_id}, "
                f"got {results[lower_name].discogs_artist_id}"
            )

    def test_unknown_artists_not_matched(self) -> None:
        """DiscogsReconciler returns no match for unknown artists."""
        results = self.__class__._results
        for artist_name in UNKNOWN_ARTISTS:
            lower_name = artist_name.lower()
            assert lower_name not in results, (
                f"{artist_name} should not have matched, got {results.get(lower_name)}"
            )
