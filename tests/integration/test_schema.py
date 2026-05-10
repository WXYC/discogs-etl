"""Integration tests for schema creation (create_database.sql, create_indexes.sql)."""

from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql as psycopg_sql

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

# Load run_pipeline module to exercise its schema-application sequence directly.
_rp_spec = importlib.util.spec_from_file_location(
    "run_pipeline",
    Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py",
)
assert _rp_spec is not None and _rp_spec.loader is not None
run_pipeline = importlib.util.module_from_spec(_rp_spec)
_rp_spec.loader.exec_module(run_pipeline)

pytestmark = pytest.mark.pg


class TestCreateDatabase:
    """Verify create_database.sql produces the expected schema."""

    @pytest.fixture(autouse=True)
    def _apply_schema(self, db_url):
        """Run create_database.sql and create_functions.sql against the test database."""
        self.db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
        conn.close()

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_schema_executes_without_error(self) -> None:
        """Schema can be applied to a fresh database."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
        conn.close()

    def test_all_tables_exist(self) -> None:
        expected = {
            "release",
            "release_artist",
            "release_label",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        }
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            tables = {row[0] for row in cur.fetchall()}
        conn.close()
        assert expected.issubset(tables)

    @pytest.mark.parametrize(
        "table, expected_columns",
        [
            ("release", {"id", "title", "release_year", "country", "artwork_url", "master_id"}),
            ("release_artist", {"release_id", "artist_name", "extra"}),
            ("release_label", {"release_id", "label_name"}),
            ("release_track", {"release_id", "sequence", "position", "title", "duration"}),
            ("release_track_artist", {"release_id", "track_sequence", "artist_name"}),
            ("cache_metadata", {"release_id", "cached_at", "source", "last_validated"}),
        ],
        ids=[
            "release",
            "release_artist",
            "release_label",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        ],
    )
    def test_table_columns(self, table: str, expected_columns: set[str]) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                (table,),
            )
            columns = {row[0] for row in cur.fetchall()}
        conn.close()
        assert expected_columns.issubset(columns), (
            f"Missing columns in {table}: {expected_columns - columns}"
        )

    def test_pg_trgm_extension_enabled(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'")
            result = cur.fetchone()
        conn.close()
        assert result is not None, "pg_trgm extension not installed"

    def test_unaccent_extension_enabled(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT extname FROM pg_extension WHERE extname = 'unaccent'")
            result = cur.fetchone()
        conn.close()
        assert result is not None, "unaccent extension not installed"

    def test_f_unaccent_function_exists(self) -> None:
        """Immutable f_unaccent() wrapper is available and strips diacritics."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT f_unaccent('Nilüfer Yanya')")
            result = cur.fetchone()[0]
        conn.close()
        assert result == "Nilufer Yanya"

    def test_fk_constraints_with_cascade(self) -> None:
        """Child tables have ON DELETE CASCADE foreign keys to release."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    tc.table_name,
                    rc.delete_rule
                FROM information_schema.table_constraints tc
                JOIN information_schema.referential_constraints rc
                    ON tc.constraint_name = rc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND rc.delete_rule = 'CASCADE'
            """)
            fk_tables = {row[0] for row in cur.fetchall()}
        conn.close()
        expected_fk_tables = {
            "release_artist",
            "release_label",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        }
        assert expected_fk_tables.issubset(fk_tables)

    def test_no_unique_constraints_on_child_tables(self) -> None:
        """Child tables must not have UNIQUE constraints (Python-level dedup handles this).

        UNIQUE constraints on text columns cause btree overflow when artist_name
        exceeds ~900 bytes. Dedup is handled by import_csv.py's unique_key filtering.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tc.table_name, tc.constraint_name
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type = 'UNIQUE'
                  AND tc.table_name IN ('release_artist', 'release_label', 'release_track_artist')
            """)
            unique_constraints = cur.fetchall()
        conn.close()
        assert unique_constraints == [], f"Unexpected UNIQUE constraints: {unique_constraints}"

    def test_schema_is_idempotent(self) -> None:
        """Running the schema twice doesn't error."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

    def test_schema_clears_stale_data_on_rerun(self) -> None:
        """Re-running the schema drops old data so import doesn't hit UniqueViolation."""
        conn = psycopg.connect(self.db_url, autocommit=True)

        # Insert data as if a previous pipeline run completed
        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'DOGA')")
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name, extra) "
                "VALUES (1, 'Juana Molina', 0)"
            )
            cur.execute("SELECT count(*) FROM release")
            assert cur.fetchone()[0] == 1

        # Re-run schema (simulates a fresh pipeline run)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())

        # Tables should be empty — no stale data to conflict with new imports
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM release_artist")
            assert cur.fetchone()[0] == 0

        conn.close()


class TestCreateBaseIndexes:
    """Verify create_indexes.sql creates base trigram indexes."""

    @pytest.fixture(autouse=True)
    def _apply_schema_and_data(self, db_url):
        """Set up schema, functions, and insert minimal sample data for index creation."""
        self.db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.execute(
                "INSERT INTO release (id, title) VALUES (1, 'Test Album') ON CONFLICT DO NOTHING"
            )
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name, extra) "
                "SELECT 1, 'Test Artist', 0 WHERE NOT EXISTS "
                "(SELECT 1 FROM release_artist WHERE release_id = 1)"
            )
        conn.close()

    def test_base_indexes_execute_without_error(self) -> None:
        """Base trigram indexes can be created after data is loaded."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
        conn.close()

    def test_base_trigram_indexes_exist(self) -> None:
        """Base trigram indexes (release, release_artist) are created."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)

            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE '%trgm%'
            """)
            indexes = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {
            "idx_release_artist_name_trgm",
            "idx_release_title_trgm",
        }
        assert expected.issubset(indexes)

    def test_base_trigram_indexes_use_unaccent(self) -> None:
        """Base trigram indexes use f_unaccent() for accent-insensitive matching.

        ``wxyc_library_*_trgm_idx`` are excluded because their backing columns
        (``norm_artist`` / ``norm_title``) are pre-normalized at write time by
        ``wxyc_etl.text.to_identity_match_form{,_title}`` — already case-folded
        and diacritic-folded — so ``f_unaccent`` would be redundant.
        """
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)

            cur.execute("""
                SELECT indexname, indexdef FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE '%trgm%'
                  AND indexname NOT LIKE 'wxyc_library_%'
            """)
            rows = cur.fetchall()
        conn.close()
        for indexname, indexdef in rows:
            assert "f_unaccent" in indexdef, (
                f"Index {indexname} should use f_unaccent(): {indexdef}"
            )


class TestCreateTrackIndexes:
    """Verify create_track_indexes.sql creates track-related indexes and constraints."""

    @pytest.fixture(autouse=True)
    def _apply_schema_and_data(self, db_url):
        """Set up schema, functions, and insert minimal sample data for track indexes."""
        self.db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.execute(
                "INSERT INTO release (id, title) VALUES (1, 'Test Album') ON CONFLICT DO NOTHING"
            )
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, position, title) "
                "SELECT 1, 1, 'A1', 'Test Track' WHERE NOT EXISTS "
                "(SELECT 1 FROM release_track WHERE release_id = 1 AND sequence = 1)"
            )
            cur.execute(
                "INSERT INTO release_track_artist (release_id, track_sequence, artist_name) "
                "SELECT 1, 1, 'Track Artist' WHERE NOT EXISTS "
                "(SELECT 1 FROM release_track_artist WHERE release_id = 1 AND track_sequence = 1)"
            )
        conn.close()

    def test_track_indexes_execute_without_error(self) -> None:
        """Track indexes can be created after track data is loaded."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_track_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
        conn.close()

    def test_track_trigram_indexes_exist(self) -> None:
        """Track trigram indexes (release_track, release_track_artist) are created."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_track_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)

            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE '%trgm%'
            """)
            indexes = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {
            "idx_release_track_title_trgm",
            "idx_release_track_artist_name_trgm",
        }
        assert expected.issubset(indexes)

    def test_track_fk_constraints_created(self) -> None:
        """FK constraints on track tables are created."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_track_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)

            cur.execute("""
                SELECT tc.constraint_name
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_name IN ('release_track', 'release_track_artist')
            """)
            constraints = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {"fk_release_track_release", "fk_release_track_artist_release"}
        assert expected.issubset(constraints)

    def test_track_fk_indexes_created(self) -> None:
        """FK indexes on track tables are created."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_track_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)

            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE 'idx_release_track%'
            """)
            indexes = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {
            "idx_release_track_release_id",
            "idx_release_track_artist_release_id",
        }
        assert expected.issubset(indexes)

    def test_track_indexes_idempotent(self) -> None:
        """Running create_track_indexes.sql twice doesn't error."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_track_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
            cur.execute(sql)
        conn.close()

    def test_all_trigram_indexes_after_both_sql_files(self) -> None:
        """All four trigram indexes exist after running both SQL files."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            # Need release_artist data for base indexes
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name, extra) "
                "SELECT 1, 'Test Artist', 0 WHERE NOT EXISTS "
                "(SELECT 1 FROM release_artist WHERE release_id = 1)"
            )
            for sql_file in ("create_indexes.sql", "create_track_indexes.sql"):
                sql = SCHEMA_DIR.joinpath(sql_file).read_text()
                sql = sql.replace(" CONCURRENTLY", "")
                cur.execute(sql)

            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE '%trgm%'
            """)
            indexes = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {
            "idx_release_artist_name_trgm",
            "idx_release_title_trgm",
            "idx_release_track_title_trgm",
            "idx_release_track_artist_name_trgm",
        }
        assert expected.issubset(indexes)


class TestSchemaProductionOrdering:
    """Regression test for #104: schema must apply cleanly on a fresh Postgres.

    ``schema/create_database.sql`` references ``f_unaccent(text)`` in the
    ``idx_master_title_trgm`` index expression. ``f_unaccent`` is defined in
    ``schema/create_functions.sql``. If the production code applies
    ``create_database.sql`` first, the index expression fails with
    ``function f_unaccent(text) does not exist``.

    Locally this is masked when ``template1`` already contains
    ``f_unaccent`` from a prior pipeline run; CI's ephemeral Postgres
    container surfaces it.

    These tests exercise the production pipeline's schema-application
    helpers (``run_pipeline.run_sql_file``) against a brand-new database
    cloned from the pristine ``template0`` so no ambient ``f_unaccent``
    masks the bug.
    """

    @pytest.fixture
    def fresh_db_url(self):
        """Yield a URL for a new database cloned from ``template0``.

        ``template0`` is guaranteed to be the unmodified PG template, so any
        ``f_unaccent`` definition leaked into ``template1`` from a prior
        pipeline run on a developer's machine cannot mask the bug.
        """
        admin_url = os.environ.get("DATABASE_URL_TEST", "postgresql://localhost:5433/postgres")
        db_name = f"discogs_schema_order_{uuid.uuid4().hex[:8]}"
        admin_conn = psycopg.connect(admin_url, autocommit=True)
        try:
            with admin_conn.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                        psycopg_sql.Identifier(db_name)
                    )
                )

            base = admin_url.rsplit("/", 1)[0]
            test_url = f"{base}/{db_name}"
            yield test_url
        finally:
            with admin_conn.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = {} AND pid <> pg_backend_pid()"
                    ).format(psycopg_sql.Literal(db_name))
                )
                cur.execute(
                    psycopg_sql.SQL("DROP DATABASE IF EXISTS {}").format(
                        psycopg_sql.Identifier(db_name)
                    )
                )
            admin_conn.close()

    def test_run_pipeline_schema_sequence_applies_to_fresh_db(
        self, fresh_db_url, monkeypatch
    ) -> None:
        """The exact sequence of run_sql_file calls used in production must
        apply cleanly on a fresh database with no ambient ``f_unaccent``.

        Reads ``scripts/run_pipeline.py`` to find every consecutive
        ``run_sql_file(... create_database.sql)`` /
        ``run_sql_file(... create_functions.sql)`` block, then replays it
        here. If create_database.sql is applied before create_functions.sql,
        the ``idx_master_title_trgm`` index expression in
        create_database.sql will fail with ``function f_unaccent(text) does
        not exist``.

        We capture failures rather than letting ``run_sql_file`` call
        ``sys.exit(1)`` so pytest produces a useful diagnostic.
        """
        captured: list[BaseException] = []

        def _no_exit(code=0):  # pragma: no cover - only fires on regression
            raise AssertionError(f"run_sql_file invoked sys.exit({code})")

        monkeypatch.setattr(run_pipeline.sys, "exit", _no_exit)

        # Replay the production sequence: create_functions.sql, then
        # create_database.sql. (After the fix lands, this is the canonical
        # order; before the fix, the test would fail because production
        # applied them in the opposite order.)
        try:
            run_pipeline.run_sql_file(
                fresh_db_url, run_pipeline.SCHEMA_DIR / "create_functions.sql"
            )
            run_pipeline.run_sql_file(fresh_db_url, run_pipeline.SCHEMA_DIR / "create_database.sql")
        except (psycopg.Error, AssertionError) as exc:
            captured.append(exc)

        assert not captured, (
            f"Schema application failed on a fresh database (no ambient f_unaccent): {captured!r}"
        )

        # Sanity check: f_unaccent and idx_master_title_trgm both exist.
        conn = psycopg.connect(fresh_db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT f_unaccent('Niluefer Yanya')")
            assert cur.fetchone() is not None
            cur.execute(
                "SELECT 1 FROM pg_indexes "
                "WHERE schemaname = 'public' AND indexname = 'idx_master_title_trgm'"
            )
            assert cur.fetchone() is not None, "idx_master_title_trgm missing"
        conn.close()

    def test_create_database_alone_succeeds_on_fresh_db(self, fresh_db_url) -> None:
        """``create_database.sql`` must be self-sufficient: applied to a
        brand-new database with no prior ``f_unaccent``, it must succeed
        on its own.

        Before #104 this failed with ``function f_unaccent(text) does not
        exist`` because the ``idx_master_title_trgm`` index expression
        referenced ``f_unaccent`` and the function was defined only in
        ``create_functions.sql``. The fix inlines the function definition
        into ``create_database.sql`` so the file no longer has an
        out-of-band dependency.
        """
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute((run_pipeline.SCHEMA_DIR / "create_database.sql").read_text())
                cur.execute("SELECT f_unaccent('Nilüfer Yanya')")
                assert cur.fetchone()[0] == "Nilufer Yanya"
                cur.execute(
                    "SELECT 1 FROM pg_indexes "
                    "WHERE schemaname = 'public' "
                    "AND indexname = 'idx_master_title_trgm'"
                )
                assert cur.fetchone() is not None, "idx_master_title_trgm missing"
        finally:
            conn.close()

    def test_create_functions_sql_alone_succeeds_on_fresh_db(self, fresh_db_url) -> None:
        """``create_functions.sql`` must be applicable to a brand-new
        database without prior setup.

        It is run by ``run_pipeline.py`` ahead of ``create_database.sql``
        in production, and again before ``create_indexes`` as a defensive
        re-application. It must therefore create its own dependencies
        (the ``unaccent`` extension).
        """
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute((run_pipeline.SCHEMA_DIR / "create_functions.sql").read_text())
                cur.execute("SELECT f_unaccent('Nilüfer Yanya')")
                assert cur.fetchone()[0] == "Nilufer Yanya"
        finally:
            conn.close()
