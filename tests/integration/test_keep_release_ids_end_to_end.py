"""End-to-end integration test for issue #327: a WXYC library pinned override
(lml_cache.library_release_override) must survive BOTH the dedup seam
(scripts/dedup_releases.py) and the prune seam (scripts/verify_cache.py)
together, while an otherwise-identical non-pinned control release is deleted
by both -- proving the --keep-release-ids exemption is targeted, not a
blanket bypass.

This test creates its own ad hoc lml_cache.library_release_override table
directly (mirroring how LML bootstraps it itself) -- discogs-etl never
migrates or owns that schema (CLAUDE.md "entity.* schema ownership").
"""

from __future__ import annotations

import importlib.util
import sys as _sys
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
FIXTURE_LIBRARY_DB = FIXTURES_DIR / "library.db"

ALL_TABLES = (
    "cache_metadata",
    "release_track_artist",
    "release_track",
    "release_label",
    "release_artist",
    "release",
)

_DEDUP_PATH = Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py"
if "dedup_releases" in _sys.modules:
    _dd = _sys.modules["dedup_releases"]
else:
    _dspec = importlib.util.spec_from_file_location("dedup_releases", _DEDUP_PATH)
    assert _dspec is not None and _dspec.loader is not None
    _dd = importlib.util.module_from_spec(_dspec)
    _sys.modules["dedup_releases"] = _dd
    _dspec.loader.exec_module(_dd)

_VC_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
if "verify_cache" in _sys.modules:
    _vc = _sys.modules["verify_cache"]
else:
    _vc_spec = importlib.util.spec_from_file_location("verify_cache", _VC_PATH)
    assert _vc_spec is not None and _vc_spec.loader is not None
    _vc = importlib.util.module_from_spec(_vc_spec)
    _sys.modules["verify_cache"] = _vc
    _vc_spec.loader.exec_module(_vc)

_RUN_PIPELINE_PATH = Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py"
_rpspec = importlib.util.spec_from_file_location("run_pipeline_e2e", _RUN_PIPELINE_PATH)
assert _rpspec is not None and _rpspec.loader is not None
_rp = importlib.util.module_from_spec(_rpspec)
_rpspec.loader.exec_module(_rp)

load_keep_release_ids = _dd.load_keep_release_ids
ensure_dedup_ids = _dd.ensure_dedup_ids
copy_table = _dd.copy_table
swap_tables = _dd.swap_tables
add_base_constraints_and_indexes = _dd.add_base_constraints_and_indexes
DEDUP_TABLES = _dd.DEDUP_TABLES

LibraryIndex = _vc.LibraryIndex
MultiIndexMatcher = _vc.MultiIndexMatcher
classify_all_releases = _vc.classify_all_releases
apply_release_overrides = _vc.apply_release_overrides
verify_load_keep_release_ids = _vc.load_keep_release_ids

write_keep_release_ids = _rp.write_keep_release_ids

pytestmark = [pytest.mark.pg]


def _drop_all(conn) -> None:
    with conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids CASCADE")
        cur.execute("DROP TABLE IF EXISTS release_track_count CASCADE")
        cur.execute("DROP TABLE IF EXISTS keep_release_ids CASCADE")
        for prefix in ("new_", ""):
            for table in ALL_TABLES:
                cur.execute(f"DROP TABLE IF EXISTS {prefix}{table}_old CASCADE")
        cur.execute("DROP SCHEMA IF EXISTS lml_cache CASCADE")


def _load_releases_sync(db_url: str) -> list[tuple[int, str, str]]:
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.id, ra.artist_name, r.title
            FROM release r
            JOIN release_artist ra ON ra.release_id = r.id AND ra.extra = 0
            ORDER BY r.id
        """)
        rows = [(row[0], row[1], row[2]) for row in cur.fetchall()]
    conn.close()
    return rows


def _run_dedup(db_url: str, *, keep_ids_loaded: bool) -> None:
    conn = psycopg.connect(db_url, autocommit=True)
    delete_count = ensure_dedup_ids(conn, keep_ids_loaded=keep_ids_loaded)
    if delete_count > 0:
        for old, new, cols, id_col in DEDUP_TABLES:
            copy_table(conn, old, new, cols, id_col)
        with conn.cursor() as cur:
            for stmt in [
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE release_genre DROP CONSTRAINT IF EXISTS fk_release_genre_release",
                "ALTER TABLE release_style DROP CONSTRAINT IF EXISTS fk_release_style_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ]:
                cur.execute(stmt)
        for old, new, _, _ in DEDUP_TABLES:
            swap_tables(conn, old, new)
        add_base_constraints_and_indexes(conn, db_url=db_url)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
        cur.execute("DROP TABLE IF EXISTS release_track_count")
    conn.close()


class TestKeepReleaseIdsEndToEnd:
    """A pinned release survives dedup + prune together; a non-pinned control does not."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _drop_all(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())

            # Pinned release (1): loses dedup to release 2 (lower track count,
            # non-US) AND has a non-library artist (fuzzy-prune candidate).
            # Deliberately vulnerable to BOTH seams.
            cur.execute(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (1, 'Obscure Album', 500, 'CD', 'UK')"
            )
            cur.execute(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (2, 'Obscure Album', 500, 'CD', 'US')"
            )
            # Non-pinned control, same vulnerability shape as release 1.
            cur.execute(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (10, 'Obscure Album B', 600, 'CD', 'UK')"
            )
            cur.execute(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (11, 'Obscure Album B', 600, 'CD', 'US')"
            )
            for rid, artist in (
                (1, "Random Pinned Artist"),
                (2, "Random Pinned Artist"),
                (10, "Random Control Artist"),
                (11, "Random Control Artist"),
            ):
                cur.execute(
                    "INSERT INTO release_artist (release_id, artist_id, artist_name, extra) "
                    "VALUES (%s, %s, %s, 0)",
                    (rid, rid, artist),
                )
            cur.execute("""
                CREATE UNLOGGED TABLE release_track_count (
                    release_id integer PRIMARY KEY,
                    track_count integer NOT NULL
                )
            """)
            cur.execute("INSERT INTO release_track_count VALUES (1, 1), (2, 5), (10, 1), (11, 5)")

            # Ad hoc lml_cache.library_release_override, mirroring LML's own
            # bootstrap -- discogs-etl never migrates this schema.
            cur.execute("CREATE SCHEMA lml_cache")
            cur.execute("""
                CREATE TABLE lml_cache.library_release_override (
                    library_id integer NOT NULL,
                    discogs_release_id integer NOT NULL,
                    source text NOT NULL
                )
            """)
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES (42, 1, 'test')"
            )
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_pinned_release_survives_both_seams(self, tmp_path) -> None:
        keep_ids_path = tmp_path / "keep_release_ids.txt"

        # Step 1: run_pipeline reads the override table read-only.
        override_count = write_keep_release_ids(self.db_url, keep_ids_path)
        assert override_count == 1

        # Step 2: dedup, with the exemption applied.
        conn = psycopg.connect(self.db_url, autocommit=True)
        load_keep_release_ids(conn, keep_ids_path)
        conn.close()
        _run_dedup(self.db_url, keep_ids_loaded=True)

        with psycopg.connect(self.db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM release ORDER BY id")
            after_dedup = {row[0] for row in cur.fetchall()}
        assert 1 in after_dedup, "pinned release must survive dedup despite rn > 1"
        assert 10 not in after_dedup, "non-pinned control must be deleted by dedup"

        # Step 3: prune, with the exemption applied.
        index = LibraryIndex.from_sqlite(FIXTURE_LIBRARY_DB)
        matcher = MultiIndexMatcher(index)
        releases = _load_releases_sync(self.db_url)
        report = classify_all_releases(releases, index, matcher)
        assert 1 in report.prune_ids, (
            "precondition: release 1 must be a genuine fuzzy-match PRUNE "
            "candidate before the override is applied, or this test proves nothing"
        )
        override_ids = verify_load_keep_release_ids(keep_ids_path)
        apply_release_overrides(report, override_ids)

        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM release WHERE id = ANY(%s::integer[])",
                (list(report.prune_ids),),
            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release ORDER BY id")
            after_prune = {row[0] for row in cur.fetchall()}
        conn.close()

        assert 1 in after_prune, "pinned release must survive prune despite non-library artist"
        assert 11 not in after_prune, (
            "non-pinned control (dedup winner, non-library artist) must be pruned"
        )
