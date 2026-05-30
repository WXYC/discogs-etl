"""Pin: NOT NULL constraints survive the copy-swap step.

Sibling regression to ``tests/integration/test_dedup.py::TestDedupCopySwapPreservesMasterId``
(WXYC/discogs-etl#129, master_id column drop) and the verify_cache column drift
fixed in WXYC/discogs-etl#233. PostgreSQL's ``CREATE TABLE AS SELECT`` preserves
only column types, not constraints — so NOT NULL on data columns is silently
stripped on every monthly rebuild unless the copy-swap path re-applies it
explicitly.

The schema in ``schema/create_database.sql`` is the source of truth for which
columns are required. This test runs the two copy-swap entrypoints
(``dedup_releases.add_base_constraints_and_indexes`` and
``verify_cache.prune_releases_copy_swap``) against a minimal fixture and
asserts the post-swap tables still report ``is_nullable = 'NO'`` on every
column the schema declared ``NOT NULL``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import psycopg
import pytest

pytestmark = pytest.mark.pg

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

# Load production modules using sys.modules guard (mirrors test_verify_cache_columns.py).
# Registering in sys.modules before exec_module so dataclasses / typing introspection
# inside the loaded module can resolve their own __module__ name.
_DEDUP_PATH = Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py"
if "dedup_releases" in sys.modules:
    _dd = sys.modules["dedup_releases"]
else:
    _dspec = importlib.util.spec_from_file_location("dedup_releases", _DEDUP_PATH)
    assert _dspec is not None and _dspec.loader is not None
    _dd = importlib.util.module_from_spec(_dspec)
    sys.modules["dedup_releases"] = _dd
    _dspec.loader.exec_module(_dd)

_VERIFY_CACHE_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _vcspec = importlib.util.spec_from_file_location("verify_cache", _VERIFY_CACHE_PATH)
    assert _vcspec is not None and _vcspec.loader is not None
    _vc = importlib.util.module_from_spec(_vcspec)
    sys.modules["verify_cache"] = _vc
    _vcspec.loader.exec_module(_vc)


# Columns expected NOT NULL on each post-swap table, transcribed by hand from
# schema/create_database.sql. ``id`` is omitted from the ``release`` entry
# because ALTER TABLE ADD PRIMARY KEY (id) already re-asserts NOT NULL.
# ``cache_metadata.release_id`` similarly is the PK and excluded.
#
# This list is the test contract; when schema/create_database.sql changes the
# NOT NULL set on these tables, update both this dict and the production
# ALTER statements at the same time. The TestNotNullPinExpectationsMatchSchema
# class below catches drift between the schema and this expectation.
_EXPECTED_NOT_NULL: dict[str, tuple[str, ...]] = {
    "release": ("title",),
    "release_artist": ("release_id", "artist_name"),
    "release_label": ("release_id", "label_name"),
    "release_genre": ("release_id", "genre"),
    "release_style": ("release_id", "style"),
    "release_track": ("release_id", "sequence", "title"),
    "release_track_artist": ("release_id", "track_sequence", "artist_name"),
    "cache_metadata": ("cached_at", "source"),
}

# Columns whose schema DEFAULT must survive the copy-swap. CTAS strips
# DEFAULTs along with NOT NULL; restoring the DEFAULT is load-bearing for
# ``cache_metadata.cached_at`` in particular — without it, LML's cache-miss
# INSERT (which omits cached_at) would violate the NOT NULL constraint we
# re-apply above. The ``extra`` columns are nullable so the DEFAULT is
# defense-in-depth, not load-bearing, but the schema specifies it.
_EXPECTED_DEFAULTS: dict[tuple[str, str], str] = {
    ("cache_metadata", "cached_at"): "now()",
    ("release_artist", "extra"): "0",
    ("release_track_artist", "extra"): "0",
}

# Tables that flow through each copy-swap entrypoint.
_DEDUP_TABLES_TO_CHECK = (
    "release",
    "release_artist",
    "release_label",
    "release_genre",
    "release_style",
    "cache_metadata",
)
_PRUNE_TABLES_TO_CHECK = (
    "release",
    "release_artist",
    "release_label",
    "release_genre",
    "release_style",
    "release_track",
    "release_track_artist",
    "cache_metadata",
)


def _drop_all_tables(conn) -> None:
    """Clear pipeline tables and any leftover ``new_`` copy-swap artifacts."""
    base = (
        "cache_metadata",
        "release_track_artist",
        "release_track",
        "release_style",
        "release_genre",
        "release_label",
        "release_artist",
        "release",
    )
    with conn.cursor() as cur:
        for t in base:
            cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS new_{t} CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS {t}_old CASCADE")
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids CASCADE")
        cur.execute("DROP TABLE IF EXISTS _keep_ids CASCADE")


def _seed_minimal_fixture(db_url: str) -> None:
    """Apply schema + insert representative rows for all 8 swap-tracked tables.

    Three releases (1, 2, 3) with child rows on each table so the copy-swap
    actually touches every table. Compositions chosen from WXYC's canonical
    example data per docs/test-fixtures.md.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())

            cur.executemany(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (%s, %s, %s, %s, %s)",
                [
                    (1, "DOGA", 200, "LP", "AR"),
                    (2, "Aluminum Tunes", 100, "CD", "UK"),
                    (3, "Edits", None, "CD", "US"),
                ],
            )
            cur.executemany(
                "INSERT INTO release_artist (release_id, artist_name) VALUES (%s, %s)",
                [
                    (1, "Juana Molina"),
                    (2, "Stereolab"),
                    (3, "Chuquimamani-Condori"),
                ],
            )
            cur.executemany(
                "INSERT INTO release_label (release_id, label_name) VALUES (%s, %s)",
                [(1, "Sonamos"), (2, "Duophonic")],
            )
            cur.executemany(
                "INSERT INTO release_genre (release_id, genre) VALUES (%s, %s)",
                [(1, "Folk"), (2, "Electronic")],
            )
            cur.executemany(
                "INSERT INTO release_style (release_id, style) VALUES (%s, %s)",
                [(1, "Folk Rock"), (2, "Indie Pop")],
            )
            cur.executemany(
                "INSERT INTO release_track (release_id, sequence, title) VALUES (%s, %s, %s)",
                [(1, 1, "Cosoco"), (2, 1, "Fuses")],
            )
            cur.executemany(
                "INSERT INTO release_track_artist "
                "(release_id, track_sequence, artist_name) VALUES (%s, %s, %s)",
                [(1, 1, "Juana Molina"), (2, 1, "Stereolab")],
            )
            cur.executemany(
                "INSERT INTO cache_metadata (release_id, source) VALUES (%s, %s)",
                [(1, "bulk_import"), (2, "bulk_import"), (3, "bulk_import")],
            )
    finally:
        conn.close()


def _not_null_columns(conn, table: str) -> set[str]:
    """Return columns marked ``NOT NULL`` on a given table in the public schema."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s "
            "AND is_nullable = 'NO'",
            (table,),
        )
        return {row[0] for row in cur.fetchall()}


def _column_default(conn, table: str, column: str) -> str | None:
    """Return the DEFAULT expression on ``table.column`` (or None if unset)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_default FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s AND column_name = %s",
            (table, column),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _run_dedup_swap(db_url: str) -> None:
    """Exercise dedup's copy-swap path end-to-end (single deletion case).

    Mirrors the production main() flow but trims it to copy_table → drop FKs →
    swap_tables → add_base_constraints_and_indexes. Marks release 3 for deletion
    so all 6 DEDUP_TABLES go through CTAS.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE UNLOGGED TABLE dedup_delete_ids (release_id integer PRIMARY KEY)")
            cur.execute("INSERT INTO dedup_delete_ids VALUES (3)")

        for old, new, cols, id_col in _dd.DEDUP_TABLES:
            _dd.copy_table(conn, old, new, cols, id_col)

        with conn.cursor() as cur:
            for stmt in (
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE release_genre DROP CONSTRAINT IF EXISTS fk_release_genre_release",
                "ALTER TABLE release_style DROP CONSTRAINT IF EXISTS fk_release_style_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ):
                cur.execute(stmt)

        for old, new, _, _ in _dd.DEDUP_TABLES:
            _dd.swap_tables(conn, old, new)

        _dd.add_base_constraints_and_indexes(conn, db_url=db_url)
    finally:
        conn.close()


class TestDedupCopySwapPreservesNotNull:
    """add_base_constraints_and_indexes re-applies NOT NULL on schema-required columns."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_dedup(self, db_url):
        self.__class__._db_url = db_url
        _seed_minimal_fixture(db_url)
        _run_dedup_swap(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    @pytest.mark.parametrize("table", _DEDUP_TABLES_TO_CHECK)
    def test_required_columns_remain_not_null(self, table):
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            actual = _not_null_columns(conn, table)
        finally:
            conn.close()

        expected = set(_EXPECTED_NOT_NULL[table])
        missing = expected - actual
        assert not missing, (
            f"After dedup copy-swap, NOT NULL was stripped from "
            f"{table}.{sorted(missing)}. CTAS preserves column types but not "
            f"constraints; scripts/dedup_releases.py:add_base_constraints_and_indexes "
            f"must re-apply SET NOT NULL on these columns."
        )

    # Dedup swaps the 6 DEDUP_TABLES; release_track_artist is re-imported
    # post-dedup (not copy-swapped) so its DEFAULT is preserved by the
    # schema-driven import. Only the in-DEDUP_TABLES defaults are pinned here.
    @pytest.mark.parametrize(
        "table, column, expected",
        [
            ("cache_metadata", "cached_at", "now()"),
            ("release_artist", "extra", "0"),
        ],
    )
    def test_required_columns_keep_their_default(self, table, column, expected):
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            actual = _column_default(conn, table, column)
        finally:
            conn.close()

        assert actual == expected, (
            f"After dedup copy-swap, DEFAULT on {table}.{column} is {actual!r} "
            f"but the schema declares {expected!r}. CTAS strips DEFAULTs along "
            f"with NOT NULL; "
            f"scripts/dedup_releases.py:add_base_constraints_and_indexes must "
            f"re-apply SET DEFAULT on these columns. "
            f"(cache_metadata.cached_at is load-bearing — LML's cache-miss "
            f"INSERT omits the column and relies on the DEFAULT to avoid a "
            f"NOT NULL violation.)"
        )


_RACE_INSERT_RELEASE_ID = 4


def _seed_extra_release_for_race(db_url: str) -> None:
    """Add a `release` row with no `cache_metadata` row, ready for the race INSERT.

    Picks an id (`_RACE_INSERT_RELEASE_ID`) outside the seeded set so the post-swap
    race INSERT into `cache_metadata` (which still has no PK at that point) can't
    collide with a sibling row, and so the FK validation against `release` passes
    when Level 2 runs.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO release (id, title, master_id, format, country) "
                "VALUES (%s, %s, %s, %s, %s)",
                (_RACE_INSERT_RELEASE_ID, "Moon Pix", 300, "LP", "US"),
            )
    finally:
        conn.close()


def _run_dedup_swap_with_race_insert(db_url: str) -> None:
    """Exercise dedup but inject an LML-style INSERT in the race window.

    Mirrors `_run_dedup_swap` but between `swap_tables()` and
    `add_base_constraints_and_indexes()` issues:

        INSERT INTO cache_metadata (release_id, source) VALUES (...)

    which is what library-metadata-lookup's cache-miss path does in steady
    state — relying on the table DEFAULT to supply `cached_at = now()`. If
    CTAS strips the DEFAULT and the dedup path doesn't restore it before the
    swap, the insert lands with `cached_at = NULL`, and the subsequent
    `ALTER COLUMN cached_at SET NOT NULL` fails with NotNullViolation —
    aborting the entire rebuild. See WXYC/discogs-etl#254.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE UNLOGGED TABLE dedup_delete_ids (release_id integer PRIMARY KEY)")
            cur.execute("INSERT INTO dedup_delete_ids VALUES (3)")

        for old, new, cols, id_col in _dd.DEDUP_TABLES:
            _dd.copy_table(conn, old, new, cols, id_col)

        with conn.cursor() as cur:
            for stmt in (
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE release_genre DROP CONSTRAINT IF EXISTS fk_release_genre_release",
                "ALTER TABLE release_style DROP CONSTRAINT IF EXISTS fk_release_style_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ):
                cur.execute(stmt)

        for old, new, _, _ in _dd.DEDUP_TABLES:
            _dd.swap_tables(conn, old, new)

        # Race insert — what LML does in steady state. Omits cached_at,
        # relying on the table DEFAULT. The release_id is one that's already
        # in `release` (seeded above) so Level 1.5 orphan cleanup leaves it
        # alone; and it's not yet in `cache_metadata` so there's no clash
        # when Level 2 adds the PK on release_id.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cache_metadata (release_id, source) VALUES (%s, %s)",
                (_RACE_INSERT_RELEASE_ID, "api_fetch"),
            )

        _dd.add_base_constraints_and_indexes(conn, db_url=db_url)
    finally:
        conn.close()


class TestDedupCopySwapToleratesRaceWindowInsert:
    """The race-window LML insert survives add_base_constraints_and_indexes.

    Pin for [#254](https://github.com/WXYC/discogs-etl/issues/254). The 2026-05-30
    rebuild failed when an LML cache-miss INSERT landed between `swap_tables()`
    and `ALTER COLUMN cached_at SET DEFAULT now()`, writing NULL `cached_at`
    and tripping the subsequent SET NOT NULL.

    Fix is structural — apply DEFAULTs on the NEW table inside `copy_table()`
    so the live table already has the DEFAULT at the moment of swap. The
    sibling pin `TestDedupCopySwapPreservesNotNull` covers end-state; this
    one covers the race window itself.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_dedup(self, db_url):
        self.__class__._db_url = db_url
        _seed_minimal_fixture(db_url)
        _seed_extra_release_for_race(db_url)
        _run_dedup_swap_with_race_insert(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_race_insert_has_non_null_cached_at(self):
        """The race-window INSERT gets a non-NULL cached_at via the table DEFAULT."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT cached_at FROM cache_metadata WHERE release_id = %s",
                    (_RACE_INSERT_RELEASE_ID,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        assert row is not None, "race-window INSERT row missing from cache_metadata"
        assert row[0] is not None, (
            "Race-window INSERT landed with NULL cached_at. CTAS strips the "
            "DEFAULT on cache_metadata.cached_at; copy_table() must re-apply "
            "the DEFAULT on new_cache_metadata before swap_tables() so the "
            "live table has the DEFAULT at the moment of swap."
        )


class TestPruneCopySwapPreservesNotNull:
    """verify_cache.prune_releases_copy_swap re-applies NOT NULL on schema-required columns."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_prune(self, db_url):
        self.__class__._db_url = db_url
        _seed_minimal_fixture(db_url)
        # Keep releases 1, 2; prune release 3. All 8 PRUNE_COPY_TABLES go through CTAS.
        _vc.prune_releases_copy_swap(db_url, keep_ids={1, 2}, review_ids=set())

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    @pytest.mark.parametrize("table", _PRUNE_TABLES_TO_CHECK)
    def test_required_columns_remain_not_null(self, table):
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            actual = _not_null_columns(conn, table)
        finally:
            conn.close()

        expected = set(_EXPECTED_NOT_NULL[table])
        missing = expected - actual
        assert not missing, (
            f"After verify_cache prune copy-swap, NOT NULL was stripped from "
            f"{table}.{sorted(missing)}. CTAS preserves column types but not "
            f"constraints; scripts/verify_cache.py:prune_releases_copy_swap "
            f"must re-apply SET NOT NULL on these columns."
        )

    # Verify_cache prune copy-swaps all 8 PRUNE_COPY_TABLES including the track
    # tables, so all three CTAS-stripped DEFAULTs are pinned here.
    @pytest.mark.parametrize(
        "table, column, expected",
        [
            ("cache_metadata", "cached_at", "now()"),
            ("release_artist", "extra", "0"),
            ("release_track_artist", "extra", "0"),
        ],
    )
    def test_required_columns_keep_their_default(self, table, column, expected):
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            actual = _column_default(conn, table, column)
        finally:
            conn.close()

        assert actual == expected, (
            f"After verify_cache prune copy-swap, DEFAULT on {table}.{column} "
            f"is {actual!r} but the schema declares {expected!r}. CTAS strips "
            f"DEFAULTs along with NOT NULL; "
            f"scripts/verify_cache.py:prune_releases_copy_swap must re-apply "
            f"SET DEFAULT on these columns. (cache_metadata.cached_at is "
            f"load-bearing — LML's cache-miss INSERT omits the column and "
            f"relies on the DEFAULT to avoid a NOT NULL violation.)"
        )


def _run_prune_swap_with_race_insert(db_url: str) -> None:
    """Exercise verify_cache's prune copy-swap with an LML-style race INSERT.

    Mirrors `_run_dedup_swap_with_race_insert` against the prune path. Splits
    the two phases of `prune_releases_copy_swap` so an LML-style INSERT can
    be injected between them — the exact race window that bit the 2026-05-30
    rebuild. See #256.
    """
    # Keep releases 1, 2 (and the extra race-seed release); prune release 3.
    keep_ids = {1, 2, _RACE_INSERT_RELEASE_ID}
    _vc._prune_copy_swap_tables(db_url, keep_ids=keep_ids, review_ids=set())

    # Race insert lands after the swap but before constraint application —
    # exactly what LML's cache-miss INSERT does in production. Omits
    # cached_at, relying on the table DEFAULT applied pre-swap.
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cache_metadata (release_id, source) VALUES (%s, %s)",
                (_RACE_INSERT_RELEASE_ID, "api_fetch"),
            )
    finally:
        conn.close()

    _vc._prune_add_base_constraints_and_indexes(db_url)


class TestPruneCopySwapToleratesRaceWindowInsert:
    """The race-window LML insert survives prune_releases_copy_swap.

    Pin for [#256](https://github.com/WXYC/discogs-etl/issues/256). The
    2026-05-30 rebuild failed when an LML cache-miss INSERT landed between
    verify_cache's swap_tables and the SET NOT NULL on cache_metadata.cached_at.
    Sibling of `TestDedupCopySwapToleratesRaceWindowInsert` — same fix pattern
    (pre-swap DEFAULT restoration via PRUNE_PRE_SWAP_COLUMN_DEFAULTS) applied
    to a different copy-swap site.
    """

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_prune(self, db_url):
        self.__class__._db_url = db_url
        _seed_minimal_fixture(db_url)
        _seed_extra_release_for_race(db_url)
        _run_prune_swap_with_race_insert(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_race_insert_has_non_null_cached_at(self):
        """The race-window INSERT gets a non-NULL cached_at via the table DEFAULT."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT cached_at FROM cache_metadata WHERE release_id = %s",
                    (_RACE_INSERT_RELEASE_ID,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        assert row is not None, "race-window INSERT row missing from cache_metadata"
        assert row[0] is not None, (
            "Race-window INSERT landed with NULL cached_at. CTAS strips the "
            "DEFAULT on cache_metadata.cached_at; _prune_copy_swap_tables must "
            "re-apply the DEFAULT on new_cache_metadata before the RENAME so "
            "the live table has the DEFAULT at the moment of swap."
        )


class TestNotNullPinExpectationsMatchSchema:
    """Catch drift between _EXPECTED_NOT_NULL and schema/create_database.sql.

    Without this, a future schema change that adds a new ``NOT NULL`` column
    on one of the swap-tracked tables would not appear in this test file's
    expectation, and the corresponding copy-swap fix could silently lag.
    """

    def test_every_schema_not_null_column_is_pinned(self, tmp_path) -> None:
        """Every NOT NULL on a swap-tracked table appears in _EXPECTED_NOT_NULL."""
        schema_text = SCHEMA_DIR.joinpath("create_database.sql").read_text()
        # Naive but sufficient for this schema: scan column-definition lines
        # inside each CREATE TABLE body. We accept some over-match (column
        # comments etc.) because the assertion is a superset check.
        for table, expected in _EXPECTED_NOT_NULL.items():
            schema_cols = _parse_not_null_columns(schema_text, table)
            # PK columns (implicit NOT NULL via PRIMARY KEY) are excluded from
            # the test expectation because PK re-creation reasserts NOT NULL.
            pk_cols = _parse_primary_key_columns(schema_text, table)
            schema_data_cols = schema_cols - pk_cols
            missing = schema_data_cols - set(expected)
            assert not missing, (
                f"Schema declares NOT NULL on {table}.{sorted(missing)} "
                f"but the pin in _EXPECTED_NOT_NULL doesn't cover them. "
                f"Update _EXPECTED_NOT_NULL and the corresponding production "
                f"ALTER statements in dedup_releases.py + verify_cache.py."
            )


def _parse_not_null_columns(schema_text: str, table: str) -> set[str]:
    """Return columns declared NOT NULL in ``CREATE TABLE <table>``."""
    cols: set[str] = set()
    for line in _iter_column_lines(schema_text, table):
        if "NOT NULL" in line:
            cols.add(line.split()[0])
    return cols


def _parse_primary_key_columns(schema_text: str, table: str) -> set[str]:
    """Return columns declared as PRIMARY KEY in ``CREATE TABLE <table>``."""
    cols: set[str] = set()
    for line in _iter_column_lines(schema_text, table):
        if "PRIMARY KEY" in line.upper():
            cols.add(line.split()[0])
    return cols


def _iter_column_lines(schema_text: str, table: str):
    """Yield individual column declarations from a ``CREATE TABLE`` body.

    Strips ``-- ...`` end-of-line comments before splitting on column
    separators so embedded commas inside comments (e.g. ``-- "A1", "B2"``)
    don't cross over into the next declaration.
    """
    # Tolerate both bare and ``IF NOT EXISTS`` forms — the file flipped to
    # the latter for the WXYC/discogs-etl#242 schema split.
    for marker in (f"CREATE TABLE {table} (", f"CREATE TABLE IF NOT EXISTS {table} ("):
        if marker in schema_text:
            start = schema_text.index(marker) + len(marker)
            break
    else:
        raise AssertionError(f"CREATE TABLE marker for {table!r} not found in schema")
    body = _extract_paren_body(schema_text, start)
    # Strip end-of-line SQL comments so commas inside comments don't bleed
    # into the next column declaration when we split on ``,``.
    cleaned = "\n".join(raw.split("--", 1)[0] for raw in body.splitlines())
    for raw in cleaned.split(","):
        line = raw.strip()
        if line:
            yield line


def _extract_paren_body(text: str, start: int) -> str:
    """Return the contents of a parenthesised body starting at ``start``.

    Walks character by character tracking paren depth so nested ``()`` (e.g.
    ``CHECK (...)`` expressions) don't terminate the body early.
    """
    depth = 1
    i = start
    while i < len(text) and depth > 0:
        ch = text[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[start:i]
        i += 1
    raise AssertionError(
        "Could not find matching close paren for CREATE TABLE body — the body "
        "extractor walked off the end of the schema. Rewrite _extract_paren_body."
    )
