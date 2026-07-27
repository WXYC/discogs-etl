"""Integration tests for ``lib/library_release_overrides.py`` against real
PostgreSQL, for discogs-etl#329's one-time backfill preparation.

Builds an ad hoc ``lml_cache.library_release_override`` table directly
(mirroring how LML bootstraps it itself -- discogs-etl never migrates or owns
that schema, per CLAUDE.md "entity.* schema ownership"), matching the pattern
established in ``tests/integration/test_keep_release_ids_end_to_end.py``.
"""

from __future__ import annotations

import psycopg
import pytest

from lib.library_release_overrides import (
    OverrideSourceSummary,
    fetch_missing_release_ids,
    fetch_override_summary,
)

pytestmark = [pytest.mark.pg]


def _set_up_schema(conn, *, nullable_release_id: bool = False) -> None:
    # ``discogs_release_id`` is nullable in LML's real table (a pin can be
    # recorded before its Discogs release_id is resolved); the pipeline's
    # ``write_keep_release_ids`` explicitly skips NULLs so they never reach the
    # consuming ``int()``. ``nullable_release_id=True`` reproduces that shape so
    # the query definitions here can be tested against the same NULLs.
    release_id_col = "discogs_release_id integer" + ("" if nullable_release_id else " NOT NULL")
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE release (id integer PRIMARY KEY, title text)")
        cur.execute("CREATE SCHEMA lml_cache")
        cur.execute(f"""
            CREATE TABLE lml_cache.library_release_override (
                library_id integer NOT NULL,
                {release_id_col},
                source text NOT NULL
            )
        """)


class TestFetchOverrideSummary:
    def test_reports_pinned_and_missing_per_source(self, fresh_db_url: str) -> None:
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn)
        with conn.cursor() as cur:
            # release 1 is present in the cache; release 2 is not.
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Present')")
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES "
                "(10, 1, 'alex-l-2026'), (11, 2, 'alex-l-2026'), (12, 3, 'alex-l-2026-masters-api')"
            )

        summary = fetch_override_summary(conn)
        conn.close()

        by_source = {row.source: row for row in summary}
        assert by_source["alex-l-2026"] == OverrideSourceSummary(
            source="alex-l-2026", pinned=2, missing_from_cache=1
        )
        assert by_source["alex-l-2026-masters-api"] == OverrideSourceSummary(
            source="alex-l-2026-masters-api", pinned=1, missing_from_cache=1
        )

    def test_zero_missing_when_every_pin_is_cached(self, fresh_db_url: str) -> None:
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Present')")
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES (10, 1, 'alex-l-2026')"
            )

        summary = fetch_override_summary(conn)
        conn.close()

        assert summary == [
            OverrideSourceSummary(source="alex-l-2026", pinned=1, missing_from_cache=0)
        ]

    def test_distinct_release_id_counted_once_despite_multiple_library_pins(
        self, fresh_db_url: str
    ) -> None:
        """Two library items pinned to the same discogs_release_id must count
        as one pinned release, matching #327's ``count(DISTINCT ...)`` query."""
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES "
                "(10, 99, 'alex-l-2026'), (11, 99, 'alex-l-2026')"
            )

        summary = fetch_override_summary(conn)
        conn.close()

        assert summary == [
            OverrideSourceSummary(source="alex-l-2026", pinned=1, missing_from_cache=1)
        ]


class TestFetchMissingReleaseIds:
    def test_returns_only_ids_absent_from_release(self, fresh_db_url: str) -> None:
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Present')")
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES "
                "(10, 1, 'alex-l-2026'), (11, 2, 'alex-l-2026'), (12, 3, 'alex-l-2026-masters-api')"
            )

        missing = fetch_missing_release_ids(conn)
        conn.close()

        assert missing == [2, 3]

    def test_null_release_id_never_surfaces_as_a_missing_id(self, fresh_db_url: str) -> None:
        """A NULL ``discogs_release_id`` must not leak into the ids-file: it is
        unmatched by the LEFT JOIN (so it survives ``r.id IS NULL``) but a None
        entry serializes as the literal ``"None"``, which the seeder's
        ``int()`` parser rejects. Mirrors the pipeline's ``write_keep_release_ids``
        NULL guard."""
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn, nullable_release_id=True)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Present')")
            cur.execute(
                "INSERT INTO lml_cache.library_release_override "
                "(library_id, discogs_release_id, source) VALUES "
                "(10, 1, 'alex-l-2026'), (11, 2, 'alex-l-2026'), (12, NULL, 'alex-l-2026')"
            )

        missing = fetch_missing_release_ids(conn)
        conn.close()

        assert missing == [2]
        assert None not in missing

    def test_empty_when_no_pins_exist(self, fresh_db_url: str) -> None:
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        _set_up_schema(conn)

        missing = fetch_missing_release_ids(conn)
        conn.close()

        assert missing == []
