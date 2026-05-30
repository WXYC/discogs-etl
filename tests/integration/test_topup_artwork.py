"""LML#221 top-up drain — integration tests.

Drives ``scripts/topup_artwork.py`` against an alembic-upgraded discogs-cache
schema with a fake Discogs client. Each fake-client closure makes the
release-id → response decision deterministic so the per-row outcome buckets
(``with_artwork`` / ``without_artwork`` / ``deleted`` / ``failed``) can be
asserted directly without HTTP mocking machinery.

The cache shape under test is the post-0008 schema: ``release.artwork_url``
+ ``release.artwork_checked_at`` + the ``release_artwork_null_idx`` partial
index that scopes the drain's candidate query.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg
import pytest

from scripts.topup_artwork import (
    TokenBucket,
    fetch_pending_ids,
    run_topup,
    write_artwork_result,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = REPO_ROOT / "schema"


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
def cache_db(fresh_db_url: str) -> str:
    """An alembic-head discogs-cache.

    The drain relies on the 0008-added ``artwork_checked_at`` column + partial
    index. Applying the canonical schema first then stamping at 0007 + upgrading
    head mirrors the legacy → alembic bridge tested in
    ``test_alembic_0008_artwork_checked_at.py``.
    """
    with psycopg.connect(fresh_db_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    stamp = _run_alembic(["stamp", "0007_wxyc_postgres_image_gate"], fresh_db_url)
    assert stamp.returncode == 0, stamp.stderr
    upgrade = _run_alembic(["upgrade", "head"], fresh_db_url)
    assert upgrade.returncode == 0, upgrade.stderr
    return fresh_db_url


def _seed_releases(db_url: str, rows: list[tuple[int, str | None, str | None]]) -> None:
    """Insert release rows: (id, artwork_url, artwork_checked_at_iso)."""
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        for release_id, artwork_url, checked_at in rows:
            cur.execute(
                """
                INSERT INTO release
                    (id, title, country, released, format,
                     artwork_url, artwork_checked_at)
                VALUES (%s, 'fixture', 'US', NULL, 'LP', %s, %s)
                """,
                (release_id, artwork_url, checked_at),
            )
        conn.commit()


def _no_sleep_bucket() -> TokenBucket:
    """A bucket whose ``acquire()`` is a no-op (tests should not block)."""
    bucket = TokenBucket(rate_per_minute=60_000)
    bucket.acquire = lambda *a, **kw: None  # type: ignore[assignment]
    return bucket


def _client_from(
    responses: dict[int, dict[str, Any] | Exception | None],
) -> Callable[[int], dict[str, Any] | None]:
    """Build a deterministic Discogs client.

    ``responses[release_id]`` selects the outcome:
        * ``dict`` → 200 JSON payload
        * ``None`` → 404 (release withdrawn)
        * ``Exception`` → raised (network / 5xx / persistent 429)
    """

    def fetch(release_id: int) -> dict[str, Any] | None:
        outcome = responses[release_id]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return fetch


# ---------------------------------------------------------------------------
# DB helpers — direct unit coverage of the SELECT + UPDATE shapes
# ---------------------------------------------------------------------------


@pytest.mark.pg
class TestFetchPendingIds:
    def test_returns_only_rows_with_both_columns_null(self, cache_db: str) -> None:
        _seed_releases(
            cache_db,
            [
                (5001, None, None),  # eligible
                (5002, "https://img/x.jpg", None),  # has artwork — skip
                (5003, None, "2026-05-30T00:00:00Z"),  # already checked — skip
                (5004, None, None),  # eligible
            ],
        )
        with psycopg.connect(cache_db) as conn:
            assert fetch_pending_ids(conn, limit=10) == [5001, 5004]

    def test_caps_at_limit(self, cache_db: str) -> None:
        _seed_releases(cache_db, [(5001, None, None), (5002, None, None), (5003, None, None)])
        with psycopg.connect(cache_db) as conn:
            assert fetch_pending_ids(conn, limit=2) == [5001, 5002]

    def test_orders_by_id_for_resumable_runs(self, cache_db: str) -> None:
        # Insert out-of-order to verify the ORDER BY clause.
        _seed_releases(cache_db, [(5099, None, None), (5001, None, None), (5050, None, None)])
        with psycopg.connect(cache_db) as conn:
            assert fetch_pending_ids(conn, limit=10) == [5001, 5050, 5099]


@pytest.mark.pg
class TestWriteArtworkResult:
    def test_sets_url_and_stamps_timestamp(self, cache_db: str) -> None:
        _seed_releases(cache_db, [(5001, None, None)])
        with psycopg.connect(cache_db) as conn:
            write_artwork_result(conn, 5001, "https://img/doga.jpg")
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT artwork_url, artwork_checked_at FROM release WHERE id = %s",
                    (5001,),
                )
                url, checked_at = cur.fetchone()
        assert url == "https://img/doga.jpg"
        assert checked_at is not None, (
            "artwork_checked_at must be stamped — that's how LML disambiguates "
            "'asked, no image' from 'never asked' per migration 0008."
        )

    def test_null_url_still_stamps_timestamp(self, cache_db: str) -> None:
        """The 'asked Discogs, genuinely no image' verdict.

        Stamping ``artwork_checked_at`` with ``artwork_url`` left NULL is the
        load-bearing signal that LML's cache-hit predicate uses to stop
        re-asking Discogs for releases that have no cover.
        """
        _seed_releases(cache_db, [(5002, None, None)])
        with psycopg.connect(cache_db) as conn:
            write_artwork_result(conn, 5002, None)
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT artwork_url, artwork_checked_at FROM release WHERE id = %s",
                    (5002,),
                )
                url, checked_at = cur.fetchone()
        assert url is None
        assert checked_at is not None


# ---------------------------------------------------------------------------
# End-to-end: run_topup orchestrates the loop
# ---------------------------------------------------------------------------


@pytest.mark.pg
class TestRunTopup:
    def test_writes_back_all_outcomes_and_counts_per_bucket(self, cache_db: str) -> None:
        """Three pending rows hit three different outcomes in one drain pass."""
        _seed_releases(
            cache_db,
            [
                (5001, None, None),  # has artwork
                (5002, None, None),  # imageless
                (5003, None, None),  # deleted (404)
            ],
        )
        client = _client_from(
            {
                5001: {"id": 5001, "images": [{"type": "primary", "uri": "https://img/x.jpg"}]},
                5002: {"id": 5002, "images": []},
                5003: None,
            }
        )

        summary = run_topup(
            cache_db,
            limit=10,
            rate_per_minute=60,
            batch_size=100,
            dry_run=False,
            discogs_client=client,
            bucket=_no_sleep_bucket(),
        )

        assert summary.candidates == 3
        assert summary.fetched == 2
        assert summary.with_artwork == 1
        assert summary.without_artwork == 1
        assert summary.deleted == 1
        assert summary.failed == 0
        assert summary.updated == 3

        with psycopg.connect(cache_db) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, artwork_url, artwork_checked_at IS NOT NULL FROM release ORDER BY id"
            )
            rows = cur.fetchall()
        assert rows == [
            (5001, "https://img/x.jpg", True),
            (5002, None, True),
            (5003, None, True),
        ]

    def test_failed_fetch_leaves_row_untouched_for_retry(self, cache_db: str) -> None:
        """Network failures must not stamp the row.

        A stamped row would be invisible to the next invocation's partial-index
        scan, so a single transient blip would permanently mark the release as
        'checked, no image'. Leaving it untouched preserves idempotent retry.
        """
        _seed_releases(cache_db, [(5010, None, None)])
        client = _client_from({5010: RuntimeError("transient discogs 500")})

        summary = run_topup(
            cache_db,
            limit=10,
            rate_per_minute=60,
            batch_size=100,
            dry_run=False,
            discogs_client=client,
            bucket=_no_sleep_bucket(),
        )

        assert summary.failed == 1
        assert summary.updated == 0
        with psycopg.connect(cache_db) as conn, conn.cursor() as cur:
            cur.execute("SELECT artwork_url, artwork_checked_at FROM release WHERE id = 5010")
            url, checked_at = cur.fetchone()
        assert url is None
        assert checked_at is None, (
            "A row that failed mid-drain must remain in the never-asked bucket "
            "so the next invocation retries it via the partial-index scan."
        )

    def test_dry_run_records_outcomes_without_writing(self, cache_db: str) -> None:
        _seed_releases(cache_db, [(5020, None, None), (5021, None, None)])
        client = _client_from(
            {
                5020: {"id": 5020, "images": [{"type": "primary", "uri": "https://img/y.jpg"}]},
                5021: {"id": 5021, "images": []},
            }
        )

        summary = run_topup(
            cache_db,
            limit=10,
            rate_per_minute=60,
            batch_size=100,
            dry_run=True,
            discogs_client=client,
            bucket=_no_sleep_bucket(),
        )

        assert summary.candidates == 2
        assert summary.fetched == 2
        assert summary.with_artwork == 1
        assert summary.without_artwork == 1
        assert summary.updated == 0, "dry-run must not write back"
        with psycopg.connect(cache_db) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT artwork_url IS NULL, artwork_checked_at IS NULL FROM release ORDER BY id"
            )
            assert cur.fetchall() == [(True, True), (True, True)]

    def test_limit_caps_candidates(self, cache_db: str) -> None:
        _seed_releases(
            cache_db,
            [(5030, None, None), (5031, None, None), (5032, None, None)],
        )
        client = _client_from({rid: {"id": rid, "images": []} for rid in (5030, 5031, 5032)})

        summary = run_topup(
            cache_db,
            limit=2,
            rate_per_minute=60,
            batch_size=100,
            dry_run=False,
            discogs_client=client,
            bucket=_no_sleep_bucket(),
        )

        assert summary.candidates == 2
        assert summary.updated == 2
        with psycopg.connect(cache_db) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM release WHERE artwork_checked_at IS NULL")
            (still_pending,) = cur.fetchone()
        assert still_pending == 1, (
            "--limit must leave the unprocessed tail eligible for the next "
            "invocation; otherwise a single bounded slice silently consumes "
            "the entire backlog by stamping it untouched."
        )
