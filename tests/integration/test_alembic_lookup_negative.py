"""Verify the 0006_lookup_negative migration creates the negative-cache table.

`lookup_negative` is consumed by library-metadata-lookup
(`discogs/cache_service.py`) to short-circuit Discogs API calls for queries
that previously returned no results. The schema contract is exercised here
so a downstream change that drops a column (e.g. `ttl_seconds`) trips this
test rather than silently breaking LML.

Tracked by WXYC/library-metadata-lookup#341 / Backend-Service epic G/A
(post-launch service hardening).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


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


@pytest.mark.pg
def test_lookup_negative_table_created_at_head(db_url: str) -> None:
    # Stamp at 0005 to skip 0001–0005 (they require the production cache
    # image's wxyc_unaccent.rules tsearch_data file, which isn't present on
    # a base postgres test container). 0006 is self-contained — no FK to
    # any earlier table — so the standalone apply is valid coverage.
    stamp = _run_alembic(["stamp", "0005_release_track_artist_role"], db_url)
    assert stamp.returncode == 0, (
        f"alembic stamp failed:\nstdout: {stamp.stdout}\nstderr: {stamp.stderr}"
    )

    result = _run_alembic(["upgrade", "head"], db_url)
    assert result.returncode == 0, (
        f"alembic upgrade head failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.lookup_negative')")
        assert cur.fetchone() == ("lookup_negative",), "lookup_negative table missing after upgrade"

        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'lookup_negative'
            ORDER BY ordinal_position
            """
        )
        rows = cur.fetchall()
        cols = {row[0]: (row[1], row[2], row[3]) for row in rows}

        # Schema contract — must mirror schema/create_database.sql exactly so
        # a fresh-rebuild end state and an alembic-upgrade end state agree
        # (the discogs-etl dual-write convention).
        assert "key_hash" in cols and cols["key_hash"][0] == "bytea", (
            f"key_hash bytea missing or wrong type: {cols.get('key_hash')}"
        )
        assert "artist" in cols and cols["artist"][0] == "text", (
            f"artist text missing or wrong type: {cols.get('artist')}"
        )
        assert "track" in cols and cols["track"][0] == "text", (
            f"track text missing or wrong type: {cols.get('track')}"
        )
        assert "artist_as_keyword" in cols and cols["artist_as_keyword"][0] == "boolean", (
            f"artist_as_keyword boolean missing or wrong type: {cols.get('artist_as_keyword')}"
        )
        assert "attempted_at" in cols, "attempted_at column missing"
        assert cols["attempted_at"][1] == "NO", "attempted_at must be NOT NULL"
        assert "now()" in (cols["attempted_at"][2] or ""), (
            f"attempted_at default must be now(): got {cols['attempted_at'][2]!r}"
        )
        assert "ttl_seconds" in cols and cols["ttl_seconds"][0] == "integer", (
            f"ttl_seconds integer missing or wrong type: {cols.get('ttl_seconds')}"
        )
        assert cols["ttl_seconds"][1] == "NO", "ttl_seconds must be NOT NULL"

        # key_hash is the primary key.
        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'public.lookup_negative'::regclass AND i.indisprimary
            """
        )
        pk_cols = {row[0] for row in cur.fetchall()}
        assert pk_cols == {"key_hash"}, f"primary key should be {{key_hash}}, got {pk_cols}"

        # attempted_at index supports TTL sweeps.
        cur.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename = 'lookup_negative' AND indexname = 'idx_lookup_negative_attempted_at'"
        )
        assert cur.fetchone() is not None, "attempted_at index missing for TTL sweep"


@pytest.mark.pg
def test_lookup_negative_round_trip_insert_then_query(db_url: str) -> None:
    # End-to-end sanity check: post-upgrade, the table accepts a row with the
    # documented shape and reads it back. Catches DDL-vs-DML drift if e.g.
    # the migration ever flips key_hash to non-PK or makes attempted_at
    # nullable.
    stamp = _run_alembic(["stamp", "0005_release_track_artist_role"], db_url)
    assert stamp.returncode == 0, f"alembic stamp failed: {stamp.stderr}"
    result = _run_alembic(["upgrade", "head"], db_url)
    assert result.returncode == 0

    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO lookup_negative
              (key_hash, artist, track, artist_as_keyword, ttl_seconds)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                b"\x01" * 32,
                "Juana Molina",
                "la paradoja",
                False,
                604800,
            ),
        )
        conn.commit()

        cur.execute(
            "SELECT artist, track, artist_as_keyword, ttl_seconds "
            "FROM lookup_negative WHERE key_hash = %s",
            (b"\x01" * 32,),
        )
        row = cur.fetchone()
        assert row == ("Juana Molina", "la paradoja", False, 604800)
