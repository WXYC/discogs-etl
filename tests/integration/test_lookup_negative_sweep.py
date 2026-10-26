"""The lookup_negative TTL sweep (#438) deletes exactly the rows LML's reader ignores.

The table DDL mirrors migration 0006 (``alembic/versions/0006_lookup_negative.py``).
"""

from __future__ import annotations

import logging

import psycopg
import pytest

from scripts import run_pipeline

DDL = """
CREATE TABLE lookup_negative (
    key_hash          bytea PRIMARY KEY,
    artist            text,
    track             text NOT NULL,
    artist_as_keyword boolean NOT NULL,
    attempted_at      timestamptz NOT NULL DEFAULT now(),
    ttl_seconds       integer NOT NULL DEFAULT 604800
)
"""


@pytest.mark.pg
def test_sweep_deletes_only_expired_rows(fresh_db_url: str, caplog) -> None:
    with psycopg.connect(fresh_db_url, autocommit=True) as conn:
        conn.execute(DDL)
        conn.execute(
            """
            INSERT INTO lookup_negative (key_hash, track, artist_as_keyword, attempted_at, ttl_seconds)
            VALUES
              ('\\x01', 'expired', false, now() - interval '30 days', 604800),
              ('\\x02', 'live', false, now(), 604800),
              ('\\x03', 'old but long ttl', false, now() - interval '30 days', 31536000)
            """
        )

    with caplog.at_level(logging.INFO, logger=run_pipeline.logger.name):
        assert run_pipeline.run_sweep_lookup_negative_step(fresh_db_url) is True
    assert any("deleted 1 expired" in r.getMessage() for r in caplog.records)

    with psycopg.connect(fresh_db_url) as conn:
        tracks = {r[0] for r in conn.execute("SELECT track FROM lookup_negative")}
        assert tracks == {"live", "old but long ttl"}
        persistence = conn.execute(
            "SELECT relpersistence FROM pg_class WHERE relname = 'lookup_negative'"
        ).fetchone()
        assert persistence == ("p",)

    caplog.clear()
    with caplog.at_level(logging.INFO, logger=run_pipeline.logger.name):
        assert run_pipeline.run_sweep_lookup_negative_step(fresh_db_url) is True
    assert any("deleted 0 expired" in r.getMessage() for r in caplog.records)
