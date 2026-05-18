"""lookup_negative: negative-result cache for Discogs lookups

Adds ``lookup_negative`` so library-metadata-lookup (LML) can persist a
"we asked Discogs and got nothing" verdict across LML process restarts.
Without this, every Railway redeploy wipes LML's in-memory cache and the
next request for a not-on-Discogs artist pays the full Discogs API cost
(~16 s for the cascade). With 3-5 deploys/day in prod, the cache stays
semi-warm at best.

The LML read path consults this table after the in-memory cache and
positive PG cache, before falling through to the Discogs API. The LML
write path inserts on every Discogs empty response. TTL is enforced by
the LML query (``WHERE now() < attempted_at + (ttl_seconds * interval
'1 second')``), so the read path naturally serves only non-expired rows.

Implementation lives in WXYC/library-metadata-lookup PR closing #341 (the
A4 ticket). Companion E1 (BS#901) surfaces ``cache_stats.pg_negative_hits``
and ``cache_stats.pg_negative_misses`` on the Sentry trace.

Schema:

| Column              | Type        | Notes |
|---------------------|-------------|-------|
| ``key_hash``        | bytea PK    | Hash of normalized (artist, track, artist_as_keyword) per LML's cache-key normalizer (LML#250 / A5). |
| ``artist``          | text NULL   | Original artist string — kept for forensics / debugging only; the key_hash is the lookup. |
| ``track``           | text NULL   | Original track string — same rationale. |
| ``artist_as_keyword`` | boolean NULL | The LML key dimension that distinguishes ``artist=Foo`` from ``q=Foo`` style lookups. Forensic. |
| ``attempted_at``    | timestamptz NOT NULL DEFAULT now() | When the negative verdict was first written. |
| ``ttl_seconds``     | integer NOT NULL DEFAULT 604800 | Per-row TTL so the LML side can vary policy per call shape if needed. 7 days = 604800 s. |

The ``idx_lookup_negative_attempted_at`` index supports a future TTL-sweep
cron (delete rows where ``now() > attempted_at + ttl_seconds * interval
'1 second'``). LML's read path uses the same predicate inline so the index
also serves correctness queries.

This migration uses the standard ``op.execute`` pattern (CREATE TABLE +
CREATE INDEX, both ``IF NOT EXISTS`` so the migration is idempotent
relative to the dual-written ``schema/create_database.sql``). No
``is_offline_mode()`` guard needed because ``op.execute`` is intercepted
by alembic's ``--sql`` machinery — the 0001/0005 side-channel pattern
isn't required here.

Revision ID: 0006_lookup_negative
Revises: 0005_release_track_artist_role
Create Date: 2026-05-17

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0006_lookup_negative"
down_revision: str | Sequence[str] | None = "0005_release_track_artist_role"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_UPGRADE_SQL = """
CREATE TABLE IF NOT EXISTS lookup_negative (
    key_hash          bytea PRIMARY KEY,
    artist            text,
    track             text,
    artist_as_keyword boolean,
    attempted_at      timestamptz NOT NULL DEFAULT now(),
    ttl_seconds       integer NOT NULL DEFAULT 604800
);

CREATE INDEX IF NOT EXISTS idx_lookup_negative_attempted_at
    ON lookup_negative (attempted_at);
"""

_DOWNGRADE_SQL = """
DROP TABLE IF EXISTS lookup_negative;
"""


def upgrade() -> None:
    op.execute(_UPGRADE_SQL)


def downgrade() -> None:
    op.execute(_DOWNGRADE_SQL)
