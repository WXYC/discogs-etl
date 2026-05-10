"""WXYC library hook loader for the Docker discogs cache.

Implements E1 §4.1.1 of the cross-cache-identity plan:
https://github.com/WXYC/wiki/blob/main/plans/library-hook-canonicalization.md#411-docker-discogs-port-5433-dev-subset

- :func:`populate_wxyc_library_v2` — the consolidated loader per §3.1.
  Target table ``wxyc_library``. Loads ALL library rows (Option B from
  ``WXYC/catalog-audits#11``); no filter. Per the wiki §4.1.1 amendment,
  this cache is schema-validation only — there is no in-repo legacy
  predecessor and no dual-write window to police.

The loader reads from a SQLite ``library.db``, is idempotent
(``ON CONFLICT (library_id) DO NOTHING``), and writes the normalization
columns required by the new schema.

Normalization
=============

The new schema requires ``norm_artist`` / ``norm_title`` / ``norm_label`` —
populated by ``wxyc_etl.text`` (the canonical Rust/PyO3 implementation of
the plan §3.3.2 algorithm; lives in ``wxyc-etl`` ≥ 0.3.0).

Per the plan, the loader uses two sibling functions:

- :func:`wxyc_etl.text.to_identity_match_form` — locked-on baseline. Used
  for ``norm_artist`` AND ``norm_label`` (labels share the artist-side
  pipeline; no ``_label`` variant exists or is needed).
- :func:`wxyc_etl.text.to_identity_match_form_title` — title-side variant.
  Used for ``norm_title``.

The opt-in variants (``_with_punctuation``, ``_with_disambiguator_strip``)
are deliberately not invoked here — the cross-cache-identity hook stays on
the locked-on baseline so every consumer cache normalizes identically.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from wxyc_etl.text import to_identity_match_form, to_identity_match_form_title

from lib.pg_text import strip_pg_null_bytes

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------
# Identifier preserved for log messages and integration-test sentinels.
NORMALIZER_NAME = "wxyc_etl.text.to_identity_match_form"


def _norm_label(value: str | None) -> str | None:
    """Identity-tier normalization for the optional label column.

    The PyO3 binding accepts ``Option<&str>`` and returns an empty string for
    ``None``; we want NULL to flow through to PostgreSQL for the nullable
    ``norm_label`` column, so re-introduce the None at the boundary.
    """
    if value is None:
        return None
    return to_identity_match_form(value)


# ---------------------------------------------------------------------------
# Row model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LibraryRow:
    """A single row for the wxyc_library hook.

    Mirrors §3.1's column list. ``artist_id`` / ``label_id`` / ``format_id``
    / ``release_year`` are nullable: per-cache loaders populate what their
    source exposes, and library.db (the SQLite catalog export this loader
    reads) does not carry Backend's integer IDs.
    """

    library_id: int
    artist_name: str
    album_title: str
    artist_id: int | None = None
    label_id: int | None = None
    label_name: str | None = None
    format_id: int | None = None
    format_name: str | None = None
    wxyc_genre: str | None = None
    call_letters: str | None = None
    call_numbers: int | None = None
    release_year: int | None = None


# ---------------------------------------------------------------------------
# Reading library.db
# ---------------------------------------------------------------------------


# library.db is produced by wxyc-catalog. The minimal-fixture schema is
# (id, artist, title, format); the prod schema has more (genre, call_letters,
# etc.). We adapt to whatever columns are present rather than failing.
_OPTIONAL_COLUMNS = (
    "label",
    "genre",
    "call_letters",
    "release_call_number",
    "format",
)


def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _read_library_db(library_db_path: Path) -> list[LibraryRow]:
    """Read every row from a library.db SQLite file into LibraryRow records."""
    if not library_db_path.exists():
        raise FileNotFoundError(f"library.db not found at {library_db_path}")

    with sqlite3.connect(library_db_path) as conn:
        conn.row_factory = sqlite3.Row
        cols = _existing_columns(conn, "library")

        select_parts = ["id", "artist", "title"]
        for c in _OPTIONAL_COLUMNS:
            if c in cols:
                select_parts.append(c)
        query = f"SELECT {', '.join(select_parts)} FROM library"  # noqa: S608

        rows: list[LibraryRow] = []
        for row in conn.execute(query):
            data: dict[str, Any] = dict(row)
            rows.append(
                LibraryRow(
                    library_id=int(data["id"]),
                    artist_name=str(data["artist"]),
                    album_title=str(data["title"]),
                    label_name=data.get("label"),
                    format_name=data.get("format"),
                    wxyc_genre=data.get("genre"),
                    call_letters=data.get("call_letters"),
                    call_numbers=(
                        int(data["release_call_number"])
                        if data.get("release_call_number") is not None
                        else None
                    ),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


_INSERT_V2 = sql.SQL(
    """
    INSERT INTO {table} (
        library_id, artist_id, artist_name, album_title,
        label_id, label_name, format_id, format_name,
        wxyc_genre, call_letters, call_numbers, release_year,
        norm_artist, norm_title, norm_label,
        snapshot_at, snapshot_source
    ) VALUES (
        %(library_id)s, %(artist_id)s, %(artist_name)s, %(album_title)s,
        %(label_id)s, %(label_name)s, %(format_id)s, %(format_name)s,
        %(wxyc_genre)s, %(call_letters)s, %(call_numbers)s, %(release_year)s,
        %(norm_artist)s, %(norm_title)s, %(norm_label)s,
        %(snapshot_at)s, %(snapshot_source)s
    )
    ON CONFLICT (library_id) DO NOTHING
    """
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def populate_wxyc_library_v2(
    pg_conn: psycopg.Connection,
    library_db_path: Path,
    *,
    snapshot_source: str = "backend",
    table: str = "wxyc_library",
    snapshot_at: datetime | None = None,
) -> int:
    """Populate the consolidated hook table from library.db.

    Per E1 §4.1.1 + §3.1: every library row is written (Option B; no filter).
    Idempotent on ``library_id`` (``ON CONFLICT DO NOTHING``).

    Args:
        pg_conn: psycopg connection to the discogs cache database.
        library_db_path: SQLite library.db produced by ``wxyc-export-to-sqlite``.
        snapshot_source: ``backend`` | ``tubafrenzy`` | ``llm`` per §3.1.
        table: target table name. Override only for tests.
        snapshot_at: timestamp to stamp on every row. Defaults to ``now()``.

    Returns the number of rows attempted (pre-conflict).
    """
    if snapshot_source not in ("backend", "tubafrenzy", "llm"):
        raise ValueError(f"snapshot_source must be backend|tubafrenzy|llm, got {snapshot_source!r}")

    rows = _read_library_db(library_db_path)
    if not rows:
        logger.warning("populate_wxyc_library_v2: no rows from %s", library_db_path)
        return 0

    stamp = snapshot_at or datetime.now(timezone.utc)
    payload = [
        {
            "library_id": r.library_id,
            "artist_id": r.artist_id,
            "artist_name": strip_pg_null_bytes(r.artist_name),
            "album_title": strip_pg_null_bytes(r.album_title),
            "label_id": r.label_id,
            "label_name": strip_pg_null_bytes(r.label_name),
            "format_id": r.format_id,
            "format_name": strip_pg_null_bytes(r.format_name),
            "wxyc_genre": strip_pg_null_bytes(r.wxyc_genre),
            "call_letters": strip_pg_null_bytes(r.call_letters),
            "call_numbers": r.call_numbers,
            "release_year": r.release_year,
            # norm_artist / norm_title are NOT NULL per §3.1; the normalizer
            # collapses to a non-empty string for any non-empty input. If it
            # ever returns an empty string for a real artist/title, that's a
            # bug worth crashing on — no `or ""` fallback.
            "norm_artist": to_identity_match_form(r.artist_name),
            "norm_title": to_identity_match_form_title(r.album_title),
            "norm_label": _norm_label(r.label_name),
            "snapshot_at": stamp,
            "snapshot_source": snapshot_source,
        }
        for r in rows
    ]

    stmt = _INSERT_V2.format(table=sql.Identifier(table))
    with pg_conn.cursor() as cur:
        cur.executemany(stmt, payload)
    pg_conn.commit()

    logger.info(
        "populate_wxyc_library_v2: wrote %d rows to %s (snapshot_source=%s, normalizer=%s)",
        len(rows),
        table,
        snapshot_source,
        NORMALIZER_NAME,
    )
    return len(rows)
