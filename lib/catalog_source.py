"""Catalog source abstraction for querying the WXYC library catalog.

Provides a Protocol with two implementations:
- TubafrenzySource: queries tubafrenzy's MySQL database
- BackendServiceSource: queries Backend-Service's PostgreSQL database

Factory function create_catalog_source() selects the implementation by name.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

import psycopg

from lib.wxyc import connect_mysql

logger = logging.getLogger(__name__)


@runtime_checkable
class CatalogSource(Protocol):
    """Read-only interface to the WXYC library catalog."""

    def fetch_library_rows(self) -> list[dict[str, Any]]:
        """Return library rows. Keys: id, title, artist, call_letters,
        artist_call_number, release_call_number, genre, format, alternate_artist_name."""
        ...

    def fetch_alternate_names(self) -> set[str]:
        """Return alternate artist names (e.g., 'Body Count' filed under Ice-T)."""
        ...

    def fetch_cross_referenced_artists(self) -> set[str]:
        """Return artist names from both sides of the artist crossreference table."""
        ...

    def fetch_release_cross_ref_artists(self) -> set[str]:
        """Return artist names from library-level crossreferences."""
        ...

    def fetch_library_labels(self) -> set[tuple[str, str, str]]:
        """Return (artist_name, release_title, label_name) triples from flowsheet plays."""
        ...

    def fetch_compilation_track_artists(self) -> list[dict[str, Any]]:
        """Return per-track artist credits from compilation releases.

        Keys: library_release_id, artist_name, track_title.
        Returns empty list if the table does not exist.
        """
        ...

    def close(self) -> None:
        """Release the underlying database connection."""
        ...


def _strip_name_rows(rows) -> set[str]:
    """Extract stripped non-empty names from single-column result rows."""
    return {row[0].strip() for row in rows if row[0] and row[0].strip()}


def _strip_label_rows(rows) -> set[tuple[str, str, str]]:
    """Extract stripped non-empty (artist, title, label) triples from result rows."""
    result: set[tuple[str, str, str]] = set()
    for artist, title, label in rows:
        if not artist or not title or not label:
            continue
        a, t, la = artist.strip(), title.strip(), label.strip()
        if a and t and la:
            result.add((a, t, la))
    return result


class TubafrenzySource:
    """Catalog source backed by tubafrenzy's MySQL database."""

    def __init__(self, db_url: str) -> None:
        self._conn = connect_mysql(db_url)

    def __enter__(self) -> TubafrenzySource:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def fetch_library_rows(self) -> list[dict[str, Any]]:
        columns = [
            "id",
            "title",
            "artist",
            "call_letters",
            "artist_call_number",
            "release_call_number",
            "genre",
            "format",
            "alternate_artist_name",
        ]
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT
                    r.ID, r.TITLE, lc.PRESENTATION_NAME, lc.CALL_LETTERS,
                    lc.CALL_NUMBERS, r.CALL_NUMBERS, g.REFERENCE_NAME,
                    f.REFERENCE_NAME, r.ALTERNATE_ARTIST_NAME
                FROM LIBRARY_RELEASE r
                JOIN LIBRARY_CODE lc ON r.LIBRARY_CODE_ID = lc.ID
                JOIN FORMAT f ON r.FORMAT_ID = f.ID
                JOIN GENRE g ON lc.GENRE_ID = g.ID
            """)
            rows = cur.fetchall()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def fetch_alternate_names(self) -> set[str]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ALTERNATE_ARTIST_NAME
                FROM LIBRARY_RELEASE
                WHERE ALTERNATE_ARTIST_NAME IS NOT NULL
                  AND ALTERNATE_ARTIST_NAME != ''
            """)
            return _strip_name_rows(cur.fetchall())

    def fetch_cross_referenced_artists(self) -> set[str]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT lc.PRESENTATION_NAME
                FROM LIBRARY_CODE_CROSS_REFERENCE cr
                JOIN LIBRARY_CODE lc ON lc.ID = cr.CROSS_REFERENCING_ARTIST_ID
                UNION
                SELECT DISTINCT lc.PRESENTATION_NAME
                FROM LIBRARY_CODE_CROSS_REFERENCE cr
                JOIN LIBRARY_CODE lc ON lc.ID = cr.CROSS_REFERENCED_LIBRARY_CODE_ID
            """)
            return _strip_name_rows(cur.fetchall())

    def fetch_release_cross_ref_artists(self) -> set[str]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT lc.PRESENTATION_NAME
                FROM RELEASE_CROSS_REFERENCE rcr
                JOIN LIBRARY_CODE lc ON lc.ID = rcr.CROSS_REFERENCING_ARTIST_ID
            """)
            return _strip_name_rows(cur.fetchall())

    def fetch_library_labels(self) -> set[tuple[str, str, str]]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT
                    lc.PRESENTATION_NAME, lr.TITLE, fe.LABEL_NAME
                FROM FLOWSHEET_ENTRY_PROD fe
                JOIN LIBRARY_RELEASE lr ON fe.LIBRARY_RELEASE_ID = lr.ID
                JOIN LIBRARY_CODE lc ON lr.LIBRARY_CODE_ID = lc.ID
                WHERE fe.LABEL_NAME IS NOT NULL
                  AND fe.LABEL_NAME != ''
                  AND fe.LIBRARY_RELEASE_ID > 0
            """)
            return _strip_label_rows(cur.fetchall())

    def fetch_compilation_track_artists(self) -> list[dict[str, Any]]:
        columns = ["library_release_id", "artist_name", "track_title"]
        try:
            with self._conn.cursor() as cur:
                cur.execute("""
                    SELECT LIBRARY_RELEASE_ID, ARTIST_NAME, TRACK_TITLE
                    FROM COMPILATION_TRACK_ARTIST
                    ORDER BY LIBRARY_RELEASE_ID
                """)
                rows = cur.fetchall()
            return [dict(zip(columns, row, strict=True)) for row in rows]
        except Exception:
            logger.info("COMPILATION_TRACK_ARTIST table not found, skipping")
            return []

    def close(self) -> None:
        self._conn.close()


class BackendServiceSource:
    """Catalog source backed by Backend-Service's PostgreSQL database."""

    def __init__(self, db_url: str) -> None:
        self._conn = psycopg.connect(db_url)

    def __enter__(self) -> BackendServiceSource:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def fetch_library_rows(self) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT
                    l.id, l.album_title AS title, a.artist_name AS artist,
                    a.code_letters AS call_letters,
                    gac.artist_genre_code AS artist_call_number,
                    l.code_number AS release_call_number,
                    g.genre_name AS genre, f.format_name AS format,
                    l.alternate_artist_name
                FROM wxyc_schema.library l
                JOIN wxyc_schema.artists a ON l.artist_id = a.id
                JOIN wxyc_schema.format f ON l.format_id = f.id
                JOIN wxyc_schema.genres g ON l.genre_id = g.id
                JOIN wxyc_schema.genre_artist_crossreference gac
                  ON gac.artist_id = l.artist_id AND gac.genre_id = l.genre_id
            """)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def fetch_alternate_names(self) -> set[str]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT alternate_artist_name
                FROM wxyc_schema.library
                WHERE alternate_artist_name IS NOT NULL
                  AND alternate_artist_name != ''
            """)
            return _strip_name_rows(cur.fetchall())

    def fetch_cross_referenced_artists(self) -> set[str]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT a.artist_name
                FROM wxyc_schema.artist_crossreference cr
                JOIN wxyc_schema.artists a ON a.id = cr.source_artist_id
                UNION
                SELECT DISTINCT a.artist_name
                FROM wxyc_schema.artist_crossreference cr
                JOIN wxyc_schema.artists a ON a.id = cr.target_artist_id
            """)
            return _strip_name_rows(cur.fetchall())

    def fetch_release_cross_ref_artists(self) -> set[str]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT a.artist_name
                FROM wxyc_schema.artist_library_crossreference alc
                JOIN wxyc_schema.artists a ON a.id = alc.artist_id
            """)
            return _strip_name_rows(cur.fetchall())

    def fetch_library_labels(self) -> set[tuple[str, str, str]]:
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT a.artist_name, l.album_title, f.record_label
                FROM wxyc_schema.flowsheet f
                JOIN wxyc_schema.library l ON f.album_id = l.id
                JOIN wxyc_schema.artists a ON l.artist_id = a.id
                WHERE f.record_label IS NOT NULL
                  AND f.record_label != ''
                  AND f.album_id IS NOT NULL
            """)
            return _strip_label_rows(cur.fetchall())

    def fetch_compilation_track_artists(self) -> list[dict[str, Any]]:
        # Backend-Service does not have COMPILATION_TRACK_ARTIST yet
        return []

    def close(self) -> None:
        self._conn.close()


def create_catalog_source(source_type: str, db_url: str) -> CatalogSource:
    """Create a CatalogSource implementation by name.

    Args:
        source_type: "tubafrenzy" or "backend-service".
        db_url: Database connection URL.

    Returns:
        A CatalogSource instance.

    Raises:
        ValueError: If source_type is not recognized.
    """
    if source_type == "tubafrenzy":
        return TubafrenzySource(db_url)
    elif source_type == "backend-service":
        return BackendServiceSource(db_url)
    else:
        raise ValueError(f"Unknown catalog source: {source_type}")
