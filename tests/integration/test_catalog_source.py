"""Integration tests for BackendServiceSource against real PostgreSQL.

Requires docker-compose PostgreSQL on port 5433:
    docker compose up db -d
    DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
      pytest -m postgres tests/integration/test_catalog_source.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from lib.catalog_source import BackendServiceSource

pytestmark = pytest.mark.postgres

# ---------------------------------------------------------------------------
# Schema setup (mirrors Backend-Service's wxyc_schema tables)
# ---------------------------------------------------------------------------

WXYC_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS wxyc_schema;

CREATE TABLE IF NOT EXISTS wxyc_schema.genres (
    id serial PRIMARY KEY,
    genre_name varchar(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS wxyc_schema.format (
    id serial PRIMARY KEY,
    format_name varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS wxyc_schema.artists (
    id serial PRIMARY KEY,
    artist_name varchar(128) NOT NULL,
    alphabetical_name varchar(128) NOT NULL,
    code_letters varchar(4) NOT NULL
);

CREATE TABLE IF NOT EXISTS wxyc_schema.library (
    id serial PRIMARY KEY,
    artist_id integer NOT NULL REFERENCES wxyc_schema.artists(id),
    genre_id integer NOT NULL REFERENCES wxyc_schema.genres(id),
    format_id integer NOT NULL REFERENCES wxyc_schema.format(id),
    alternate_artist_name varchar(128),
    album_title varchar(128) NOT NULL,
    code_number smallint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS wxyc_schema.genre_artist_crossreference (
    artist_id integer NOT NULL REFERENCES wxyc_schema.artists(id),
    genre_id integer NOT NULL REFERENCES wxyc_schema.genres(id),
    artist_genre_code integer NOT NULL,
    UNIQUE(artist_id, genre_id)
);

CREATE TABLE IF NOT EXISTS wxyc_schema.artist_crossreference (
    source_artist_id integer NOT NULL REFERENCES wxyc_schema.artists(id),
    target_artist_id integer NOT NULL REFERENCES wxyc_schema.artists(id),
    comment varchar(255),
    UNIQUE(source_artist_id, target_artist_id)
);

CREATE TABLE IF NOT EXISTS wxyc_schema.artist_library_crossreference (
    artist_id integer NOT NULL REFERENCES wxyc_schema.artists(id),
    library_id integer NOT NULL REFERENCES wxyc_schema.library(id),
    comment varchar(255),
    UNIQUE(artist_id, library_id)
);

CREATE TABLE IF NOT EXISTS wxyc_schema.flowsheet (
    id serial PRIMARY KEY,
    album_id integer REFERENCES wxyc_schema.library(id),
    artist_name varchar(128),
    album_title varchar(128),
    record_label varchar(128)
);
"""

# WXYC example data from CLAUDE.md
SEED_DATA_SQL = """
-- Genres
INSERT INTO wxyc_schema.genres (id, genre_name) VALUES
    (1, 'Rock'), (2, 'Electronic'), (3, 'Jazz'), (4, 'Latin');

-- Formats
INSERT INTO wxyc_schema.format (id, format_name) VALUES
    (1, 'vinyl 12"'), (2, 'cd');

-- Artists
INSERT INTO wxyc_schema.artists (id, artist_name, alphabetical_name, code_letters) VALUES
    (1, 'Juana Molina', 'Molina, Juana', 'JM'),
    (2, 'Stereolab', 'Stereolab', 'ST'),
    (3, 'Cat Power', 'Cat Power', 'CP'),
    (4, 'Jessica Pratt', 'Pratt, Jessica', 'JP'),
    (5, 'Chuquimamani-Condori', 'Chuquimamani-Condori', 'CH'),
    (6, 'Duke Ellington', 'Ellington, Duke', 'DE'),
    (7, 'John Coltrane', 'Coltrane, John', 'JC'),
    (8, 'Ice-T', 'Ice-T', 'IT'),
    (9, 'Body Count', 'Body Count', 'BC');

-- Genre-artist crossreference (code numbers within genre)
INSERT INTO wxyc_schema.genre_artist_crossreference (artist_id, genre_id, artist_genre_code) VALUES
    (1, 1, 42), (2, 1, 43), (3, 1, 44), (4, 1, 45),
    (5, 2, 10), (6, 3, 1), (7, 3, 2), (8, 1, 50), (9, 1, 51);

-- Library releases
INSERT INTO wxyc_schema.library (id, artist_id, genre_id, format_id, album_title, alternate_artist_name, code_number) VALUES
    (1, 1, 1, 1, 'DOGA', NULL, 1),
    (2, 2, 1, 2, 'Aluminum Tunes', NULL, 1),
    (3, 3, 1, 1, 'Moon Pix', NULL, 1),
    (4, 4, 1, 1, 'On Your Own Love Again', NULL, 1),
    (5, 5, 2, 2, 'Edits', NULL, 1),
    (6, 6, 3, 1, 'Duke Ellington & John Coltrane', NULL, 1),
    (7, 9, 1, 2, 'Body Count', 'Ice-T', 1);

-- Artist cross-references (artist <-> artist)
INSERT INTO wxyc_schema.artist_crossreference (source_artist_id, target_artist_id, comment) VALUES
    (8, 9, 'Ice-T / Body Count'),
    (6, 7, 'Ellington-Coltrane collaborations');

-- Release cross-references (artist -> album)
INSERT INTO wxyc_schema.artist_library_crossreference (artist_id, library_id, comment) VALUES
    (7, 6, 'Coltrane appears on Ellington album');

-- Flowsheet entries with labels
INSERT INTO wxyc_schema.flowsheet (album_id, artist_name, album_title, record_label) VALUES
    (1, 'Juana Molina', 'DOGA', 'Sonamos'),
    (2, 'Stereolab', 'Aluminum Tunes', 'Duophonic'),
    (3, 'Cat Power', 'Moon Pix', 'Matador Records'),
    (4, 'Jessica Pratt', 'On Your Own Love Again', 'Drag City'),
    (NULL, 'Unknown Artist', 'Unknown Album', 'Some Label'),
    (1, 'Juana Molina', 'DOGA', NULL);
"""


@pytest.fixture(scope="module")
def wxyc_db_url(db_url):
    """Set up wxyc_schema tables and seed data in the test database."""
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(WXYC_SCHEMA_SQL)
        cur.execute(SEED_DATA_SQL)
    conn.close()
    return db_url


# ---------------------------------------------------------------------------
# fetch_library_rows
# ---------------------------------------------------------------------------


class TestFetchLibraryRows:
    """BackendServiceSource.fetch_library_rows against real PostgreSQL."""

    def test_returns_all_library_entries(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            rows = source.fetch_library_rows()
        assert len(rows) == 7

    def test_row_has_expected_keys(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            rows = source.fetch_library_rows()
        expected_keys = {
            "id",
            "title",
            "artist",
            "call_letters",
            "artist_call_number",
            "release_call_number",
            "genre",
            "format",
            "alternate_artist_name",
            "label",
        }
        assert set(rows[0].keys()) == expected_keys

    def test_known_row_values(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            rows = source.fetch_library_rows()
        by_title = {r["title"]: r for r in rows}
        doga = by_title["DOGA"]
        assert doga["artist"] == "Juana Molina"
        assert doga["call_letters"] == "JM"
        assert doga["genre"] == "Rock"
        assert doga["format"] == 'vinyl 12"'
        assert doga["alternate_artist_name"] is None

    def test_alternate_artist_name(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            rows = source.fetch_library_rows()
        by_title = {r["title"]: r for r in rows}
        body_count = by_title["Body Count"]
        assert body_count["alternate_artist_name"] == "Ice-T"


# ---------------------------------------------------------------------------
# fetch_alternate_names
# ---------------------------------------------------------------------------


class TestFetchAlternateNames:
    """BackendServiceSource.fetch_alternate_names against real PostgreSQL."""

    def test_returns_alternate_names(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            names = source.fetch_alternate_names()
        assert "Ice-T" in names

    def test_excludes_null_alternates(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            names = source.fetch_alternate_names()
        # Only 1 library entry has a non-null alternate_artist_name
        assert len(names) == 1


# ---------------------------------------------------------------------------
# fetch_cross_referenced_artists
# ---------------------------------------------------------------------------


class TestFetchCrossReferencedArtists:
    """BackendServiceSource.fetch_cross_referenced_artists against real PostgreSQL."""

    def test_returns_artists_from_both_sides(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            names = source.fetch_cross_referenced_artists()
        # source_artist_id=8 (Ice-T) -> target_artist_id=9 (Body Count)
        # source_artist_id=6 (Duke Ellington) -> target_artist_id=7 (John Coltrane)
        assert "Ice-T" in names
        assert "Body Count" in names
        assert "Duke Ellington" in names
        assert "John Coltrane" in names

    def test_returns_correct_count(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            names = source.fetch_cross_referenced_artists()
        assert len(names) == 4


# ---------------------------------------------------------------------------
# fetch_release_cross_ref_artists
# ---------------------------------------------------------------------------


class TestFetchReleaseCrossRefArtists:
    """BackendServiceSource.fetch_release_cross_ref_artists against real PostgreSQL."""

    def test_returns_cross_referenced_artist(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            names = source.fetch_release_cross_ref_artists()
        # artist_id=7 (John Coltrane) -> library_id=6 (Duke Ellington & John Coltrane)
        assert "John Coltrane" in names

    def test_returns_correct_count(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            names = source.fetch_release_cross_ref_artists()
        assert len(names) == 1


# ---------------------------------------------------------------------------
# fetch_library_labels
# ---------------------------------------------------------------------------


class TestFetchLibraryLabels:
    """BackendServiceSource.fetch_library_labels against real PostgreSQL."""

    def test_returns_label_triples(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            labels = source.fetch_library_labels()
        assert ("Juana Molina", "DOGA", "Sonamos") in labels
        assert ("Stereolab", "Aluminum Tunes", "Duophonic") in labels
        assert ("Cat Power", "Moon Pix", "Matador Records") in labels
        assert ("Jessica Pratt", "On Your Own Love Again", "Drag City") in labels

    def test_excludes_null_album_id(self, wxyc_db_url) -> None:
        """Flowsheet entries without album_id are excluded."""
        with BackendServiceSource(wxyc_db_url) as source:
            labels = source.fetch_library_labels()
        artist_names = {t[0] for t in labels}
        assert "Unknown Artist" not in artist_names

    def test_excludes_null_label(self, wxyc_db_url) -> None:
        """Flowsheet entries without record_label are excluded."""
        with BackendServiceSource(wxyc_db_url) as source:
            labels = source.fetch_library_labels()
        # The Juana Molina entry with NULL label should not produce a duplicate
        jm_labels = [t for t in labels if t[0] == "Juana Molina"]
        assert len(jm_labels) == 1

    def test_returns_correct_count(self, wxyc_db_url) -> None:
        with BackendServiceSource(wxyc_db_url) as source:
            labels = source.fetch_library_labels()
        assert len(labels) == 4
