"""Pin the SQL-layer NULL-handling fix in the daily sync's MySQL SELECTs.

``mysql -B -N`` (the CLI mode ``scripts/sync-library.sh`` uses to dump
tubafrenzy's ``LIBRARY_RELEASE``/``COMPILATION_TRACK_ARTIST`` tables) prints a
genuine SQL NULL as the literal 4-character text ``NULL`` on this server, not
the ``\\N`` sentinel ``tsv_to_sqlite.py``'s parser expects. Left unwrapped,
that literal text lands in SQLite as the *string* ``'NULL'`` instead of SQL
NULL -- and since ``album_artist`` feeds the ``library_fts`` index, a typed
search for "null" matched the entire catalog (verified in prod 2026-08-02:
64,780 ``album_artist`` rows and 63,904 ``cross_reference_names`` rows held
the literal string).

The fix lives in the SQL text itself (``IFNULL(<col>, '')``), not in
``tsv_to_sqlite.py``: an artist genuinely named "NULL" must survive, and
string-sniffing for the text ``'NULL'`` in Python would silently corrupt that
row instead. IFNULL only ever substitutes for a *real* SQL NULL, so a literal
``'NULL'`` value passes through untouched while a genuine NULL becomes ``''``.

These tests inspect the SELECT text in ``scripts/sync-library.sh`` directly
(no MySQL server is available in this environment to run the query against)
mirroring the wiring-test convention already used for this same script in
``test_sync_library_derive_wiring.py``. See WXYC/discogs-etl (literal-NULL
bugfix).
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNC_SCRIPT = REPO_ROOT / "scripts" / "sync-library.sh"

# The LIBRARY_RELEASE SELECT (11 columns) that feeds the primary library TSV.
_LIBRARY_SELECT_RE = re.compile(
    r"-e \"(SELECT r\.ID.*?"
    r"FROM LIBRARY_RELEASE r JOIN LIBRARY_CODE lc ON r\.LIBRARY_CODE_ID = lc\.ID "
    r"JOIN FORMAT f ON r\.FORMAT_ID = f\.ID JOIN GENRE g ON lc\.GENRE_ID = g\.ID)\"",
    re.DOTALL,
)

# The COMPILATION_TRACK_ARTIST SELECT (3 columns) that feeds the supplementary
# compilation-track-artist TSV (WXYC/discogs-etl#332).
_CTA_SELECT_RE = re.compile(
    r"-e \"(SELECT LIBRARY_RELEASE_ID.*?FROM COMPILATION_TRACK_ARTIST ORDER BY LIBRARY_RELEASE_ID)\"",
    re.DOTALL,
)


def _library_select() -> str:
    source = SYNC_SCRIPT.read_text()
    match = _LIBRARY_SELECT_RE.search(source)
    assert match, (
        "could not locate the LIBRARY_RELEASE SELECT in sync-library.sh -- "
        "has the query been reshaped?"
    )
    return match.group(1)


def _cta_select() -> str:
    source = SYNC_SCRIPT.read_text()
    match = _CTA_SELECT_RE.search(source)
    assert match, (
        "could not locate the COMPILATION_TRACK_ARTIST SELECT in sync-library.sh -- "
        "has the query been reshaped?"
    )
    return match.group(1)


class TestLibrarySelectNullableTextColumnsWrapped:
    """Nullable TEXT columns feeding the FTS-searchable library TSV must emit
    ``''`` (via IFNULL) for a real SQL NULL, never the literal text ``NULL``."""

    def test_album_artist_wrapped_in_ifnull(self) -> None:
        assert "IFNULL(r.ALBUM_ARTIST, '')" in _library_select()

    def test_alternate_artist_name_wrapped_in_ifnull(self) -> None:
        """ALTERNATE_ARTIST_NAME is the same shape of column as ALBUM_ARTIST:
        nullable TEXT (~3,935 of ~65k rows populated per
        docs/discogs-etl-technical-overview.md), and it feeds the same
        library_fts index, so it is exposed to the identical bug."""
        assert "IFNULL(r.ALTERNATE_ARTIST_NAME, '')" in _library_select()

    def test_cross_reference_names_subquery_wrapped_in_ifnull(self) -> None:
        """The 11th column is a correlated GROUP_CONCAT subquery that returns
        SQL NULL when a library code has no cross-references (the common
        case: 63,904 NULL rows in prod). The *entire* subquery must be
        wrapped, not just its inner expression."""
        select_text = _library_select()
        assert "IFNULL((SELECT GROUP_CONCAT" in select_text
        # The subquery's closing paren must be immediately followed by the
        # IFNULL default arg and then the outer FROM clause -- i.e. the wrap
        # closes right before the query moves on to LIBRARY_RELEASE r JOIN...,
        # not merely appearing somewhere earlier in the string.
        assert re.search(
            r"AND xlc\.ID != lc\.ID\), ''\) FROM LIBRARY_RELEASE r JOIN",
            select_text,
        ), "cross_reference_names subquery is not wrapped all the way to its closing paren"

    def test_not_null_columns_left_unwrapped(self) -> None:
        """Columns that are always populated must NOT be wrapped -- doing so
        would be unjustified scope creep on this fix. Guards against a future
        edit accidentally over-applying IFNULL."""
        select_text = _library_select()
        always_populated = [
            "r.ID",
            "r.TITLE",
            "lc.PRESENTATION_NAME",
            "lc.CALL_LETTERS",
            "lc.CALL_NUMBERS",
            "r.CALL_NUMBERS",
            "g.REFERENCE_NAME",
            "f.REFERENCE_NAME",
        ]
        for col in always_populated:
            assert f"IFNULL({col}" not in select_text, f"{col} should not be IFNULL-wrapped"
            assert col in select_text, f"{col} should still be selected plainly"


class TestCompilationTrackArtistSelectNullableTextColumnsWrapped:
    """The supplementary COMPILATION_TRACK_ARTIST SELECT is dumped via the
    same ``mysql -B -N`` invocation style, so its nullable TEXT column
    (TRACK_TITLE) is exposed to the identical literal-``NULL``-text bug."""

    def test_track_title_wrapped_in_ifnull(self) -> None:
        assert "IFNULL(TRACK_TITLE, '')" in _cta_select()

    def test_not_null_columns_left_unwrapped(self) -> None:
        """LIBRARY_RELEASE_ID and ARTIST_NAME are documented NOT NULL
        (lib/library_db.py's create_compilation_track_artists declares the
        columns; parse_compilation_track_tsv beside it skips rows that
        violate them) -- they must stay unwrapped."""
        select_text = _cta_select()
        for col in ("LIBRARY_RELEASE_ID", "ARTIST_NAME"):
            assert f"IFNULL({col}" not in select_text, f"{col} should not be IFNULL-wrapped"
            assert col in select_text, f"{col} should still be selected plainly"
