"""Pin the SQL-layer NULL-handling fix in the tubafrenzy MySQL SELECTs.

``mysql -B -N`` (the CLI mode used to dump tubafrenzy's
``LIBRARY_RELEASE``/``COMPILATION_TRACK_ARTIST`` tables) prints a genuine SQL
NULL as the literal 4-character text ``NULL`` on this server, not the ``\\N``
sentinel ``parse_library_tsv``'s parser expects. Left unwrapped, that literal
text lands in SQLite as the *string* ``'NULL'`` instead of SQL NULL -- and
since ``album_artist`` feeds the ``library_fts`` index, a typed search for
"null" matched the entire catalog (verified in prod 2026-08-02: 64,780
``album_artist`` rows and 63,904 ``cross_reference_names`` rows held the
literal string).

The fix lives in the SQL text itself (``IFNULL(<col>, '')``), not in the TSV
parser: an artist genuinely named "NULL" must survive, and string-sniffing for
the text ``'NULL'`` in Python would silently corrupt that row instead. IFNULL
only ever substitutes for a *real* SQL NULL, so a literal ``'NULL'`` value
passes through untouched while a genuine NULL becomes ``''``.

**These assertions used to read ``scripts/sync-library.sh``.** WXYC/discogs-etl#346
moved the daily sync onto the Backend producer, which never runs a SELECT, so
the only remaining copy of this SQL is ``catalog_parity_diff.py``'s -- the
parity harness's MySQL producer, which lives until the Kattare host does. The
bug is no longer a production-data bug there but it is still a parity bug: a
literal ``'NULL'`` on the mysql side is a field value Backend does not have,
and this harness's whole output is a count of the fields where the two sides
disagree. Re-pointed rather than deleted for that reason.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "catalog_parity_diff.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("catalog_parity_diff", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["catalog_parity_diff"] = mod
    spec.loader.exec_module(mod)
    return mod


def _library_select() -> str:
    return _load_module().LIBRARY_SELECT_SQL


def _cta_select() -> str:
    return _load_module().COMPILATION_TRACK_SELECT_SQL


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
            r"AND xlc\.ID != lc\.ID\), ''\)\s+FROM LIBRARY_RELEASE r JOIN",
            select_text,
        ), "cross_reference_names subquery is not wrapped all the way to its closing paren"

    def test_not_null_columns_left_unwrapped(self) -> None:
        """Columns that carry no SQL NULL must NOT be wrapped -- doing so
        would be unjustified scope creep on this fix. Guards against a future
        edit accidentally over-applying IFNULL.

        Two of these have measured backing as of 2026-08-14
        (WXYC/discogs-etl#375): `TITLE IS NULL` and `PRESENTATION_NAME IS
        NULL` both counted 0 against prod. The remaining six rest on the
        original "always populated" reading and are unmeasured. Note TITLE is
        not the same as *non-empty*: six rows hold a byte-exact empty-string
        TITLE, which is expected residue handled downstream by
        `catalog_parity_diff.py::_rule_b_missing_reason`, not a reason to
        wrap."""
        select_text = _library_select()
        unwrapped = [
            "r.ID",
            "r.TITLE",
            "lc.PRESENTATION_NAME",
            "lc.CALL_LETTERS",
            "lc.CALL_NUMBERS",
            "r.CALL_NUMBERS",
            "g.REFERENCE_NAME",
            "f.REFERENCE_NAME",
        ]
        for col in unwrapped:
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
