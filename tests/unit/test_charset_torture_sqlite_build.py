"""WX-1.2.6 detector: catches future regressions in tsv_to_sqlite that would
silently mangle non-ASCII bytes during the library.db build for LML."""

from __future__ import annotations

import sqlite3

import pytest

from scripts.tsv_to_sqlite import tsv_to_sqlite
from tests.charset_torture import CharsetTortureEntry, entry_id, iter_entries

CORPUS_ENTRIES = list(iter_entries())

SQLITE_BUILD_XFAIL_INPUTS: dict[tuple[str, str], str] = {
    # tsv_to_sqlite splits on '\t' and '\n'; entries containing those bytes
    # cannot survive the line-oriented TSV parser. Not a discogs-etl bug —
    # it's a property of the upstream MySQL `mysql -B -N` TSV format.
    ("quoting", "tab\there"): (
        "[dxe:tsv-tab-byte] MySQL -B -N TSV uses literal tabs as field separators"
    ),
}

# NOTE (WXYC/discogs-etl#370): test_tsv_to_sqlite_roundtrip writes corpus inputs
# into the TSV *raw* (unescaped) and asserts byte-identity, so it now implicitly
# requires that no corpus input contains a sequence the parser unescapes. It
# passes today only because the corpus's one backslash entry is `back\slash` —
# `\s` is outside the vocabulary and passes through untouched. A future corpus
# entry containing `\\`, `\t`, `\n` or `\0` would redden this job even though the
# parser is behaving correctly; the fix then is to escape the input on the way
# into the TSV (as the CTA test below does), not to change the parser. Worth
# knowing because charset-torture.json is the shared cross-repo artifact governed
# by check-charset-corpus-drift.yml@gha/v1, so the reddening commit could land in
# wxyc-shared rather than here.


@pytest.mark.parametrize("entry", CORPUS_ENTRIES, ids=entry_id)
def test_tsv_to_sqlite_roundtrip(
    tmp_path, entry: CharsetTortureEntry, request: pytest.FixtureRequest
) -> None:
    """A TSV row carrying entry["input"] in the artist + title columns must
    round-trip byte-for-byte through tsv_to_sqlite into the SQLite library table."""
    xfail_reason = SQLITE_BUILD_XFAIL_INPUTS.get((entry["category"], entry["input"]))
    if xfail_reason is not None:
        request.applymarker(pytest.mark.xfail(reason=xfail_reason, strict=True))

    tsv_path = tmp_path / "library.tsv"
    db_path = tmp_path / "library.db"

    # Schema (11 columns): id, title, artist, call_letters, artist_call_number,
    # release_call_number, genre, format, alternate_artist_name, album_artist,
    # cross_reference_names
    fields = [
        "1",
        entry["input"],  # title
        entry["input"],  # artist
        "RO",
        "1",
        "1",
        "Rock",
        "CD",
        "\\N",  # alternate_artist_name
        "\\N",  # album_artist
        "\\N",  # cross_reference_names
    ]
    tsv_path.write_text("\t".join(fields) + "\n", encoding="utf-8")

    count = tsv_to_sqlite(str(tsv_path), str(db_path))
    assert count == 1, f"{entry['category']}: import dropped the row"

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT title, artist FROM library WHERE id = 1").fetchone()
    conn.close()

    assert row is not None, f"{entry['category']}: row missing after import"
    assert row[0] == entry["input"], f"{entry['category']}: title corrupted ({entry['notes']})"
    assert row[1] == entry["input"], f"{entry['category']}: artist corrupted ({entry['notes']})"


def test_compilation_track_artist_tab_newline_unicode_survives(tmp_path) -> None:
    r"""A CTA track_title carrying an embedded tab, an embedded newline, AND unicode
    must survive the compilation_track_artist import intact -- not get silently
    dropped by the 3-column field-count guard, and unescaped back to the real
    bytes (WXYC/discogs-etl#370).

    mysql -B -N already escapes embedded tab/newline/backslash bytes in field
    values into the two-char sequences \\t / \\n / \\\\ before they ever reach the
    TSV (this is why the `library` table survives such data -- see
    test_tab_in_field_value / test_newline_in_field_value in
    test_tsv_to_sqlite.py). So a literal DB-column tab/newline never appears as a
    raw tab/newline byte in the TSV; splitting on real tab/newline bytes is safe.
    The escaped 2-char sequences are then unescaped back to a real tab/newline,
    same as the `library` table's behavior -- the unicode passes through
    untouched either way, since it was never escaped on the wire. See
    WXYC/discogs-etl#332 and #370.
    """
    # Simulates mysql -B -N output for a track_title that originally contained a
    # real tab, a real newline, and unicode: the tab/newline arrive pre-escaped
    # as the two-char sequences below; the unicode passes through untouched.
    wire_track_title = "Side A\\tTrack 1\\nEncore éà 中文 موسيقى"
    expected_track_title = "Side A\tTrack 1\nEncore éà 中文 موسيقى"
    cta_path = tmp_path / "cta.tsv"
    cta_path.write_text(f"1\tVarious Artists\t{wire_track_title}\n", encoding="utf-8")

    # 11 fields (WXYC/discogs-etl#334 appended cross_reference_names as the
    # 11th column); the trailing \N keeps this row valid under the current
    # schema so it isn't silently skipped by the field-count guard.
    library_path = tmp_path / "library.tsv"
    library_path.write_text(
        "1\tVintage Palmwine\tVarious Artists\tZX\t1\t1\tWorld\tCD\t\\N\t\\N\t\\N\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "library.db"

    tsv_to_sqlite(str(library_path), str(db_path), str(cta_path))

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT track_title FROM compilation_track_artist WHERE library_release_id = 1"
    ).fetchone()
    conn.close()

    assert row is not None, "CTA row silently dropped"
    # Unescaped: real TAB and newline bytes, not the two-char sequences.
    assert row[0] == expected_track_title
