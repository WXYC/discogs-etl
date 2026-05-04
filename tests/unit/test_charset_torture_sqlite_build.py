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

    # Schema (10 columns): id, title, artist, call_letters, artist_call_number,
    # release_call_number, genre, format, alternate_artist_name, album_artist
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
