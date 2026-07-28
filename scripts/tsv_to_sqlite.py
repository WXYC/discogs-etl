"""Convert a MySQL TSV dump to a SQLite database with FTS5 index.

Reads a tab-separated file (as produced by ``mysql -B -N``) with 11 columns
corresponding to the WXYC library catalog schema and creates a SQLite
database containing:

- A ``library`` table with id, title, artist, call_letters,
  artist_call_number, release_call_number, genre, format,
  alternate_artist_name, album_artist, label, and cross_reference_names
  columns (label is always NULL since the MySQL query doesn't include it).
- An FTS5 virtual table (``library_fts``) for full-text search on title,
  artist, alternate_artist_name, and album_artist.
- Indexes on artist, title, alternate_artist_name, and album_artist.

``cross_reference_names`` holds the pipe-joined (``" | "``) PRESENTATION_NAMEs
of any WXYC ``LIBRARY_CODE``s cataloger-cross-referenced to this row's own
code (e.g. a release filed under a band name carries its member's personal
name), sourced from ``LIBRARY_CODE_CROSS_REFERENCE`` via the correlated
subquery in ``sync-library.sh``. See WXYC/discogs-etl#334.

MySQL ``\\N`` values are converted to SQL NULL. Rows that do not contain
exactly 11 tab-separated fields are skipped with a warning on stderr.

Optionally also imports a ``compilation_track_artist`` table from a second,
3-column TSV (``library_release_id``, ``artist_name``, ``track_title``)
sourced from tubafrenzy's ``COMPILATION_TRACK_ARTIST`` table. This restores
the export dropped in the #65 slim-down (see WXYC/discogs-etl#332); LML's
``library/db.py`` detects the table at connect time and UNIONs in
compilations featuring a searched track artist. ``artist_name`` and
``track_title`` are free text and can themselves contain embedded
tab/newline/backslash bytes -- ``mysql -B -N`` escapes those into two-char
``\\t``/``\\n``/``\\\\`` sequences before they ever reach the TSV, which is
why splitting on real tab/newline bytes (the same approach the ``library``
table above already relies on) is safe here too.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402


def _import_compilation_track_artists(cta_tsv_path: str, cur: sqlite3.Cursor) -> int:
    """Import compilation_track_artist rows from a MySQL TSV dump into SQLite.

    Reads a 3-column TSV (library_release_id, artist_name, track_title) as
    produced by ``mysql -B -N`` against tubafrenzy's COMPILATION_TRACK_ARTIST
    table. track_title is nullable (MySQL ``\\N``); library_release_id and
    artist_name are not.

    The table (plus its indexes) is created only if at least one row parses
    successfully. This is the graceful-degradation path: when the source
    table doesn't exist (pre-V008 fixtures, the Backend-Service catalog
    source) or the query fails, callers pass no ``cta_tsv_path`` at all, and
    when it's genuinely empty this function is a no-op -- either way, no
    ``compilation_track_artist`` table is created, so LML's
    ``_has_compilation_track_artist`` presence check stays False rather than
    seeing a stray empty table.

    Rows with the wrong field count are skipped with a WARNING on stderr
    (never silently dropped), matching the ``library`` table's malformed-row
    handling above.

    Returns:
        The number of compilation_track_artist rows imported.
    """
    rows: list[tuple[int, str, str | None]] = []
    with open(cta_tsv_path, encoding="utf-8") as f:
        for line in f:
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 3:
                print(
                    f"WARNING: skipping malformed compilation_track_artist row with "
                    f"{len(fields)} fields",
                    file=sys.stderr,
                )
                continue
            library_release_id_raw, artist_name, track_title = (
                None if v == "\\N" else v for v in fields
            )
            if library_release_id_raw is None or artist_name is None:
                print(
                    "WARNING: skipping compilation_track_artist row with NULL "
                    "library_release_id or artist_name",
                    file=sys.stderr,
                )
                continue
            rows.append((int(library_release_id_raw), artist_name, track_title))

    if not rows:
        return 0

    cur.execute("""CREATE TABLE compilation_track_artist (
        library_release_id INTEGER NOT NULL,
        artist_name TEXT NOT NULL,
        track_title TEXT
    )""")
    cur.executemany(
        "INSERT INTO compilation_track_artist"
        " (library_release_id, artist_name, track_title) VALUES (?,?,?)",
        rows,
    )
    cur.execute("CREATE INDEX idx_cta_release ON compilation_track_artist(library_release_id)")
    cur.execute("CREATE INDEX idx_cta_artist ON compilation_track_artist(artist_name)")

    releases = len({row[0] for row in rows})
    print(f"Exported {len(rows)} compilation track artists across {releases} releases")
    return len(rows)


def tsv_to_sqlite(tsv_path: str, db_path: str, cta_tsv_path: str | None = None) -> int:
    """Import a MySQL TSV dump into a new SQLite database.

    Args:
        tsv_path: Path to the tab-separated input file.
        db_path: Path where the SQLite database will be created.
        cta_tsv_path: Optional path to a compilation_track_artist TSV (3
            columns: library_release_id, artist_name, track_title). Omit to
            skip the compilation_track_artist table entirely.

    Returns:
        The number of library rows successfully imported (unaffected by
        compilation_track_artist import counts).
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE library (
        id INTEGER PRIMARY KEY, title TEXT, artist TEXT, call_letters TEXT,
        artist_call_number INTEGER, release_call_number INTEGER,
        genre TEXT, format TEXT, alternate_artist_name TEXT,
        album_artist TEXT, label TEXT, cross_reference_names TEXT
    )""")
    # FTS5 tokenizer extends unicode61 with the Symbol category and U+200D ZWJ so
    # bare-emoji rows (waving-hand, musical-note) and ZWJ graphemes (family,
    # flag, person-with-profession) get tokenized. The default unicode61
    # categories ('L* N* Co') drop everything else, leaving the FTS index with
    # no token to anchor a supplementary-plane row on.
    # Cf is NOT included wholesale -- that would merge tokens around
    # zero-width-space, BOM, soft-hyphen, etc. ZWJ is opted in explicitly via
    # tokenchars; the U+200D codepoint is written as the explicit \\u200d escape
    # below so the source stays unambiguous in editors and diffs that render
    # zero-width characters invisibly.
    # Must stay in sync with `library.db.LIBRARY_FTS_CREATE_SQL` in
    # WXYC/library-metadata-lookup. See WXYC/discogs-etl#161 and
    # WXYC/library-metadata-lookup#251.
    fts_tokenizer = "unicode61 categories 'L* N* Co S*' tokenchars '\u200d'"
    cur.execute(
        "CREATE VIRTUAL TABLE library_fts USING fts5("
        "title, artist, alternate_artist_name, album_artist, "
        f'tokenize="{fts_tokenizer}", '
        "content='library', content_rowid='id'"
        ")"
    )

    count = 0
    with open(tsv_path, encoding="utf-8") as f:
        for line in f:
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 11:
                print(
                    f"WARNING: skipping malformed row with {len(fields)} fields",
                    file=sys.stderr,
                )
                continue
            # MySQL -B outputs \N for NULL
            row = [None if v == "\\N" else v for v in fields]
            cur.execute(
                "INSERT INTO library (id, title, artist, call_letters,"
                " artist_call_number, release_call_number, genre, format,"
                " alternate_artist_name, album_artist, cross_reference_names)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                row,
            )
            count += 1

    cur.execute("""INSERT INTO library_fts(rowid, title, artist, alternate_artist_name, album_artist)
        SELECT id, title, artist, alternate_artist_name, album_artist FROM library""")
    cur.execute("CREATE INDEX idx_artist ON library(artist)")
    cur.execute("CREATE INDEX idx_title ON library(title)")
    cur.execute("CREATE INDEX idx_alternate_artist ON library(alternate_artist_name)")
    cur.execute("CREATE INDEX idx_album_artist ON library(album_artist)")

    if cta_tsv_path:
        _import_compilation_track_artists(cta_tsv_path, cur)

    conn.commit()
    conn.close()
    return count


def _parse_args(argv: list[str]) -> tuple[str, str, str | None]:
    """Parse CLI args: <tsv_path> <db_path> [--cta-tsv PATH]."""
    cta_tsv_path: str | None = None
    if "--cta-tsv" in argv:
        flag_index = argv.index("--cta-tsv")
        try:
            cta_tsv_path = argv[flag_index + 1]
        except IndexError:
            print("Usage: --cta-tsv requires a path argument", file=sys.stderr)
            sys.exit(1)
        positional = argv[:flag_index] + argv[flag_index + 2 :]
    else:
        positional = list(argv)

    if len(positional) != 2:
        print(
            f"Usage: {sys.argv[0]} <tsv_path> <db_path> [--cta-tsv <cta_tsv_path>]",
            file=sys.stderr,
        )
        sys.exit(1)

    return positional[0], positional[1], cta_tsv_path


if __name__ == "__main__":
    init_logger(repo="discogs-etl", tool="discogs-etl tsv_to_sqlite")
    tsv_path, db_path, cta_tsv_path = _parse_args(sys.argv[1:])
    n = tsv_to_sqlite(tsv_path, db_path, cta_tsv_path)
    print(f"Exported {n} rows to {db_path}")
