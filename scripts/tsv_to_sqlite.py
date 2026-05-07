"""Convert a MySQL TSV dump to a SQLite database with FTS5 index.

Reads a tab-separated file (as produced by ``mysql -B -N``) with 10 columns
corresponding to the WXYC library catalog schema and creates a SQLite
database containing:

- A ``library`` table with id, title, artist, call_letters,
  artist_call_number, release_call_number, genre, format,
  alternate_artist_name, album_artist, and label columns (label is always
  NULL since the MySQL query doesn't include it).
- An FTS5 virtual table (``library_fts``) for full-text search on title,
  artist, alternate_artist_name, and album_artist.
- Indexes on artist, title, alternate_artist_name, and album_artist.

MySQL ``\\N`` values are converted to SQL NULL. Rows that do not contain
exactly 10 tab-separated fields are skipped with a warning on stderr.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402


def tsv_to_sqlite(tsv_path: str, db_path: str) -> int:
    """Import a MySQL TSV dump into a new SQLite database.

    Args:
        tsv_path: Path to the tab-separated input file.
        db_path: Path where the SQLite database will be created.

    Returns:
        The number of rows successfully imported.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE library (
        id INTEGER PRIMARY KEY, title TEXT, artist TEXT, call_letters TEXT,
        artist_call_number INTEGER, release_call_number INTEGER,
        genre TEXT, format TEXT, alternate_artist_name TEXT,
        album_artist TEXT, label TEXT
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
            if len(fields) != 10:
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
                " alternate_artist_name, album_artist) VALUES (?,?,?,?,?,?,?,?,?,?)",
                row,
            )
            count += 1

    cur.execute("""INSERT INTO library_fts(rowid, title, artist, alternate_artist_name, album_artist)
        SELECT id, title, artist, alternate_artist_name, album_artist FROM library""")
    cur.execute("CREATE INDEX idx_artist ON library(artist)")
    cur.execute("CREATE INDEX idx_title ON library(title)")
    cur.execute("CREATE INDEX idx_alternate_artist ON library(alternate_artist_name)")
    cur.execute("CREATE INDEX idx_album_artist ON library(album_artist)")
    conn.commit()
    conn.close()
    return count


if __name__ == "__main__":
    init_logger(repo="discogs-etl", tool="discogs-etl tsv_to_sqlite")
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <tsv_path> <db_path>", file=sys.stderr)
        sys.exit(1)
    n = tsv_to_sqlite(sys.argv[1], sys.argv[2])
    print(f"Exported {n} rows to {sys.argv[2]}")
