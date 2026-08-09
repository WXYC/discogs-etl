"""The canonical ``library.db`` shape, shared by every producer that builds one.

``library.db`` is the SQLite catalog snapshot that library-metadata-lookup
serves searches from. Production builds it nightly from tubafrenzy's MySQL
(``scripts/sync-library.sh`` -> ``scripts/tsv_to_sqlite.py``); the
tubafrenzy turndown (WXYC/wiki#89, Phase 3.5) moves that build to
Backend-Service over HTTP (``scripts/catalog_parity_diff.py
--backend-source``, WXYC/discogs-etl#351).

Both paths must emit a **byte-identical schema**, so the DDL lives here once
rather than being copied into each producer. That is not a tidiness
preference: the Backend-sourced build is not merely a diff input, it becomes
the real ``library.db`` at the cutover, and a divergent FTS tokenizer or a
missing index would degrade live search in a way the row-by-row parity diff
(which compares column *values*, not table definitions) cannot see.

The ``library`` table's 12 columns are ``id, title, artist, call_letters,
artist_call_number, release_call_number, genre, format,
alternate_artist_name, album_artist, label, cross_reference_names``.
``label`` is never inserted -- it exists in the schema but is always NULL in
production, because the MySQL SELECT that feeds the daily build has no label
column.
"""

from __future__ import annotations

import sqlite3
import sys
from collections.abc import Iterable, Sequence
from typing import Callable

# Every column of the `library` table, in declaration order.
LIBRARY_COLUMNS = (
    "id",
    "title",
    "artist",
    "call_letters",
    "artist_call_number",
    "release_call_number",
    "genre",
    "format",
    "alternate_artist_name",
    "album_artist",
    "label",
    "cross_reference_names",
)

# The columns a producer actually supplies, in the order `build_library_db`
# expects each row tuple. `label` is absent on purpose (see the module
# docstring); it stays NULL for every row.
LIBRARY_INSERT_COLUMNS = tuple(c for c in LIBRARY_COLUMNS if c != "label")

CROSS_REFERENCE_SEPARATOR = " | "

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
FTS_TOKENIZER = "unicode61 categories 'L* N* Co S*' tokenchars '\u200d'"


def create_library_schema(cur: sqlite3.Cursor) -> None:
    """Create the empty ``library`` table and its FTS5 companion."""
    cur.execute("""CREATE TABLE library (
        id INTEGER PRIMARY KEY, title TEXT, artist TEXT, call_letters TEXT,
        artist_call_number INTEGER, release_call_number INTEGER,
        genre TEXT, format TEXT, alternate_artist_name TEXT,
        album_artist TEXT, label TEXT, cross_reference_names TEXT
    )""")
    cur.execute(
        "CREATE VIRTUAL TABLE library_fts USING fts5("
        "title, artist, alternate_artist_name, album_artist, "
        f'tokenize="{FTS_TOKENIZER}", '
        "content='library', content_rowid='id'"
        ")"
    )


def insert_library_rows(cur: sqlite3.Cursor, rows: Iterable[Sequence[object]]) -> int:
    """Insert ``rows`` (11-value sequences, ``LIBRARY_INSERT_COLUMNS`` order).

    Consumes ``rows`` lazily so a producer can stream and emit its own
    per-row warnings interleaved with the insert, as the TSV parser does.

    Returns:
        The number of rows inserted.
    """
    columns = ", ".join(LIBRARY_INSERT_COLUMNS)
    placeholders = ", ".join("?" for _ in LIBRARY_INSERT_COLUMNS)
    count = 0
    for row in rows:
        cur.execute(f"INSERT INTO library ({columns}) VALUES ({placeholders})", row)
        count += 1
    return count


def finalize_library(cur: sqlite3.Cursor) -> None:
    """Populate the FTS index from ``library`` and create the search indexes."""
    cur.execute("""INSERT INTO library_fts(rowid, title, artist, alternate_artist_name, album_artist)
        SELECT id, title, artist, alternate_artist_name, album_artist FROM library""")
    cur.execute("CREATE INDEX idx_artist ON library(artist)")
    cur.execute("CREATE INDEX idx_title ON library(title)")
    cur.execute("CREATE INDEX idx_alternate_artist ON library(alternate_artist_name)")
    cur.execute("CREATE INDEX idx_album_artist ON library(album_artist)")


def _print_to_stdout(message: str) -> None:
    print(message)


def create_compilation_track_artists(
    cur: sqlite3.Cursor,
    rows: Iterable[Sequence[object]],
    report: Callable[[str], None] = _print_to_stdout,
) -> int:
    """Create and populate ``compilation_track_artist`` from ``rows``.

    Each row is a ``(library_release_id, artist_name, track_title)`` triple.

    ``report`` receives the progress line. It defaults to stdout, which is
    where the daily sync's log tee expects it; a caller whose stdout is a
    machine-readable channel (``catalog_parity_diff.py --json``) passes a
    stderr writer instead.

    The table (plus its indexes) is created only if at least one row is
    supplied. This is the graceful-degradation path: when the source table
    doesn't exist (pre-V008 fixtures, a catalog source with no compilation
    tracks) or the query fails, callers supply no rows, and when it's
    genuinely empty this function is a no-op -- either way, no
    ``compilation_track_artist`` table is created, so LML's
    ``_has_compilation_track_artist`` presence check stays False rather than
    seeing a stray empty table.

    Returns:
        The number of rows imported.
    """
    materialized = list(rows)
    if not materialized:
        return 0

    cur.execute("""CREATE TABLE compilation_track_artist (
        library_release_id INTEGER NOT NULL,
        artist_name TEXT NOT NULL,
        track_title TEXT
    )""")
    cur.executemany(
        "INSERT INTO compilation_track_artist"
        " (library_release_id, artist_name, track_title) VALUES (?,?,?)",
        materialized,
    )
    cur.execute("CREATE INDEX idx_cta_release ON compilation_track_artist(library_release_id)")
    cur.execute("CREATE INDEX idx_cta_artist ON compilation_track_artist(artist_name)")

    releases = len({row[0] for row in materialized})
    report(f"Exported {len(materialized)} compilation track artists across {releases} releases")
    return len(materialized)


def build_library_db(
    db_path: str,
    library_rows: Iterable[Sequence[object]],
    cta_rows: Iterable[Sequence[object]] | None = None,
    report: Callable[[str], None] = _print_to_stdout,
) -> int:
    """Build a complete ``library.db`` at ``db_path``.

    Args:
        db_path: Where the SQLite database is created. Must not already
            exist as a populated database -- callers are responsible for
            choosing a fresh path (SQLite would otherwise fail on the
            ``CREATE TABLE``).
        library_rows: Iterable of 11-value sequences in
            ``LIBRARY_INSERT_COLUMNS`` order.
        cta_rows: Optional iterable of ``(library_release_id, artist_name,
            track_title)`` triples. ``None`` (or empty) skips the
            ``compilation_track_artist`` table entirely.
        report: Where progress lines go; defaults to stdout.

    Returns:
        The number of ``library`` rows inserted (unaffected by the
        compilation-track count).
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        create_library_schema(cur)
        count = insert_library_rows(cur, library_rows)
        finalize_library(cur)
        if cta_rows is not None:
            create_compilation_track_artists(cur, cta_rows, report=report)
        conn.commit()
    finally:
        conn.close()
    return count


def parse_library_tsv(tsv_path: str) -> Iterable[Sequence[object]]:
    """Yield ``library`` row tuples from a ``mysql -B -N`` TSV dump.

    The file has 11 tab-separated fields per line, matching
    ``LIBRARY_INSERT_COLUMNS``. MySQL ``\\N`` becomes SQL NULL. Rows without
    exactly 11 fields are skipped with a WARNING on stderr (never silently
    dropped).

    A generator, so warnings interleave with the inserts they describe.
    """
    with open(tsv_path, encoding="utf-8") as f:
        for line in f:
            fields = line.rstrip("\n").split("\t")
            if len(fields) != len(LIBRARY_INSERT_COLUMNS):
                print(
                    f"WARNING: skipping malformed row with {len(fields)} fields",
                    file=sys.stderr,
                )
                continue
            # MySQL -B outputs \N for NULL
            yield [None if v == "\\N" else v for v in fields]


def parse_compilation_track_tsv(tsv_path: str) -> Iterable[Sequence[object]]:
    """Yield ``compilation_track_artist`` row triples from a ``mysql -B -N`` TSV.

    3 columns (``library_release_id``, ``artist_name``, ``track_title``) as
    produced against tubafrenzy's ``COMPILATION_TRACK_ARTIST`` table.
    ``track_title`` is nullable (MySQL ``\\N``); the other two are not, and a
    row missing either is skipped with a WARNING.

    ``artist_name`` and ``track_title`` are free text and can themselves
    contain embedded tab/newline/backslash bytes -- ``mysql -B -N`` escapes
    those into two-char ``\\t`` / ``\\n`` / ``\\\\`` sequences before they ever
    reach the TSV, which is why splitting on real tab/newline bytes (the same
    approach ``parse_library_tsv`` relies on) is safe here too.
    """
    with open(tsv_path, encoding="utf-8") as f:
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
            try:
                library_release_id = int(library_release_id_raw)
            except ValueError:
                print(
                    "WARNING: skipping compilation_track_artist row with "
                    f"non-integer library_release_id {library_release_id_raw!r}",
                    file=sys.stderr,
                )
                continue
            yield (library_release_id, artist_name, track_title)
