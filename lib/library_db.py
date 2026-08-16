"""The canonical ``library.db`` shape, shared by both producers of the served one.

``library.db`` is the SQLite catalog snapshot that library-metadata-lookup
serves searches from. Production builds it nightly from tubafrenzy's MySQL
(``scripts/sync-library.sh`` -> ``scripts/tsv_to_sqlite.py``); the
tubafrenzy turndown (WXYC/wiki#89, Phase 3.5) moves that build to
Backend-Service over HTTP (``scripts/catalog_parity_diff.py
--backend-source``, WXYC/discogs-etl#351).

Both of those paths must emit a **byte-identical schema**, so the DDL lives
here once rather than being copied into each producer. That is not a
tidiness preference: the Backend-sourced build is not merely a diff input,
it becomes the real ``library.db`` at the cutover, and a divergent FTS
tokenizer or a missing index would degrade live search in a way the
row-by-row parity diff (which compares column *values*, not table
definitions) cannot see.

**One further producer exists and does not use this module.**
``scripts/run_pipeline.py --generate-library-db`` shells out to
wxyc-catalog's ``wxyc-export-to-sqlite``, which builds a **10-column**
``library`` table (no ``album_artist``, no ``cross_reference_names``), an
FTS5 table over three columns with the **default** unicode61 tokenizer, and
no ``idx_album_artist``. It is already drifted, and this module cannot stop
it. What bounds the damage is where its output goes: a tempdir, consumed by
the pipeline's own KEEP/PRUNE artist filter, never uploaded to LML -- so the
missing tokenizer categories and index cost nothing today. Do not treat that
as safe by construction; if that database ever becomes a served artifact, it
has to move onto this module first.

The ``library`` table's 12 columns are ``id, title, artist, call_letters,
artist_call_number, release_call_number, genre, format,
alternate_artist_name, album_artist, label, cross_reference_names``.
``label`` is never inserted -- it exists in the schema but is always NULL in
production, because the MySQL SELECT that feeds the daily build has no label
column.

**Charset-torture corpus scope.** The shared corpus this repo pins
(``tests/fixtures/charset-torture.json``, guarded by
``.github/workflows/charset-corpus-drift.yml``) also defines
``expected_storage`` and ``expected_match_form`` -- the WX-2 Normalizer
Charter's ``to_storage_form``/``to_match_form`` outputs. discogs-etl
implements neither: ``to_storage_form`` has zero call sites in this repo,
and both round-trip detectors (``tests/unit/test_charset_torture_sqlite_build.py``,
``tests/integration/test_charset_torture_pg_cache.py``) assert only that
``entry["input"]`` survives byte-for-byte -- ``library.db`` is
byte-preserving by design. The drift guard is a whole-file SHA-256 check, so
it will not catch a future test here written against ``expected_storage`` or
``expected_match_form``; don't add one. That canonicalization contract
belongs to whichever service actually normalizes metadata (e.g. LML's
identity layer), not to this byte-preserving cache.
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
    cur.execute("""
        INSERT INTO library_fts(rowid, title, artist, alternate_artist_name, album_artist)
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


def _unescape_mysql_field(value: str) -> str:
    r"""Undo ``mysql -B -N``'s (no ``--raw``) escaping of a TSV field.

    A single left-to-right pass: ``\\``->``\``, ``\t``->TAB, ``\n``->NL,
    ``\0``->NUL. Any other backslash (including one immediately followed by
    ``N``, which is not one of these four) passes through unchanged, because
    that is the only escape vocabulary ``mysql -B -N`` uses.

    Deliberately **not** a sequence of ``str.replace`` calls: for a field
    holding a literal backslash immediately followed by ``t`` (wire: the
    three chars ``\\t``, an escaped backslash then a bare ``t``), a chained
    ``.replace("\\t", " ")`` would match at offset 1 -- the second backslash
    plus the ``t`` -- and corrupt the result to backslash+space instead of
    the correct backslash+``t``. A single left-to-right scan consumes the
    ``\\`` pair first and gets it right. See
    ``tests/unit/test_tsv_to_sqlite.py`` for the pinned case.

    ``\0``->NUL is a deliberate tie-break taken at 0 observed rows (see
    ``plans/346-parity-clean-definition.md``), and its blast radius is wider
    than SQLite: besides truncating the value for any ``sqlite3_column_text``
    consumer, ``scripts/sync-library.sh`` feeds the CTA table into LML's
    ``build_compilation_track_location``, which writes those columns into
    **PostgreSQL** ``text`` -- which cannot hold U+0000 at all (server raises
    22021; ``tests/fixtures/charset-torture.json`` records the same). That
    build is soft-failed, so one NUL-bearing row would stop the recall index
    updating behind nothing louder than a WARNING. If a live measurement ever
    finds such a row, that belongs in the follow-up ticket's criteria.
    """
    out: list[str] = []
    i = 0
    n = len(value)
    while i < n:
        ch = value[i]
        if ch == "\\" and i + 1 < n and value[i + 1] in ("\\", "t", "n", "0"):
            escape = value[i + 1]
            out.append({"\\": "\\", "t": "\t", "n": "\n", "0": "\x00"}[escape])
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _parse_nullable_field(raw: str) -> str | None:
    r"""Test the ``\N`` NULL sentinel against the *raw* wire field, then
    unescape only the survivors.

    Order matters: unescaping before testing would turn a field holding a
    literal backslash immediately followed by ``N`` (wire: the three chars
    ``\\N``) into the same two-char ``\N`` spelling as the sentinel, and the
    value would be silently read as SQL NULL -- data loss introduced by the
    unescape itself. So the sentinel test runs first, against the field as
    it arrived on the wire, and only a field that survives that test gets
    unescaped. See WXYC/discogs-etl#370 and
    ``tests/unit/test_tsv_to_sqlite.py``.
    """
    if raw == "\\N":
        return None
    return _unescape_mysql_field(raw)


def parse_library_tsv(tsv_path: str) -> Iterable[Sequence[object]]:
    r"""Yield ``library`` row tuples from a ``mysql -B -N`` TSV dump.

    The file has 11 tab-separated fields per line, matching
    ``LIBRARY_INSERT_COLUMNS``. MySQL ``\N`` becomes SQL NULL (tested
    against the raw field, before unescaping -- see
    ``_parse_nullable_field``). Every surviving field is then unescaped:
    ``mysql -B -N`` (no ``--raw``) escapes embedded backslash/tab/newline/NUL
    bytes into the two-char sequences ``\\``/``\t``/``\n``/``\0``, and this
    parser undoes that so the served catalog holds the real bytes rather
    than their escaped spelling. Rows without exactly 11 fields are skipped
    with a WARNING on stderr (never silently dropped).

    **Fragile by construction, and the dependency is one-sided.** The
    unescape is correct only while *this repo's* producers keep escaping
    enabled: ``scripts/sync-library.sh`` and ``catalog_parity_diff.py``'s
    ``_mysql_invocation`` both run ``mysql -B -N`` **without** ``--raw``.
    Adding ``--raw`` to either silently inverts this function -- the wire
    would then carry real bytes, and a title holding the literal two chars
    backslash-``t`` would be converted into a TAB that was never in the
    source. The two callers of this parser are ``scripts/tsv_to_sqlite.py``
    and ``scripts/catalog_parity_diff.py``.

    Backend-Service's ``shared/database/src/legacy/sql.mirror.ts`` does run
    ``--raw``, but that is **not** a dependency of this function: its stdout
    is parsed by Backend's own TypeScript and never reaches here, and the
    parity harness reads Backend over HTTP JSON
    (``catalog_parity_diff.py::_build_from_backend_snapshot``), not from
    mirror output. Its flag matters only to whether the two sides end up
    *comparable* -- both holding true bytes -- so do not read it as
    load-bearing for this parser.

    **Carriage return is outside the escape vocabulary, and this parser does
    not handle it.** ``mysql``'s ``safe_put_field`` escapes exactly NUL, TAB,
    NL and backslash, never CR, while this ``open()`` runs in universal-
    newline mode -- so a raw CR in source data is read as a line terminator.
    Mid-row it splits the line and both fragments fail the field-count check
    (dropped, with a WARNING); in the **final** column the first fragment
    still has the right field count and is accepted with the value silently
    truncated at the CR. Unlike the four escapes above this class leaves no
    trace in the built artifact -- a ``char(13)`` probe against ``library.db``
    cannot see a row that was dropped or truncated on the way in -- so it has
    to be measured against MySQL (``INSTR(TITLE, CHAR(13))``). Deliberately
    left unfixed here: correcting it changes which rows are ingested, which
    needs that measurement first. See WXYC/discogs-etl#370.

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
            yield [_parse_nullable_field(v) for v in fields]


def parse_compilation_track_tsv(tsv_path: str) -> Iterable[Sequence[object]]:
    r"""Yield ``compilation_track_artist`` row triples from a ``mysql -B -N`` TSV.

    3 columns (``library_release_id``, ``artist_name``, ``track_title``) as
    produced against tubafrenzy's ``COMPILATION_TRACK_ARTIST`` table.
    ``track_title`` is nullable (MySQL ``\N``, tested against the raw field
    -- see ``_parse_nullable_field``); the other two are not, and a row
    missing either is skipped with a WARNING.

    ``artist_name`` and ``track_title`` are free text and can themselves
    contain embedded backslash/tab/newline/NUL bytes. ``mysql -B -N`` (no
    ``--raw``) escapes those into the two-char sequences ``\\``/``\t``/``\n``/
    ``\0`` before they ever reach the TSV; splitting on real tab/newline
    bytes (the same approach ``parse_library_tsv`` relies on) is safe here
    too, because the escaping happens on the wire before the split ever
    sees it. Every surviving field is then unescaped back to the real
    bytes, same as ``parse_library_tsv`` -- this parser is **not** safe to
    reuse unchanged; see that function's docstring for the ``--raw``
    fragility both parsers share.
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
                _parse_nullable_field(v) for v in fields
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
