"""E2E test for the library sync pipeline: tsv_to_sqlite + export_streaming_links."""

from __future__ import annotations

import importlib.util
import json
import re
import sqlite3
import subprocess
import sys
from argparse import Namespace
from pathlib import Path

import pytest

# Load tsv_to_sqlite from scripts directory
_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "tsv_to_sqlite.py"
_spec = importlib.util.spec_from_file_location("tsv_to_sqlite", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_tsv_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_tsv_mod)

tsv_to_sqlite = _tsv_mod.tsv_to_sqlite

# Load export_streaming_links from the sibling library-metadata-lookup repo
_LML_DIR = Path(__file__).resolve().parents[3] / "library-metadata-lookup"
_LML_SCRIPT = _LML_DIR / "scripts" / "export_streaming_links.py"

_has_lml = _LML_SCRIPT.exists()

# WXYC/discogs-etl#339: build_compilation_track_location.py (unlike
# export_streaming_links.py above) imports LML's own application package and
# third-party dependencies (aiosqlite, asyncpg, fastapi, wxyc_etl, ...), so it
# can't be dynamically loaded into *this* process the way export_streaming_links
# is -- discogs-etl's own venv doesn't carry them, and scripts/sync-library.sh
# doesn't run it that way either (see the LML_PYTHON resolution there). Instead
# this shells out to the LML checkout's own venv, the same interpreter the
# production wiring prefers.
_LML_BUILD_SCRIPT = _LML_DIR / "scripts" / "build_compilation_track_location.py"
_LML_BUILD_VENV_PYTHON = _LML_DIR / ".venv" / "bin" / "python"

_has_lml_build_venv = _LML_BUILD_SCRIPT.exists() and _LML_BUILD_VENV_PYTHON.exists()


def _load_export_streaming_links():
    """Dynamically load export_streaming_links.main from the LML repo."""
    sys.path.insert(0, str(_LML_DIR))
    try:
        from scripts.export_streaming_links import main

        return main
    finally:
        sys.path.pop(0)


@pytest.mark.skipif(not _has_lml, reason="library-metadata-lookup repo not found")
class TestSyncLibraryE2E:
    """End-to-end test: generate TSV, build library.db, enrich with streaming links."""

    def test_tsv_to_sqlite_then_streaming_export(self, tmp_path: Path) -> None:
        """Generate TSV, run tsv_to_sqlite, create streaming_availability.db,
        run export_streaming_links, verify both tables exist with correct data."""
        # Step 1: Generate TSV data
        lines = [
            "10001\tAluminum Tunes\tStereolab\tST\t1234\t1\tRock\tCD\t\\N",
            "10002\tDOGA\tJuana Molina\tMO\t5678\t2\tRock\tLP\t\\N",
            "10003\tConfield\tAutechre\tAU\t9012\t3\tElectronic\tCD\t\\N",
        ]
        tsv_file = tmp_path / "mysql_output.tsv"
        tsv_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        # Step 2: Run tsv_to_sqlite
        library_db_path = tmp_path / "library.db"
        count = tsv_to_sqlite(str(tsv_file), str(library_db_path))
        assert count == 3

        # Step 3: Create a streaming_availability.db with test data
        streaming_db_path = tmp_path / "streaming_availability.db"
        sa_conn = sqlite3.connect(str(streaming_db_path))
        sa_conn.execute("""
            CREATE TABLE albums (
                id INTEGER PRIMARY KEY,
                library_ids TEXT,
                spotify_url TEXT,
                apple_url TEXT,
                deezer_url TEXT,
                bandcamp_url TEXT,
                tidal_url TEXT,
                youtube_music_url TEXT,
                soundcloud_url TEXT
            )
        """)
        sa_conn.execute(
            "INSERT INTO albums (library_ids, spotify_url, apple_url, deezer_url, "
            "bandcamp_url, tidal_url, youtube_music_url, soundcloud_url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                json.dumps([10001]),
                "https://open.spotify.com/album/stereolab-aluminum",
                "https://music.apple.com/album/stereolab-aluminum",
                None,
                None,
                None,
                None,
                None,
            ),
        )
        sa_conn.execute(
            "INSERT INTO albums (library_ids, spotify_url, apple_url, deezer_url, "
            "bandcamp_url, tidal_url, youtube_music_url, soundcloud_url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                json.dumps([10003]),
                "https://open.spotify.com/album/autechre-confield",
                None,
                "https://www.deezer.com/album/autechre-confield",
                None,
                None,
                None,
                None,
            ),
        )
        sa_conn.commit()
        sa_conn.close()

        # Step 4: Run export_streaming_links
        export_main = _load_export_streaming_links()
        args = Namespace(
            library_db=str(library_db_path),
            streaming_db=str(streaming_db_path),
            dry_run=False,
        )
        export_main(args)

        # Step 5: Verify both tables exist with correct data
        conn = sqlite3.connect(str(library_db_path))

        # Verify library table
        lib_count = conn.execute("SELECT COUNT(*) FROM library").fetchone()[0]
        assert lib_count == 3

        # Verify streaming_links table was created
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert "library" in tables
        assert "streaming_links" in tables

        # Verify streaming links content
        links = conn.execute(
            "SELECT library_id, spotify_url, apple_music_url, deezer_url "
            "FROM streaming_links ORDER BY library_id"
        ).fetchall()
        assert len(links) == 2

        # Stereolab entry
        assert links[0][0] == 10001
        assert links[0][1] == "https://open.spotify.com/album/stereolab-aluminum"
        assert links[0][2] == "https://music.apple.com/album/stereolab-aluminum"
        assert links[0][3] is None

        # Autechre entry
        assert links[1][0] == 10003
        assert links[1][1] == "https://open.spotify.com/album/autechre-confield"
        assert links[1][2] is None
        assert links[1][3] == "https://www.deezer.com/album/autechre-confield"

        # Verify FTS still works after streaming enrichment
        fts_hits = conn.execute(
            "SELECT rowid FROM library_fts WHERE library_fts MATCH 'Autechre'"
        ).fetchall()
        assert len(fts_hits) == 1
        assert fts_hits[0][0] == 10003

        conn.close()


@pytest.mark.skipif(
    not _has_lml_build_venv,
    reason="library-metadata-lookup build_compilation_track_location.py or its .venv not found",
)
class TestCompilationTrackLocationInvocation:
    """Light coverage for the sync-library.sh -> build_compilation_track_location.py
    wiring (WXYC/discogs-etl#339).

    Not a full recall-index build -- that needs a live discogs-cache Postgres
    with va_release/release_track_artist data, which is out of scope for an e2e
    test in this repo. This exercises the two DB-independent pieces of the
    cross-repo contract instead: the CLI accepts the flags sync-library.sh
    passes (--incremental/--full + --library-db), and the script's
    library.db reader (load_library_compilations) correctly recognizes
    compilation-shelf rows in a library.db built by *this* repo's
    tsv_to_sqlite.py. A schema drift between the two repos fails this test
    instead of only surfacing as a silent "0 candidates" in production.
    """

    def test_incremental_args_and_library_reader_against_real_library_db(
        self, tmp_path: Path
    ) -> None:
        # 11 columns: id, title, artist, call_letters, artist_call_number,
        # release_call_number, genre, format, alternate_artist_name,
        # album_artist, cross_reference_names (tsv_to_sqlite.py's current schema).
        lines = [
            "20001\tVarious Artists Sampler\tVarious Artists\tVA\t0001\t1\tRock\tCD\t\\N\t\\N\t\\N",
            "20002\tAluminum Tunes\tStereolab\tST\t1234\t2\tRock\tCD\t\\N\t\\N\t\\N",
        ]
        tsv_file = tmp_path / "mysql_output.tsv"
        tsv_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        library_db_path = tmp_path / "library.db"
        count = tsv_to_sqlite(str(tsv_file), str(library_db_path))
        assert count == 2

        # Run under LML's own venv (has aiosqlite, wxyc_etl, ...), not this
        # process's -- the same interpreter scripts/sync-library.sh prefers.
        probe = (
            "import asyncio\n"
            "from scripts.build_compilation_track_location import (\n"
            "    _build_arg_parser,\n"
            "    load_library_compilations,\n"
            ")\n"
            "\n"
            f"args = _build_arg_parser().parse_args(\n"
            f"    ['--incremental', '--library-db', {str(library_db_path)!r}]\n"
            ")\n"
            "assert args.incremental is True\n"
            "assert args.full is False\n"
            f"assert args.library_db == {str(library_db_path)!r}\n"
            "\n"
            "comps = asyncio.run(load_library_compilations(args.library_db))\n"
            "print(','.join(str(c.library_id) for c in comps))\n"
        )
        result = subprocess.run(
            [str(_LML_BUILD_VENV_PYTHON), "-c", probe],
            capture_output=True,
            text=True,
            cwd=str(_LML_DIR),
        )
        assert result.returncode == 0, result.stderr

        comp_ids = {int(x) for x in result.stdout.strip().split(",") if x}
        # Only the "Various Artists" row is compilation-shelf (Stereolab isn't);
        # is_compilation_artist is the same wxyc_etl classifier discogs-etl uses.
        assert comp_ids == {20001}


# --- literal-NULL-string bugfix (verified in prod 2026-08-02) --------------
#
# ``mysql -B -N`` (the CLI mode sync-library.sh uses) prints a genuine SQL
# NULL on this server as the literal 4-character text "NULL", not the "\N"
# sentinel tsv_to_sqlite.py's parser expects. Left unwrapped, that literal
# text lands in SQLite as the *string* 'NULL' -- and since album_artist feeds
# library_fts, a typed search for "null" matched the whole catalog.
#
# The fix is in the SQL text (IFNULL(<col>, '')), not in tsv_to_sqlite.py's
# Python: an artist genuinely named "NULL" must survive, so string-sniffing
# for the text 'NULL' would silently corrupt that row instead. There is no
# MySQL server available in this environment, so these tests execute the
# *actual* wrapped column expressions -- extracted live from
# scripts/sync-library.sh -- against SQLite, which implements IFNULL
# identically to MySQL for a plain column reference. This is a stronger,
# self-updating proof than a hand-duplicated SQL string: if the wrap is
# missing or malformed, the test fails for the right reason (the bare column
# selects real NULL/None instead of '').
_SYNC_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "sync-library.sh"

# Anchor on the actual `-e "SELECT ..."` mysql invocations, not the whole file
# -- the file's own explanatory comments (necessarily) mention these same
# column names in prose, and a naive whole-file search could match a comment
# instead of the live SQL.
_LIBRARY_SELECT_RE = re.compile(
    r"-e \"(SELECT r\.ID.*?"
    r"FROM LIBRARY_RELEASE r JOIN LIBRARY_CODE lc ON r\.LIBRARY_CODE_ID = lc\.ID "
    r"JOIN FORMAT f ON r\.FORMAT_ID = f\.ID JOIN GENRE g ON lc\.GENRE_ID = g\.ID)\"",
    re.DOTALL,
)
_CTA_SELECT_RE = re.compile(
    r"-e \"(SELECT LIBRARY_RELEASE_ID.*?FROM COMPILATION_TRACK_ARTIST ORDER BY LIBRARY_RELEASE_ID)\"",
    re.DOTALL,
)


def _library_select_text() -> str:
    match = _LIBRARY_SELECT_RE.search(_SYNC_SCRIPT.read_text())
    assert match, f"could not locate the LIBRARY_RELEASE SELECT in {_SYNC_SCRIPT.name}"
    return match.group(1)


def _cta_select_text() -> str:
    match = _CTA_SELECT_RE.search(_SYNC_SCRIPT.read_text())
    assert match, f"could not locate the COMPILATION_TRACK_ARTIST SELECT in {_SYNC_SCRIPT.name}"
    return match.group(1)


def _extract_column_expr(select_text: str, table_alias: str, column: str) -> str:
    """Return whatever expression currently selects `column` in `select_text`
    (the isolated SQL SELECT clause, not the whole script): either the bare
    `alias.COLUMN` reference or an `IFNULL(alias.COLUMN, '')` wrap, whichever
    the script actually contains right now."""
    pattern = re.compile(
        rf"(IFNULL\({re.escape(table_alias)}\.{re.escape(column)}, ''\)"
        rf"|{re.escape(table_alias)}\.{re.escape(column)})"
    )
    match = pattern.search(select_text)
    assert match, f"could not find {table_alias}.{column} in the SELECT clause"
    return match.group(1)


def _extract_bare_column_expr(select_text: str, column: str) -> str:
    """Same as _extract_column_expr but for the un-aliased CTA SELECT."""
    pattern = re.compile(rf"(IFNULL\({re.escape(column)}, ''\)|{re.escape(column)})")
    match = pattern.search(select_text)
    assert match, f"could not find {column} in the SELECT clause"
    return match.group(1)


class TestLibrarySelectRealNullVsLiteralNullText:
    """A real SQL NULL must become '' (via IFNULL); an artist/track genuinely
    named the text "NULL" must pass through untouched."""

    def test_album_artist_real_null_becomes_empty_string_literal_null_text_survives(
        self,
    ) -> None:
        expr = _extract_column_expr(_library_select_text(), "r", "ALBUM_ARTIST")

        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE r (ALBUM_ARTIST TEXT)")
        conn.execute("INSERT INTO r (ALBUM_ARTIST) VALUES (NULL)")
        conn.execute("INSERT INTO r (ALBUM_ARTIST) VALUES ('NULL')")
        rows = [row[0] for row in conn.execute(f"SELECT {expr} FROM r").fetchall()]
        conn.close()

        assert rows == ["", "NULL"], (
            "a real SQL NULL album_artist must become '' and a literal text "
            "'NULL' album_artist must survive unchanged"
        )

    def test_alternate_artist_name_real_null_becomes_empty_string_literal_null_text_survives(
        self,
    ) -> None:
        expr = _extract_column_expr(_library_select_text(), "r", "ALTERNATE_ARTIST_NAME")

        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE r (ALTERNATE_ARTIST_NAME TEXT)")
        conn.execute("INSERT INTO r (ALTERNATE_ARTIST_NAME) VALUES (NULL)")
        conn.execute("INSERT INTO r (ALTERNATE_ARTIST_NAME) VALUES ('NULL')")
        rows = [row[0] for row in conn.execute(f"SELECT {expr} FROM r").fetchall()]
        conn.close()

        assert rows == ["", "NULL"]

    def test_cross_reference_names_ifnull_pattern_converts_null_subquery_to_empty_string(
        self,
    ) -> None:
        """The real cross_reference_names expression is a correlated
        GROUP_CONCAT subquery using MySQL-only syntax (``SEPARATOR``, an
        old-style comma-join) that doesn't parse under SQLite, so this
        doesn't execute the literal production text the way the two tests
        above do. It instead proves the same *pattern* the fix applies to
        that subquery -- IFNULL wrapped around an expression that yields SQL
        NULL when no cross-reference exists (the common case: 63,904 NULL
        rows in prod) -- converts that NULL to ''. The wiring test in
        tests/unit/test_sync_library_null_handling.py separately pins that
        the actual subquery in sync-library.sh is wrapped this way.
        """
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE lc (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE xref (lc_id INTEGER, other_name TEXT)")
        conn.execute("INSERT INTO lc (id) VALUES (1)")
        # No matching xref row for lc.id = 1 -> the correlated subquery below
        # returns SQL NULL, mirroring the no-cross-reference case in prod.
        rows = conn.execute(
            "SELECT IFNULL("
            "  (SELECT group_concat(other_name) FROM xref WHERE xref.lc_id = lc.id),"
            "  ''"
            ") FROM lc"
        ).fetchall()
        conn.close()

        assert rows == [("",)]


class TestCompilationTrackArtistRealNullVsLiteralNullText:
    def test_track_title_real_null_becomes_empty_string_literal_null_text_survives(
        self,
    ) -> None:
        expr = _extract_bare_column_expr(_cta_select_text(), "TRACK_TITLE")

        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE t (TRACK_TITLE TEXT)")
        conn.execute("INSERT INTO t (TRACK_TITLE) VALUES (NULL)")
        conn.execute("INSERT INTO t (TRACK_TITLE) VALUES ('NULL')")
        rows = [row[0] for row in conn.execute(f"SELECT {expr} FROM t").fetchall()]
        conn.close()

        assert rows == ["", "NULL"]
