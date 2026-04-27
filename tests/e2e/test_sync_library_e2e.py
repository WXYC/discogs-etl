"""E2E test for the library sync pipeline: tsv_to_sqlite + export_streaming_links."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
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
