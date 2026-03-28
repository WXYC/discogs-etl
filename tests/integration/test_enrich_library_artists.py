"""Integration tests for scripts/enrich_library_artists.py against WXYC MySQL."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

# Load enrich_library_artists module
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "enrich_library_artists.py"
_spec = importlib.util.spec_from_file_location("enrich_library_artists", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
sys.modules["enrich_library_artists"] = _mod
_spec.loader.exec_module(_mod)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from lib.wxyc import connect_mysql  # noqa: E402

extract_alternate_names = _mod.extract_alternate_names
extract_cross_referenced_artists = _mod.extract_cross_referenced_artists
extract_release_cross_ref_artists = _mod.extract_release_cross_ref_artists
extract_base_artists = _mod.extract_base_artists
merge_and_write = _mod.merge_and_write

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

pytestmark = pytest.mark.mysql

MYSQL_URL = os.environ.get("WXYC_DB_URL", "mysql://root:wxyc@localhost:3307/wxycmusic")


@pytest.fixture(scope="module")
def mysql_conn():
    """Connect to the WXYC MySQL database."""
    conn = connect_mysql(MYSQL_URL)
    yield conn
    conn.close()


class TestExtractAlternateNames:
    """Extract alternate artist names from LIBRARY_RELEASE."""

    def test_returns_nonempty_set(self, mysql_conn) -> None:
        names = extract_alternate_names(mysql_conn)
        assert len(names) > 100

    def test_contains_known_alternates(self, mysql_conn) -> None:
        names = extract_alternate_names(mysql_conn)
        names_lower = {n.lower() for n in names}
        assert "body count" in names_lower
        assert "bobby digital" in names_lower
        assert "common sense" in names_lower


class TestExtractCrossReferencedArtists:
    """Extract artist names from LIBRARY_CODE_CROSS_REFERENCE."""

    def test_returns_nonempty_set(self, mysql_conn) -> None:
        names = extract_cross_referenced_artists(mysql_conn)
        assert len(names) > 10

    def test_contains_known_cross_refs(self, mysql_conn) -> None:
        names = extract_cross_referenced_artists(mysql_conn)
        names_lower = {n.lower() for n in names}
        # "Crooked Fingers is filed w/ Eric Bachmann"
        assert "eric bachmann" in names_lower
        assert "crooked fingers" in names_lower


class TestExtractReleaseCrossRefArtists:
    """Extract artist names from RELEASE_CROSS_REFERENCE."""

    def test_returns_nonempty_set(self, mysql_conn) -> None:
        names = extract_release_cross_ref_artists(mysql_conn)
        assert len(names) > 5


class TestFullEnrichment:
    """End-to-end enrichment produces more artists than base set."""

    def test_enriched_set_is_larger(self, mysql_conn, tmp_path: Path) -> None:
        base = extract_base_artists(FIXTURES_DIR / "library.db")
        alternates = extract_alternate_names(mysql_conn)
        cross_refs = extract_cross_referenced_artists(mysql_conn)
        release_cross_refs = extract_release_cross_ref_artists(mysql_conn)

        output = tmp_path / "enriched.txt"
        merge_and_write(base, alternates, cross_refs, release_cross_refs, output)

        lines = output.read_text().splitlines()
        assert len(lines) > len(base)
