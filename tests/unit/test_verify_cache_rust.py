"""Tests for the Rust (wxyc_etl) integration path in verify_cache.

These tests verify that:
1. The conditional import + fallback logic works correctly
2. The Rust path produces correct classifications (when available)
3. The WXYC_ETL_NO_RUST env var forces the Python fallback
4. Both paths handle compilations and format filtering
"""

import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Load verify_cache module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
_spec = importlib.util.spec_from_file_location("verify_cache_rust_test", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_vc = importlib.util.module_from_spec(_spec)
sys.modules["verify_cache_rust_test"] = _vc
_spec.loader.exec_module(_vc)

normalize_artist = _vc.normalize_artist
normalize_title = _vc.normalize_title
LibraryIndex = _vc.LibraryIndex
MultiIndexMatcher = _vc.MultiIndexMatcher
Decision = _vc.Decision
classify_all_releases = _vc.classify_all_releases

# Check if wxyc_etl is available
try:
    import wxyc_etl  # noqa: F401
    from wxyc_etl.fuzzy import batch_classify_releases  # noqa: F401

    HAS_WXYC_ETL = True
except ImportError:
    HAS_WXYC_ETL = False

# Shared fixture matching the WXYC example data
SAMPLE_LIBRARY_ROWS = [
    ("Juana Molina", "DOGA", "LP"),
    ("Stereolab", "Aluminum Tunes", "CD"),
    ("Cat Power", "Moon Pix", "LP"),
    ("Jessica Pratt", "On Your Own Love Again", "LP"),
    ("Chuquimamani-Condori", "Edits", "CD"),
    ("Duke Ellington", "Duke Ellington & John Coltrane", "LP"),
    ("Autechre", "Confield", "CD"),
    ("Prince Jammy", "...Destroys The Space Invaders", "LP"),
    ("Sessa", "Pequena Vertigem de Amor", "LP"),
    ("Various Artists", "Sugar Hill", None),
    ("Soundtracks - S", "Lost In Translation", None),
]


@pytest.fixture
def library_index():
    """Build a LibraryIndex from WXYC example data."""
    return LibraryIndex.from_rows(SAMPLE_LIBRARY_ROWS)


# ---------------------------------------------------------------------------
# Conditional import / fallback flag tests
# ---------------------------------------------------------------------------


class TestConditionalImport:
    """Verify the _HAS_WXYC_ETL flag and WXYC_ETL_NO_RUST behavior."""

    def test_has_wxyc_etl_flag_exists(self):
        """verify_cache module exposes _HAS_WXYC_ETL flag."""
        assert hasattr(_vc, "_HAS_WXYC_ETL")

    def test_has_wxyc_etl_flag_is_bool(self):
        """_HAS_WXYC_ETL is a boolean."""
        assert isinstance(_vc._HAS_WXYC_ETL, bool)

    def test_fallback_env_var_forces_python_path(self, library_index):
        """WXYC_ETL_NO_RUST=1 forces the Python fallback path even if Rust is available."""
        matcher = MultiIndexMatcher(library_index)
        releases = [
            (1, "Juana Molina", "DOGA"),
            (2, "Unknown Artist", "Nonexistent Album"),
        ]

        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            report = classify_all_releases(releases, library_index, matcher)

        assert 1 in report.keep_ids
        assert 2 in report.prune_ids


# ---------------------------------------------------------------------------
# Rust path classification tests (skip if wxyc_etl not installed)
# ---------------------------------------------------------------------------

needs_wxyc_etl = pytest.mark.skipif(
    not HAS_WXYC_ETL,
    reason="wxyc_etl package not installed (prerequisite 1g)",
)


@needs_wxyc_etl
class TestBatchClassifyParityWithPython:
    """Verify the Rust path produces identical results to the Python path."""

    def test_exact_matches_are_keep(self, library_index):
        """Exact artist+title matches produce KEEP from both paths."""
        matcher = MultiIndexMatcher(library_index)
        releases = [
            (1, "Juana Molina", "DOGA"),
            (2, "Stereolab", "Aluminum Tunes"),
            (3, "Cat Power", "Moon Pix"),
        ]

        # Rust path
        report_rust = classify_all_releases(releases, library_index, matcher)
        # Python fallback
        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            report_python = classify_all_releases(releases, library_index, matcher)

        assert report_rust.keep_ids == report_python.keep_ids
        assert report_rust.prune_ids == report_python.prune_ids
        assert report_rust.review_ids == report_python.review_ids

    def test_non_matches_are_prune(self, library_index):
        """Completely unknown artists produce PRUNE from both paths."""
        matcher = MultiIndexMatcher(library_index)
        releases = [
            (10, "Totally Unknown XYZ", "Nonexistent Album QRS"),
            (11, "Another Fake Band", "Phantom Record ABC"),
        ]

        report_rust = classify_all_releases(releases, library_index, matcher)
        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            report_python = classify_all_releases(releases, library_index, matcher)

        assert report_rust.keep_ids == report_python.keep_ids
        assert report_rust.prune_ids == report_python.prune_ids
        assert report_rust.review_ids == report_python.review_ids

    def test_mixed_releases_parity(self, library_index):
        """Mix of matches, near-misses, and non-matches produce same decisions."""
        matcher = MultiIndexMatcher(library_index)
        releases = [
            # Exact matches
            (1, "Juana Molina", "DOGA"),
            (2, "Autechre", "Confield"),
            # Non-matches
            (3, "Totally Unknown XYZ", "Phantom Record"),
            (4, "Fake Band ABC", "Nonexistent Title"),
            # Near-miss / fuzzy
            (5, "Juana Molinna", "DOGA"),  # slight misspelling
            (6, "Cat Power", "Moon Pix (Deluxe Edition)"),  # title suffix
        ]

        report_rust = classify_all_releases(releases, library_index, matcher)
        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            report_python = classify_all_releases(releases, library_index, matcher)

        # Exact matches should agree
        assert 1 in report_rust.keep_ids and 1 in report_python.keep_ids
        assert 2 in report_rust.keep_ids and 2 in report_python.keep_ids

        # Full parity
        assert report_rust.keep_ids == report_python.keep_ids
        assert report_rust.prune_ids == report_python.prune_ids
        assert report_rust.review_ids == report_python.review_ids


@needs_wxyc_etl
class TestBatchClassifyCompilationHandling:
    """Verify compilation releases are handled identically in both paths."""

    def test_compilation_title_match_is_keep(self, library_index):
        """Compilation releases with matching titles are KEEP in both paths."""
        matcher = MultiIndexMatcher(library_index)
        releases = [
            (20, "Various Artists", "Sugar Hill"),
            (21, "Various Artists", "Unknown Compilation XYZ"),
        ]

        report_rust = classify_all_releases(releases, library_index, matcher)
        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            report_python = classify_all_releases(releases, library_index, matcher)

        assert report_rust.keep_ids == report_python.keep_ids
        assert report_rust.prune_ids == report_python.prune_ids


@needs_wxyc_etl
class TestBatchClassifyFormatFiltering:
    """Verify format-aware filtering works identically in both paths."""

    def test_format_mismatch_downgraded_to_prune(self):
        """Exact match with wrong format is PRUNE in both paths."""
        rows = [("Juana Molina", "DOGA", "LP")]
        idx = LibraryIndex.from_rows(rows)
        matcher = MultiIndexMatcher(idx)
        # Release is CD but library only has LP
        releases = [(30, "Juana Molina", "DOGA", "CD")]

        report_rust = classify_all_releases(releases, idx, matcher)
        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            report_python = classify_all_releases(releases, idx, matcher)

        assert 30 in report_rust.prune_ids
        assert report_rust.prune_ids == report_python.prune_ids

    def test_format_match_stays_keep(self):
        """Exact match with matching format stays KEEP in both paths."""
        rows = [("Juana Molina", "DOGA", "LP")]
        idx = LibraryIndex.from_rows(rows)
        matcher = MultiIndexMatcher(idx)
        releases = [(31, "Juana Molina", "DOGA", "LP")]

        report_rust = classify_all_releases(releases, idx, matcher)
        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            report_python = classify_all_releases(releases, idx, matcher)

        assert 31 in report_rust.keep_ids
        assert report_rust.keep_ids == report_python.keep_ids

    def test_null_format_stays_keep(self):
        """Exact match with NULL release format stays KEEP."""
        rows = [("Juana Molina", "DOGA", "LP")]
        idx = LibraryIndex.from_rows(rows)
        matcher = MultiIndexMatcher(idx)
        releases = [(32, "Juana Molina", "DOGA", None)]

        report_rust = classify_all_releases(releases, idx, matcher)
        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            report_python = classify_all_releases(releases, idx, matcher)

        assert 32 in report_rust.keep_ids
        assert report_rust.keep_ids == report_python.keep_ids


# ---------------------------------------------------------------------------
# Performance benchmark (slow test, skip by default)
# ---------------------------------------------------------------------------


@needs_wxyc_etl
@pytest.mark.slow
class TestBatchClassifyPerformance:
    """Benchmark: Rust path should be at least 5x faster than Python fallback."""

    def test_rust_path_faster_than_python(self, library_index):
        """Rust path is at least 5x faster on 10K synthetic releases."""
        import random
        import time

        random.seed(42)

        # Generate 10K synthetic releases
        real_artists = ["Juana Molina", "Stereolab", "Cat Power", "Autechre", "Jessica Pratt"]
        fake_artists = [f"Unknown Artist {i}" for i in range(200)]
        all_artists = real_artists * 20 + fake_artists * 40
        random.shuffle(all_artists)

        releases = []
        for i, artist in enumerate(all_artists[:10000]):
            if artist in real_artists:
                title = random.choice(["DOGA", "Aluminum Tunes", "Moon Pix", "Confield"])
            else:
                title = f"Fake Album {random.randint(0, 1000)}"
            releases.append((i, artist, title))

        matcher = MultiIndexMatcher(library_index)

        # Time Rust path
        start = time.monotonic()
        classify_all_releases(releases, library_index, matcher)
        rust_elapsed = time.monotonic() - start

        # Time Python fallback
        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            start = time.monotonic()
            classify_all_releases(releases, library_index, matcher)
            python_elapsed = time.monotonic() - start

        speedup = python_elapsed / rust_elapsed if rust_elapsed > 0 else float("inf")
        assert speedup >= 5.0, (
            f"Rust path ({rust_elapsed:.2f}s) should be 5x faster than "
            f"Python ({python_elapsed:.2f}s), got {speedup:.1f}x"
        )
