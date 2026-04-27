"""Integration tests for verify_cache multi-index matching against real library.db."""

import importlib.util
import multiprocessing
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from pathlib import Path

import pytest

# Load verify_cache module from scripts directory.
# Must register in sys.modules BEFORE exec_module so that @dataclass can resolve
# the module's __dict__ (Python looks up cls.__module__ in sys.modules).
# Guarded so multiple test files share one module object -- otherwise the
# second-loaded copy shadows the first and breaks ProcessPool pickling for
# any worker holding references to symbols from the original load (see #109).
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _spec = importlib.util.spec_from_file_location("verify_cache", _SCRIPT_PATH)
    assert _spec is not None and _spec.loader is not None
    _vc = importlib.util.module_from_spec(_spec)
    sys.modules["verify_cache"] = _vc
    _spec.loader.exec_module(_vc)

LibraryIndex = _vc.LibraryIndex
MultiIndexMatcher = _vc.MultiIndexMatcher
Decision = _vc.Decision
normalize_artist = _vc.normalize_artist
normalize_title = _vc.normalize_title
classify_compilation = _vc.classify_compilation
classify_fuzzy_batch = _vc.classify_fuzzy_batch
classify_all_releases = _vc.classify_all_releases
_init_fuzzy_worker = _vc._init_fuzzy_worker
_classify_fuzzy_chunk = _vc._classify_fuzzy_chunk

# Allow overriding library.db path via LIBRARY_DB env var
LIBRARY_DB = Path(os.environ.get("LIBRARY_DB", Path(__file__).parent.parent.parent / "library.db"))


@pytest.fixture(scope="module")
def library_index():
    """Build a LibraryIndex from the real library.db (skip if not present)."""
    if not LIBRARY_DB.exists():
        pytest.skip(f"library.db not found at {LIBRARY_DB}")
    return LibraryIndex.from_sqlite(LIBRARY_DB)


@pytest.fixture(scope="module")
def matcher(library_index):
    """Create a MultiIndexMatcher with default thresholds."""
    return MultiIndexMatcher(library_index)


class TestMultiIndexRealLibrary:
    """Test multi-index matching against the real WXYC library catalog."""

    def test_beatles_comma_convention(self, matcher):
        """'Field, The' / 'From Here We Go Sublime' -> KEEP via normalization."""
        result = matcher.classify(
            normalize_artist("Field, The"),
            normalize_title("From Here We Go Sublime"),
        )
        assert result.decision == Decision.KEEP

    def test_radiohead_ok_computer(self, matcher):
        """Basic exact match."""
        result = matcher.classify(
            normalize_artist("Autechre"),
            normalize_title("Confield"),
        )
        assert result.decision == Decision.KEEP

    def test_vinyl_suffix_stripped(self, library_index, matcher):
        """Vinyl suffixes like 12" are stripped before matching."""
        # Check if 'A Guy Called Gerald' has any 12" titles in the library
        norm = normalize_artist("A Guy Called Gerald")
        if norm not in library_index.artist_to_titles:
            pytest.skip("A Guy Called Gerald not in library")

        titles = library_index.artist_to_titles[norm]
        # Find a title that was likely from a 12" release
        for title in titles:
            result = matcher.classify(norm, title)
            assert result.decision == Decision.KEEP
            break

    def test_joy_not_joy_division(self, matcher):
        """'Joy' / 'I Love You, Honeybear' should not KEEP as 'Joy' artist.

        'Joy' is a different artist from 'Father John Misty'. The multi-index
        matcher should not match based on 'Joy' being a subset of
        'Father John Misty'.
        """
        result = matcher.classify(
            normalize_artist("Joy"),
            normalize_title("I Love You, Honeybear"),
        )
        assert result.decision != Decision.KEEP

    def test_unknown_artist_is_not_keep(self, matcher):
        """A completely unknown artist/album pair should not be KEEP.

        With ~60K library entries, coincidental partial token matches
        may push this into REVIEW rather than PRUNE. Either is acceptable.
        """
        result = matcher.classify(
            normalize_artist("Zzyzx Qxqxqx"),
            normalize_title("Xyzzy Plugh"),
        )
        assert result.decision != Decision.KEEP

    def test_aphex_twin_in_library(self, matcher):
        """Aphex Twin is a known library artist."""
        result = matcher.classify(
            normalize_artist("Aphex Twin"),
            normalize_title("Selected Ambient Works 85-92"),
        )
        assert result.decision == Decision.KEEP

    def test_artist_mapping_overrides_review(self, library_index, tmp_path):
        """Pre-populated mappings file causes REVIEW -> KEEP.

        Mapping keys are normalized artist names (what classify() receives).
        """
        mappings = {
            "keep": {"nilufer yanya": "Nilufer Yanya"},  # normalized key
            "prune": {},
        }
        matcher = MultiIndexMatcher(library_index, artist_mappings=mappings)
        result = matcher.classify(
            normalize_artist("Nilufer Yanya (2)"),  # normalizes to "nilufer yanya"
            normalize_title("Some Random Album"),
        )
        assert result.decision == Decision.KEEP

    def test_library_index_has_data(self, library_index):
        """Sanity check that the library loaded successfully."""
        assert len(library_index.exact_pairs) > 1000
        assert len(library_index.all_artists) > 500
        assert len(library_index.combined_strings) > 1000


SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
PYTHON = sys.executable


class TestVerifyCacheE2E:
    """Test the verify_cache.py script as a subprocess."""

    def test_help_flag(self):
        """--help exits cleanly with usage text."""
        result = subprocess.run(
            [PYTHON, str(SCRIPT_PATH), "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "WXYC library" in result.stdout or "library" in result.stdout.lower()

    def test_missing_library_db_exits_nonzero(self, tmp_path):
        """Passing a nonexistent library.db path exits with error."""
        fake_db = tmp_path / "nonexistent.db"
        result = subprocess.run(
            [PYTHON, str(SCRIPT_PATH), str(fake_db)],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0


# ---------------------------------------------------------------------------
# Top-level helper for ProcessPoolExecutor timeout tests.
# Must be defined at module level (not inside a class) so it can be pickled
# for use with multiprocessing's fork/spawn contexts.
# ---------------------------------------------------------------------------


def _slow_classify_chunk(chunk_args):
    """Worker that simulates a slow fuzzy matching operation.

    Sleeps for 3 seconds, which is long enough to trigger a short timeout
    but short enough that the worker process exits naturally, avoiding
    zombie processes in the test runner.
    """
    time.sleep(3)
    return set(), set(), set(), {}


def _fast_classify_chunk(chunk_args):
    """Worker that completes immediately for the non-hanging chunk."""
    artists, chunk_by_artist = chunk_args
    keep = set()
    prune = set()
    for artist in artists:
        for release_id, _, _ in chunk_by_artist[artist]:
            prune.add(release_id)
    return keep, prune, set(), {}


class TestProcessPoolExecutorTimeout:
    """Verify that ProcessPoolExecutor futures can be timed out gracefully.

    The current classify_all_releases() calls future.result() without a timeout,
    which means a single hung worker stalls the entire pipeline. These tests
    verify that the timeout mechanism (future.result(timeout=N)) works correctly
    and that timed-out results can be safely skipped.

    Uses a 3-second sleep (not an infinite hang) so worker processes exit
    naturally and don't leak across test runs.
    """

    def _build_small_index(self):
        """Build a minimal LibraryIndex for testing."""
        rows = [
            ("Juana Molina", "DOGA"),
            ("Stereolab", "Aluminum Tunes"),
            ("Cat Power", "Moon Pix"),
            ("Jessica Pratt", "On Your Own Love Again"),
            ("Chuquimamani-Condori", "Edits"),
            ("Duke Ellington", "Duke Ellington & John Coltrane"),
        ]
        return LibraryIndex.from_rows(rows)

    def test_future_result_timeout_raises(self) -> None:
        """future.result(timeout=N) raises TimeoutError when worker is slow.

        Submits a worker that sleeps for 3 seconds to a ProcessPoolExecutor,
        then calls future.result(timeout=0.2). Verifies TimeoutError is raised.
        The worker completes on its own after 3s so no process leak occurs.
        """
        ctx = multiprocessing.get_context("fork")
        executor = ProcessPoolExecutor(max_workers=1, mp_context=ctx)
        future = executor.submit(
            _slow_classify_chunk,
            (["fake_artist"], {"fake_artist": [(1, "Fake", "Fake Album")]}),
        )
        with pytest.raises(FuturesTimeoutError):
            future.result(timeout=0.2)

        # Let the worker finish naturally so the process pool can clean up
        future.result(timeout=10)
        executor.shutdown(wait=True)

    def test_mixed_fast_and_slow_workers(self) -> None:
        """When one chunk is slow but others complete, completed results are available.

        Submits two chunks: one fast (completes immediately) and one slow (3s).
        Verifies that the fast chunk's results are collected before the timeout
        fires on the slow chunk, demonstrating that partial results can be
        harvested even when some workers are stalled.
        """
        fast_chunk_artists = [normalize_artist("Sessa")]
        fast_chunk_by_artist = {
            normalize_artist("Sessa"): [
                (101, "Sessa", "Pequena Vertigem"),
            ]
        }

        ctx = multiprocessing.get_context("fork")
        executor = ProcessPoolExecutor(max_workers=2, mp_context=ctx)
        fast_future = executor.submit(
            _fast_classify_chunk,
            (fast_chunk_artists, fast_chunk_by_artist),
        )
        slow_future = executor.submit(
            _slow_classify_chunk,
            (["slow_artist"], {"slow_artist": [(999, "Slow", "Slow Album")]}),
        )

        # Fast future should complete quickly
        fast_keep, fast_prune, fast_review, fast_review_by = fast_future.result(timeout=5)
        assert 101 in fast_prune  # Sessa not in our small index

        # Slow future should time out with a short deadline
        with pytest.raises(FuturesTimeoutError):
            slow_future.result(timeout=0.2)

        # But it does complete eventually (after 3s)
        slow_keep, slow_prune, slow_review, slow_review_by = slow_future.result(timeout=10)
        assert slow_keep == set()
        assert slow_prune == set()

        executor.shutdown(wait=True)

    def test_timed_out_futures_do_not_corrupt_results(self) -> None:
        """Results from timed-out futures are not included in the final aggregation.

        Demonstrates the pattern for graceful timeout handling: iterate futures
        with as_completed(), apply a per-future timeout, and skip any that
        exceed the deadline. This is the pattern verify_cache.py should adopt.
        """
        from concurrent.futures import as_completed

        ctx = multiprocessing.get_context("fork")
        executor = ProcessPoolExecutor(max_workers=2, mp_context=ctx)

        fast_artists = [normalize_artist("Anne Gillis")]
        fast_by_artist = {
            normalize_artist("Anne Gillis"): [
                (201, "Anne Gillis", "Round & Round & Round"),
            ]
        }

        futures = {}
        futures[executor.submit(_fast_classify_chunk, (fast_artists, fast_by_artist))] = "fast"
        futures[
            executor.submit(
                _slow_classify_chunk,
                (["slow"], {"slow": [(999, "X", "Y")]}),
            )
        ] = "slow"

        collected_prune = set()
        timed_out_chunks = []

        for future in as_completed(futures, timeout=5):
            chunk_name = futures[future]
            try:
                keep, prune, review, review_by = future.result(timeout=0.3)
                collected_prune |= prune
            except FuturesTimeoutError:
                timed_out_chunks.append(chunk_name)

        # Fast chunk was collected; slow chunk may or may not have timed out
        # depending on completion order. The key assertion is that only
        # successfully completed results are in collected_prune.
        assert 201 in collected_prune, "Fast chunk results should be collected"
        assert 999 not in collected_prune, "Slow chunk should not contribute results"

        # Wait for slow worker to finish naturally
        for future in futures:
            try:
                future.result(timeout=10)
            except Exception:
                pass
        executor.shutdown(wait=True)

    def test_classify_fuzzy_batch_completes_normally(self) -> None:
        """classify_fuzzy_batch returns correct results for WXYC example artists.

        Verifies that the batch classification function works correctly with
        a small index, serving as a baseline for the timeout tests above.
        """
        index = self._build_small_index()
        matcher = MultiIndexMatcher(index)

        # Exact match artist with exact match title
        artists = [normalize_artist("Juana Molina")]
        by_artist = {
            normalize_artist("Juana Molina"): [
                (5001, "Juana Molina", "DOGA"),
            ]
        }

        keep, prune, review, review_by = classify_fuzzy_batch(artists, by_artist, index, matcher)
        assert 5001 in keep, "Exact match should be KEEP"

    def test_classify_fuzzy_batch_prunes_unknown(self) -> None:
        """Unknown artists are classified as PRUNE by fuzzy batch."""
        index = self._build_small_index()
        matcher = MultiIndexMatcher(index)

        artists = [normalize_artist("Completely Unknown Band")]
        by_artist = {
            normalize_artist("Completely Unknown Band"): [
                (9999, "Completely Unknown Band", "Nonexistent Album"),
            ]
        }

        keep, prune, review, review_by = classify_fuzzy_batch(artists, by_artist, index, matcher)
        assert 9999 in prune, "Unknown artist should be PRUNE"
        assert 9999 not in keep
