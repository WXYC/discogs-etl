"""Parity tests: verify wxyc-etl Rust fuzzy matching matches Python rapidfuzz.

These tests ensure behavioral equivalence between the Rust implementations
in wxyc-etl (exposed via PyO3) and the Python rapidfuzz library for fuzzy
string matching operations critical to the KEEP/PRUNE classification pipeline.

The Rust functions being tested:
- wxyc_etl.fuzzy.jaro_winkler_similarity  (replaces rapidfuzz.distance.JaroWinkler.similarity)
- wxyc_etl.fuzzy.batch_fuzzy_resolve      (replaces rapidfuzz.process.extract + ambiguity guard)

The Python classification pipeline (verify_cache.py MultiIndexMatcher) uses
rapidfuzz token_set_ratio / token_sort_ratio / two-stage scorers. Since the
Rust crate does not yet expose batch_classify_releases, the classification
parity tests verify that the Python MultiIndexMatcher produces expected
KEEP/PRUNE/REVIEW decisions for WXYC example data, establishing a reference
baseline for when that Rust function is added.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from rapidfuzz import process
from rapidfuzz.distance import JaroWinkler
from wxyc_etl.fuzzy import batch_fuzzy_resolve, jaro_winkler_similarity

pytestmark = pytest.mark.parity

# ---------------------------------------------------------------------------
# Load verify_cache module from scripts directory (same pattern as test_verify_cache.py)
# ---------------------------------------------------------------------------

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCORE_TOLERANCE = 0.05  # 5% tolerance for fuzzy scores

# WXYC canonical library rows: (artist, title, format)
WXYC_LIBRARY_ROWS = [
    ("Juana Molina", "DOGA", "LP"),
    ("Stereolab", "Aluminum Tunes", "CD"),
    ("Cat Power", "Moon Pix", "LP"),
    ("Jessica Pratt", "On Your Own Love Again", "LP"),
    ("Chuquimamani-Condori", "Edits", "CD"),
    ("Duke Ellington & John Coltrane", "Duke Ellington & John Coltrane", "LP"),
    ("Sessa", "Pequena Vertigem de Amor", None),
    ("Anne Gillis", "Eyry", None),
    ("Father John Misty", "I Love You, Honeybear", None),
    ("Rafael Toral", "Traveling Light", None),
    ("Buck Meek", "Gasoline", None),
    ("Nourished by Time", "The Passionate Ones", None),
    ("For Tracy Hyde", "Hotel Insomnia", None),
    ("Rochelle Jordan", "Through the Wall", None),
    ("Large Professor", "1st Class", None),
]

# Normalized catalog for batch_fuzzy_resolve tests
WXYC_ARTIST_CATALOG = [
    "juana molina",
    "stereolab",
    "cat power",
    "jessica pratt",
    "chuquimamani-condori",
    "duke ellington",
    "sessa",
    "anne gillis",
    "father john misty",
    "rafael toral",
    "buck meek",
    "nourished by time",
    "for tracy hyde",
    "rochelle jordan",
    "large professor",
]


# ---------------------------------------------------------------------------
# Jaro-Winkler Similarity Parity
# ---------------------------------------------------------------------------


class TestJaroWinklerSimilarityParity:
    """Rust jaro_winkler_similarity vs rapidfuzz JaroWinkler.similarity.

    Reference: rapidfuzz.distance.JaroWinkler.similarity (Python)
    Replacement: wxyc_etl.fuzzy.jaro_winkler_similarity (Rust/PyO3)
    """

    @pytest.mark.parametrize(
        "a, b, expected_score",
        [
            ("stereolab", "stereolab", 1.0),
            ("juana molina", "juana molina", 1.0),
            ("cat power", "cat power", 1.0),
            ("stereolab", "stereo lab", 0.98),
            ("juana mollina", "juana molina", 0.9846),
            ("duke ellington", "ellington duke", 0.6762),
            ("chuquimamani-condori", "chuquimamani condori", 0.98),
            ("jessica pratt", "jessika pratt", 0.9692),
            ("cat power", "cat stevens", 0.6700),
            ("sessa", "sessanta", 0.925),
            ("anne gillis", "ann gillis", 0.9788),
            ("buck meek", "buck meeks", 0.98),
        ],
        ids=[
            "exact-stereolab",
            "exact-juana-molina",
            "exact-cat-power",
            "space-insertion",
            "typo-juana-mollina",
            "reordered-duke-ellington",
            "hyphen-vs-space",
            "typo-jessika",
            "partial-collision-cat",
            "prefix-sessa-sessanta",
            "one-letter-diff-anne",
            "trailing-s-buck-meeks",
        ],
    )
    def test_score_within_tolerance(self, a: str, b: str, expected_score: float) -> None:
        """Both implementations produce scores within 5% tolerance."""
        rust_score = jaro_winkler_similarity(a, b)
        python_score = JaroWinkler.similarity(a, b)

        assert abs(rust_score - python_score) <= SCORE_TOLERANCE, (
            f"Rust ({rust_score:.6f}) and Python ({python_score:.6f}) "
            f"differ by more than {SCORE_TOLERANCE}"
        )
        assert abs(rust_score - expected_score) <= SCORE_TOLERANCE, (
            f"Rust score ({rust_score:.6f}) differs from expected ({expected_score:.4f}) "
            f"by more than {SCORE_TOLERANCE}"
        )

    @pytest.mark.parametrize(
        "a, b",
        [
            ("stereolab", "stereolab"),
            ("juana molina", "juana molina"),
            ("cat power", "cat power"),
            ("stereolab", "stereo lab"),
            ("juana mollina", "juana molina"),
            ("duke ellington", "ellington duke"),
            ("chuquimamani-condori", "chuquimamani condori"),
            ("jessica pratt", "jessika pratt"),
            ("cat power", "cat stevens"),
            ("sessa", "sessanta"),
            ("anne gillis", "ann gillis"),
            ("buck meek", "buck meeks"),
        ],
        ids=[
            "exact-stereolab",
            "exact-juana-molina",
            "exact-cat-power",
            "space-insertion",
            "typo-juana-mollina",
            "reordered-duke-ellington",
            "hyphen-vs-space",
            "typo-jessika",
            "partial-collision-cat",
            "prefix-sessa-sessanta",
            "one-letter-diff-anne",
            "trailing-s-buck-meeks",
        ],
    )
    def test_exact_score_match(self, a: str, b: str) -> None:
        """Rust and Python produce identical scores (not just within tolerance)."""
        rust_score = jaro_winkler_similarity(a, b)
        python_score = JaroWinkler.similarity(a, b)
        assert rust_score == pytest.approx(python_score, abs=1e-10), (
            f"Scores differ: Rust={rust_score:.10f} Python={python_score:.10f}"
        )

    def test_ranking_order_preserved(self) -> None:
        """If rapidfuzz scores A > B, Rust must also score A > B.

        This verifies that relative ranking is preserved, which is more
        important than absolute score agreement for classification decisions.
        """
        query = "stereolab"
        candidates = [
            "stereolab",       # exact match (highest)
            "stereo lab",      # near match
            "stereolaab",      # typo
            "stereolabs",      # suffix
            "stereo",          # truncated
            "cat power",       # unrelated
        ]

        python_scores = [JaroWinkler.similarity(query, c) for c in candidates]
        rust_scores = [jaro_winkler_similarity(query, c) for c in candidates]

        # Sort both by score descending and verify same ordering
        python_ranking = sorted(range(len(candidates)), key=lambda i: -python_scores[i])
        rust_ranking = sorted(range(len(candidates)), key=lambda i: -rust_scores[i])

        assert python_ranking == rust_ranking, (
            f"Ranking order differs:\n"
            f"  Python: {[(candidates[i], python_scores[i]) for i in python_ranking]}\n"
            f"  Rust:   {[(candidates[i], rust_scores[i]) for i in rust_ranking]}"
        )

    def test_symmetry(self) -> None:
        """Both implementations are symmetric: score(a, b) == score(b, a)."""
        pairs = [
            ("stereolab", "stereo lab"),
            ("duke ellington", "ellington duke"),
            ("juana molina", "juana mollina"),
        ]
        for a, b in pairs:
            assert jaro_winkler_similarity(a, b) == pytest.approx(
                jaro_winkler_similarity(b, a), abs=1e-10
            )
            assert JaroWinkler.similarity(a, b) == pytest.approx(
                JaroWinkler.similarity(b, a), abs=1e-10
            )

    def test_empty_strings(self) -> None:
        """Both implementations handle empty strings identically."""
        assert jaro_winkler_similarity("", "") == pytest.approx(
            JaroWinkler.similarity("", ""), abs=1e-10
        )
        assert jaro_winkler_similarity("stereolab", "") == pytest.approx(
            JaroWinkler.similarity("stereolab", ""), abs=1e-10
        )
        assert jaro_winkler_similarity("", "stereolab") == pytest.approx(
            JaroWinkler.similarity("", "stereolab"), abs=1e-10
        )


# ---------------------------------------------------------------------------
# Batch Fuzzy Resolve Parity
# ---------------------------------------------------------------------------


class TestBatchFuzzyResolveParity:
    """Rust batch_fuzzy_resolve vs rapidfuzz.process.extract + ambiguity guard.

    Reference: rapidfuzz.process.extract(query, choices, scorer=JaroWinkler.similarity,
               score_cutoff=0.85, limit=2) with manual ambiguity rejection (gap < 0.02)
    Replacement: wxyc_etl.fuzzy.batch_fuzzy_resolve(names, catalog, 0.85, 2, 0.02)
    """

    @staticmethod
    def _python_resolve(
        name: str,
        catalog: list[str],
        threshold: float = 0.85,
        limit: int = 2,
        ambiguity_threshold: float = 0.02,
    ) -> str | None:
        """Python reference implementation using rapidfuzz."""
        results = process.extract(
            name, catalog, scorer=JaroWinkler.similarity, score_cutoff=threshold, limit=limit
        )
        if not results:
            return None
        if len(results) >= 2 and (results[0][1] - results[1][1]) < ambiguity_threshold:
            return None
        return results[0][0]

    @pytest.mark.parametrize(
        "query, expected_match",
        [
            ("stereolab", "stereolab"),
            ("juana mollina", "juana molina"),
            ("cat power", "cat power"),
            ("unknown artist", None),
            ("jessika pratt", "jessica pratt"),
            ("chuquimamani condori", "chuquimamani-condori"),
            ("duke ellington", "duke ellington"),
            ("duke ellingtn", "duke ellington"),
            ("sessa", "sessa"),
            ("anne gillis", "anne gillis"),
            ("ann gillis", "anne gillis"),
            ("buck meeks", "buck meek"),
            ("cat stevens", None),
        ],
        ids=[
            "exact-stereolab",
            "typo-juana-mollina",
            "exact-cat-power",
            "no-match",
            "typo-jessika-pratt",
            "hyphen-removed",
            "exact-duke-ellington",
            "typo-duke-ellingtn",
            "exact-sessa",
            "exact-anne-gillis",
            "one-letter-diff-ann",
            "trailing-s-buck-meeks",
            "below-threshold-cat-stevens",
        ],
    )
    def test_same_resolved_name(self, query: str, expected_match: str | None) -> None:
        """Rust and Python resolve to the same catalog entry (or both None)."""
        rust_results = batch_fuzzy_resolve(
            [query], WXYC_ARTIST_CATALOG, 0.85, 2, 0.02
        )
        python_result = self._python_resolve(query, WXYC_ARTIST_CATALOG)

        assert rust_results[0] == python_result, (
            f"Mismatch for {query!r}: Rust={rust_results[0]!r} Python={python_result!r}"
        )
        assert rust_results[0] == expected_match, (
            f"Unexpected result for {query!r}: got={rust_results[0]!r} expected={expected_match!r}"
        )

    def test_batch_matches_individual(self) -> None:
        """Batch results match running queries one at a time."""
        queries = [
            "stereolab", "juana mollina", "cat power",
            "unknown artist", "jessika pratt", "duke ellington",
        ]
        batch_results = batch_fuzzy_resolve(queries, WXYC_ARTIST_CATALOG, 0.85, 2, 0.02)

        for i, query in enumerate(queries):
            individual = batch_fuzzy_resolve([query], WXYC_ARTIST_CATALOG, 0.85, 2, 0.02)
            assert batch_results[i] == individual[0], (
                f"Batch vs individual mismatch for {query!r}: "
                f"batch={batch_results[i]!r} individual={individual[0]!r}"
            )

    def test_ambiguity_rejection_duplicate_catalog(self) -> None:
        """When top-2 candidates score identically (duplicate), both return None.

        This verifies the ambiguity guard: if the gap between the top two
        scores is less than ambiguity_threshold (0.02), the match is rejected.
        """
        # Catalog with duplicate entry
        catalog_with_dup = ["for tracy hyde", "for tracy hyde", "buck meek"]
        query = "for tracy hide"  # typo -- matches both duplicates equally

        rust_results = batch_fuzzy_resolve([query], catalog_with_dup, 0.85, 2, 0.02)
        python_result = self._python_resolve(query, catalog_with_dup)

        assert rust_results[0] is None, "Rust should reject ambiguous match"
        assert python_result is None, "Python should reject ambiguous match"

    def test_threshold_boundary(self) -> None:
        """Queries just below and just above threshold are handled consistently."""
        # "sessa" vs "sessanta" has JW similarity of 0.925 -- above 0.85
        above = batch_fuzzy_resolve(["sessanta"], ["sessa"], 0.85, 2, 0.02)
        assert above[0] == "sessa"

        # With a higher threshold (0.95), the same pair should not match
        above_strict = batch_fuzzy_resolve(["sessanta"], ["sessa"], 0.95, 2, 0.02)
        assert above_strict[0] is None

    def test_empty_inputs(self) -> None:
        """Empty query list and empty catalog are handled without errors."""
        assert batch_fuzzy_resolve([], WXYC_ARTIST_CATALOG, 0.85, 2, 0.02) == []
        assert batch_fuzzy_resolve(["stereolab"], [], 0.85, 2, 0.02) == [None]


# ---------------------------------------------------------------------------
# Classification Parity (Python MultiIndexMatcher reference baseline)
# ---------------------------------------------------------------------------


class TestClassificationParity:
    """Verify MultiIndexMatcher KEEP/PRUNE/REVIEW decisions on WXYC example data.

    This class establishes the reference classification baseline using the
    Python rapidfuzz-backed MultiIndexMatcher (verify_cache.py). When
    batch_classify_releases is added to wxyc-etl, a companion test class
    should verify identical classifications from the Rust implementation.

    Reference: verify_cache.py MultiIndexMatcher.classify() with rapidfuzz scorers
    Future replacement: wxyc_etl.fuzzy.batch_classify_releases()
    """

    @pytest.fixture()
    def library_index(self) -> LibraryIndex:
        """Build LibraryIndex from WXYC example data."""
        return LibraryIndex.from_rows(WXYC_LIBRARY_ROWS)

    @pytest.fixture()
    def matcher(self, library_index: LibraryIndex) -> MultiIndexMatcher:
        """Create a MultiIndexMatcher with default thresholds."""
        return MultiIndexMatcher(library_index)

    @pytest.mark.parametrize(
        "artist, title, expected_decision",
        [
            # Exact matches -> KEEP
            ("Juana Molina", "DOGA", "KEEP"),
            ("Stereolab", "Aluminum Tunes", "KEEP"),
            ("Cat Power", "Moon Pix", "KEEP"),
            ("Jessica Pratt", "On Your Own Love Again", "KEEP"),
            ("Chuquimamani-Condori", "Edits", "KEEP"),
            ("Buck Meek", "Gasoline", "KEEP"),
            # Case and diacritics variations -> KEEP (normalizer handles)
            ("STEREOLAB", "ALUMINUM TUNES", "KEEP"),
            ("juana molina", "doga", "KEEP"),
            # Title variations -> KEEP (normalize_title strips suffixes)
            ("Jessica Pratt", "On Your Own Love Again (Reissue)", "KEEP"),
            ("Chuquimamani-Condori (2)", "Edits", "KEEP"),
            ("Buck Meek", "Gasoline (LP)", "KEEP"),
            # Artist subset of multi-artist entry -> KEEP (component splitting)
            ("Duke Ellington", "Duke Ellington & John Coltrane", "KEEP"),
            # No match -> PRUNE
            ("Autechre", "Confield", "PRUNE"),
            ("Prince Jammy", "Destroys The Space Invaders", "PRUNE"),
            ("Aphex Twin", "Selected Ambient Works", "PRUNE"),
        ],
        ids=[
            "exact-juana-molina",
            "exact-stereolab",
            "exact-cat-power",
            "exact-jessica-pratt",
            "exact-chuquimamani",
            "exact-buck-meek",
            "case-stereolab",
            "case-juana-molina",
            "title-reissue-suffix",
            "discogs-disambiguation",
            "title-lp-suffix",
            "artist-subset-duke-ellington",
            "no-match-autechre",
            "no-match-prince-jammy",
            "no-match-aphex-twin",
        ],
    )
    def test_classification_decision(
        self,
        matcher: MultiIndexMatcher,
        artist: str,
        title: str,
        expected_decision: str,
    ) -> None:
        """Verify KEEP/PRUNE/REVIEW classification for each test case."""
        norm_artist = normalize_artist(artist)
        norm_title = normalize_title(title)
        result = matcher.classify(norm_artist, norm_title)
        assert result.decision.name == expected_decision, (
            f"({artist!r}, {title!r}) classified as {result.decision.name}, "
            f"expected {expected_decision}. "
            f"Scores: exact={result.exact_score:.2f} "
            f"token_set={result.token_set_score:.2f} "
            f"token_sort={result.token_sort_score:.2f} "
            f"two_stage={result.two_stage_score:.2f}"
        )

    def test_exact_match_all_scores_are_1(self, matcher: MultiIndexMatcher) -> None:
        """Exact matches should produce score 1.0 for all scorers."""
        norm_artist = normalize_artist("Stereolab")
        norm_title = normalize_title("Aluminum Tunes")
        result = matcher.classify(norm_artist, norm_title)

        assert result.decision == Decision.KEEP
        assert result.exact_score == 1.0
        assert result.token_set_score == 1.0
        assert result.token_sort_score == 1.0
        assert result.two_stage_score == 1.0

    def test_prune_max_fuzzy_score_below_review_threshold(
        self, matcher: MultiIndexMatcher
    ) -> None:
        """PRUNE decisions should have max fuzzy score below 0.65 (review threshold)."""
        norm_artist = normalize_artist("Autechre")
        norm_title = normalize_title("Confield")
        result = matcher.classify(norm_artist, norm_title)

        assert result.decision == Decision.PRUNE
        assert result.max_fuzzy_score < 0.65, (
            f"PRUNE but max_fuzzy_score={result.max_fuzzy_score:.2f} >= 0.65"
        )

    def test_keep_decisions_consistent_across_calls(
        self, matcher: MultiIndexMatcher
    ) -> None:
        """Classification is deterministic -- same input always produces same output."""
        norm_artist = normalize_artist("Cat Power")
        norm_title = normalize_title("Moon Pix")

        results = [matcher.classify(norm_artist, norm_title) for _ in range(10)]
        decisions = {r.decision for r in results}
        assert len(decisions) == 1, f"Non-deterministic: got {decisions}"

    def test_scorer_agreement_on_keep(self, matcher: MultiIndexMatcher) -> None:
        """For fuzzy KEEP decisions, at least 2-of-3 scorers agree above threshold.

        The two-stage scorer must be one of the agreeing scorers.
        """
        # Duke Ellington matching via component splitting -- fuzzy, not exact
        norm_artist = normalize_artist("Duke Ellington")
        norm_title = normalize_title("Duke Ellington & John Coltrane")
        result = matcher.classify(norm_artist, norm_title)

        assert result.decision == Decision.KEEP

        keep_threshold = 0.75
        scores = [result.token_set_score, result.token_sort_score, result.two_stage_score]
        above_keep = sum(1 for s in scores if s >= keep_threshold)
        assert above_keep >= 2, (
            f"Expected 2+ scorers >= {keep_threshold}, got {above_keep}. "
            f"Scores: ts={result.token_set_score:.2f} "
            f"tso={result.token_sort_score:.2f} "
            f"two={result.two_stage_score:.2f}"
        )
        assert result.two_stage_score >= keep_threshold, (
            f"Two-stage scorer ({result.two_stage_score:.2f}) must be >= {keep_threshold} for KEEP"
        )
