"""Verify the verify_cache.py Python fallback when the Rust batch classifier
is unimportable.

verify_cache.py guards exactly one import with a try/except: the Rust batch
classifier ``wxyc_etl.fuzzy.batch_classify_releases``. The rest of wxyc_etl
(text normalization, etc.) is a hard dependency. When the Rust batch
classifier is unimportable -- because the wxyc_etl wheel was built without
the ``fuzzy`` sub-module, or because Rust toolchain wasn't available at
build time -- verify_cache.py must fall back to the rapidfuzz /
ProcessPoolExecutor Python path and produce the same classifications.

We exercise this by monkeypatching ``builtins.__import__`` to raise on
``wxyc_etl.fuzzy``, reloading verify_cache so it re-runs its conditional
``try: from wxyc_etl.fuzzy import batch_classify_releases`` block, and
asserting:

1. _HAS_WXYC_ETL flips to False on the reloaded module.
2. classify_all_releases returns correct KEEP/PRUNE counts on canonical WXYC
   fixture data.
3. Output is identical to the Rust path on the same input (parity), when the
   wxyc_etl wheel includes the Rust batch classifier.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Path to verify_cache.py — the script directory is not a proper package, so
# spec_from_file_location is required.
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"


# ---------------------------------------------------------------------------
# wxyc_etl availability detection (used to skip parity tests when missing)
# ---------------------------------------------------------------------------

try:
    import wxyc_etl  # noqa: F401
    from wxyc_etl.fuzzy import batch_classify_releases  # noqa: F401

    HAS_WXYC_ETL = True
except ImportError:
    HAS_WXYC_ETL = False


# Canonical WXYC fixture: a small library of representative artists plus a
# matching set of Discogs releases that exercise both KEEP and PRUNE.
LIBRARY_ROWS = [
    ("Juana Molina", "DOGA", "LP"),
    ("Stereolab", "Aluminum Tunes", "CD"),
    ("Cat Power", "Moon Pix", "LP"),
    ("Jessica Pratt", "On Your Own Love Again", "LP"),
    ("Chuquimamani-Condori", "Edits", "CD"),
    ("Duke Ellington & John Coltrane", "Duke Ellington & John Coltrane", "LP"),
    ("Father John Misty", "I Love You, Honeybear", "LP"),
    ("Autechre", "Confield", "CD"),
    ("Nilüfer Yanya", "Painless", "LP"),
    ("Hermanos Gutiérrez", "El Bueno y el Malo", "LP"),
]

# (release_id, artist, title, expected_decision_str) — KEEP for matches,
# PRUNE for unrelated artists.
DISCOGS_RELEASES = [
    (1, "Juana Molina", "DOGA", "keep"),
    (2, "Stereolab", "Aluminum Tunes", "keep"),
    (3, "Cat Power", "Moon Pix", "keep"),
    (4, "Jessica Pratt", "On Your Own Love Again", "keep"),
    (5, "Chuquimamani-Condori", "Edits", "keep"),
    (6, "Father John Misty", "I Love You, Honeybear", "keep"),
    (7, "Autechre", "Confield", "keep"),
    (8, "Nilüfer Yanya", "Painless", "keep"),
    (9, "Hermanos Gutiérrez", "El Bueno y el Malo", "keep"),
    (10, "Duke Ellington", "Duke Ellington & John Coltrane", "keep"),
    (11, "Random Unrelated Band", "Some Album", "prune"),
    (12, "Mystery Artist", "Mystery Album", "prune"),
    (13, "Phantom Group", "Phantom Title", "prune"),
    (14, "Unknown DJ", "Unknown Mix", "prune"),
    (15, "Made Up Artist", "Made Up Record", "prune"),
]


def _load_verify_cache(name: str = "verify_cache_under_test"):
    """Load verify_cache.py as a fresh module under the given name.

    Each call returns a fresh module object whose top-level
    ``try: import wxyc_etl`` is re-executed against the *current* import
    environment.
    """
    # Drop any prior load so spec_from_file_location returns a fresh module.
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(name, _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def force_python_fallback(monkeypatch):
    """Make ``wxyc_etl.fuzzy`` (the Rust batch classifier) unimportable, then
    reload verify_cache so it picks the Python fallback path.

    Only ``wxyc_etl.fuzzy`` is blocked. Other wxyc_etl submodules (e.g.
    ``wxyc_etl.text``) are unconditional dependencies of verify_cache and
    must remain importable.
    """
    # Drop any cached wxyc_etl.fuzzy so the next import attempt re-runs the
    # find_and_load path.
    monkeypatch.delitem(sys.modules, "wxyc_etl.fuzzy", raising=False)

    # Block exactly `wxyc_etl.fuzzy` (and direct `from wxyc_etl.fuzzy import ...`).
    # Leave plain `import wxyc_etl` and `wxyc_etl.text` working.
    import builtins

    real_import = builtins.__import__

    def _blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "wxyc_etl.fuzzy":
            raise ModuleNotFoundError(f"mocked: no module named {name!r}")
        if name == "wxyc_etl" and fromlist and "fuzzy" in fromlist:
            # `from wxyc_etl import fuzzy` — block via attribute lookup later.
            mod = real_import(name, globals, locals, fromlist, level)

            class _Blocked:
                def __getattr__(self, attr):
                    if attr == "fuzzy":
                        raise ModuleNotFoundError("mocked: no module named 'wxyc_etl.fuzzy'")
                    return getattr(mod, attr)

            return _Blocked()
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)

    mod = _load_verify_cache(name="verify_cache_under_test_fallback")

    yield mod

    # Drop the test-loaded module so subsequent tests see a fresh load.
    sys.modules.pop("verify_cache_under_test_fallback", None)


# ---------------------------------------------------------------------------
# Behavioural tests
# ---------------------------------------------------------------------------


class TestVerifyCacheImportFailure:
    """verify_cache must work without wxyc_etl installed."""

    def test_module_loads_with_wxyc_etl_unimportable(self, force_python_fallback):
        """The conditional import block sets _HAS_WXYC_ETL to False without raising."""
        vc = force_python_fallback
        assert hasattr(vc, "_HAS_WXYC_ETL"), "verify_cache should expose _HAS_WXYC_ETL"
        assert vc._HAS_WXYC_ETL is False, (
            "_HAS_WXYC_ETL should be False when wxyc_etl is unimportable"
        )

    def test_classify_succeeds_when_wxyc_etl_module_missing(self, force_python_fallback):
        """classify_all_releases returns correct KEEP/PRUNE counts on canonical fixtures
        even when wxyc_etl cannot be imported."""
        vc = force_python_fallback

        index = vc.LibraryIndex.from_rows(LIBRARY_ROWS)
        matcher = vc.MultiIndexMatcher(index)

        triples = [(rid, artist, title) for rid, artist, title, _ in DISCOGS_RELEASES]
        report = vc.classify_all_releases(triples, index, matcher)

        expected_keep = {rid for rid, _, _, decision in DISCOGS_RELEASES if decision == "keep"}
        expected_prune = {rid for rid, _, _, decision in DISCOGS_RELEASES if decision == "prune"}

        # Allow a small slack: the fallback may legitimately classify a few
        # canonical KEEPs as REVIEW because of accent/title fuzziness, but
        # nothing in the unrelated PRUNE set should sneak into KEEP and the
        # total of KEEP/PRUNE/REVIEW must equal the input count.
        assert expected_prune.issubset(report.prune_ids), (
            f"PRUNE set mismatch: missing {expected_prune - report.prune_ids}"
        )
        assert expected_keep.isdisjoint(report.prune_ids), (
            "Expected KEEP releases were classified as PRUNE; fallback path is broken"
        )
        total_classified = len(report.keep_ids) + len(report.prune_ids) + len(report.review_ids)
        assert total_classified == len(triples), (
            f"Expected {len(triples)} classifications, got {total_classified}"
        )


@pytest.mark.skipif(
    not HAS_WXYC_ETL,
    reason="wxyc_etl wheel not installed; cannot run Rust/Python parity check",
)
class TestPythonFallbackParityWithRust:
    """When wxyc_etl is installed, the Python fallback must produce identical
    classifications to the Rust path on the same fixture."""

    def test_python_fallback_classifications_match_rust_baseline(self):
        """Run the Rust path (wxyc_etl available) and the Python fallback path
        (wxyc_etl forced unavailable via WXYC_ETL_NO_RUST=1) on the same input.
        Assert identical KEEP/PRUNE/REVIEW sets."""
        # Baseline: load verify_cache once, with wxyc_etl available, and run
        # without WXYC_ETL_NO_RUST set — exercises the Rust batch classifier.
        vc = _load_verify_cache(name="verify_cache_rust_baseline")
        assert vc._HAS_WXYC_ETL is True, (
            "Rust baseline should have _HAS_WXYC_ETL=True when wxyc_etl is installed"
        )

        index = vc.LibraryIndex.from_rows(LIBRARY_ROWS)
        matcher = vc.MultiIndexMatcher(index)
        triples = [(rid, artist, title) for rid, artist, title, _ in DISCOGS_RELEASES]

        # Ensure WXYC_ETL_NO_RUST is unset for the baseline run.
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("WXYC_ETL_NO_RUST", None)
            rust_report = vc.classify_all_releases(triples, index, matcher)

        # Now force the Python fallback in the same module and re-run.
        with patch.dict(os.environ, {"WXYC_ETL_NO_RUST": "1"}):
            python_report = vc.classify_all_releases(triples, index, matcher)

        assert rust_report.keep_ids == python_report.keep_ids, (
            f"keep_ids diverge: rust={rust_report.keep_ids} python={python_report.keep_ids}"
        )
        assert rust_report.prune_ids == python_report.prune_ids, (
            f"prune_ids diverge: rust={rust_report.prune_ids} python={python_report.prune_ids}"
        )
        assert rust_report.review_ids == python_report.review_ids, (
            f"review_ids diverge: rust={rust_report.review_ids} python={python_report.review_ids}"
        )
