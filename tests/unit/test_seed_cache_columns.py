"""Unit tests (no DB) for scripts/seed_cache_from_clone.py column mapping.

Pins the clone-vs-prod column-intersection contract that lets the seed tolerate
schema drift: the real clone predates release.artwork_checked_at /
release.not_found and artist.not_found, so those prod-only columns must fall out
of the COPY column list (and default on insert) rather than crash the COPY.
"""

from __future__ import annotations

import importlib.util
import sys as _sys
from pathlib import Path

_SEED_PATH = Path(__file__).parent.parent.parent / "scripts" / "seed_cache_from_clone.py"
if "seed_cache_from_clone" in _sys.modules:
    _seed = _sys.modules["seed_cache_from_clone"]
else:
    _spec = importlib.util.spec_from_file_location("seed_cache_from_clone", _SEED_PATH)
    assert _spec is not None and _spec.loader is not None
    _seed = importlib.util.module_from_spec(_spec)
    _sys.modules["seed_cache_from_clone"] = _seed
    _spec.loader.exec_module(_seed)

intersect_columns = _seed.intersect_columns


RELEASE_SPEC_COLS = [
    "id",
    "title",
    "release_year",
    "country",
    "artwork_url",
    "released",
    "format",
    "master_id",
    "artwork_checked_at",
    "not_found",
]


def test_prod_only_columns_dropped_when_absent_on_clone():
    """Clone lacks artwork_checked_at / not_found -> they drop from the copy list."""
    clone_cols = [c for c in RELEASE_SPEC_COLS if c not in {"artwork_checked_at", "not_found"}]
    prod_cols = RELEASE_SPEC_COLS
    result = intersect_columns(RELEASE_SPEC_COLS, clone_cols, prod_cols)
    assert "artwork_checked_at" not in result
    assert "not_found" not in result
    assert result == clone_cols


def test_spec_order_preserved():
    result = intersect_columns(RELEASE_SPEC_COLS, RELEASE_SPEC_COLS, RELEASE_SPEC_COLS)
    assert result == RELEASE_SPEC_COLS


def test_clone_only_column_not_selected():
    """A column present only on the clone is never selected (target can't take it)."""
    clone_cols = RELEASE_SPEC_COLS + ["clone_only_junk"]
    result = intersect_columns(RELEASE_SPEC_COLS, clone_cols, RELEASE_SPEC_COLS)
    assert "clone_only_junk" not in result
    assert result == RELEASE_SPEC_COLS


def test_columns_outside_spec_ignored():
    """Only spec columns are candidates, even if both DBs share extras."""
    extra_both = RELEASE_SPEC_COLS + ["some_extra"]
    result = intersect_columns(RELEASE_SPEC_COLS, extra_both, extra_both)
    assert "some_extra" not in result
