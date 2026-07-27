"""Unit tests (no DB) for scripts/seed_cache_from_clone.py's CLI surface:
--source/--target swap validation, --seed-artists dispatch, and --ids-file
parsing. Companion to test_seed_cache_columns.py (which covers the
clone-vs-prod column-intersection contract).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SEED_PATH = Path(__file__).parent.parent.parent / "scripts" / "seed_cache_from_clone.py"
if "seed_cache_from_clone" in sys.modules:
    _seed = sys.modules["seed_cache_from_clone"]
else:
    _spec = importlib.util.spec_from_file_location("seed_cache_from_clone", _SEED_PATH)
    assert _spec is not None and _spec.loader is not None
    _seed = importlib.util.module_from_spec(_spec)
    sys.modules["seed_cache_from_clone"] = _seed
    _spec.loader.exec_module(_seed)


# --- --source/--target swap validation (finding #1) -------------------------


def test_validate_distinct_dsns_rejects_identical():
    with pytest.raises(ValueError, match="identical DSN"):
        _seed._validate_distinct_dsns(
            "postgresql://localhost:5432/discogs", "postgresql://localhost:5432/discogs"
        )


def test_validate_distinct_dsns_allows_different():
    # Must not raise.
    _seed._validate_distinct_dsns(
        "postgresql://localhost:5432/discogs", "postgresql://localhost:5433/discogs_cache"
    )


def test_main_rejects_identical_source_and_target(monkeypatch, tmp_path):
    ids_file = tmp_path / "ids.txt"
    ids_file.write_text("500\n")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "seed_cache_from_clone.py",
            "--source",
            "postgresql://localhost:5432/discogs",
            "--target",
            "postgresql://localhost:5432/discogs",
            "--ids-file",
            str(ids_file),
        ],
    )
    with pytest.raises(SystemExit) as exc_info:
        _seed.main()
    assert exc_info.value.code == 2


# --- --seed-artists CLI dispatch (finding #4) --------------------------------


def test_main_dispatches_to_seed_releases_by_default(monkeypatch, tmp_path):
    ids_file = tmp_path / "ids.txt"
    ids_file.write_text("100\n101\n")
    calls: dict = {}

    def fake_seed_releases_additive(source, target, ids, *, dry_run=False):
        calls["fn"] = "release"
        calls["ids"] = ids
        calls["dry_run"] = dry_run
        return {"release": 0}

    def fake_seed_artists_additive(*args, **kwargs):
        raise AssertionError("should not be called without --seed-artists")

    monkeypatch.setattr(_seed, "seed_releases_additive", fake_seed_releases_additive)
    monkeypatch.setattr(_seed, "seed_artists_additive", fake_seed_artists_additive)
    monkeypatch.setattr(_seed, "init_logger", lambda **kwargs: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "seed_cache_from_clone.py",
            "--source",
            "postgresql://localhost:5432/discogs",
            "--target",
            "postgresql://localhost:5433/discogs_cache",
            "--ids-file",
            str(ids_file),
            "--dry-run",
        ],
    )
    rc = _seed.main()
    assert rc == 0
    assert calls["fn"] == "release"
    assert calls["ids"] == [100, 101]
    assert calls["dry_run"] is True


def test_main_dispatches_to_seed_artists_with_flag(monkeypatch, tmp_path):
    ids_file = tmp_path / "ids.txt"
    ids_file.write_text("500\n501\n")
    calls: dict = {}

    def fake_seed_artists_additive(source, target, ids, *, dry_run=False):
        calls["fn"] = "artist"
        calls["ids"] = ids
        calls["dry_run"] = dry_run
        return {"artist": 0}

    def fake_seed_releases_additive(*args, **kwargs):
        raise AssertionError("should not be called with --seed-artists")

    monkeypatch.setattr(_seed, "seed_artists_additive", fake_seed_artists_additive)
    monkeypatch.setattr(_seed, "seed_releases_additive", fake_seed_releases_additive)
    monkeypatch.setattr(_seed, "init_logger", lambda **kwargs: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "seed_cache_from_clone.py",
            "--source",
            "postgresql://localhost:5432/discogs",
            "--target",
            "postgresql://localhost:5433/discogs_cache",
            "--ids-file",
            str(ids_file),
            "--seed-artists",
        ],
    )
    rc = _seed.main()
    assert rc == 0
    assert calls["fn"] == "artist"
    assert calls["ids"] == [500, 501]
    assert calls["dry_run"] is False


# --- --ids-file parsing delegates to the shared allowlist parser (finding #8) -


def test_read_ids_file_delegates_to_shared_parser(tmp_path):
    p = tmp_path / "ids.txt"
    p.write_text("100\n# a comment\n\n200\n100\n")
    result = _seed._read_ids_file(p)
    # Shared parser is set-based (dedups) and this wrapper sorts for determinism.
    assert result == [100, 200]


def test_read_ids_file_missing_raises():
    with pytest.raises(FileNotFoundError):
        _seed._read_ids_file(Path("/nonexistent/path/to/ids.txt"))
