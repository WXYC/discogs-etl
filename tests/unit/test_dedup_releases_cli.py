"""Unit tests for scripts/dedup_releases.py CLI argument parsing."""

from __future__ import annotations

import importlib.util
from pathlib import Path

# Load dedup_releases as a module (it's a script, not a package).
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py"
_spec = importlib.util.spec_from_file_location("dedup_releases", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_dr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dr)


class TestKeepReleaseIdsArg:
    def test_default_none(self) -> None:
        args = _dr.parse_args(["postgresql:///discogs"])
        assert args.keep_release_ids is None

    def test_parsed_as_path(self) -> None:
        args = _dr.parse_args(["postgresql:///discogs", "--keep-release-ids", "/tmp/keep_ids.txt"])
        assert args.keep_release_ids == Path("/tmp/keep_ids.txt")
