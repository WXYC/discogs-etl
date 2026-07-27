"""Unit tests for scripts/dedup_releases.py CLI argument parsing."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

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


class TestMainKeepIdsGating:
    """main() engages the exemption path in ensure_dedup_ids -- which
    force-recreates dedup_delete_ids and so forfeits the reuse-verbatim
    short-circuit -- only when the allowlist actually contains ids. An empty
    allowlist (the only case in production today) must stay byte-identical to
    the no-flag path, preserving the short-circuit.
    """

    def _run_main(self, keep_file, loaded_count):
        ns = argparse.Namespace(
            database_url="postgresql:///t",
            library_labels=None,
            label_hierarchy=None,
            keep_release_ids=keep_file,
        )
        mock_conn = MagicMock()
        with (
            patch.object(_dr, "parse_args", return_value=ns),
            patch.object(_dr, "init_logger"),
            patch.object(_dr.psycopg, "connect", return_value=mock_conn),
            patch.object(_dr, "load_keep_release_ids", return_value=loaded_count),
            patch.object(_dr, "ensure_dedup_ids", return_value=0) as mock_ensure,
        ):
            _dr.main()
        return mock_ensure

    def test_empty_allowlist_does_not_engage_exemption(self, tmp_path) -> None:
        keep_file = tmp_path / "keep.txt"
        keep_file.write_text("")
        mock_ensure = self._run_main(keep_file, loaded_count=0)
        assert mock_ensure.call_args.kwargs["keep_ids_loaded"] is False

    def test_nonempty_allowlist_engages_exemption(self, tmp_path) -> None:
        keep_file = tmp_path / "keep.txt"
        keep_file.write_text("1\n2\n3\n")
        mock_ensure = self._run_main(keep_file, loaded_count=3)
        assert mock_ensure.call_args.kwargs["keep_ids_loaded"] is True
