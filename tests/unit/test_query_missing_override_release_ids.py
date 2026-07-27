"""Unit tests for ``scripts/query_missing_override_release_ids.py``.

Covers the pure ``write_ids_file`` helper and the argparse/env-fallback
plumbing in ``main`` (DB calls mocked). The DB-bound query functions
themselves live in ``lib/library_release_overrides.py`` and are exercised
against real PostgreSQL in ``tests/integration/test_library_release_overrides.py``
and ``tests/integration/test_query_missing_override_release_ids.py``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "query_missing_override_release_ids.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("query_missing_override_release_ids", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["query_missing_override_release_ids"] = mod
    spec.loader.exec_module(mod)
    return mod


class TestWriteIdsFile:
    def test_writes_one_id_per_line(self, tmp_path: Path) -> None:
        mod = _load_module()
        out = tmp_path / "missing_ids.txt"
        mod.write_ids_file(out, [4821, 12033, 99])
        lines = out.read_text().splitlines()
        assert lines[0].startswith("#")
        assert [int(x) for x in lines if not x.startswith("#")] == [4821, 12033, 99]

    def test_empty_list_writes_header_comment_only(self, tmp_path: Path) -> None:
        mod = _load_module()
        out = tmp_path / "missing_ids.txt"
        mod.write_ids_file(out, [])
        content = out.read_text()
        assert content.strip().startswith("#")
        # No data lines.
        assert all(line.startswith("#") for line in content.splitlines() if line.strip())

    def test_written_file_round_trips_through_seed_cache_reader(self, tmp_path: Path) -> None:
        """The file this script writes must be readable by
        seed_cache_from_clone.py's --ids-file parser (blank/# lines skipped).

        That reader (``_read_ids_file`` -> ``parse_keep_release_ids``) returns a
        sorted, de-duplicated list, so the round-trip normalizes the ids; the
        contract this asserts is "every written id is read back exactly once",
        not order/dup preservation."""
        seed_spec = importlib.util.spec_from_file_location(
            "seed_cache_from_clone", REPO_ROOT / "scripts" / "seed_cache_from_clone.py"
        )
        assert seed_spec is not None and seed_spec.loader is not None
        seed_mod = importlib.util.module_from_spec(seed_spec)
        sys.modules["seed_cache_from_clone"] = seed_mod
        seed_spec.loader.exec_module(seed_mod)

        mod = _load_module()
        out = tmp_path / "missing_ids.txt"
        mod.write_ids_file(out, [5, 3, 5, 1])
        assert seed_mod._read_ids_file(out) == [1, 3, 5]


class TestMain:
    def test_errors_when_no_database_url_available(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.delenv("DATABASE_URL_DISCOGS", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)
        rc = mod.main(["--output", str(tmp_path / "out.txt")])
        assert rc == 2

    def test_connects_and_writes_output(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        out = tmp_path / "out.txt"

        fake_conn = MagicMock()
        with (
            patch.object(mod.psycopg, "connect", return_value=fake_conn) as mock_connect,
            patch.object(mod, "fetch_missing_release_ids", return_value=[1, 2, 3]),
            patch.object(mod, "fetch_override_summary", return_value=[]),
        ):
            rc = mod.main(["--database-url", "postgresql://x/y", "--output", str(out)])

        assert rc == 0
        mock_connect.assert_called_once_with("postgresql://x/y")
        assert [int(x) for x in out.read_text().splitlines() if not x.startswith("#")] == [1, 2, 3]
