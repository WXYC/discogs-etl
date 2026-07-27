"""Unit tests for ``scripts/check_override_parity.py``.

Covers the pure ``evaluate_parity`` decision function and the ``main``
plumbing (DB calls mocked). The DB-bound query itself lives in
``lib/library_release_overrides.py`` (tested against real PostgreSQL in
``tests/integration/test_library_release_overrides.py``).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_override_parity.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("check_override_parity", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["check_override_parity"] = mod
    spec.loader.exec_module(mod)
    return mod


class TestEvaluateParity:
    def test_healthy_when_every_source_has_zero_missing(self) -> None:
        mod = _load_module()
        from lib.library_release_overrides import OverrideSourceSummary

        rows = [
            OverrideSourceSummary(source="a", pinned=10, missing_from_cache=0),
            OverrideSourceSummary(source="b", pinned=20, missing_from_cache=0),
        ]
        assert mod.evaluate_parity(rows) is True

    def test_unhealthy_when_any_source_has_missing(self) -> None:
        mod = _load_module()
        from lib.library_release_overrides import OverrideSourceSummary

        rows = [
            OverrideSourceSummary(source="a", pinned=10, missing_from_cache=0),
            OverrideSourceSummary(source="b", pinned=20, missing_from_cache=3),
        ]
        assert mod.evaluate_parity(rows) is False

    def test_no_sources_is_vacuously_healthy(self) -> None:
        mod = _load_module()
        assert mod.evaluate_parity([]) is True


class TestMain:
    def test_errors_when_no_database_url_available(self, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.delenv("DATABASE_URL_DISCOGS", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)
        rc = mod.main([])
        assert rc == 2

    def test_returns_zero_when_parity_holds(self) -> None:
        mod = _load_module()
        from lib.library_release_overrides import OverrideSourceSummary

        fake_conn = MagicMock()
        with (
            patch.object(mod.psycopg, "connect", return_value=fake_conn),
            patch.object(
                mod,
                "fetch_override_summary",
                return_value=[OverrideSourceSummary(source="a", pinned=1, missing_from_cache=0)],
            ),
        ):
            rc = mod.main(["--database-url", "postgresql://x/y"])
        assert rc == 0

    def test_returns_one_when_parity_fails(self) -> None:
        mod = _load_module()
        from lib.library_release_overrides import OverrideSourceSummary

        fake_conn = MagicMock()
        with (
            patch.object(mod.psycopg, "connect", return_value=fake_conn),
            patch.object(
                mod,
                "fetch_override_summary",
                return_value=[OverrideSourceSummary(source="a", pinned=5, missing_from_cache=2)],
            ),
        ):
            rc = mod.main(["--database-url", "postgresql://x/y"])
        assert rc == 1

    def test_empty_override_table_passes_but_warns(self, caplog) -> None:
        """An empty override table is vacuously healthy (exit 0), but that also
        means a wrong-database target (whose lml_cache table exists yet holds no
        pins) would pass silently. Keep the exit code but surface a WARNING so
        the vacuous pass can't be mistaken for a verified backfill."""
        mod = _load_module()

        fake_conn = MagicMock()
        with (
            patch.object(mod.psycopg, "connect", return_value=fake_conn),
            patch.object(mod, "fetch_override_summary", return_value=[]),
            caplog.at_level("WARNING"),
        ):
            rc = mod.main(["--database-url", "postgresql://x/y"])

        assert rc == 0
        assert any(
            "no override" in r.message.lower() or "empty" in r.message.lower()
            for r in caplog.records
        )
