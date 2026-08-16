"""Unit tests for ``scripts/derive_va_release.py``'s CLI surface (#344).

The daily sync invokes the script bare (env cascade) and soft-fails on any
non-zero exit, so the URL-resolution order, the exit-code contract, and the
``VA_RELEASE_FLOOR`` handling are all load-bearing wiring — a regression in
any of them only surfaces in a prod daily-run log otherwise.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

_spec = importlib.util.spec_from_file_location(
    "derive_va_release", REPO_ROOT / "scripts" / "derive_va_release.py"
)
derive_module = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("derive_va_release", derive_module)
_spec.loader.exec_module(derive_module)


@pytest.fixture()
def clean_env(monkeypatch):
    for var in ("DATABASE_URL_DISCOGS", "DATABASE_URL", "VA_RELEASE_FLOOR"):
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def _run_main(argv: list[str]) -> tuple[int, MagicMock, MagicMock]:
    """Run main() with connect + derivation stubbed; return (exit, connect, derive)."""
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    with (
        patch.object(derive_module.psycopg, "connect", return_value=conn) as mock_connect,
        patch.object(derive_module, "derive_va_release", return_value=42) as mock_derive,
    ):
        code = derive_module.main(argv)
    return code, mock_connect, mock_derive


class TestUrlResolution:
    def test_explicit_flag_wins_over_env(self, clean_env) -> None:
        clean_env.setenv("DATABASE_URL_DISCOGS", "postgresql://env/discogs")
        code, mock_connect, _ = _run_main(["--database-url", "postgresql://flag/cache"])
        assert code == 0
        assert mock_connect.call_args[0][0] == "postgresql://flag/cache"

    def test_database_url_discogs_preferred_over_generic(self, clean_env) -> None:
        clean_env.setenv("DATABASE_URL_DISCOGS", "postgresql://specific/discogs")
        clean_env.setenv("DATABASE_URL", "postgresql://generic/other")
        code, mock_connect, _ = _run_main([])
        assert code == 0
        assert mock_connect.call_args[0][0] == "postgresql://specific/discogs"

    def test_generic_database_url_fallback_used_last(self, clean_env) -> None:
        clean_env.setenv("DATABASE_URL", "postgresql://generic/db")
        code, mock_connect, _ = _run_main([])
        assert code == 0
        assert mock_connect.call_args[0][0] == "postgresql://generic/db"

    def test_no_url_exits_2_without_connecting(self, clean_env, capsys) -> None:
        code, mock_connect, _ = _run_main([])
        assert code == 2
        mock_connect.assert_not_called()
        assert "error" in capsys.readouterr().err


class TestExitCodes:
    def test_floor_violation_exits_1(self, clean_env) -> None:
        """sync-library.sh's soft-fail WARN only fires on a non-zero exit; a
        FloorViolation swallowed into exit 0 would hide the guard entirely."""
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        with (
            patch.object(derive_module.psycopg, "connect", return_value=conn),
            patch.object(
                derive_module,
                "derive_va_release",
                side_effect=derive_module.FloorViolation("thin"),
            ),
        ):
            code = derive_module.main(["--database-url", "postgresql://x/y"])
        assert code == 1

    def test_success_exits_0(self, clean_env) -> None:
        code, _, mock_derive = _run_main(["--database-url", "postgresql://x/y"])
        assert code == 0
        mock_derive.assert_called_once()


class TestFloorResolution:
    """VA_RELEASE_FLOOR is resolved lazily and tolerantly — never at argparse
    construction time, where a malformed env var would crash even callers
    passing an explicit --floor (run_pipeline always passes --floor 0)."""

    def test_explicit_floor_wins_and_ignores_garbage_env(self, clean_env) -> None:
        clean_env.setenv("VA_RELEASE_FLOOR", "not-a-number")
        code, _, mock_derive = _run_main(["--database-url", "postgresql://x/y", "--floor", "0"])
        assert code == 0
        assert mock_derive.call_args.kwargs["floor"] == 0

    def test_env_floor_applies_when_flag_absent(self, clean_env) -> None:
        clean_env.setenv("VA_RELEASE_FLOOR", "250")
        code, _, mock_derive = _run_main(["--database-url", "postgresql://x/y"])
        assert code == 0
        assert mock_derive.call_args.kwargs["floor"] == 250

    def test_empty_env_treated_as_unset(self, clean_env) -> None:
        """`VA_RELEASE_FLOOR=` (a classic .env artifact — sync-library.sh
        sources .env with set -a) must fall back to the default, matching the
        shell siblings' ${VAR:-default} tolerance."""
        clean_env.setenv("VA_RELEASE_FLOOR", "")
        code, _, mock_derive = _run_main(["--database-url", "postgresql://x/y"])
        assert code == 0
        assert mock_derive.call_args.kwargs["floor"] == derive_module.DEFAULT_FLOOR

    def test_garbage_env_without_flag_errors_cleanly(self, clean_env) -> None:
        clean_env.setenv("VA_RELEASE_FLOOR", "1.5")
        with pytest.raises(SystemExit, match="VA_RELEASE_FLOOR"):
            _run_main(["--database-url", "postgresql://x/y"])


class TestTargetDescription:
    """The "Deriving va_release on ..." line is routed through the shared
    redactor in lib/dsn.py; the parser's own edge cases (both DSN forms, bad
    port, unparseable input) are pinned in tests/unit/test_dsn.py. What is
    load-bearing *here* is that this call site still goes through it — the
    daily sync tees this log, so an unredacted target publishes the cache
    credential on the happy path.
    """

    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql://etl:hunter2@db.example.com:5433/discogs",
            "host=db.example.com port=5433 dbname=discogs user=etl password=hunter2",
        ],
        ids=["url", "conninfo"],
    )
    def test_target_line_redacts_dsn(self, clean_env, caplog, dsn: str) -> None:
        with caplog.at_level(logging.INFO, logger=derive_module.logger.name):
            code, _, _ = _run_main(["--database-url", dsn])
        assert code == 0
        logged = " ".join(record.getMessage() for record in caplog.records)
        assert "hunter2" not in logged
        assert "password" not in logged
        # Still useful to an operator: the target survives redaction.
        assert "db.example.com:5433/discogs" in logged
