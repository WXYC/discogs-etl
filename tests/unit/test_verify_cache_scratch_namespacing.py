"""Unit tests for scripts/verify_cache.py's per-invocation scratch-table
namespacing (WXYC/discogs-etl#356).

Mock-based (no PostgreSQL needed), mirroring
tests/unit/test_verify_cache.py::TestPruneCopySwapSQL and
tests/unit/test_dedup_scratch_namespacing.py. Real end-to-end
collision-safety and crash-residue behavior are covered by the pg-marked
integration tests in tests/integration/.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

_VERIFY_CACHE_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _vcspec = importlib.util.spec_from_file_location("verify_cache", _VERIFY_CACHE_PATH)
    assert _vcspec is not None and _vcspec.loader is not None
    _vc = importlib.util.module_from_spec(_vcspec)
    sys.modules["verify_cache"] = _vc
    _vcspec.loader.exec_module(_vc)


def _make_mock_conn(fetchone_value=(0,)):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = fetchone_value
    mock_cursor.copy.return_value.__enter__ = MagicMock()
    mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.info.transaction_status = psycopg.pq.TransactionStatus.IDLE
    return mock_conn, mock_cursor


def _executed_sql(mock_cursor) -> list[str]:
    sqls: list[str] = []
    for c in mock_cursor.execute.call_args_list:
        if c.args:
            sqls.append(c.args[0])
    return sqls


class TestPruneBuildScratchTablesSuffix:
    def test_default_suffix_uses_bare_keep_ids_and_new_table_names(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(5,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            _vc._prune_build_scratch_tables("postgresql:///test", {1, 2}, {3})
        sqls = _executed_sql(mock_cursor)
        assert any("CREATE UNLOGGED TABLE _keep_ids (" in s for s in sqls)
        assert any("CREATE TABLE new_release AS" in s for s in sqls)
        assert not any("_keep_ids_" in s for s in sqls)

    def test_suffix_namespaces_keep_ids_and_every_new_table(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(5,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            _vc._prune_build_scratch_tables("postgresql:///test", {1, 2}, {3}, suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("CREATE UNLOGGED TABLE _keep_ids_ab12cd34 (" in s for s in sqls)
        for _old, new, _cols, _id_col in _vc.PRUNE_COPY_TABLES:
            assert any(f"CREATE TABLE {new}_ab12cd34 AS" in s for s in sqls), (
                f"expected a suffixed CREATE TABLE for {new}"
            )
            assert any("_keep_ids_ab12cd34 k WHERE k.release_id" in s for s in sqls)

    def test_commits_once_after_building_every_scratch_table(self) -> None:
        mock_conn, _ = _make_mock_conn(fetchone_value=(5,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn) as mock_connect:
            _vc._prune_build_scratch_tables("postgresql:///test", {1, 2}, {3}, suffix="ab12cd34")
        mock_connect.assert_called_once_with("postgresql:///test")
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_rolls_back_and_reraises_on_failure(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(5,))
        mock_cursor.execute.side_effect = RuntimeError("boom")
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            with pytest.raises(RuntimeError):
                _vc._prune_build_scratch_tables(
                    "postgresql:///test", {1, 2}, {3}, suffix="ab12cd34"
                )
        mock_conn.commit.assert_not_called()
        mock_conn.rollback.assert_called_once()


class TestPruneCopySwapTablesSuffix:
    def test_swap_uses_suffixed_source_table(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(5,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            _vc._prune_copy_swap_tables("postgresql:///test", {1, 2}, {3}, suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("ALTER TABLE new_release_ab12cd34 RENAME TO release" in s for s in sqls)


class TestPruneAddBaseConstraintsAndIndexesSuffix:
    def test_drops_suffixed_keep_ids_table_on_exit(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(0,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            _vc._prune_add_base_constraints_and_indexes("postgresql:///test", suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("DROP TABLE IF EXISTS _keep_ids_ab12cd34" in s for s in sqls)

    def test_default_suffix_drops_bare_keep_ids_table(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(0,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            _vc._prune_add_base_constraints_and_indexes("postgresql:///test")
        sqls = _executed_sql(mock_cursor)
        assert any(s == "DROP TABLE IF EXISTS _keep_ids" for s in sqls)


class TestPruneReleasesCopySwapAutoSuffix:
    """The public entry point mints a fresh suffix by default so real
    invocations (verify_cache.py's own --prune CLI and run_pipeline.py's
    subprocess launch of it) are collision-safe with zero call-site
    changes."""

    def test_default_invocation_passes_a_generated_suffix_downstream(self) -> None:
        seen_suffixes: list[str] = []

        def _fake_copy_swap(db_url, keep_ids, review_ids, *, suffix=""):
            seen_suffixes.append(suffix)

        def _fake_add_constraints(db_url, *, suffix=""):
            seen_suffixes.append(suffix)

        with (
            patch.object(_vc, "_prune_copy_swap_tables", side_effect=_fake_copy_swap),
            patch.object(
                _vc, "_prune_add_base_constraints_and_indexes", side_effect=_fake_add_constraints
            ),
        ):
            _vc.prune_releases_copy_swap("postgresql:///test", keep_ids={1}, review_ids=set())

        assert len(seen_suffixes) == 2
        assert seen_suffixes[0] == seen_suffixes[1], "both phases must share one suffix"
        assert seen_suffixes[0] != "", "default invocation must not fall back to bare names"

    def test_two_default_invocations_generate_different_suffixes(self) -> None:
        seen_suffixes: list[str] = []

        def _fake_copy_swap(db_url, keep_ids, review_ids, *, suffix=""):
            seen_suffixes.append(suffix)

        with (
            patch.object(_vc, "_prune_copy_swap_tables", side_effect=_fake_copy_swap),
            patch.object(_vc, "_prune_add_base_constraints_and_indexes"),
        ):
            _vc.prune_releases_copy_swap("postgresql:///test", keep_ids={1}, review_ids=set())
            _vc.prune_releases_copy_swap("postgresql:///test", keep_ids={1}, review_ids=set())

        assert len(seen_suffixes) == 2
        assert seen_suffixes[0] != seen_suffixes[1]

    def test_explicit_empty_suffix_opts_out(self) -> None:
        seen_suffixes: list[str] = []

        def _fake_copy_swap(db_url, keep_ids, review_ids, *, suffix=""):
            seen_suffixes.append(suffix)

        with (
            patch.object(_vc, "_prune_copy_swap_tables", side_effect=_fake_copy_swap),
            patch.object(_vc, "_prune_add_base_constraints_and_indexes"),
        ):
            _vc.prune_releases_copy_swap(
                "postgresql:///test", keep_ids={1}, review_ids=set(), suffix=""
            )

        assert seen_suffixes == [""]
