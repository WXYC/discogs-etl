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
        # fetchone_value backs every SELECT count(*) in the run, including the
        # WXYC/discogs-etl#357 pre-swap row-count guard -- it must equal
        # len({1, 2} | {3}) == 3 or the guard aborts before the RENAME.
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(3,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            _vc._prune_copy_swap_tables("postgresql:///test", {1, 2}, {3}, suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("ALTER TABLE new_release_ab12cd34 RENAME TO release" in s for s in sqls)

    def test_backup_name_is_namespaced_too(self) -> None:
        """Mirrors dedup_releases.swap_tables: the ``<old_table>_old``
        backup was the last globally-named scratch table, and two
        invocations that both reach the swap would contend on it. See
        WXYC/discogs-etl#356.
        """
        # Must equal len({1, 2} | {3}) == 3: fetchone_value backs the
        # WXYC/discogs-etl#357 pre-swap row-count guard too, and a mismatch
        # aborts before any RENAME this test asserts on is generated.
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(3,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            _vc._prune_copy_swap_tables("postgresql:///test", {1, 2}, {3}, suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        for old_table, _new, _cols, _id_col in _vc.PRUNE_COPY_TABLES:
            assert any(
                f"ALTER TABLE {old_table} RENAME TO {old_table}_old_ab12cd34" in s for s in sqls
            ), f"expected a namespaced backup rename for {old_table}"
            assert any(f"DROP TABLE {old_table}_old_ab12cd34 CASCADE" in s for s in sqls)
            assert not any(s == f"ALTER TABLE {old_table} RENAME TO {old_table}_old" for s in sqls)

    def test_default_suffix_preserves_unsuffixed_backup_name(self) -> None:
        # See test_backup_name_is_namespaced_too: 3 == len({1, 2} | {3}).
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(3,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            _vc._prune_copy_swap_tables("postgresql:///test", {1, 2}, {3})
        sqls = _executed_sql(mock_cursor)
        assert any(s == "ALTER TABLE release RENAME TO release_old" for s in sqls)
        assert any(s == "DROP TABLE release_old CASCADE" for s in sqls)


class TestPruneCopySwapTablesCountGuard:
    """WXYC/discogs-etl#357: _prune_copy_swap_tables aborts before the RENAME
    swap when the mocked ``SELECT count(*) FROM new_release...`` doesn't
    match ``len(keep_ids | review_ids)``. Mock-based sibling of the pg-marked
    ``TestPruneCopySwapCountGuard`` in
    tests/integration/test_copy_swap_preserves_not_null.py, which exercises
    the same guard end-to-end against real tables."""

    def test_shortfall_raises_before_any_rename(self) -> None:
        # keep_ids | review_ids has 3 ids; the mocked count(*) reports only 2,
        # so every SELECT count(*) call in the run -- including the guard's --
        # returns (2,) and the shortfall must fire.
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(2,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            with pytest.raises(_vc.CopySwapShortfallError, match="copied 2 of 3"):
                _vc._prune_copy_swap_tables("postgresql:///test", {1, 2}, {3}, suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert not any("RENAME TO" in s for s in sqls), (
            "the guard must raise before _prune_copy_swap_tables opens the "
            "swap connection and runs any RENAME statement"
        )
        assert not any("DROP CONSTRAINT" in s for s in sqls), (
            "the guard must raise before the FK DROP CONSTRAINT loop, "
            "which runs on the same connection as the RENAME swap"
        )

    def test_matching_count_does_not_raise(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(3,))
        with patch.object(_vc.psycopg, "connect", return_value=mock_conn):
            _vc._prune_copy_swap_tables("postgresql:///test", {1, 2}, {3}, suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("RENAME TO" in s for s in sqls)


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


class TestPruneFailurePathCleansUpItsOwnScratchTables:
    """#356 regression guard: once ``_prune_build_scratch_tables`` commits,
    PostgreSQL's rollback-on-disconnect no longer covers the scratch
    tables, and per-invocation suffixing means no later run reclaims them
    by name. ``_prune_add_base_constraints_and_indexes`` is the only thing
    that drops ``_keep_ids`` -- and it never runs if the swap raises first
    (``add_constraint_safely`` re-raises once SWAP_PATH_ATTEMPTS is
    exhausted). Without an explicit handler that orphans ``_keep_ids`` plus
    every not-yet-swapped ``new_X`` permanently.
    """

    def test_swap_failure_drops_this_invocations_scratch_tables(self) -> None:
        dropped: list[tuple[str, str]] = []

        def _boom(db_url, keep_ids, review_ids, *, suffix=""):
            raise RuntimeError("lock timeout during swap")

        def _fake_drop(db_url, suffix):
            dropped.append((db_url, suffix))

        with (
            patch.object(_vc, "_prune_copy_swap_tables", side_effect=_boom),
            patch.object(_vc, "_prune_add_base_constraints_and_indexes"),
            patch.object(_vc, "_drop_prune_scratch_tables", side_effect=_fake_drop),
            pytest.raises(RuntimeError, match="lock timeout"),
        ):
            _vc.prune_releases_copy_swap("postgresql:///test", keep_ids={1}, review_ids=set())

        assert len(dropped) == 1, "a failed prune must clean up its own scratch tables"
        assert dropped[0][1] != "", "cleanup must target the suffix this invocation minted"

    def test_constraint_phase_failure_also_cleans_up(self) -> None:
        dropped: list[str] = []

        with (
            patch.object(_vc, "_prune_copy_swap_tables"),
            patch.object(
                _vc,
                "_prune_add_base_constraints_and_indexes",
                side_effect=RuntimeError("constraint add failed"),
            ),
            patch.object(
                _vc,
                "_drop_prune_scratch_tables",
                side_effect=lambda db_url, suffix: dropped.append(suffix),
            ),
            pytest.raises(RuntimeError, match="constraint add failed"),
        ):
            _vc.prune_releases_copy_swap("postgresql:///test", keep_ids={1}, review_ids=set())

        assert len(dropped) == 1

    def test_success_path_does_not_invoke_the_failure_cleanup(self) -> None:
        with (
            patch.object(_vc, "_prune_copy_swap_tables"),
            patch.object(_vc, "_prune_add_base_constraints_and_indexes"),
            patch.object(_vc, "_drop_prune_scratch_tables") as mock_drop,
        ):
            _vc.prune_releases_copy_swap("postgresql:///test", keep_ids={1}, review_ids=set())

        mock_drop.assert_not_called()

    def test_scratch_bases_cover_keep_ids_and_every_copy_table(self) -> None:
        """The cleanup list has to stay in lockstep with PRUNE_COPY_TABLES;
        a table added there but missed here leaks silently."""
        assert "_keep_ids" in _vc.PRUNE_SCRATCH_BASES
        for _, new_table, _, _ in _vc.PRUNE_COPY_TABLES:
            assert new_table in _vc.PRUNE_SCRATCH_BASES

    def test_shortfall_abort_retains_keep_ids_but_drops_the_new_table_copies(self) -> None:
        """WXYC/discogs-etl#357 forensics exemption.

        #356's blanket cleanup and #357's guard compose badly on their own:
        the abort drops the very ``_keep_ids_<suffix>`` table the alert's
        missing-id diff has to be re-run against, so the ids beyond the
        20-id sample become unrecoverable. The exemption keeps that one
        narrow id table and still drops the multi-GB ``new_X`` copies the
        #356 cleanup exists to prevent leaking.
        """
        with (
            patch.object(
                _vc,
                "_prune_copy_swap_tables",
                side_effect=_vc.CopySwapShortfallError("copied 2 of 3"),
            ),
            patch.object(_vc, "_prune_add_base_constraints_and_indexes"),
            patch.object(_vc, "_drop_prune_scratch_tables") as mock_drop,
            pytest.raises(_vc.CopySwapShortfallError),
        ):
            _vc.prune_releases_copy_swap("postgresql:///test", keep_ids={1}, review_ids=set())

        mock_drop.assert_called_once()
        bases = mock_drop.call_args.kwargs.get("bases", _vc.PRUNE_SCRATCH_BASES)
        assert "_keep_ids" not in bases, (
            "a shortfall abort must retain _keep_ids_<suffix> so an operator "
            "can re-run the missing-id diff past the 20-id alert sample"
        )
        assert set(bases) == {new for _, new, _, _ in _vc.PRUNE_COPY_TABLES}, (
            "the exemption is narrow: every new_X copy is still dropped, only _keep_ids is spared"
        )

    def test_shortfall_abort_logs_the_retained_table_name_at_error(self, caplog) -> None:
        """The retained table is only useful if the operator is told its name."""
        with (
            patch.object(
                _vc,
                "_prune_copy_swap_tables",
                side_effect=_vc.CopySwapShortfallError("copied 2 of 3"),
            ),
            patch.object(_vc, "_prune_add_base_constraints_and_indexes"),
            patch.object(_vc, "_drop_prune_scratch_tables"),
            caplog.at_level("ERROR", logger=_vc.logger.name),
            pytest.raises(_vc.CopySwapShortfallError),
        ):
            _vc.prune_releases_copy_swap(
                "postgresql:///test", keep_ids={1}, review_ids=set(), suffix="fx357abc"
            )

        errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
        assert any("_keep_ids_fx357abc" in m for m in errors), (
            f"the abort must name the retained table at ERROR, got: {errors!r}"
        )

    def test_non_shortfall_failure_still_drops_keep_ids_too(self) -> None:
        """The exemption is keyed to CopySwapShortfallError alone.

        Every other failure keeps #356's behavior: drop this invocation's
        entire scratch set, ``_keep_ids`` included. Nothing is retained,
        because nothing about a lock timeout or a constraint failure needs
        the keep-ids table to diagnose.
        """
        with (
            patch.object(
                _vc, "_prune_copy_swap_tables", side_effect=RuntimeError("lock timeout during swap")
            ),
            patch.object(_vc, "_prune_add_base_constraints_and_indexes"),
            patch.object(_vc, "_drop_prune_scratch_tables") as mock_drop,
            pytest.raises(RuntimeError, match="lock timeout"),
        ):
            _vc.prune_releases_copy_swap("postgresql:///test", keep_ids={1}, review_ids=set())

        mock_drop.assert_called_once()
        bases = mock_drop.call_args.kwargs.get("bases", _vc.PRUNE_SCRATCH_BASES)
        assert set(bases) == set(_vc.PRUNE_SCRATCH_BASES)
        assert "_keep_ids" in bases

    def test_cleanup_never_masks_the_original_error(self) -> None:
        """A cleanup that itself fails must not replace the exception that
        triggered it -- the original failure is the diagnostic signal."""
        with (
            patch.object(
                _vc, "_prune_copy_swap_tables", side_effect=RuntimeError("original failure")
            ),
            patch.object(_vc, "_prune_add_base_constraints_and_indexes"),
            patch.object(_vc, "psycopg") as mock_psycopg,
        ):
            mock_psycopg.connect.side_effect = OSError("db unreachable")
            with pytest.raises(RuntimeError, match="original failure"):
                _vc.prune_releases_copy_swap("postgresql:///test", keep_ids={1}, review_ids=set())
