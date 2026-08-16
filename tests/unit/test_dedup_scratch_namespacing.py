"""Unit tests for scripts/dedup_releases.py's per-invocation scratch-table
namespacing (WXYC/discogs-etl#356).

Mock-based (no PostgreSQL needed): asserts the exact SQL text each function
emits, mirroring the pattern already used for verify_cache.py in
tests/unit/test_verify_cache.py::TestPruneCopySwapSQL. Real end-to-end
collision-safety and crash-residue behavior are covered by the pg-marked
integration tests in tests/integration/.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py"
_spec = importlib.util.spec_from_file_location("dedup_releases", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_dr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dr)


def _make_mock_conn(fetchone_value=(0,)):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = fetchone_value
    mock_cursor.copy.return_value.__enter__ = MagicMock()
    mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def _executed_sql(mock_cursor) -> list[str]:
    return [c.args[0] if c.args else "" for c in mock_cursor.execute.call_args_list]


class TestCopyTableSuffix:
    def test_default_suffix_preserves_unsuffixed_name(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(5,))
        _dr.copy_table(mock_conn, "release", "new_release", "id, title", "id")
        sqls = _executed_sql(mock_cursor)
        assert any("CREATE TABLE new_release AS" in s for s in sqls)
        assert not any("new_release_" in s for s in sqls)

    def test_suffix_is_appended_to_the_new_table_name(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(5,))
        _dr.copy_table(mock_conn, "release", "new_release", "id, title", "id", suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("DROP TABLE IF EXISTS new_release_ab12cd34" in s for s in sqls)
        assert any("CREATE TABLE new_release_ab12cd34 AS" in s for s in sqls)
        # The base (unsuffixed) name must never appear as a bare identifier.
        assert not any(s.strip().endswith("new_release") for s in sqls)

    def test_suffix_threads_into_dedup_delete_ids_filter(self) -> None:
        """copy_table's WHERE NOT EXISTS filter reads dedup_delete_ids --
        it must read the SAME invocation's suffixed table, not the bare
        (potentially some other invocation's) name."""
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(5,))
        _dr.copy_table(mock_conn, "release", "new_release", "id, title", "id", suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("dedup_delete_ids_ab12cd34" in s for s in sqls)

    def test_column_defaults_lookup_uses_base_name_not_physical_name(self) -> None:
        """PRE_SWAP_COLUMN_DEFAULTS is keyed by the base scratch name
        (e.g. 'new_cache_metadata'); the DEFAULT must still be applied to
        the physical (suffixed) table."""
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(5,))
        _dr.copy_table(
            mock_conn,
            "cache_metadata",
            "new_cache_metadata",
            "release_id, cached_at, source, last_validated",
            "release_id",
            suffix="deadbeef",
        )
        sqls = _executed_sql(mock_cursor)
        assert any(
            "ALTER TABLE new_cache_metadata_deadbeef ALTER COLUMN cached_at SET DEFAULT" in s
            for s in sqls
        )

    def test_does_not_commit_internally(self) -> None:
        """copy_table must not commit on its own -- production code wraps
        the whole scratch-build phase (every DEDUP_TABLES copy_table call)
        in one transaction so a crashed run leaves no residue; an internal
        commit here would defeat that."""
        mock_conn, _ = _make_mock_conn(fetchone_value=(5,))
        _dr.copy_table(mock_conn, "release", "new_release", "id, title", "id")
        mock_conn.commit.assert_not_called()


class TestSwapTablesSuffix:
    def test_default_suffix_preserves_unsuffixed_rename_source(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn()
        _dr.swap_tables(mock_conn, "release", "new_release")
        sqls = _executed_sql(mock_cursor)
        assert "ALTER TABLE new_release RENAME TO release" in sqls

    def test_suffix_is_the_rename_source(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn()
        _dr.swap_tables(mock_conn, "release", "new_release", suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert "ALTER TABLE new_release_ab12cd34 RENAME TO release" in sqls

    def test_backup_name_is_namespaced_too(self) -> None:
        """The copy-swap backup is a scratch table like any other.

        Left global, ``release_old`` is the one name two concurrent
        invocations still share after WXYC/discogs-etl#356 namespaced
        everything else -- and it is the most destructive one to share,
        because it briefly holds the *only* copy of the live table's rows.
        Before #356 the two runs collided on their ``dedup_delete_ids`` /
        ``new_X`` names long before either reached the swap, which
        incidentally kept them apart here; removing that early collision
        means both now arrive at the swap cleanly.
        """
        mock_conn, mock_cursor = _make_mock_conn()
        _dr.swap_tables(mock_conn, "release", "new_release", suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert "ALTER TABLE release RENAME TO release_old_ab12cd34" in sqls
        assert "DROP TABLE release_old_ab12cd34 CASCADE" in sqls
        assert "ALTER TABLE release RENAME TO release_old" not in sqls
        assert "DROP TABLE release_old CASCADE" not in sqls

    def test_default_suffix_preserves_unsuffixed_backup_name(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn()
        _dr.swap_tables(mock_conn, "release", "new_release")
        sqls = _executed_sql(mock_cursor)
        assert "ALTER TABLE release RENAME TO release_old" in sqls
        assert "DROP TABLE release_old CASCADE" in sqls

    def test_two_invocations_never_share_a_backup_name(self) -> None:
        """The property the fix exists for, asserted directly."""
        sqls: list[list[str]] = []
        for suffix in ("ab12cd34", "99887766"):
            mock_conn, mock_cursor = _make_mock_conn()
            _dr.swap_tables(mock_conn, "release", "new_release", suffix=suffix)
            sqls.append(_executed_sql(mock_cursor))
        backups = [{s for s in run if "_old" in s} for run in sqls]
        assert backups[0] and backups[1]
        assert not backups[0] & backups[1]


class TestEnsureDedupIdsSuffix:
    def test_default_suffix_uses_bare_dedup_delete_ids_name(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(False,))
        # First fetchone (exists check) -> False so we fall into CREATE path.
        # Second fetchone (SELECT count(*)) -> reuse same mock (returns
        # tuple either way; count path only cares about [0]).
        _dr.ensure_dedup_ids(mock_conn)
        sqls = _executed_sql(mock_cursor)
        assert any("CREATE UNLOGGED TABLE dedup_delete_ids AS" in s for s in sqls)

    def test_suffix_is_appended_to_dedup_delete_ids(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(False,))
        _dr.ensure_dedup_ids(mock_conn, suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("CREATE UNLOGGED TABLE dedup_delete_ids_ab12cd34 AS" in s for s in sqls)
        assert any("ALTER TABLE dedup_delete_ids_ab12cd34 ADD PRIMARY KEY" in s for s in sqls)

    def test_keep_ids_loaded_references_suffixed_keep_release_ids(self) -> None:
        mock_conn, mock_cursor = _make_mock_conn(fetchone_value=(False,))
        _dr.ensure_dedup_ids(mock_conn, keep_ids_loaded=True, suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("keep_release_ids_ab12cd34" in s for s in sqls)

    def test_does_not_commit_internally(self) -> None:
        mock_conn, _ = _make_mock_conn(fetchone_value=(False,))
        _dr.ensure_dedup_ids(mock_conn, suffix="ab12cd34")
        mock_conn.commit.assert_not_called()


class TestLoadKeepReleaseIdsSuffix:
    def test_suffix_is_appended_to_keep_release_ids_table(self, tmp_path) -> None:
        mock_conn, mock_cursor = _make_mock_conn()
        path = tmp_path / "keep.txt"
        path.write_text("1\n2\n")
        _dr.load_keep_release_ids(mock_conn, path, suffix="ab12cd34")
        sqls = _executed_sql(mock_cursor)
        assert any("CREATE UNLOGGED TABLE keep_release_ids_ab12cd34" in s for s in sqls)


class TestBuildDedupScratchTablesTransactionSafety:
    """build_dedup_scratch_tables opens its OWN non-autocommit connection
    and commits exactly once, at the end, after every scratch table for
    this invocation has been built -- see WXYC/discogs-etl#356's crash
    residue acceptance criterion. Mock-based: verifies the commit/rollback
    contract without needing PostgreSQL; the real "kill mid-DDL, assert
    cleanliness" property is covered by the pg-marked integration test.
    """

    def test_commits_once_after_building_all_scratch_tables(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (3,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(_dr.psycopg, "connect", return_value=mock_conn) as mock_connect:
            delete_count, keep_ids_loaded = _dr.build_dedup_scratch_tables(
                "postgresql:///test", "ab12cd34"
            )

        # Opened with NO autocommit kwarg (i.e. non-autocommit default).
        mock_connect.assert_called_once_with("postgresql:///test")
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()
        mock_conn.close.assert_called_once()
        assert delete_count == 3
        assert keep_ids_loaded is False

    def test_rolls_back_and_reraises_on_failure(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = RuntimeError("boom")
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(_dr.psycopg, "connect", return_value=mock_conn):
            try:
                _dr.build_dedup_scratch_tables("postgresql:///test", "ab12cd34")
            except RuntimeError:
                pass
            else:
                raise AssertionError("expected RuntimeError to propagate")

        mock_conn.commit.assert_not_called()
        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_threads_the_same_suffix_through_every_copy_table_call(self) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (3,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(_dr.psycopg, "connect", return_value=mock_conn):
            _dr.build_dedup_scratch_tables("postgresql:///test", "ab12cd34")

        sqls = _executed_sql(mock_cursor)
        for _old, new, _cols, _id_col in _dr.DEDUP_TABLES:
            assert any(f"CREATE TABLE {new}_ab12cd34 AS" in s for s in sqls), (
                f"expected a suffixed CREATE TABLE for {new}"
            )


class TestDedupScratchBases:
    """#356 regression guard: the failure-path cleanup list must cover every
    scratch table an invocation can create. A table added to DEDUP_TABLES
    but missed here leaks a full copy of a live table permanently, because
    per-invocation suffixing means no later run reclaims it by name.
    """

    def test_covers_the_id_tables_and_every_dedup_copy_target(self) -> None:
        assert "dedup_delete_ids" in _dr.DEDUP_SCRATCH_BASES
        assert "keep_release_ids" in _dr.DEDUP_SCRATCH_BASES
        for _old, new, _cols, _id_col in _dr.DEDUP_TABLES:
            assert new in _dr.DEDUP_SCRATCH_BASES

    def test_excludes_the_deliberately_unnamespaced_tables(self) -> None:
        """release_track_count / wxyc_label_pref / release_label_match /
        label_hierarchy are cross-subprocess or flag-gated tables that are
        NOT suffixed (see docs/architecture.md); main() drops them by their
        literal names, so suffixing them here would emit DROPs for tables
        that never existed and, worse, imply they were namespaced."""
        for unnamespaced in (
            "release_track_count",
            "wxyc_label_pref",
            "release_label_match",
            "label_hierarchy",
        ):
            assert unnamespaced not in _dr.DEDUP_SCRATCH_BASES
