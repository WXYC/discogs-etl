"""Unit tests for the FK-constraint race-tolerance changes in
``scripts/dedup_releases.py::add_base_constraints_and_indexes``.

The live library-metadata-lookup service inserts release + release_label +
release_artist + cache_metadata rows for every Discogs API miss. During the
dedup copy-swap window, LML can produce child rows referencing release ids
that are NOT in the post-dedup release table. Those orphans would cause
``ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`` to fail with
``ForeignKeyViolation`` and abort the whole rebuild (the 2026-05-13 23:42
UTC run, instance i-03e2afe2410ad43f8).

The fix combines two changes:

  1. A Level 1.5 step that deletes orphan child rows before constraint
     creation (so the bulk of the orphans are gone).
  2. NOT VALID on each FK constraint (so the small race window between
     cleanup and ADD doesn't matter — Postgres skips validation of
     existing rows, but new inserts are still checked).

This test pins both pieces against regression. The SQL shape is the
behavior — refactoring to a different valid SQL pattern would fail these
tests, which is appropriate because the WHOLE POINT of the change is the
specific SQL shape that's race-tolerant.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest  # noqa: F401  (kept for marker consistency; tests use plain assertions)

# Load dedup_releases as a module (it's a script, not a package).
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py"
_spec = importlib.util.spec_from_file_location("dedup_releases", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_dr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dr)


class TestLevel15OrphanCleanup:
    """The dedup pipeline must clean orphan child rows BEFORE adding FK
    constraints. Otherwise LML's concurrent writes during the dedup
    swap window produce orphans that block the FK validation.
    """

    def _captured_statements(self) -> list[str]:
        """Run ``add_base_constraints_and_indexes`` with all parallel
        execution stubbed out, capturing the SQL it would have run."""
        from unittest.mock import MagicMock, patch

        captured: list[str] = []

        def fake_exec_one(db_url, stmt):
            captured.append(stmt)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.info.dsn = "postgresql:///test"

        with patch.object(_dr, "_exec_one", side_effect=fake_exec_one):
            _dr.add_base_constraints_and_indexes(mock_conn, db_url="postgresql:///test")

        # The serial cur.execute calls also produce SQL — capture those too.
        for call in mock_cursor.execute.call_args_list:
            captured.append(call.args[0])

        return captured

    def test_orphan_cleanup_runs_for_release_label(self) -> None:
        """release_label is the table that caused the 2026-05-13 failure."""
        stmts = self._captured_statements()
        cleanup_stmts = [s for s in stmts if "DELETE FROM release_label" in s and "NOT EXISTS" in s]
        assert len(cleanup_stmts) == 1, (
            f"expected exactly one orphan-cleanup DELETE for release_label; got: {cleanup_stmts}"
        )

    def test_orphan_cleanup_covers_all_fk_child_tables(self) -> None:
        """Every table with an FK to release(id) needs orphan cleanup.
        Otherwise the cleanup is incomplete and the constraint can still race.
        """
        stmts = self._captured_statements()
        for table in [
            "release_artist",
            "release_label",
            "release_genre",
            "release_style",
            "cache_metadata",
        ]:
            cleanup = [s for s in stmts if f"DELETE FROM {table}" in s and "NOT EXISTS" in s]
            assert len(cleanup) == 1, (
                f"missing orphan-cleanup DELETE for {table!r}; "
                f"got {len(cleanup)} matching statements: {cleanup}"
            )

    def test_cleanup_runs_before_fk_constraints(self) -> None:
        """Order matters: cleanup must complete before the ADD CONSTRAINT
        block starts. _exec_parallel waits for each level to finish before
        the next is dispatched (it uses as_completed inside one call), so
        the cleanup batch must be earlier in the statement list than the
        FK batch."""
        stmts = self._captured_statements()
        cleanup_index = next(
            (i for i, s in enumerate(stmts) if "DELETE FROM release_label" in s),
            -1,
        )
        fk_index = next(
            (i for i, s in enumerate(stmts) if "ADD CONSTRAINT fk_release_label_release" in s),
            -1,
        )
        assert cleanup_index >= 0, "release_label cleanup statement not found"
        assert fk_index >= 0, "release_label FK constraint statement not found"
        assert cleanup_index < fk_index, (
            f"orphan cleanup at index {cleanup_index} must come before FK creation "
            f"at index {fk_index}; otherwise the FK validates against the orphans"
        )


class TestFkConstraintsUseNotValid:
    """All FK constraints added by ``add_base_constraints_and_indexes`` use
    the NOT VALID modifier so the ADD step itself can't race-fail on orphan
    rows that LML inserts during the brief window between Level-1.5 cleanup
    and the constraint creation.
    """

    def _captured_statements(self) -> list[str]:
        from unittest.mock import MagicMock, patch

        captured: list[str] = []

        def fake_exec_one(db_url, stmt):
            captured.append(stmt)

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.info.dsn = "postgresql:///test"

        with patch.object(_dr, "_exec_one", side_effect=fake_exec_one):
            _dr.add_base_constraints_and_indexes(mock_conn, db_url="postgresql:///test")
        return captured

    def test_every_fk_constraint_uses_not_valid(self) -> None:
        """No FK ADD CONSTRAINT statement may omit NOT VALID. Re-checking
        existing rows is exactly the failure mode the race exposes."""
        stmts = self._captured_statements()
        fk_stmts = [s for s in stmts if "ADD CONSTRAINT fk_" in s and "FOREIGN KEY" in s]
        assert len(fk_stmts) == 5, (
            f"expected 5 FK constraints (release_artist, release_label, release_genre, "
            f"release_style, cache_metadata); got {len(fk_stmts)}: {fk_stmts}"
        )
        for stmt in fk_stmts:
            assert "NOT VALID" in stmt, (
                f"FK constraint must include NOT VALID to avoid race-validation; got: {stmt}"
            )

    def test_not_valid_preserves_on_delete_cascade(self) -> None:
        """NOT VALID is a separate modifier from ON DELETE CASCADE — both
        must be present. CASCADE keeps the schema's referential cleanup
        semantics; NOT VALID keeps the ADD step from re-validating."""
        stmts = self._captured_statements()
        fk_stmts = [s for s in stmts if "ADD CONSTRAINT fk_" in s and "FOREIGN KEY" in s]
        for stmt in fk_stmts:
            assert "ON DELETE CASCADE" in stmt, (
                f"FK constraint must keep ON DELETE CASCADE; got: {stmt}"
            )
