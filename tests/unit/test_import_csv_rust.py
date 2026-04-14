"""Tests for Rust DedupSet integration in import_csv.py."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Load import_csv module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

import_csv = _ic.import_csv

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"

# Skip all tests if wxyc_etl is not installed
pytest.importorskip("wxyc_etl", reason="wxyc-etl not installed")
from wxyc_etl.import_utils import DedupSet  # noqa: E402

# ---------------------------------------------------------------------------
# DedupSet parity with Python set
# ---------------------------------------------------------------------------


class TestDedupSetParity:
    """DedupSet must behave identically to Python set() for import_csv key types."""

    def test_single_column_keys(self) -> None:
        """Single-element tuple keys (e.g., release.id dedup)."""
        keys = [("5001",), ("5002",), ("5003",), ("5001",), ("5004",), ("5002",)]
        py_set: set[tuple[str | None, ...]] = set()
        rust_set = DedupSet()

        for key in keys:
            py_set.add(key)
            rust_set.add(key)

        for key in keys:
            assert (key in py_set) == (key in rust_set), f"Mismatch for key {key}"
        assert len(py_set) == len(rust_set)

    def test_two_column_keys(self) -> None:
        """Two-element tuple keys (e.g., release_artist dedup on release_id + artist_name)."""
        keys = [
            ("5001", "Juana Molina"),
            ("5002", "Stereolab"),
            ("5001", "Juana Molina"),  # duplicate
            ("5003", "Cat Power"),
            ("5001", "Cat Power"),  # same release_id, different artist
        ]
        py_set: set[tuple[str | None, ...]] = set()
        rust_set = DedupSet()

        for key in keys:
            py_set.add(key)
            rust_set.add(key)

        for key in keys:
            assert (key in py_set) == (key in rust_set), f"Mismatch for key {key}"
        assert len(py_set) == len(rust_set)

    def test_three_column_keys(self) -> None:
        """Three-element tuple keys (e.g., release_label dedup)."""
        keys = [
            ("5001", "Sonamos", "SON-001"),
            ("5002", "Duophonic", "D-UHF-CD22"),
            ("5001", "Sonamos", "SON-001"),  # duplicate
            ("5003", "Matador Records", "OLE 325-1"),
        ]
        py_set: set[tuple[str | None, ...]] = set()
        rust_set = DedupSet()

        for key in keys:
            py_set.add(key)
            rust_set.add(key)

        for key in keys:
            assert (key in py_set) == (key in rust_set), f"Mismatch for key {key}"
        assert len(py_set) == len(rust_set)

    def test_large_key_set(self) -> None:
        """1,000 representative keys with ~10% duplicates."""
        import random

        random.seed(42)
        base_keys = [(str(i), f"Artist {i % 100}") for i in range(900)]
        # Add ~100 duplicates from the existing keys
        dupes = [random.choice(base_keys) for _ in range(100)]
        all_keys = base_keys + dupes
        random.shuffle(all_keys)

        py_set: set[tuple[str | None, ...]] = set()
        rust_set = DedupSet()

        for key in all_keys:
            py_set.add(key)
            rust_set.add(key)

        for key in all_keys:
            assert (key in py_set) == (key in rust_set)
        assert len(py_set) == len(rust_set)


# ---------------------------------------------------------------------------
# None handling
# ---------------------------------------------------------------------------


class TestDedupSetNoneHandling:
    """DedupSet must handle None values in tuple keys correctly.

    Note: DedupSet converts None to "" internally, so (None,) and ("",) are
    treated as the same key. This is safe for import_csv because empty CSV
    values are converted to None before key construction (line 260-261),
    so both None and "" never coexist for the same field position.
    """

    def test_none_in_second_position(self) -> None:
        """("artist", None) is a valid key."""
        d = DedupSet()
        d.add(("5001", None))
        assert ("5001", None) in d
        assert len(d) == 1

    def test_none_in_first_position(self) -> None:
        """(None, "title") is a valid key."""
        d = DedupSet()
        d.add((None, "DOGA"))
        assert (None, "DOGA") in d
        assert len(d) == 1

    def test_all_none_key(self) -> None:
        """(None, None) is a valid key."""
        d = DedupSet()
        d.add((None, None))
        assert (None, None) in d
        assert len(d) == 1

    def test_none_keys_are_distinct_from_non_none(self) -> None:
        """Keys differing only in None vs non-None-non-empty values are distinct."""
        d = DedupSet()
        d.add(("5001", None))
        d.add(("5001", "Juana Molina"))
        assert ("5001", None) in d
        assert ("5001", "Juana Molina") in d
        assert len(d) == 2

    def test_add_returns_true_for_new_key(self) -> None:
        """add() returns True for a new key (not a duplicate)."""
        d = DedupSet()
        assert d.add(("5001", None)) is True

    def test_add_returns_false_for_duplicate_key(self) -> None:
        """add() returns False for a duplicate key."""
        d = DedupSet()
        d.add(("5001", None))
        assert d.add(("5001", None)) is False


# ---------------------------------------------------------------------------
# import_csv dedup parity
# ---------------------------------------------------------------------------


class TestImportCsvDedupParity:
    """import_csv() must produce identical results with DedupSet and Python set.

    This exercises the full import loop with real CSV fixtures, comparing
    row counts (imported, skipped, duplicates) between the two paths.
    """

    def _run_import_with_env(
        self, tmp_path: Path, csv_name: str, table_config: dict, *, no_rust: bool
    ) -> tuple[int, list]:
        """Run import_csv capturing COPY rows via mock, with or without Rust."""
        csv_path = CSV_DIR / csv_name
        rows_written: list[tuple] = []

        mock_copy = MagicMock()
        mock_copy.write_row = lambda row: rows_written.append(tuple(row))
        mock_copy.__enter__ = MagicMock(return_value=mock_copy)
        mock_copy.__exit__ = MagicMock(return_value=False)

        mock_cursor = MagicMock()
        mock_cursor.copy = MagicMock(return_value=mock_copy)

        mock_conn = MagicMock()
        mock_conn.cursor = MagicMock(
            return_value=MagicMock(
                __enter__=MagicMock(return_value=mock_cursor),
                __exit__=MagicMock(return_value=False),
            )
        )

        env_patch = {}
        if no_rust:
            env_patch["WXYC_ETL_NO_RUST"] = "1"

        old_env = {}
        for k, v in env_patch.items():
            old_env[k] = os.environ.get(k)
            os.environ[k] = v

        try:
            count = import_csv(
                mock_conn,
                csv_path,
                table=table_config["table"],
                csv_columns=table_config["csv_columns"],
                db_columns=table_config["db_columns"],
                required_columns=table_config["required"],
                transforms=table_config.get("transforms", {}),
                unique_key=table_config.get("unique_key"),
            )
        finally:
            for k, v in old_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

        return count, rows_written

    @pytest.mark.parametrize(
        "table_name",
        ["release", "release_artist", "release_label", "release_track", "release_track_artist"],
    )
    def test_parity_across_all_tables(self, tmp_path: Path, table_name: str) -> None:
        """Row counts and written rows must match between Rust DedupSet and Python set."""
        all_tables = _ic.BASE_TABLES + _ic.TRACK_TABLES
        table_config = next(t for t in all_tables if t["table"] == table_name)
        csv_name = table_config["csv_file"]

        count_rust, rows_rust = self._run_import_with_env(
            tmp_path, csv_name, table_config, no_rust=False
        )
        count_python, rows_python = self._run_import_with_env(
            tmp_path, csv_name, table_config, no_rust=True
        )

        assert count_rust == count_python, (
            f"Row count mismatch for {table_name}: rust={count_rust}, python={count_python}"
        )
        assert rows_rust == rows_python, f"Written rows differ for {table_name}"


# ---------------------------------------------------------------------------
# import_csv uses DedupSet when available
# ---------------------------------------------------------------------------


class TestImportCsvUsesDedupSet:
    """Verify import_csv() actually uses the Rust DedupSet when wxyc_etl is available."""

    def test_uses_rust_dedup_set_by_default(self) -> None:
        """import_csv module should expose _HAS_WXYC_ETL = True when wxyc_etl is installed."""
        assert hasattr(_ic, "_HAS_WXYC_ETL"), (
            "import_csv.py must define _HAS_WXYC_ETL for Rust DedupSet fallback"
        )
        assert _ic._HAS_WXYC_ETL is True

    def test_respects_no_rust_env_var(self, tmp_path: Path) -> None:
        """WXYC_ETL_NO_RUST=1 forces Python set() fallback."""
        # Create a minimal CSV with a duplicate
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id,name\n1,Juana Molina\n1,Juana Molina\n2,Stereolab\n")

        mock_copy = MagicMock()
        rows: list = []
        mock_copy.write_row = lambda row: rows.append(tuple(row))
        mock_copy.__enter__ = MagicMock(return_value=mock_copy)
        mock_copy.__exit__ = MagicMock(return_value=False)

        mock_cursor = MagicMock()
        mock_cursor.copy = MagicMock(return_value=mock_copy)

        mock_conn = MagicMock()
        mock_conn.cursor = MagicMock(
            return_value=MagicMock(
                __enter__=MagicMock(return_value=mock_cursor),
                __exit__=MagicMock(return_value=False),
            )
        )

        old_val = os.environ.get("WXYC_ETL_NO_RUST")
        os.environ["WXYC_ETL_NO_RUST"] = "1"
        try:
            count = import_csv(
                mock_conn,
                csv_path,
                table="test",
                csv_columns=["id", "name"],
                db_columns=["id", "name"],
                required_columns=["id"],
                transforms={},
                unique_key=["id"],
            )
        finally:
            if old_val is None:
                os.environ.pop("WXYC_ETL_NO_RUST", None)
            else:
                os.environ["WXYC_ETL_NO_RUST"] = old_val

        # Should still dedup correctly with Python set
        assert count == 2
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# Performance benchmark
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestDedupSetPerformance:
    """Benchmark DedupSet vs Python set on realistic import workloads.

    The primary benefit of DedupSet is reduced Python-side memory (Rust
    allocations live outside the Python heap). Speed is comparable to Python
    set because PyO3 boundary crossing (tuple -> Vec<Option<String>> conversion)
    on each call offsets the faster Rust hashing. A future batch API would
    eliminate the per-key crossing overhead.
    """

    def test_dedup_set_throughput(self) -> None:
        """Benchmark DedupSet vs Python set on 1M two-column string keys."""
        import random
        import time

        random.seed(42)

        n = 1_000_000
        base_keys = [(str(i), f"Artist {i % 50_000}") for i in range(n)]
        dupes = [random.choice(base_keys) for _ in range(n // 10)]
        all_keys = base_keys + dupes
        random.shuffle(all_keys)

        # Benchmark Python set
        py_set: set[tuple[str | None, ...]] = set()
        t0 = time.perf_counter()
        for key in all_keys:
            if key not in py_set:
                py_set.add(key)
        py_time = time.perf_counter() - t0

        # Benchmark Rust DedupSet
        rust_set = DedupSet()
        t0 = time.perf_counter()
        for key in all_keys:
            if key not in rust_set:
                rust_set.add(key)
        rust_time = time.perf_counter() - t0

        assert len(py_set) == len(rust_set)
        speedup = py_time / rust_time
        print(
            f"\nPython set: {py_time:.3f}s, Rust DedupSet: {rust_time:.3f}s, ratio: {speedup:.1f}x"
        )

    def test_dedup_set_lower_memory(self) -> None:
        """DedupSet should use less Python-side memory than Python set on 500K keys.

        Rust allocations live outside the Python heap, so tracemalloc measures
        only the Python-side overhead of the DedupSet wrapper, not the actual
        hash set storage.
        """
        import tracemalloc

        n = 500_000
        keys = [(str(i), f"Artist {i % 25_000}") for i in range(n)]

        # Measure Python set memory
        tracemalloc.start()
        py_set: set[tuple[str | None, ...]] = set()
        for key in keys:
            py_set.add(key)
        _, py_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        del py_set

        # Measure Rust DedupSet memory (only Python-side overhead is tracked)
        tracemalloc.start()
        rust_set = DedupSet()
        for key in keys:
            rust_set.add(key)
        _, rust_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        del rust_set

        print(
            f"\nPython set peak: {py_peak / 1024 / 1024:.1f} MB, "
            f"Rust DedupSet Python-side peak: {rust_peak / 1024 / 1024:.1f} MB"
        )
        assert rust_peak < py_peak, (
            f"Expected Rust DedupSet to have lower Python-side memory, "
            f"got Python: {py_peak / 1024:.0f} KB, Rust: {rust_peak / 1024:.0f} KB"
        )
