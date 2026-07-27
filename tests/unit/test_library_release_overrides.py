"""Unit tests for ``lib/library_release_overrides.py``'s pure formatting logic.

The DB-bound query functions (``fetch_override_summary``,
``fetch_missing_release_ids``) are exercised against real PostgreSQL in
``tests/integration/test_library_release_overrides.py``; this module covers
only the pure, DB-free ``format_summary_table`` and the ``OverrideSourceSummary``
shape, per discogs-etl#329.
"""

from __future__ import annotations

from lib.library_release_overrides import OverrideSourceSummary, format_summary_table


class TestFormatSummaryTable:
    def test_renders_a_row_per_source(self) -> None:
        rows = [
            OverrideSourceSummary(source="alex-l-2026", pinned=12040, missing_from_cache=4821),
            OverrideSourceSummary(source="alex-l-2026-masters", pinned=27466, missing_from_cache=0),
        ]
        table = format_summary_table(rows)
        assert "alex-l-2026" in table
        assert "12,040" in table
        assert "4,821" in table
        assert "alex-l-2026-masters" in table
        assert "27,466" in table

    def test_empty_rows_renders_zero_totals_and_no_source_row(self) -> None:
        table = format_summary_table([])
        assert "source" in table.lower()
        assert "TOTAL" in table
        # Zero-width totals line, no per-source data row.
        total_line = [line for line in table.splitlines() if line.startswith("TOTAL")][0]
        assert total_line.split()[1:] == ["0", "0"]

    def test_totals_line_sums_missing_across_sources(self) -> None:
        rows = [
            OverrideSourceSummary(source="a", pinned=100, missing_from_cache=10),
            OverrideSourceSummary(source="b", pinned=200, missing_from_cache=5),
        ]
        table = format_summary_table(rows)
        assert "15" in table


class TestOverrideSourceSummary:
    def test_is_a_frozen_dataclass(self) -> None:
        row = OverrideSourceSummary(source="x", pinned=1, missing_from_cache=1)
        assert row.source == "x"
        assert row.pinned == 1
        assert row.missing_from_cache == 1
