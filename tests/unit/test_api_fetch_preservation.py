"""Unit tests for lib/api_fetch_preservation.py's count and age-distribution summary (#450)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from lib.api_fetch_preservation import PreservationSummary, summarize_ages

DAY = timedelta(days=1)


class TestSummarizeAges:
    def test_empty_has_no_oldest_or_median(self) -> None:
        assert summarize_ages([]) == (None, None, 0)

    def test_single_age(self) -> None:
        assert summarize_ages([10 * DAY]) == (10 * DAY, 10 * DAY, 0)

    @pytest.mark.parametrize(
        ("ages", "oldest", "median", "older_than_six_months"),
        [
            ([1 * DAY, 3 * DAY, 2 * DAY], 3 * DAY, 2 * DAY, 0),
            # Even count: median is the midpoint of the middle pair.
            ([1 * DAY, 2 * DAY, 3 * DAY, 4 * DAY], 4 * DAY, 2.5 * DAY, 0),
            # Six months is 183 days; the boundary itself is not "older than".
            ([183 * DAY, 184 * DAY, 400 * DAY], 400 * DAY, 184 * DAY, 2),
        ],
    )
    def test_distribution(self, ages, oldest, median, older_than_six_months) -> None:
        assert summarize_ages(ages) == (oldest, median, older_than_six_months)


class TestPreservationSummaryFormat:
    def test_reports_counts_and_age_distribution(self) -> None:
        summary = PreservationSummary(
            snapshotted=10,
            preserved=7,
            rehydrated=1,
            superseded=2,
            oldest_age=timedelta(days=400),
            median_age=timedelta(days=30),
            older_than_six_months=3,
        )
        line = summary.format()
        assert "snapshotted=10" in line
        assert "preserved=7" in line
        assert "superseded_by_dump=2" in line
        assert "rehydrated=1" in line
        assert "oldest=400d" in line
        assert "median=30d" in line
        assert "older_than_6mo=3" in line

    def test_nothing_preserved_omits_ages(self) -> None:
        summary = PreservationSummary(snapshotted=2, preserved=0, rehydrated=0, superseded=2)
        line = summary.format()
        assert "preserved=0" in line
        assert "oldest" not in line
