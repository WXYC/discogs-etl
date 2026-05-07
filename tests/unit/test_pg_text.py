"""Unit tests for lib/pg_text.py."""

from __future__ import annotations

import pytest

from lib.pg_text import strip_pg_null_bytes


class TestStripPgNullBytes:
    """Strip U+0000 from strings; pass other types through unchanged."""

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("null\x00byte", "nullbyte"),
            ("\x00leading", "leading"),
            ("trailing\x00", "trailing"),
            ("\x00\x00both\x00\x00", "both"),
            ("no nulls here", "no nulls here"),
            ("", ""),
        ],
    )
    def test_strips_nul_from_strings(self, value: str, expected: str) -> None:
        assert strip_pg_null_bytes(value) == expected

    @pytest.mark.parametrize("value", [None, 0, 42, 3.14, b"bytes\x00", ("a", "b"), [1, 2]])
    def test_passes_non_strings_through(self, value: object) -> None:
        assert strip_pg_null_bytes(value) is value

    def test_idempotent(self) -> None:
        once = strip_pg_null_bytes("a\x00b\x00c")
        twice = strip_pg_null_bytes(once)
        assert once == twice == "abc"
