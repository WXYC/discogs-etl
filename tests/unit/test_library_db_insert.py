"""Arity guard for ``insert_library_rows``."""

from __future__ import annotations

import sqlite3

import pytest

from lib.library_db import (
    LIBRARY_INSERT_COLUMNS,
    create_library_schema,
    insert_library_rows,
)


def _cur() -> sqlite3.Cursor:
    cur = sqlite3.connect(":memory:").cursor()
    create_library_schema(cur)
    return cur


@pytest.mark.parametrize("width", [0, 1, 5, 10, 13])
def test_row_of_unsupported_length_raises(width: int) -> None:
    with pytest.raises(ValueError, match="columns"):
        insert_library_rows(_cur(), [[1] * width])


@pytest.mark.parametrize("width", [len(LIBRARY_INSERT_COLUMNS) - 1, len(LIBRARY_INSERT_COLUMNS)])
def test_full_and_short_trailing_rows_insert(width: int) -> None:
    cur = _cur()
    assert insert_library_rows(cur, [list(range(1, width + 1))]) == 1
