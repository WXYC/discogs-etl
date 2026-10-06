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


@pytest.mark.parametrize("width", [0, 1, 5, 10, 14])
def test_row_of_unsupported_length_raises(width: int) -> None:
    with pytest.raises(ValueError, match="columns"):
        insert_library_rows(_cur(), [[1] * width])


@pytest.mark.parametrize("width", [11, 12, len(LIBRARY_INSERT_COLUMNS)])
def test_full_and_short_trailing_rows_insert(width: int) -> None:
    cur = _cur()
    assert insert_library_rows(cur, [list(range(1, width + 1))]) == 1


_BASE = 11
_FULL = len(LIBRARY_INSERT_COLUMNS)


@pytest.mark.parametrize(
    ("width", "release_letters", "comp_letter"),
    [
        (_BASE, None, None),  # 11-field tubafrenzy TSV
        (_BASE + 1, "b", None),  # 12-field MySQL row, volume letter not yet folded
        (_FULL, "B", "M"),  # 13-value Backend row
    ],
)
def test_each_producer_width_lands_its_columns(
    width: int, release_letters: str | None, comp_letter: str | None
) -> None:
    cur = _cur()
    row = [*range(1, _BASE + 1), release_letters, comp_letter][:width]
    assert insert_library_rows(cur, [row]) == 1
    got = cur.execute("SELECT release_call_letters, artist_comp_letter FROM library").fetchone()
    assert got == (release_letters, comp_letter)


def test_fold_volume_letters_folds_by_column_index() -> None:
    from lib.library_db import fold_volume_letters

    rows = [
        [*range(1, _BASE + 1), "b"],
        [*range(1, _BASE + 1), "b", "M"],
        list(range(1, _BASE + 1)),
    ]
    folded = [list(r) for r in fold_volume_letters(rows)]
    assert folded[0][_BASE] == "B"
    assert folded[1][_BASE:] == ["B", "M"]
    assert len(folded[2]) == _BASE
