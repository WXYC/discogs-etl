"""PostgreSQL TEXT boundary helpers.

PostgreSQL's TEXT type rejects U+0000 (psycopg surfaces it as
``CharacterNotInRepertoireError``). Per the WXYC mojibake-prevention policy
(WXYC/docs#18), we strip U+0000 at every TEXT write boundary rather than
rejecting the row. U+0000 in metadata is always corruption, never intent;
stripping is idempotent and cheap.
"""

from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")


def strip_pg_null_bytes(value: T) -> T:
    """Return ``value`` with U+0000 removed if it is a string; pass through otherwise.

    Non-string values (None, int, etc.) are returned unchanged so this can be
    applied uniformly to heterogeneous CSV row tuples.
    """
    if isinstance(value, str):
        return value.replace("\x00", "")  # type: ignore[return-value]
    return value
