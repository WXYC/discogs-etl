"""WX-1.2.6 detector: catches future regressions in the PG cache write path
that would silently corrupt non-ASCII bytes flowing into the discogs-cache.

Per WXYC/docs#18 the write path strips U+0000 at the boundary (idempotent;
NUL in metadata is always corruption). This test mirrors that boundary by
applying ``strip_pg_null_bytes`` before INSERT and asserting the round-trip
matches the stripped form.
"""

from __future__ import annotations

import pytest

from lib.pg_text import strip_pg_null_bytes
from tests.charset_torture import CharsetTortureEntry, entry_id, iter_entries

CORPUS_ENTRIES = list(iter_entries())


@pytest.mark.pg
@pytest.mark.parametrize("entry", CORPUS_ENTRIES, ids=entry_id)
def test_pg_text_roundtrip(db_conn, entry: CharsetTortureEntry) -> None:
    """INSERT into a TEXT column + SELECT must preserve every corpus entry
    after the boundary strip."""
    boundary_value = strip_pg_null_bytes(entry["input"])

    with db_conn.cursor() as cur:
        # Defensive: TEMP TABLEs survive rollbacks; if a future db_conn fixture
        # widens its scope, the next test would fail with "relation already exists".
        cur.execute("DROP TABLE IF EXISTS charset_probe")
        cur.execute("CREATE TEMP TABLE charset_probe (id SERIAL PRIMARY KEY, value TEXT NOT NULL)")
        cur.execute("INSERT INTO charset_probe (value) VALUES (%s) RETURNING id", (boundary_value,))
        row_id = cur.fetchone()[0]
        cur.execute("SELECT value FROM charset_probe WHERE id = %s", (row_id,))
        result = cur.fetchone()
    db_conn.commit()

    assert result is not None, f"{entry['category']}: row not found"
    assert result[0] == boundary_value, (
        f"{entry['category']}: round-trip lost bytes ({entry['notes']})"
    )
