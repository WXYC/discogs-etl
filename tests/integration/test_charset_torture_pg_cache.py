"""WX-1.2.6 detector: catches future regressions in the PG cache write path
that would silently corrupt non-ASCII bytes flowing into the discogs-cache."""

from __future__ import annotations

import pytest

from tests.charset_torture import CharsetTortureEntry, entry_id, iter_entries

CORPUS_ENTRIES = list(iter_entries())

PG_TEXT_XFAIL_INPUTS: dict[tuple[str, str], str] = {
    ("quoting", "null\x00byte"): (
        "[dxe:pg-null-byte] PostgreSQL TEXT rejects U+0000 (SQL standard)"
    ),
}


@pytest.mark.pg
@pytest.mark.parametrize("entry", CORPUS_ENTRIES, ids=entry_id)
def test_pg_text_roundtrip(
    db_conn, entry: CharsetTortureEntry, request: pytest.FixtureRequest
) -> None:
    """INSERT into a TEXT column + SELECT must preserve every corpus entry."""
    xfail_reason = PG_TEXT_XFAIL_INPUTS.get((entry["category"], entry["input"]))
    if xfail_reason is not None:
        request.applymarker(pytest.mark.xfail(reason=xfail_reason, strict=True, raises=Exception))

    with db_conn.cursor() as cur:
        # Defensive: TEMP TABLEs survive rollbacks; if a future db_conn fixture
        # widens its scope, the next test would fail with "relation already exists".
        cur.execute("DROP TABLE IF EXISTS charset_probe")
        cur.execute("CREATE TEMP TABLE charset_probe (id SERIAL PRIMARY KEY, value TEXT NOT NULL)")
        cur.execute("INSERT INTO charset_probe (value) VALUES (%s) RETURNING id", (entry["input"],))
        row_id = cur.fetchone()[0]
        cur.execute("SELECT value FROM charset_probe WHERE id = %s", (row_id,))
        result = cur.fetchone()
    db_conn.commit()

    assert result is not None, f"{entry['category']}: row not found"
    assert result[0] == entry["input"], (
        f"{entry['category']}: round-trip lost bytes ({entry['notes']})"
    )
