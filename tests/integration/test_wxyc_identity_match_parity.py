"""Parity check for the wxyc_identity_match_* plpgsql functions deployed by
alembic migration 0004.

Three independent assertions:

1. **Pin freshness** — `vendor/wxyc-etl/wxyc_unaccent.rules`,
   `vendor/wxyc-etl/wxyc_identity_match_functions.sql`, and
   `tests/fixtures/identity_normalization_cases.csv` hash to the SHA-256
   values recorded in `wxyc-etl-pin.txt`. If any vendored file drifts from
   the pin, fail with a re-vendoring hint.
2. **Function deploy** — after migration 0004 applies, all four entry points
   exist with the documented signature.
3. **PG byte-equality** (the big one) — each of the 252 fixture rows is fed
   through the corresponding plpgsql function on the live DB; the result
   must match the fixture's `expected` column byte-for-byte. Implicit
   Rust↔PG parity: the fixture IS the Rust-validated reference matrix from
   `wxyc-etl/wxyc-etl/tests/fixtures/identity_normalization_cases.csv`.

Plus a small idempotence smoke probe.
"""

from __future__ import annotations

import csv
import hashlib
import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PIN_PATH = REPO_ROOT / "wxyc-etl-pin.txt"
VENDOR_RULES = REPO_ROOT / "vendor" / "wxyc-etl" / "wxyc_unaccent.rules"
VENDOR_FUNCTIONS = REPO_ROOT / "vendor" / "wxyc-etl" / "wxyc_identity_match_functions.sql"
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "identity_normalization_cases.csv"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _parse_pin() -> dict[str, str]:
    out: dict[str, str] = {}
    for line in PIN_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip()
    return out


def test_pin_file_sha256s_match_vendored_files() -> None:
    pin = _parse_pin()
    pairs = [
        (VENDOR_RULES, "unaccent_rules_sha256"),
        (VENDOR_FUNCTIONS, "functions_sql_sha256"),
        (FIXTURE_PATH, "fixture_csv_sha256"),
    ]
    for path, key in pairs:
        actual = _sha256(path)
        expected = pin.get(key)
        assert expected is not None, f"missing pin entry {key!r}"
        assert actual == expected, (
            f"{path.relative_to(REPO_ROOT)} drifted from pin {key} — re-vendor "
            f"from wxyc-etl@v{pin.get('wxyc_etl_version', '?')} and bump "
            "wxyc-etl-pin.txt"
        )


# --- Live-PG assertions ---


def _run_alembic_upgrade(db_url: str) -> None:
    env = {**os.environ, "DATABASE_URL_DISCOGS": db_url}
    env.pop("DATABASE_URL", None)
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "0004_wxyc_identity_match_fns"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"alembic upgrade to 0004 failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


@pytest.fixture(scope="module")
def migrated_db_url(db_url: str) -> str:
    """Module-scoped: upgrade to 0004 once, reuse across the three PG tests.

    The `db_url` fixture from `tests/conftest.py` is module-scoped, so this
    just amortizes the one-shot alembic apply (`subprocess.run`, ~2s) across
    every test in this module. Re-running per-test would triple that cost.
    The migration is idempotent (CREATE OR REPLACE FUNCTION), so the
    `migration_double_apply_*` invariant still holds — that property is
    exercised separately by the `idempotence` test below.
    """
    _run_alembic_upgrade(db_url)
    return db_url


@pytest.mark.pg
def test_functions_deploy_after_migration_0004(migrated_db_url: str) -> None:
    db_url = migrated_db_url
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT proname
            FROM pg_proc
            WHERE proname IN (
                'wxyc_identity_match_artist',
                'wxyc_identity_match_title',
                'wxyc_identity_match_with_punctuation',
                'wxyc_identity_match_with_disambiguator_strip'
            )
            ORDER BY proname
            """
        )
        deployed = {row[0] for row in cur.fetchall()}
    assert deployed == {
        "wxyc_identity_match_artist",
        "wxyc_identity_match_title",
        "wxyc_identity_match_with_punctuation",
        "wxyc_identity_match_with_disambiguator_strip",
    }


_VARIANT_TO_FN = {
    "base": "wxyc_identity_match_artist",
    "title": "wxyc_identity_match_title",
    "punct": "wxyc_identity_match_with_punctuation",
    "disamb": "wxyc_identity_match_with_disambiguator_strip",
}


def _fixture_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with FIXTURE_PATH.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line_no, row in enumerate(reader, start=2):
            # Skip comment lines that csv.DictReader surfaces as rows where
            # every field is empty except `input` starting with `#`. The
            # fixture's leading `#` lines have only one field after the comma
            # split, so `expected` ends up empty for them; they're harmless
            # but we want to avoid asserting on them.
            inp = (row.get("input") or "").strip()
            if not inp or inp.startswith("#"):
                continue
            rows.append(
                {
                    "line_no": str(line_no),
                    "input": row["input"],
                    "expected": row["expected"],
                    "variant": row["variant"],
                    "category": row["category"],
                }
            )
    return rows


@pytest.mark.pg
def test_postgres_functions_match_fixture_row_for_row(migrated_db_url: str) -> None:
    db_url = migrated_db_url
    rows = _fixture_rows()
    assert len(rows) >= 250, f"fixture row count {len(rows)} < 250"

    failures: list[str] = []
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        for row in rows:
            fn = _VARIANT_TO_FN[row["variant"]]
            cur.execute(f"SELECT {fn}(%s)", (row["input"],))
            result = cur.fetchone()
            assert result is not None
            pg_out = result[0] or ""
            if pg_out != row["expected"]:
                failures.append(
                    f"  line {row['line_no']} [{row['variant']}/{row['category']}] "
                    f"input={row['input']!r}\n    expected={row['expected']!r}\n"
                    f"          pg={pg_out!r}"
                )

    assert not failures, (
        f"{len(failures)} of {len(rows)} parity rows failed:\n"
        + "\n".join(failures[:20])
        + ("\n..." if len(failures) > 20 else "")
    )


@pytest.mark.pg
def test_postgres_functions_idempotent(migrated_db_url: str) -> None:
    db_url = migrated_db_url
    probe = "   The Foo Fighters (1995)   "
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        for fn in _VARIANT_TO_FN.values():
            cur.execute(f"SELECT {fn}(%s)", (probe,))
            once = (cur.fetchone() or (None,))[0] or ""
            cur.execute(f"SELECT {fn}(%s)", (once,))
            twice = (cur.fetchone() or (None,))[0] or ""
            assert once == twice, f"{fn} not idempotent: once={once!r} twice={twice!r}"


@pytest.mark.pg
def test_migration_double_apply_is_a_no_op(db_url: str) -> None:
    """Re-applying migration 0004 must not throw and must leave the deploy
    intact. Verifies the contract `CREATE OR REPLACE FUNCTION` +
    `DROP TEXT SEARCH DICTIONARY IF EXISTS` + `CREATE TEXT SEARCH DICTIONARY`
    compose cleanly when alembic re-runs end-to-end (a `--resume` after a
    crash, or a re-stamp scenario).

    Uses the function-scoped `db_url` directly (not the module-scoped
    `migrated_db_url`) so this test exercises both calls itself.
    """
    _run_alembic_upgrade(db_url)
    _run_alembic_upgrade(db_url)
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT wxyc_identity_match_artist('Stereolab')")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "stereolab"
