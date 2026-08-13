"""Pin-enforcement test for the discogs-etl#370 catalog-parity residue vendor
(``vendor/parity-residue/``, pinned by ``parity-residue-pin.txt``).

This is the load-bearing half of the vendor/pin precedent
``wxyc-etl-pin.txt`` established: without an equivalent hash check,
``parity-residue-pin.txt`` is just a comment file, and a hand-edited
``vendor/parity-residue/ledger.json`` would silently change what the release
gate subtracts. Mirrors
``tests/integration/test_wxyc_identity_match_parity.py``'s
``test_pin_file_sha256s_match_vendored_files``, but lives under
``tests/unit/`` (not ``tests/integration/``) and carries no marker, so it
runs in CI's default ``test`` job -- no live PG needed, this only hashes
files already on disk.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PIN_PATH = REPO_ROOT / "parity-residue-pin.txt"
LEDGER_JSON = REPO_ROOT / "vendor" / "parity-residue" / "ledger.json"
LEDGER_MD = REPO_ROOT / "vendor" / "parity-residue" / "residue-ledger.md"


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


def test_pin_file_exists_and_names_both_vendored_files() -> None:
    pin = _parse_pin()
    assert "ledger_json_sha256" in pin
    assert "residue_ledger_md_sha256" in pin
    assert "measured_date" in pin


def test_pin_file_sha256s_match_vendored_files() -> None:
    pin = _parse_pin()
    pairs = [
        (LEDGER_JSON, "ledger_json_sha256"),
        (LEDGER_MD, "residue_ledger_md_sha256"),
    ]
    for path, key in pairs:
        assert path.is_file(), f"expected vendored file missing: {path.relative_to(REPO_ROOT)}"
        actual = _sha256(path)
        expected = pin[key]
        assert actual == expected, (
            f"{path.relative_to(REPO_ROOT)} drifted from pin {key} -- re-vendor per "
            "parity-residue-pin.txt's re-derivation procedure and update the pin"
        )


def test_only_two_source_files_are_vendored() -> None:
    """residue-absent-12.txt and residue-orphan-115.txt are deliberately
    excluded (see parity-residue-pin.txt) -- guard against a future vendor
    run silently widening what gets copied in."""
    vendor_dir = REPO_ROOT / "vendor" / "parity-residue"
    names = {p.name for p in vendor_dir.iterdir() if p.is_file()}
    assert names == {"ledger.json", "residue-ledger.md"}
