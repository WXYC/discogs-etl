"""Tests for ``scripts/vendor_parity_residue.py`` (discogs-etl#370, Part 2).

The script's source repo (``jakebromberg/wxyc-workspace``,
``plans/tubafrenzy-phase3-close-out/``) is untracked and personal -- it is
never checked out in CI -- so these tests exercise the conversion against a
small committed sample (``tests/fixtures/residue-collapsed-sample.tsv``)
rather than the live path.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "vendor_parity_residue.py"
SAMPLE_TSV = REPO_ROOT / "tests" / "fixtures" / "residue-collapsed-sample.tsv"


def _load_module():
    spec = importlib.util.spec_from_file_location("vendor_parity_residue", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["vendor_parity_residue"] = mod
    spec.loader.exec_module(mod)
    return mod


def _write_source_dir(tmp_path: Path, *, ledger_md: str = "# residue ledger\n") -> Path:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "residue-collapsed-599.tsv").write_bytes(SAMPLE_TSV.read_bytes())
    (source_dir / "residue-ledger.md").write_text(ledger_md, encoding="utf-8")
    return source_dir


class TestParseCollapsedTsv:
    def test_skips_comment_and_header_lines(self) -> None:
        mod = _load_module()
        pairs = mod.parse_collapsed_tsv(SAMPLE_TSV)
        assert pairs == {8: 7, 151: 150, 236: 235}

    def test_malformed_row_raises(self, tmp_path: Path) -> None:
        mod = _load_module()
        bad = tmp_path / "bad.tsv"
        bad.write_text("mysql_id\tbackend_kept_legacy_id\n8\t7\textra\n", encoding="utf-8")
        with pytest.raises(ValueError):
            mod.parse_collapsed_tsv(bad)


class TestVendorParityResidue:
    def test_writes_ledger_json_and_copies_narrative(self, tmp_path: Path) -> None:
        mod = _load_module()
        source_dir = _write_source_dir(
            tmp_path, ledger_md="# residue ledger\n\nMeasured 2026-08-11.\n"
        )
        out_dir = tmp_path / "out"

        mod.vendor_parity_residue(source_dir, out_dir, measured_date="2026-08-11")

        ledger_path = out_dir / "ledger.json"
        assert ledger_path.is_file()
        data = json.loads(ledger_path.read_text(encoding="utf-8"))
        assert data["collapsed_mysql_ids"] == {"8": 7, "151": 150, "236": 235}
        assert data["measured_date"] == "2026-08-11"
        assert data["baselines"] == {
            "measured_date": None,
            "normalizations": {},
            "cta_missing": None,
            "cta_extra": None,
        }

        copied_md = out_dir / "residue-ledger.md"
        assert copied_md.read_text(encoding="utf-8") == (
            source_dir / "residue-ledger.md"
        ).read_text(encoding="utf-8")

    def test_rerun_preserves_existing_baselines(self, tmp_path: Path) -> None:
        """Re-vendoring the collapsed-id set must not wipe a baseline step 6 populated."""
        mod = _load_module()
        source_dir = _write_source_dir(tmp_path)
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        (out_dir / "ledger.json").write_text(
            json.dumps(
                {
                    "collapsed_mysql_ids": {"1": 2},
                    "measured_date": "2026-01-01",
                    "baselines": {
                        "measured_date": "2026-08-13",
                        "normalizations": {"genre": {"case_folded": 42}},
                        "cta_missing": 2771,
                        "cta_extra": 6932,
                    },
                }
            ),
            encoding="utf-8",
        )

        mod.vendor_parity_residue(source_dir, out_dir, measured_date="2026-08-12")

        data = json.loads((out_dir / "ledger.json").read_text(encoding="utf-8"))
        assert data["collapsed_mysql_ids"] == {"8": 7, "151": 150, "236": 235}
        assert data["measured_date"] == "2026-08-12"
        assert data["baselines"]["cta_missing"] == 2771
        assert data["baselines"]["normalizations"] == {"genre": {"case_folded": 42}}

    def test_missing_collapsed_tsv_raises(self, tmp_path: Path) -> None:
        mod = _load_module()
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "residue-ledger.md").write_text("# residue ledger\n", encoding="utf-8")
        with pytest.raises(FileNotFoundError):
            mod.vendor_parity_residue(source_dir, tmp_path / "out", measured_date="2026-08-11")

    def test_missing_ledger_md_raises(self, tmp_path: Path) -> None:
        mod = _load_module()
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "residue-collapsed-599.tsv").write_bytes(SAMPLE_TSV.read_bytes())
        with pytest.raises(FileNotFoundError):
            mod.vendor_parity_residue(source_dir, tmp_path / "out", measured_date="2026-08-11")


class TestVendorParityResidueCli:
    def test_cli_subprocess_invocation(self, tmp_path: Path) -> None:
        source_dir = _write_source_dir(tmp_path)
        out_dir = tmp_path / "out"

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--source-dir",
                str(source_dir),
                "--out-dir",
                str(out_dir),
                "--measured-date",
                "2026-08-11",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr
        assert (out_dir / "ledger.json").is_file()
        assert (out_dir / "residue-ledger.md").is_file()

    def test_cli_missing_source_dir_exits_nonzero(self, tmp_path: Path) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--source-dir",
                str(tmp_path / "does-not-exist"),
                "--out-dir",
                str(tmp_path / "out"),
                "--measured-date",
                "2026-08-11",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "error" in result.stderr.lower()
