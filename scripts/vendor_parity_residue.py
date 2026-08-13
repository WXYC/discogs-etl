"""Vendor the discogs-etl#370 catalog-parity residue ledger (Part 2 of the
346-parity-clean-definition plan).

The input to the catalog-parity release gate -- the ids the daily-sync ETL
deliberately collapses or skips -- exists today only as untracked files in a
personal repo (``jakebromberg/wxyc-workspace``,
``plans/tubafrenzy-phase3-close-out/``). That means the gate is only
reproducible if those bytes get copied into this repo and pinned, the same
"vendor a canonical artifact, pin its hash" shape ``wxyc-etl-pin.txt`` uses
for the postgres-analog SQL (see that file's header, and
``tests/integration/test_wxyc_identity_match_parity.py``).

This script does that copy for two of the four source files -- the ones
Rule B in ``scripts/catalog_parity_diff.py`` genuinely cannot derive:

- ``residue-collapsed-599.tsv`` (the frozen mysql_id -> backend_kept_legacy_id
  pairs for the ``findExistingRelease`` duplicate-collapse residue, skip path
  7) is parsed and written to ``<out-dir>/ledger.json``, alongside a
  ``baselines`` block (the column -> class normalization-count shape plus
  the CTA missing/extra pair, populated in a later step -- see
  ``parity-residue-pin.txt``).
- ``residue-ledger.md`` (the provenance narrative the pin cites in place of
  a source commit -- this repo has no tag or commit to point at, since the
  source is untracked) is copied byte-for-byte to
  ``<out-dir>/residue-ledger.md``.

``residue-absent-12.txt`` and ``residue-orphan-115.txt`` are deliberately
NOT vendored -- see ``parity-residue-pin.txt`` for why (the first is Rule
B's own re-derivable output; the second is superseded by BS#2108's
not-yet-written delete script, which must emit its own kept-set artifact).

**Never runs in CI.** The source directory is a personal, untracked
checkout that CI never has -- this is a manual, run-by-hand conversion.
Tested against a small committed sample instead of the live path (see
``tests/unit/test_vendor_parity_residue.py`` and
``tests/fixtures/residue-collapsed-sample.tsv``).

Usage::

    python scripts/vendor_parity_residue.py \\
        --source-dir /path/to/wxyc-workspace/plans/tubafrenzy-phase3-close-out \\
        --measured-date 2026-08-11 \\
        [--out-dir vendor/parity-residue]

After running, recompute the SHA-256 of both output files and update
``parity-residue-pin.txt`` -- ``tests/unit/test_parity_residue_pin.py`` fails
the build otherwise.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)

_COLLAPSED_TSV_NAME = "residue-collapsed-599.tsv"
_LEDGER_MD_NAME = "residue-ledger.md"
_HEADER_ROW = ("mysql_id", "backend_kept_legacy_id")

# Structurally present, clearly unpopulated -- step 6 of the plan measures
# and fills these in. Kept as its own constant so a re-vendor that finds no
# existing ledger.json still produces a shape step 5's loader can parse.
EMPTY_BASELINES: dict[str, object] = {
    "measured_date": None,
    "normalizations": {},
    "cta_missing": None,
    "cta_extra": None,
}


def parse_collapsed_tsv(path: Path) -> dict[int, int]:
    """Parse a ``residue-collapsed-599.tsv``-shaped file into
    ``{mysql_id: backend_kept_legacy_id}``.

    Lines starting with ``#`` (a hand-committed sample's explanatory header,
    absent from the real 600-line file) and the ``mysql_id`` /
    ``backend_kept_legacy_id`` column-name header row are both skipped.
    """
    pairs: dict[int, int] = {}
    with path.open(encoding="utf-8") as f:
        for line_no, raw_line in enumerate(f, start=1):
            line = raw_line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            fields = tuple(line.split("\t"))
            if fields == _HEADER_ROW:
                continue
            if len(fields) != 2:
                raise ValueError(
                    f"{path}:{line_no}: expected 2 tab-separated fields, got {fields!r}"
                )
            mysql_id_raw, backend_kept_legacy_id_raw = fields
            try:
                pairs[int(mysql_id_raw)] = int(backend_kept_legacy_id_raw)
            except ValueError as exc:
                raise ValueError(f"{path}:{line_no}: non-integer id in {fields!r}") from exc
    return pairs


def _load_existing_baselines(ledger_json_path: Path) -> dict[str, object]:
    """Preserve a previously-populated ``baselines`` block across a re-vendor.

    Re-running this script only refreshes the collapsed-id set; it must
    never silently wipe out step 6's measured normalization/CTA baseline.
    Any read/parse failure (missing file, corrupt JSON, unexpected shape)
    falls back to the empty schema rather than raising -- a first vendor
    run has nothing to preserve, and that is not an error.
    """
    if not ledger_json_path.is_file():
        return dict(EMPTY_BASELINES)
    try:
        existing = json.loads(ledger_json_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return dict(EMPTY_BASELINES)
    baselines = existing.get("baselines") if isinstance(existing, dict) else None
    return baselines if isinstance(baselines, dict) else dict(EMPTY_BASELINES)


def vendor_parity_residue(source_dir: Path, out_dir: Path, *, measured_date: str) -> None:
    """Convert and copy the two vendorable residue files from ``source_dir``
    into ``out_dir`` (``vendor/parity-residue/`` in the normal invocation).

    Raises ``FileNotFoundError`` if either source file is absent, and
    ``ValueError`` if the TSV is malformed -- both meant for a human running
    this by hand, not for CI.
    """
    collapsed_tsv = source_dir / _COLLAPSED_TSV_NAME
    ledger_md = source_dir / _LEDGER_MD_NAME
    if not collapsed_tsv.is_file():
        raise FileNotFoundError(collapsed_tsv)
    if not ledger_md.is_file():
        raise FileNotFoundError(ledger_md)

    out_dir.mkdir(parents=True, exist_ok=True)
    ledger_json_path = out_dir / "ledger.json"

    collapsed = parse_collapsed_tsv(collapsed_tsv)
    baselines = _load_existing_baselines(ledger_json_path)

    ledger = {
        "measured_date": measured_date,
        "collapsed_mysql_ids": {str(k): v for k, v in sorted(collapsed.items())},
        "baselines": baselines,
    }
    ledger_json_path.write_text(
        json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    shutil.copyfile(ledger_md, out_dir / _LEDGER_MD_NAME)

    logger.info(
        "vendored catalog parity residue",
        extra={
            "step": "vendor_parity_residue",
            "collapsed_count": len(collapsed),
            "measured_date": measured_date,
        },
    )
    print(f"Wrote {ledger_json_path} ({len(collapsed)} collapsed ids) and its ledger.md copy")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Vendor the discogs-etl#370 catalog-parity residue ledger from its "
            "untracked source repo. Manual only -- never runs in CI."
        )
    )
    p.add_argument(
        "--source-dir",
        required=True,
        type=Path,
        help=(
            f"Directory holding {_COLLAPSED_TSV_NAME} and {_LEDGER_MD_NAME} "
            "(e.g. the wxyc-workspace checkout's "
            "plans/tubafrenzy-phase3-close-out/)."
        ),
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "vendor" / "parity-residue",
        help="Where to write ledger.json and copy residue-ledger.md. Defaults to "
        "vendor/parity-residue/.",
    )
    p.add_argument(
        "--measured-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="The date the source files were measured against prod (see "
        "residue-ledger.md's own 'Measured ...' line).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    init_logger(repo="discogs-etl", tool="discogs-etl vendor_parity_residue")
    try:
        vendor_parity_residue(args.source_dir, args.out_dir, measured_date=args.measured_date)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
