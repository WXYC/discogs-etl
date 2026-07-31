#!/usr/bin/env python3
"""Analyze a ``prune-audit-<date>/`` dump — discogs-etl#217 Phase 2.

Phase 0 (``verify_cache.py --dump-classification``) captures the prune
classification during a monthly rebuild. This script turns that on-disk dump
into the Phase 2 answers, as pure file arithmetic plus an optional re-scoring
pass:

1. **Counts + pinned/non-pinned split** — keep / prune / review, ``pinned =
   |keep ∩ override|``, ``non-pinned survivors = |keep − override|``, and (given
   the post-rebuild ``release`` count) the **dedup delta** ``(keep + review) −
   release`` — the ``dedup_releases.py`` collapse that happens after the prune.
2. **#305 uplift** — ``|prune_ids_no_anv − prune_ids|``, releases pruned under
   pre-#305 logic but kept now via ``alternate_artist_name`` (marginal over
   pins). A pure diff of the two dumped id files.
3. **Residual false-negative rate** — a deterministic sample of ``prune_ids``
   (``random.Random(217)``) re-scored against the library index (reuses
   ``verify_cache``'s ``LibraryIndex`` + rapidfuzz scorer). The re-scoring is
   **faithful to the phase the prune actually used**: an exact-library-artist
   prune is re-judged title-only + format (Phase 2), a fuzzy prune by the
   artist×title geometric mean (Phase 4) — applying one formula to both would
   inflate the exact-artist prunes' scores and over-count false negatives. Each
   row is called a **true reject** (correct prune — no library album, or a
   format edition) or a **candidate false negative** (a library artist
   near-miss), with the miss mechanism noted, and the rate is reported with a
   Wilson score interval. Needs ``--library-db``.

Everything is read-only. The only writes are the report, the per-row CSV, and —
with ``--write-dedup-note`` — the ``dedup_note`` field of the dump's
``counts.json``.

Usage::

    python analyze_prune_audit.py PRUNE_AUDIT_DIR \\
        --library-db data/library.db \\
        --final-release-count 38662 \\
        --output-dir ./phase2-out
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import random
import sys
from dataclasses import dataclass
from pathlib import Path

from rapidfuzz import fuzz, process

# ---------------------------------------------------------------------------
# Reuse verify_cache's LibraryIndex + normalizers. Guard the import so the
# module object is shared if verify_cache is already loaded (mirrors the test
# modules — a second copy breaks identity checks and pickling; see #109).
# ---------------------------------------------------------------------------
_VC_PATH = Path(__file__).parent / "verify_cache.py"
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _spec = importlib.util.spec_from_file_location("verify_cache", _VC_PATH)
    assert _spec is not None and _spec.loader is not None
    _vc = importlib.util.module_from_spec(_spec)
    sys.modules["verify_cache"] = _vc
    _spec.loader.exec_module(_vc)

LibraryIndex = _vc.LibraryIndex
normalize_artist = _vc.normalize_artist
normalize_title = _vc.normalize_title

# Classifier thresholds, mirrored from verify_cache.MultiIndexMatcher /
# classify_artist_fuzzy so the re-scoring uses the same cutoffs the prune did.
DEFAULT_KEEP_THRESHOLD = 0.75
DEFAULT_REVIEW_THRESHOLD = 0.65
DEFAULT_ARTIST_THRESHOLD = 60  # token_set_ratio 0-100, artist-match floor
DEFAULT_SAMPLE_SIZE = 200
DEFAULT_SEED = 217


# ---------------------------------------------------------------------------
# Dump loading
# ---------------------------------------------------------------------------


@dataclass
class AuditDump:
    """The parsed contents of a ``prune-audit-<date>/`` directory."""

    keep: set[int]
    prune: set[int]
    review: set[int]
    override: set[int]
    prune_no_anv: set[int]
    counts: dict
    # release_id -> {"artist", "title", "format"} for every pruned id
    prune_releases: dict[int, dict]


def read_ids(path: Path) -> set[int]:
    """Read a sorted, newline-delimited id file into a set (missing -> empty)."""
    if not path.exists():
        return set()
    return {int(line) for line in path.read_text().split() if line.strip()}


def load_prune_releases(path: Path) -> dict[int, dict]:
    """Load ``prune_releases.jsonl`` into ``{id: {artist,title,format}}``."""
    out: dict[int, dict] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        out[int(obj["id"])] = obj
    return out


def load_audit(audit_dir: Path) -> AuditDump:
    """Load every artifact in *audit_dir* into an :class:`AuditDump`."""
    counts_path = audit_dir / "counts.json"
    counts = json.loads(counts_path.read_text()) if counts_path.exists() else {}
    return AuditDump(
        keep=read_ids(audit_dir / "keep_ids.txt"),
        prune=read_ids(audit_dir / "prune_ids.txt"),
        review=read_ids(audit_dir / "review_ids.txt"),
        override=read_ids(audit_dir / "override_ids.txt"),
        prune_no_anv=read_ids(audit_dir / "prune_ids_no_anv.txt"),
        counts=counts,
        prune_releases=load_prune_releases(audit_dir / "prune_releases.jsonl"),
    )


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval for a binomial proportion.

    Returns ``(low, high)`` clamped to ``[0, 1]``. ``n == 0`` -> ``(0.0, 0.0)``.
    Default ``z = 1.96`` is the 95% two-sided level. Preferred over the normal
    approximation because the sample proportion is often near 0 here (few
    false negatives), where the Wald interval misbehaves.
    """
    if n <= 0:
        return (0.0, 0.0)
    p = successes / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p + z2 / (2 * n)) / denom
    margin = (z / denom) * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n))
    return (max(0.0, center - margin), min(1.0, center + margin))


# ---------------------------------------------------------------------------
# Step 1 + 2: counts, splits, uplift, dedup delta
# ---------------------------------------------------------------------------


def compute_summary(audit: AuditDump, final_release_count: int | None = None) -> dict:
    """Compute the Phase 2 headline numbers from the dump (pure file arithmetic).

    - keep / prune / review counts
    - ``pinned`` = ``|keep ∩ override|``
    - ``non_pinned_survivors`` = ``|keep − override|`` (the comparable-to-history
      series vs 38,662 / 39,016, before the prod ``bulk_import`` confirmation)
    - ``uplift_305`` = ``|prune_no_anv − prune|``
    - ``dedup_delta`` = ``(keep + review) − final_release_count`` (``None`` when
      the final ``release`` count isn't supplied)
    """
    survivors = len(audit.keep) + len(audit.review)
    summary = {
        "keep": len(audit.keep),
        "prune": len(audit.prune),
        "review": len(audit.review),
        "override_ids": len(audit.override),
        "pinned": len(audit.keep & audit.override),
        "non_pinned_survivors": len(audit.keep - audit.override),
        "survivors_keep_plus_review": survivors,
        "uplift_305": len(audit.prune_no_anv - audit.prune),
        "final_release_count": final_release_count,
        "dedup_delta": (survivors - final_release_count)
        if final_release_count is not None
        else None,
    }
    return summary


def uplift_305_ids(audit: AuditDump) -> list[int]:
    """Sorted ids rescued by the ANV index (``prune_no_anv − prune``)."""
    return sorted(audit.prune_no_anv - audit.prune)


# ---------------------------------------------------------------------------
# Step 3: false-negative sampling + re-scoring
# ---------------------------------------------------------------------------


def sample_prune_ids(prune: set[int], sample_size: int, seed: int) -> list[int]:
    """Deterministically sample up to *sample_size* pruned ids.

    Sorted first so the pool is order-independent, then sampled with a seeded
    ``random.Random`` — same dump + same seed -> same sample. Returns all ids
    (sorted) when the set is smaller than *sample_size*.
    """
    pool = sorted(prune)
    if len(pool) <= sample_size:
        return pool
    rng = random.Random(seed)
    return sorted(rng.sample(pool, sample_size))


def _is_substring_variant(norm_artist: str, lib_artist: str, min_len: int = 4) -> bool:
    """True when one normalized artist name contains the other (but they differ).

    Guards against trivial short-token containment with *min_len*.
    """
    if not norm_artist or not lib_artist or norm_artist == lib_artist:
        return False
    shorter = min(norm_artist, lib_artist, key=len)
    if len(shorter) < min_len:
        return False
    return norm_artist in lib_artist or lib_artist in norm_artist


def _decide_call(
    artist_score: float,
    combined: float,
    is_substring: bool,
    keep_threshold: float,
    review_threshold: float,
    artist_floor: float,
) -> tuple[str, str]:
    """Pure band logic for a re-scored miss (all scores in ``[0, 1]``).

    Returns ``(call, mechanism)``. Kept separate from the rapidfuzz scoring so
    the threshold boundaries are testable without version-sensitive fuzzy
    scores. See :func:`classify_miss` for the mechanism descriptions.
    """
    if artist_score < artist_floor:
        return ("true_reject", "no_artist_match")
    if review_threshold <= combined < keep_threshold:
        return ("candidate_false_negative", "near_keep_threshold")
    if is_substring:
        return ("candidate_false_negative", "substring_variant")
    if combined >= keep_threshold:
        return ("candidate_false_negative", "scores_above_keep_on_rescore")
    return ("true_reject", "weak_match")


def _decide_title_only(
    title_score: float, keep_threshold: float, review_threshold: float
) -> tuple[str, str]:
    """Pure band logic for an **exact-artist** prune (title-only, all in [0, 1]).

    The prune's Phase-2 path (``verify_cache.classify_known_artist`` +
    ``classify_all_releases`` format filter) keeps an exact library artist's
    release when title ``>= keep``, reviews it in ``[review, keep)`` (still
    retained), and prunes it below ``review``. So a *pruned* exact-artist
    release with a title that now re-scores ``>= review`` is a discrepancy worth
    inspecting; below ``review`` it is a genuine title miss (the album isn't in
    the library) and a correct reject.
    """
    if title_score >= keep_threshold:
        return ("candidate_false_negative", "exact_artist_title_above_keep_on_rescore")
    if title_score >= review_threshold:
        return ("candidate_false_negative", "exact_artist_rescore_review")
    return ("true_reject", "title_miss")


def _row(
    phase: str,
    call: str,
    mechanism: str,
    artist_score: float,
    best_lib_artist: str,
    title_score: float,
    combined: float,
) -> dict:
    return {
        "phase": phase,
        "call": call,
        "mechanism": mechanism,
        "artist_score": artist_score,
        "best_lib_artist": best_lib_artist,
        "title_score": title_score,
        "combined": combined,
    }


def classify_miss(
    raw_artist: str,
    raw_title: str,
    index: LibraryIndex,
    keep_threshold: float = DEFAULT_KEEP_THRESHOLD,
    review_threshold: float = DEFAULT_REVIEW_THRESHOLD,
    artist_threshold: int = DEFAULT_ARTIST_THRESHOLD,
) -> dict:
    """Re-score one pruned release against the library index, faithful to the phase.

    ``verify_cache.classify_all_releases`` prunes in *different* ways depending
    on how the Discogs artist relates to the library, so re-scoring every prune
    with one formula (the Phase-4 geometric mean) over-flags the exact-artist
    prunes: their artist score is ~1.0, which inflates ``sqrt(artist·title)``
    and turns a correct title/format prune into a spurious "candidate FN". This
    reconstructs the phase the prune actually used:

    **Exact library artist** (``norm_artist`` in the index) — Phase-2 title-only
    + format filter (see :func:`_decide_title_only`):

    - ``format_mismatch`` — the exact ``(artist, title)`` pair was retained by
      title but dropped by the format filter -> **true reject** (a deliberate
      edition-level prune, not an artist false negative).
    - ``title_miss`` — title re-scores below ``review`` -> **true reject** (the
      album genuinely isn't in the library).
    - ``exact_artist_rescore_review`` / ``exact_artist_title_above_keep_on_rescore``
      — title now re-scores at/above the review/keep line although the release
      was pruned -> **candidate FN** (a discrepancy, e.g. a library snapshot that
      drifted from the one the prune ran against).

    **Fuzzy / non-library artist** — Phase-4 geometry (see :func:`_decide_call`),
    best artist by ``token_set_ratio`` then title within that artist's albums,
    ``combined = sqrt(artist·title)``:

    - ``no_artist_match`` — best artist below the floor -> **true reject**.
    - ``near_keep_threshold`` — ``combined`` in ``[review, keep)`` -> **candidate FN**.
    - ``substring_variant`` — a sub/superstring of a library artist -> **candidate FN**.
    - ``scores_above_keep_on_rescore`` — ``combined >= keep`` -> **candidate FN**.
    - ``weak_match`` — ``combined`` below ``review`` -> **true reject**.

    Returns a row dict with ``phase`` (``exact`` / ``fuzzy``), ``call``,
    ``mechanism``, and the scores (all ``[0, 1]``).
    """
    norm_artist = normalize_artist(raw_artist or "")
    norm_title = normalize_title(raw_title or "")

    # Phase-2 reconstruction: is the Discogs artist an exact library artist?
    exact_titles = index.artist_to_titles_list.get(norm_artist)
    if exact_titles is not None:
        if (norm_artist, norm_title) in index.exact_pairs:
            # Exact pair -> classify_known_artist returns KEEP; the only way it
            # still lands in prune is the format filter.
            return _row("exact", "true_reject", "format_mismatch", 1.0, norm_artist, 1.0, 1.0)
        title_result = process.extractOne(norm_title, exact_titles, scorer=fuzz.token_set_ratio)
        title_score = (float(title_result[1]) if title_result else 0.0) / 100.0
        call, mechanism = _decide_title_only(title_score, keep_threshold, review_threshold)
        # combined == title_score: the artist is exact, so title carries the call.
        return _row("exact", call, mechanism, 1.0, norm_artist, title_score, title_score)

    # Phase-3/4 reconstruction: fuzzy artist match.
    result = process.extractOne(norm_artist, index.all_artists, scorer=fuzz.token_set_ratio)
    if result is None:
        return _row("fuzzy", "true_reject", "empty_library", 0.0, "", 0.0, 0.0)
    best_artist, artist_score, _ = result

    titles = index.artist_to_titles_list.get(best_artist) or []
    if titles:
        title_result = process.extractOne(norm_title, titles, scorer=fuzz.token_set_ratio)
        title_score = float(title_result[1]) if title_result else 0.0
    else:
        title_score = 0.0

    combined = float((float(artist_score) * title_score) ** 0.5) / 100.0
    call, mechanism = _decide_call(
        artist_score=float(artist_score) / 100.0,
        combined=combined,
        is_substring=_is_substring_variant(norm_artist, best_artist),
        keep_threshold=keep_threshold,
        review_threshold=review_threshold,
        artist_floor=artist_threshold / 100.0,
    )
    return _row(
        "fuzzy",
        call,
        mechanism,
        float(artist_score) / 100.0,
        best_artist,
        title_score / 100.0,
        combined,
    )


def score_sample(
    sample_ids: list[int],
    audit: AuditDump,
    index: LibraryIndex,
    keep_threshold: float = DEFAULT_KEEP_THRESHOLD,
    review_threshold: float = DEFAULT_REVIEW_THRESHOLD,
    artist_threshold: int = DEFAULT_ARTIST_THRESHOLD,
) -> list[dict]:
    """Re-score every sampled id, joining to ``prune_releases`` for raw text."""
    rows: list[dict] = []
    for rid in sample_ids:
        rel = audit.prune_releases.get(rid, {})
        raw_artist = rel.get("artist") or ""
        raw_title = rel.get("title") or ""
        call = classify_miss(
            raw_artist, raw_title, index, keep_threshold, review_threshold, artist_threshold
        )
        rows.append(
            {
                "id": rid,
                "artist": raw_artist,
                "title": raw_title,
                "format": rel.get("format"),
                **call,
            }
        )
    return rows


def false_negative_rate(rows: list[dict]) -> dict:
    """Summarize a scored sample: candidate-FN count, rate, Wilson interval."""
    n = len(rows)
    fn = sum(1 for r in rows if r["call"] == "candidate_false_negative")
    low, high = wilson_interval(fn, n)
    mechanisms: dict[str, int] = {}
    for r in rows:
        if r["call"] == "candidate_false_negative":
            mechanisms[r["mechanism"]] = mechanisms.get(r["mechanism"], 0) + 1
    return {
        "sample_size": n,
        "candidate_false_negatives": fn,
        "rate": (fn / n) if n else 0.0,
        "wilson_low": low,
        "wilson_high": high,
        "fn_mechanisms": mechanisms,
    }


def write_sample_csv(path: Path, rows: list[dict]) -> None:
    """Write the per-row re-scoring CSV."""
    fields = [
        "id",
        "artist",
        "title",
        "format",
        "phase",
        "call",
        "mechanism",
        "artist_score",
        "best_lib_artist",
        "title_score",
        "combined",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Reporting + counts.json patch
# ---------------------------------------------------------------------------

# The comparable-to-history series (vs 38,662 / 39,016) is non-pinned
# bulk_import survivors. Confirm the file arithmetic against prod with this:
COMPARABLE_SERIES_SQL = (
    "SELECT count(*) FROM cache_metadata cm "
    "WHERE cm.source = 'bulk_import' "
    "AND cm.release_id NOT IN ("
    "SELECT discogs_release_id FROM lml_cache.library_release_override "
    "WHERE discogs_release_id IS NOT NULL)"
)


def format_report(
    summary: dict,
    fn_summary: dict | None,
    uplift_sample: list[int],
) -> str:
    """Render a Markdown Phase 2 report."""
    lines: list[str] = []
    lines.append("# Prune-audit analysis (discogs-etl#217 Phase 2)\n")

    lines.append("## Counts (partitioned; pinned split)\n")
    lines.append(f"- KEEP: {summary['keep']:,}")
    lines.append(f"- PRUNE: {summary['prune']:,}")
    lines.append(f"- REVIEW: {summary['review']:,}")
    lines.append(f"- survivors (keep + review): {summary['survivors_keep_plus_review']:,}")
    lines.append(f"- override ids applied: {summary['override_ids']:,}")
    lines.append(f"- pinned survivors (keep ∩ override): {summary['pinned']:,}")
    lines.append(
        f"- **non-pinned survivors (keep − override): {summary['non_pinned_survivors']:,}** "
        "— approximate comparable-to-history series (vs 38,662 / 39,016). This counts "
        "KEEP survivors from *all* cache sources; the exact history is `bulk_import`-only, "
        "so confirm with the SQL below before quoting a gain vs those figures."
    )
    if summary["final_release_count"] is not None:
        lines.append(f"- final `release` count: {summary['final_release_count']:,}")
        lines.append(f"- **dedup delta ((keep+review) − release): {summary['dedup_delta']:,}**")
    else:
        lines.append("- dedup delta: _not computed_ (pass `--final-release-count`)")
    lines.append("")
    lines.append(
        "Confirm the comparable series against prod:\n\n```sql\n"
        + COMPARABLE_SERIES_SQL
        + "\n```\n"
    )

    lines.append("## #305 uplift (marginal over pins)\n")
    lines.append(
        f"- **uplift = |prune_no_anv − prune| = {summary['uplift_305']:,}** — releases pruned "
        "under pre-#305 logic, kept now via `alternate_artist_name`."
    )
    if uplift_sample:
        preview = ", ".join(str(i) for i in uplift_sample[:10])
        lines.append(f"- spot-check ids (first 10): {preview}")
    lines.append("")

    if fn_summary is not None:
        lines.append("## Residual false-negative rate\n")
        lines.append(f"- sample size: {fn_summary['sample_size']:,} (seed {DEFAULT_SEED})")
        lines.append(f"- candidate false negatives: {fn_summary['candidate_false_negatives']:,}")
        lines.append(
            f"- **rate: {fn_summary['rate']:.3%} "
            f"(95% Wilson [{fn_summary['wilson_low']:.3%}, {fn_summary['wilson_high']:.3%}])**"
        )
        if fn_summary["fn_mechanisms"]:
            lines.append("- candidate-FN mechanisms:")
            for mech, count in sorted(fn_summary["fn_mechanisms"].items()):
                lines.append(f"  - {mech}: {count}")
        lines.append("")
    else:
        lines.append("## Residual false-negative rate\n")
        lines.append("- _not computed_ (pass `--library-db` to enable re-scoring)\n")

    return "\n".join(lines)


def write_dedup_note(counts_path: Path, dedup_delta: int | None) -> None:
    """Patch ``counts.json`` in place, filling the ``dedup_note`` slot.

    No-op (with a warning) when the delta is unknown or ``counts.json`` is
    absent — so a ``--write-dedup-note`` run without ``--final-release-count``
    never clobbers a value a prior run computed.
    """
    if dedup_delta is None:
        print(
            "warning: dedup delta unknown (no --final-release-count); leaving dedup_note unchanged",
            file=sys.stderr,
        )
        return
    if not counts_path.exists():
        print(f"warning: {counts_path} not found; cannot write dedup_note", file=sys.stderr)
        return
    counts = json.loads(counts_path.read_text())
    counts["dedup_note"] = f"dedup collapse (keep+review - release) = {dedup_delta}"
    counts_path.write_text(json.dumps(counts, indent=2) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze a verify_cache prune-audit dump (discogs-etl#217 Phase 2)."
    )
    parser.add_argument("audit_dir", type=Path, help="Path to a prune-audit-<date>/ directory")
    parser.add_argument(
        "--library-db",
        type=Path,
        default=None,
        help="library.db to enable false-negative re-scoring (step 3). Omit to skip.",
    )
    parser.add_argument(
        "--final-release-count",
        type=int,
        default=None,
        help="Post-rebuild `release` row count, to compute the dedup delta.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Prune sample size (default 200)",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Sample seed (default 217)")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Write report.md + sample.csv here (default: the audit dir).",
    )
    parser.add_argument(
        "--write-dedup-note",
        action="store_true",
        help="Patch the dump's counts.json dedup_note with the computed delta.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.audit_dir.is_dir():
        print(f"error: {args.audit_dir} is not a directory", file=sys.stderr)
        return 1
    if args.library_db is not None and not args.library_db.exists():
        # Guard before LibraryIndex.from_sqlite -> sqlite3.connect, which would
        # otherwise create an empty db file and fail with a confusing
        # "no such table: library".
        print(f"error: --library-db {args.library_db} does not exist", file=sys.stderr)
        return 1

    audit = load_audit(args.audit_dir)
    summary = compute_summary(audit, args.final_release_count)
    uplift_sample = uplift_305_ids(audit)

    fn_summary = None
    sample_rows: list[dict] = []
    if args.library_db is not None:
        index = LibraryIndex.from_sqlite(args.library_db)
        sample_ids = sample_prune_ids(audit.prune, args.sample_size, args.seed)
        sample_rows = score_sample(sample_ids, audit, index)
        fn_summary = false_negative_rate(sample_rows)

    out_dir = args.output_dir or args.audit_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    report = format_report(summary, fn_summary, uplift_sample)
    (out_dir / "report.md").write_text(report + "\n")
    if sample_rows:
        write_sample_csv(out_dir / "sample.csv", sample_rows)

    if args.write_dedup_note:
        write_dedup_note(args.audit_dir / "counts.json", summary["dedup_delta"])

    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
