"""Tests for scripts/analyze_prune_audit.py (discogs-etl#217 Phase 2 tooling)."""

import csv
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "analyze_prune_audit.py"
if "analyze_prune_audit" in sys.modules:
    _apa = sys.modules["analyze_prune_audit"]
else:
    _spec = importlib.util.spec_from_file_location("analyze_prune_audit", _SCRIPT_PATH)
    assert _spec is not None and _spec.loader is not None
    _apa = importlib.util.module_from_spec(_spec)
    sys.modules["analyze_prune_audit"] = _apa
    _spec.loader.exec_module(_apa)

LibraryIndex = _apa.LibraryIndex


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_ids(path: Path, ids) -> None:
    path.write_text("".join(f"{i}\n" for i in sorted(ids)))


def _make_dump(
    tmp_path: Path,
    *,
    keep=(),
    prune=(),
    review=(),
    override=(),
    prune_no_anv=(),
    prune_releases=(),
    counts=None,
) -> Path:
    d = tmp_path / "prune-audit-2026-08-01"
    d.mkdir(parents=True, exist_ok=True)
    _write_ids(d / "keep_ids.txt", keep)
    _write_ids(d / "prune_ids.txt", prune)
    _write_ids(d / "review_ids.txt", review)
    _write_ids(d / "override_ids.txt", override)
    _write_ids(d / "prune_ids_no_anv.txt", prune_no_anv)
    with open(d / "prune_releases.jsonl", "w", encoding="utf-8") as f:
        for obj in prune_releases:
            f.write(json.dumps(obj) + "\n")
    default_counts = {
        "keep": len(keep),
        "prune": len(prune),
        "review": len(review),
        "override_applied": len(override),
        "total_release_rows": len(keep) + len(prune) + len(review),
        "dedup_note": None,
    }
    (d / "counts.json").write_text(json.dumps(counts or default_counts, indent=2) + "\n")
    return d


# ---------------------------------------------------------------------------
# Wilson interval
# ---------------------------------------------------------------------------


class TestWilsonInterval:
    def test_zero_n_returns_zero_zero(self):
        assert _apa.wilson_interval(0, 0) == (0.0, 0.0)

    def test_zero_successes(self):
        low, high = _apa.wilson_interval(0, 200)
        assert low == 0.0
        assert high == pytest.approx(0.0188, abs=1e-3)

    def test_all_successes(self):
        low, high = _apa.wilson_interval(200, 200)
        assert high == 1.0
        assert low == pytest.approx(0.9812, abs=1e-3)

    def test_ten_of_two_hundred(self):
        low, high = _apa.wilson_interval(10, 200)
        assert low == pytest.approx(0.0274, abs=2e-3)
        assert high == pytest.approx(0.0896, abs=2e-3)
        assert low < 0.05 < high


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


class TestLoadAudit:
    def test_read_ids_missing_file_is_empty(self, tmp_path):
        assert _apa.read_ids(tmp_path / "nope.txt") == set()

    def test_loads_all_artifacts(self, tmp_path):
        d = _make_dump(
            tmp_path,
            keep=[1, 2, 3],
            prune=[5],
            review=[4],
            override=[3],
            prune_no_anv=[2, 5],
            prune_releases=[{"id": 5, "artist": "Junk", "title": "Nowhere", "format": "CD"}],
        )
        audit = _apa.load_audit(d)
        assert audit.keep == {1, 2, 3}
        assert audit.prune == {5}
        assert audit.review == {4}
        assert audit.override == {3}
        assert audit.prune_no_anv == {2, 5}
        assert audit.prune_releases[5]["artist"] == "Junk"
        assert audit.counts["prune"] == 1


# ---------------------------------------------------------------------------
# Summary / uplift
# ---------------------------------------------------------------------------


class TestComputeSummary:
    def test_counts_pinned_split_uplift_dedup(self, tmp_path):
        d = _make_dump(
            tmp_path,
            keep=[1, 2, 3, 4],
            prune=[5],
            review=[],
            override=[4],
            prune_no_anv=[2, 5],
        )
        audit = _apa.load_audit(d)
        s = _apa.compute_summary(audit, final_release_count=3)
        assert s["keep"] == 4
        assert s["prune"] == 1
        assert s["review"] == 0
        assert s["pinned"] == 1  # {1,2,3,4} ∩ {4}
        assert s["non_pinned_survivors"] == 3  # {1,2,3}
        assert s["survivors_keep_plus_review"] == 4
        assert s["uplift_305"] == 1  # {2,5} − {5}
        assert s["dedup_delta"] == 1  # 4 − 3

    def test_dedup_delta_none_without_final_count(self, tmp_path):
        d = _make_dump(tmp_path, keep=[1], prune=[2])
        s = _apa.compute_summary(_apa.load_audit(d))
        assert s["dedup_delta"] is None
        assert s["final_release_count"] is None

    def test_uplift_ids_sorted(self, tmp_path):
        d = _make_dump(tmp_path, prune=[5], prune_no_anv=[9, 2, 5])
        assert _apa.uplift_305_ids(_apa.load_audit(d)) == [2, 9]


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


class TestSamplePruneIds:
    def test_small_set_returns_all_sorted(self):
        assert _apa.sample_prune_ids({3, 1, 2}, 200, 217) == [1, 2, 3]

    def test_deterministic_with_seed(self):
        pool = set(range(1000))
        a = _apa.sample_prune_ids(pool, 200, 217)
        b = _apa.sample_prune_ids(pool, 200, 217)
        assert a == b
        assert len(a) == 200
        assert a == sorted(a)

    def test_different_seed_differs(self):
        pool = set(range(1000))
        assert _apa.sample_prune_ids(pool, 200, 217) != _apa.sample_prune_ids(pool, 200, 99)


# ---------------------------------------------------------------------------
# Decision bands (pure, deterministic)
# ---------------------------------------------------------------------------


class TestDecideCall:
    def test_below_artist_floor_is_true_reject(self):
        assert _apa._decide_call(0.5, 0.9, False, 0.75, 0.65, 0.60) == (
            "true_reject",
            "no_artist_match",
        )

    def test_near_keep_threshold_is_candidate(self):
        assert _apa._decide_call(0.9, 0.70, False, 0.75, 0.65, 0.60) == (
            "candidate_false_negative",
            "near_keep_threshold",
        )

    def test_review_boundary_is_near(self):
        assert _apa._decide_call(0.9, 0.65, False, 0.75, 0.65, 0.60)[1] == "near_keep_threshold"

    def test_substring_below_review_is_candidate(self):
        assert _apa._decide_call(0.9, 0.40, True, 0.75, 0.65, 0.60) == (
            "candidate_false_negative",
            "substring_variant",
        )

    def test_scores_above_keep_is_candidate(self):
        assert _apa._decide_call(0.9, 0.80, False, 0.75, 0.65, 0.60) == (
            "candidate_false_negative",
            "scores_above_keep_on_rescore",
        )

    def test_weak_match_is_true_reject(self):
        assert _apa._decide_call(0.9, 0.40, False, 0.75, 0.65, 0.60) == (
            "true_reject",
            "weak_match",
        )


class TestSubstringVariant:
    def test_containment(self):
        assert _apa._is_substring_variant("stereolab reissue", "stereolab")

    def test_equal_is_not_variant(self):
        assert not _apa._is_substring_variant("stereolab", "stereolab")

    def test_short_shared_is_guarded(self):
        assert not _apa._is_substring_variant("abc", "abc def")


# ---------------------------------------------------------------------------
# classify_miss (integration with a real LibraryIndex)
# ---------------------------------------------------------------------------


class TestClassifyMiss:
    @pytest.fixture
    def index(self):
        return LibraryIndex.from_rows(
            [
                ("Juana Molina", "DOGA", "CD"),
                ("Stereolab", "Aluminum Tunes", "CD"),
                ("Cat Power", "Moon Pix", "CD"),
            ]
        )

    def test_unknown_artist_is_true_reject(self, index):
        row = _apa.classify_miss("Zzyzx Qxqxqx", "Xyzzy Plugh", index)
        assert row["phase"] == "fuzzy"
        assert row["call"] == "true_reject"
        assert row["mechanism"] == "no_artist_match"

    def test_exact_pair_pruned_is_format_mismatch(self, index):
        # An exact (artist, title) library pair that still landed in prune was
        # dropped by the format filter — a deliberate edition prune, NOT a
        # false negative. The old uniform geometry mislabeled this a candidate FN.
        row = _apa.classify_miss("Juana Molina", "DOGA", index)
        assert row["phase"] == "exact"
        assert row["call"] == "true_reject"
        assert row["mechanism"] == "format_mismatch"

    def test_exact_artist_title_miss_is_true_reject(self, index):
        # Exact library artist, but the album isn't in the library -> title-only
        # score below review -> correct reject (not an artist-level FN).
        row = _apa.classify_miss("Juana Molina", "Totally Different Nonexistent Record", index)
        assert row["phase"] == "exact"
        assert row["call"] == "true_reject"
        assert row["mechanism"] == "title_miss"

    def test_substring_artist_junk_title_is_substring_variant(self, index):
        # Fuzzy (non-exact) artist that contains a library artist; junk title, so
        # combined stays below review and the substring mechanism fires.
        row = _apa.classify_miss("Stereolab Reissue", "Zzzz Nonexistent Qqqq", index)
        assert row["phase"] == "fuzzy"
        assert row["best_lib_artist"] == "stereolab"
        assert row["call"] == "candidate_false_negative"
        assert row["mechanism"] == "substring_variant"

    def test_fuzzy_near_artist_exact_title_is_candidate(self, index):
        # A misspelled artist ("Molena" for "Molina") with an exact-title match
        # re-scores above keep on the fuzzy path -> genuine candidate FN.
        row = _apa.classify_miss("Juana Molena", "DOGA", index)
        assert row["phase"] == "fuzzy"
        assert row["best_lib_artist"] == "juana molina"
        assert row["call"] == "candidate_false_negative"


class TestDecideTitleOnly:
    def test_above_keep_is_candidate(self):
        assert _apa._decide_title_only(0.80, 0.75, 0.65) == (
            "candidate_false_negative",
            "exact_artist_title_above_keep_on_rescore",
        )

    def test_review_band_is_candidate(self):
        assert _apa._decide_title_only(0.70, 0.75, 0.65) == (
            "candidate_false_negative",
            "exact_artist_rescore_review",
        )

    def test_review_boundary_is_candidate(self):
        assert _apa._decide_title_only(0.65, 0.75, 0.65)[1] == "exact_artist_rescore_review"

    def test_below_review_is_title_miss(self):
        assert _apa._decide_title_only(0.50, 0.75, 0.65) == ("true_reject", "title_miss")


# ---------------------------------------------------------------------------
# score_sample + false_negative_rate
# ---------------------------------------------------------------------------


class TestScoreSampleAndRate:
    @pytest.fixture
    def audit(self, tmp_path):
        d = _make_dump(
            tmp_path,
            prune=[5, 6],
            prune_releases=[
                {"id": 5, "artist": "Zzyzx Qxqxqx", "title": "Xyzzy Plugh", "format": None},
                # Misspelled artist + exact title -> a genuine fuzzy-path FN.
                {"id": 6, "artist": "Juana Molena", "title": "DOGA", "format": "CD"},
            ],
        )
        return _apa.load_audit(d)

    @pytest.fixture
    def index(self):
        return LibraryIndex.from_rows([("Juana Molina", "DOGA", "CD")])

    def test_score_and_rate(self, audit, index):
        sample = _apa.sample_prune_ids(audit.prune, 200, 217)
        rows = _apa.score_sample(sample, audit, index)
        assert {r["id"] for r in rows} == {5, 6}
        summary = _apa.false_negative_rate(rows)
        assert summary["sample_size"] == 2
        # id=6 (fuzzy near-match) is a candidate FN; id=5 (junk) a true reject.
        assert summary["candidate_false_negatives"] == 1
        assert summary["rate"] == pytest.approx(0.5)
        assert summary["wilson_low"] < 0.5 < summary["wilson_high"]


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


class TestWriteSampleCsv:
    def test_writes_header_and_rows(self, tmp_path):
        rows = [
            {
                "id": 5,
                "artist": "Junk",
                "title": "Nowhere",
                "format": None,
                "phase": "fuzzy",
                "call": "true_reject",
                "mechanism": "no_artist_match",
                "artist_score": 0.2,
                "best_lib_artist": "juana molina",
                "title_score": 0.1,
                "combined": 0.14,
            }
        ]
        path = tmp_path / "sample.csv"
        _apa.write_sample_csv(path, rows)
        parsed = list(csv.DictReader(path.open()))
        assert parsed[0]["id"] == "5"
        assert parsed[0]["phase"] == "fuzzy"
        assert parsed[0]["call"] == "true_reject"
        assert parsed[0]["mechanism"] == "no_artist_match"


class TestFormatReport:
    def test_report_contains_key_numbers(self, tmp_path):
        d = _make_dump(tmp_path, keep=[1, 2, 3, 4], prune=[5], override=[4], prune_no_anv=[2, 5])
        audit = _apa.load_audit(d)
        summary = _apa.compute_summary(audit, final_release_count=3)
        report = _apa.format_report(summary, None, _apa.uplift_305_ids(audit))
        assert "non-pinned survivors" in report
        assert "#305 uplift" in report
        assert "cache_metadata" in report  # the comparable-series SQL
        assert "not computed" in report  # FN section without --library-db


class TestWriteDedupNote:
    def test_patches_counts_json(self, tmp_path):
        d = _make_dump(tmp_path, keep=[1], prune=[2])
        _apa.write_dedup_note(d / "counts.json", 1234)
        counts = json.loads((d / "counts.json").read_text())
        assert "1234" in counts["dedup_note"]

    def test_none_delta_does_not_clobber(self, tmp_path):
        d = _make_dump(
            tmp_path,
            keep=[1],
            prune=[2],
            counts={
                "keep": 1,
                "prune": 1,
                "review": 0,
                "override_applied": 0,
                "total_release_rows": 2,
                "dedup_note": "prior value",
            },
        )
        _apa.write_dedup_note(d / "counts.json", None)
        counts = json.loads((d / "counts.json").read_text())
        assert counts["dedup_note"] == "prior value"

    def test_missing_counts_json_is_noop(self, tmp_path):
        # No counts.json present: warn, don't raise.
        _apa.write_dedup_note(tmp_path / "counts.json", 5)
        assert not (tmp_path / "counts.json").exists()


# ---------------------------------------------------------------------------
# End-to-end main()
# ---------------------------------------------------------------------------


class TestMainEndToEnd:
    def _library_db(self, tmp_path: Path) -> Path:
        db = tmp_path / "library.db"
        conn = sqlite3.connect(str(db))
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE library (id INTEGER PRIMARY KEY, artist TEXT, title TEXT, "
            "format TEXT, alternate_artist_name TEXT)"
        )
        cur.executemany(
            "INSERT INTO library (artist, title, format, alternate_artist_name) VALUES (?,?,?,?)",
            [("Juana Molina", "DOGA", "CD", None)],
        )
        conn.commit()
        conn.close()
        return db

    def test_full_run_writes_report_csv_and_dedup_note(self, tmp_path):
        d = _make_dump(
            tmp_path,
            keep=[1, 2, 3, 4],
            prune=[5, 6],
            override=[4],
            prune_no_anv=[2, 5, 6],
            prune_releases=[
                {"id": 5, "artist": "Zzyzx Qxqxqx", "title": "Xyzzy Plugh", "format": None},
                {"id": 6, "artist": "Juana Molina", "title": "DOGA", "format": "CD"},
            ],
        )
        library_db = self._library_db(tmp_path)
        out = tmp_path / "phase2-out"
        rc = _apa.main(
            [
                str(d),
                "--library-db",
                str(library_db),
                "--final-release-count",
                "3",
                "--output-dir",
                str(out),
                "--write-dedup-note",
            ]
        )
        assert rc == 0
        assert (out / "report.md").exists()
        assert (out / "sample.csv").exists()
        # dedup_note patched in the ORIGINAL dump's counts.json
        counts = json.loads((d / "counts.json").read_text())
        assert counts["dedup_note"] is not None
        # report reflects the split
        report = (out / "report.md").read_text()
        assert "non-pinned survivors" in report

    def test_missing_dir_returns_error(self, tmp_path):
        assert _apa.main([str(tmp_path / "nope")]) == 1

    def test_missing_library_db_returns_error_without_creating_file(self, tmp_path):
        d = _make_dump(tmp_path, keep=[1], prune=[2])
        missing = tmp_path / "nope.db"
        assert _apa.main([str(d), "--library-db", str(missing)]) == 1
        # Guarded before sqlite3.connect, so no empty db file is left behind.
        assert not missing.exists()
