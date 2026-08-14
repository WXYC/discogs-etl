"""Unit tests for ``scripts/parity_run_summary.py`` (discogs-etl#378).

The scheduled soak (``.github/workflows/catalog-parity.yml``) is the first
consumer of ``catalog_parity_diff.py --fail-on-drift``'s exit 4. The whole
point of that exit code is that it means something *different* from the other
non-zero exits, and the difference is not self-evident to whoever opens a red
run:

    0  the catalogs agree                     -- a clean day, countable toward
                                                 wiki#89 AC#4's seven-day streak
    4  the harness ran correctly, the data     -- a VERDICT. Expected every day
       does not agree                            until BS#2108 and the 28
                                                 residual field mismatches clear
    3  a producer could not build its input    -- infrastructure: the Kattare
                                                 tunnel, MySQL, or Backend auth
    2  bad arguments                          -- a defect in the workflow file
                                                 itself, not in the data
    1  (or anything else) an uncaught crash

Read the wrong way round, exit 4 reads as "the soak is broken" (so it gets
muted) and exit 3 reads as "parity failed" (so someone hunts a data problem
that isn't there). Neither mistake is recoverable from the run's exit status
alone, which is why this renderer exists rather than a line of inline bash.

The renderer must also survive a degraded ``--json`` file: on exits 2 and 3
the harness writes nothing to stdout, so the redirect leaves a zero-byte file
behind, and a crash mid-write leaves a truncated one. A summary step that
tracebacks on that turns an already-failing run into two mysteries.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "parity_run_summary.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("parity_run_summary", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


prs = _load_module()


# Shaped after the real 2026-08-13 prod run (#346 step 8a, post-widening):
# every expected-residue counter populated, `clean: false` driven by
# extra_unexplained (BS#2108's pending delete) plus 28 field mismatches.
_PROD_SHAPED: dict = {
    "matched": 64122,
    "missing_in_backend": 609,
    "extra_in_backend": 191,
    "field_mismatches": {
        "title": 3,
        "artist": 10,
        "format": 7,
        "alternate_artist_name": 8,
        "label": 0,
        "call_letters": 0,
        "artist_call_number": 0,
        "cross_reference_names": 0,
    },
    "cta_missing": 2701,
    "cta_extra": 6862,
    "clean": False,
    "missing_unexplained": 0,
    "missing_expected": 609,
    "extra_unexplained": 115,
    "extra_expected": 76,
    "normalizations": {
        "call_letters": {"fold_collapsed": 33},
        "artist_call_number": {"fold_collapsed": 59},
        "cross_reference_names": {"cardinality_gain": 11},
    },
    "missing_in_backend_ids": [],
    "extra_in_backend_ids": [],
}


def _payload(**overrides) -> dict:
    """A prod-shaped payload with the given top-level fields replaced."""
    return {**_PROD_SHAPED, **overrides}


def _clean_payload() -> dict:
    return _payload(
        clean=True,
        extra_unexplained=0,
        extra_in_backend=76,
        field_mismatches=dict.fromkeys(_PROD_SHAPED["field_mismatches"], 0),
    )


def _write(tmp_path: Path, payload: object, name: str = "parity.json") -> Path:
    path = tmp_path / name
    path.write_text(payload if isinstance(payload, str) else json.dumps(payload))
    return path


def _run(capsys, exit_code: int, json_path: Path | None) -> tuple[int, str, str]:
    argv = ["--exit-code", str(exit_code)]
    if json_path is not None:
        argv += ["--json", str(json_path)]
    rc = prs.main(argv)
    captured = capsys.readouterr()
    return rc, captured.out, captured.err


class TestExitCodeTaxonomy:
    """Each exit code must be *named* in the summary as the kind of thing it
    is. A reader of a red run should never have to look up what 4 means."""

    def test_clean_run_is_reported_as_a_clean_day(self, tmp_path, capsys) -> None:
        rc, out, _ = _run(capsys, 0, _write(tmp_path, _clean_payload()))
        assert rc == 0
        assert "clean" in out.lower()
        # A clean run is a countable day toward the seven-day streak; say so.
        assert "wiki#89" in out or "streak" in out.lower()

    def test_drift_is_labelled_a_verdict_not_a_broken_run(self, tmp_path, capsys) -> None:
        rc, out, _ = _run(capsys, 4, _write(tmp_path, _payload()))
        assert rc == 4
        lowered = out.lower()
        assert "verdict" in lowered
        # The distinguishing claim: the harness itself worked.
        assert "harness ran" in lowered or "ran correctly" in lowered

    def test_source_error_is_labelled_infrastructure_not_parity(self, tmp_path, capsys) -> None:
        rc, out, _ = _run(capsys, 3, _write(tmp_path, "", name="empty.json"))
        assert rc == 3
        lowered = out.lower()
        # Must NOT claim a parity result -- no verdict was computed.
        assert "no verdict" in lowered
        assert "tunnel" in lowered or "producer" in lowered

    def test_bad_args_is_labelled_a_workflow_defect(self, tmp_path, capsys) -> None:
        rc, out, _ = _run(capsys, 2, _write(tmp_path, "", name="empty.json"))
        assert rc == 2
        lowered = out.lower()
        assert "workflow" in lowered
        assert "no verdict" in lowered

    @pytest.mark.parametrize("code", [1, 139])
    def test_unexpected_exit_is_labelled_unexpected(self, tmp_path, capsys, code) -> None:
        rc, out, _ = _run(capsys, code, _write(tmp_path, "", name="empty.json"))
        assert rc == code
        lowered = out.lower()
        assert "unexpected" in lowered
        assert "no verdict" in lowered

    @pytest.mark.parametrize("code", [0, 2, 3, 4, 1, 139])
    def test_exit_code_is_propagated_verbatim(self, tmp_path, capsys, code) -> None:
        """The job's failing step should carry the harness's own number, so
        "Process completed with exit code 4" appears in the log."""
        payload = _clean_payload() if code == 0 else _payload()
        rc, _, _ = _run(capsys, code, _write(tmp_path, payload))
        assert rc == code

    def test_every_taxonomy_entry_names_its_exit_code(self, tmp_path, capsys) -> None:
        for code in (0, 2, 3, 4):
            payload = _clean_payload() if code == 0 else _payload()
            _, out, _ = _run(capsys, code, _write(tmp_path, payload))
            assert f"exit {code}" in out.lower(), f"exit {code} unnamed in its own summary"


class TestDriftAttribution:
    """`clean` is a conjunction of four conditions. A red run must say which
    of them failed, or the operator re-derives it by hand from the JSON every
    single day."""

    def test_unexplained_extra_is_named_with_its_count(self, tmp_path, capsys) -> None:
        _, out, _ = _run(capsys, 4, _write(tmp_path, _payload()))
        assert "extra_unexplained" in out
        assert "115" in out

    def test_unexplained_missing_is_named_with_its_count(self, tmp_path, capsys) -> None:
        _, out, _ = _run(capsys, 4, _write(tmp_path, _payload(missing_unexplained=7)))
        assert "missing_unexplained" in out
        assert "7" in out

    def test_expected_residue_is_not_named_as_a_cause(self, tmp_path, capsys) -> None:
        """609 expected-missing rows are the frozen ledger doing its job. If
        they show up in the cause list, the list is noise."""
        _, out, _ = _run(capsys, 4, _write(tmp_path, _payload()))
        causes = prs.verdict_causes(_payload())
        assert not any("missing_unexplained" in c for c in causes)
        assert "609" not in "".join(causes)
        assert out  # rendered fine regardless

    def test_field_mismatch_columns_are_named_individually(self, tmp_path, capsys) -> None:
        _, out, _ = _run(capsys, 4, _write(tmp_path, _payload()))
        for col in ("title", "artist", "format", "alternate_artist_name"):
            assert col in out, f"{col} carries a mismatch but is not named"

    def test_zero_count_columns_are_not_named_as_causes(self) -> None:
        causes = "".join(prs.verdict_causes(_payload()))
        assert "label" not in causes
        assert "call_letters" not in causes

    def test_cta_baseline_is_named_when_nothing_else_explains_the_verdict(self) -> None:
        """`cta_within_baseline` is not in the JSON -- only the raw counts
        are. So when every other condition is satisfied and `clean` is still
        false, the CTA baseline is the remaining explanation, by elimination.
        Saying nothing there would leave a red run with no stated cause."""
        payload = _payload(
            clean=False,
            missing_unexplained=0,
            extra_unexplained=0,
            field_mismatches=dict.fromkeys(_PROD_SHAPED["field_mismatches"], 0),
        )
        causes = "".join(prs.verdict_causes(payload)).lower()
        assert "cta" in causes
        assert "baseline" in causes

    def test_a_clean_payload_has_no_causes(self) -> None:
        assert prs.verdict_causes(_clean_payload()) == []


class TestDegradedJson:
    """Exits 2 and 3 leave a zero-byte file behind (the harness writes its
    JSON to stdout, and the redirect creates the file before it runs); a
    crash can leave a truncated one. Neither may traceback."""

    def test_missing_file_still_renders(self, tmp_path, capsys) -> None:
        rc, out, _ = _run(capsys, 3, tmp_path / "never-written.json")
        assert rc == 3
        assert "no verdict" in out.lower()

    def test_no_json_argument_at_all_still_renders(self, capsys) -> None:
        rc, out, _ = _run(capsys, 3, None)
        assert rc == 3
        assert out.strip()

    def test_empty_file_still_renders(self, tmp_path, capsys) -> None:
        rc, out, _ = _run(capsys, 3, _write(tmp_path, ""))
        assert rc == 3
        assert "no verdict" in out.lower()

    def test_truncated_json_does_not_crash(self, tmp_path, capsys) -> None:
        rc, out, _ = _run(capsys, 4, _write(tmp_path, '{"matched": 64122, "field_mis'))
        assert rc == 4
        lowered = out.lower()
        assert "could not" in lowered or "unreadable" in lowered

    def test_json_of_the_wrong_shape_does_not_crash(self, tmp_path, capsys) -> None:
        """A JSON array, or an object missing the keys the renderer reads,
        must degrade to the same 'unreadable' path rather than KeyError."""
        rc, out, _ = _run(capsys, 4, _write(tmp_path, [1, 2, 3]))
        assert rc == 4
        assert out.strip()

    def test_missing_keys_render_without_keyerror(self, tmp_path, capsys) -> None:
        rc, out, _ = _run(capsys, 4, _write(tmp_path, {"matched": 5}))
        assert rc == 4
        assert out.strip()

    def test_null_verdict_is_reported_as_unavailable(self, tmp_path, capsys) -> None:
        """`clean: null` means the run had no residue ledger. --fail-on-drift
        refuses that combination (exit 2), so it should be unreachable here --
        but if it ever lands, 'null' must not render as 'clean'."""
        _, out, _ = _run(capsys, 4, _write(tmp_path, _payload(clean=None)))
        lowered = out.lower()
        assert "unavailable" in lowered or "no ledger" in lowered


class TestAnnotations:
    """GitHub surfaces ``::error::`` annotations on the run's summary page
    above the fold. The drift annotation and the infrastructure annotation
    must not read alike there."""

    def test_annotations_go_to_stderr_not_into_the_summary(self, tmp_path, capsys) -> None:
        _, out, err = _run(capsys, 4, _write(tmp_path, _payload()))
        assert "::" in err
        assert "::error" not in out

    def test_drift_annotation_is_distinct_from_infrastructure(self, tmp_path, capsys) -> None:
        _, _, drift_err = _run(capsys, 4, _write(tmp_path, _payload()))
        _, _, infra_err = _run(capsys, 3, _write(tmp_path, "", name="empty.json"))
        assert drift_err.startswith("::error")
        assert infra_err.startswith("::error")
        assert drift_err != infra_err
        assert "drift" in drift_err.lower()
        assert "drift" not in infra_err.lower()

    def test_clean_run_annotates_as_notice_not_error(self, tmp_path, capsys) -> None:
        _, _, err = _run(capsys, 0, _write(tmp_path, _clean_payload()))
        assert err.startswith("::notice")

    def test_annotation_is_a_single_line(self, tmp_path, capsys) -> None:
        """A multi-line annotation is truncated by GitHub at the first
        newline, silently dropping whatever followed."""
        for code in (0, 2, 3, 4, 139):
            payload = _clean_payload() if code == 0 else _payload()
            _, _, err = _run(capsys, code, _write(tmp_path, payload))
            assert len(err.strip().splitlines()) == 1, f"exit {code} annotation spans lines"


class TestMarkdownShape:
    def test_summary_is_markdown_with_a_heading(self, tmp_path, capsys) -> None:
        _, out, _ = _run(capsys, 4, _write(tmp_path, _payload()))
        assert out.lstrip().startswith("#")

    def test_row_set_counts_are_reported(self, tmp_path, capsys) -> None:
        _, out, _ = _run(capsys, 4, _write(tmp_path, _payload()))
        for value in ("64122", "609", "191"):
            assert value in out

    def test_normalizations_are_reported_but_not_as_causes(self, tmp_path, capsys) -> None:
        """Field tiering is reported, never gating (#370). A spike in a
        normalization class should be *visible* without being a failure."""
        _, out, _ = _run(capsys, 4, _write(tmp_path, _payload()))
        assert "fold_collapsed" in out
        assert "fold_collapsed" not in "".join(prs.verdict_causes(_payload()))


class TestClaimsTheRendererCannotSupport:
    """The renderer exists to stop people misreading a run. Its own text is
    held to the same standard: a confident sentence pointing at the wrong
    cause is worse than the bare exit code it replaced."""

    def test_cta_elimination_does_not_assert_an_exceeded_baseline(self) -> None:
        """``cta_within_baseline`` is false when a baseline is *absent* as
        well as when it is exceeded -- ``diff_library_dbs`` requires
        ``ledger.cta_missing_baseline is not None and ...``, and
        ``load_residue_ledger`` accepts null baselines (the ledger's own
        shape before the 2026-08-13 CTA measurement). The report carries the
        raw counts but not the baselines, so the renderer genuinely cannot
        tell the two apart -- and must not pick one. A re-vendored ledger
        with a null baselines block would otherwise send an operator hunting
        CTA drift that is not there."""
        payload = _payload(
            clean=False,
            missing_unexplained=0,
            extra_unexplained=0,
            field_mismatches=dict.fromkeys(_PROD_SHAPED["field_mismatches"], 0),
        )
        cause = "".join(prs.verdict_causes(payload)).lower()
        assert "cta" in cause
        # Both readings must be on the table, and the operator told where to look.
        assert "not populated" in cause or "absent" in cause or "null" in cause
        assert "ledger.json" in cause or "baselines" in cause

    def test_producer_failure_names_the_client_version_trap(self, tmp_path, capsys) -> None:
        """A non-MariaDB client dying on a signal surfaces as exit 3, not
        139: ``_default_mysql_runner`` returns ``returncode == 0``, so the
        signal death becomes False -> SourceError -> 3. The repo's most-cited
        producer failure therefore belongs in the exit-3 text."""
        _, out, _ = _run(capsys, 3, _write(tmp_path, "", name="empty.json"))
        assert "mariadb" in out.lower()

    def test_the_catch_all_does_not_blame_the_mysql_client(self, tmp_path, capsys) -> None:
        """139 cannot reach this branch -- Python does not re-raise a child's
        signal as its own exit status. Claiming it does sends the reader
        looking for a client-version problem in the one case where the
        harness itself misbehaved."""
        _, out, _ = _run(capsys, 139, _write(tmp_path, "", name="empty.json"))
        lowered = out.lower()
        assert "mariadb" not in lowered
        assert "segfault" not in lowered
