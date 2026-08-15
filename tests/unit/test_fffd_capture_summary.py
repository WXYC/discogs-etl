"""Unit tests for ``scripts/fffd_capture_summary.py`` (discogs-etl#382).

The U+FFFD pair capture runs as an opt-in step of the catalog-parity soak
(``.github/workflows/catalog-parity.yml``), which means two different verdicts
land in the same run summary, and **both of them use exit 4**:

    catalog_parity_diff.py --fail-on-drift   4 -> the catalogs disagree
    catalog_parity_diff.py --capture-...     4 -> rows were left UNRESOLVED

Those call for opposite responses. A drift 4 is the expected daily state and
wants no action; a capture 4 means the repair set handed to
[BS#2152](https://github.com/WXYC/Backend-Service/issues/2152) is short some
rows, and which *reason* they failed on decides what to do next -- a
``corrupt_candidates`` row means tubafrenzy lost the bytes too and no re-import
can recover it, while ``multiple_candidates`` just needs a human to pick. A
bare red X, or a summary that reuses the drift wording, loses that distinction
at exactly the moment someone is deciding whether the capture is usable.

The report is untrusted for the same reason ``parity_run_summary.py``'s is: on
exits 2 and 3 the harness writes nothing, so the workflow's redirect leaves a
zero-byte file, and a crash mid-write leaves a truncated one. A summary step
that tracebacks turns one legible failure into two.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "fffd_capture_summary.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fffd_capture_summary", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # See test_parity_run_summary.py: a module defining a dataclass under
    # `from __future__ import annotations` needs its sys.modules entry before
    # exec_module, or the field-type resolution dies on a bare AttributeError.
    sys.modules["fffd_capture_summary"] = mod
    spec.loader.exec_module(mod)
    return mod


fcs = _load_module()


# Shaped after the real BS#2152 rows: the corruption is latin1 bytes decoded as
# UTF-8, so each undecodable byte became one U+FFFD and only tubafrenzy still
# holds the original.
RESOLVED_ROW = {
    "legacy_release_id": 41827,
    "track_position": 3,
    "current_artist_name": "Csillagrabl�k",
    "current_track_title": "Ez a nap",
    "true_artist_name": "Csillagrablók",
    "true_track_title": "Ez a nap",
    "true_artist_name_codepoints": [{"index": 11, "char": "ó", "codepoint": "U+00F3"}],
    "true_track_title_codepoints": [],
}


def _payload(resolved=1, unresolved=()):
    return {
        "resolved": [RESOLVED_ROW] * resolved,
        "unresolved": [
            {
                "legacy_release_id": 50000 + i,
                "track_position": 1,
                "current_artist_name": "Nil�fer Yanya",
                "current_track_title": "Stabilise",
                "reason": reason,
                "candidates": [],
            }
            for i, reason in enumerate(unresolved)
        ],
        "sql_values": "(41827, 3, 'Csillagrablók', 'Ez a nap')",
    }


def _write(tmp_path: Path, payload) -> str:
    path = tmp_path / "fffd-pairs.json"
    path.write_text(json.dumps(payload))
    return str(path)


class TestExitTaxonomy:
    def test_exit_zero_reads_as_a_complete_capture(self) -> None:
        out = fcs.render(0, _payload(resolved=14), None)
        assert "complete" in out.lower()
        assert "| resolved | 14 |" in out
        assert "| unresolved | 0 |" in out

    def test_exit_four_is_an_incomplete_capture_not_drift(self) -> None:
        """The distinction this renderer exists for. Someone reading both
        blocks in one run summary has to be able to tell, from the heading
        alone, which verdict they are looking at -- and the body has to say
        what *this* 4 means rather than leaving it to be inferred."""
        out = fcs.render(4, _payload(resolved=11, unresolved=("zero_candidates",)), None)
        heading = out.splitlines()[0].lower()
        assert "capture" in heading
        assert "parity" not in heading
        assert "unresolved" in out.lower()
        assert "exit 4" in out.lower()

    def test_exit_three_says_no_capture_was_taken(self) -> None:
        out = fcs.render(3, None, "`fffd-pairs.json` is empty")
        assert "no capture" in out.lower() or "no rows were captured" in out.lower()

    def test_exit_two_points_at_the_workflow_not_the_data(self) -> None:
        out = fcs.render(2, None, "`fffd-pairs.json` was never written")
        assert "catalog-parity.yml" in out

    def test_unknown_exit_code_is_not_silently_rendered_as_success(self) -> None:
        out = fcs.render(137, None, "`fffd-pairs.json` was never written")
        assert "137" in out
        assert "unexpected" in out.lower()

    @pytest.mark.parametrize("code", [0, 2, 3, 4, 137])
    def test_every_code_renders_a_heading_and_a_lead(self, code) -> None:
        out = fcs.render(code, None, "no report path was given")
        assert out.startswith("## ")
        assert len(out.strip().splitlines()) >= 3


class TestCaptureNeverRan:
    """A capture that was gated off is not a capture that failed.

    The step is skipped when the parity harness exits 2 or 3, which leaves
    ``CAPTURE_EXIT_CODE`` unset. Defaulting that to 3 would render the
    producer-failure block -- naming causes that did not occur (a Backend
    sign-in, a destination path) and telling the reader to recover the capture
    from stdout, where nothing was ever written. That is the same conflation
    this renderer exists to prevent, one level down.
    """

    def test_not_run_is_its_own_outcome(self) -> None:
        out = fcs.render(fcs.NOT_RUN, None, "no report path was given")
        assert "not run" in out.lower() or "did not run" in out.lower()

    def test_not_run_does_not_send_the_reader_hunting_stdout(self) -> None:
        out = fcs.render(fcs.NOT_RUN, None, "no report path was given")
        assert "stdout" not in out.lower()

    def test_not_run_says_why_it_was_skipped(self) -> None:
        """The reason is always the same one, and it is already on screen in
        the parity block: the harness did not complete, so there was no
        library.db to pair against."""
        out = fcs.render(fcs.NOT_RUN, None, "no report path was given").lower()
        assert "parity" in out

    def test_not_run_does_not_complain_about_a_report_it_never_expected(self) -> None:
        """The other no-payload outcomes explain *why* the report is missing,
        because for them it should have been there. Here nothing was ever
        going to write one, so the line is noise on top of a lead that
        already says so."""
        out = fcs.render(fcs.NOT_RUN, None, "no report path was given")
        assert "No report to summarise" not in out

    def test_not_run_does_not_claim_an_artifact_that_does_not_exist(self) -> None:
        out = fcs.render(fcs.NOT_RUN, None, "no report path was given")
        assert "attached to this run as an artifact" not in out

    def test_not_run_is_a_notice_not_an_error(self) -> None:
        """The parity verdict step already fails the job for the exit 2/3
        that caused this. A second error annotation for one cause reads as
        two independent failures."""
        assert fcs.annotation(fcs.NOT_RUN, None).startswith("::notice")

    def test_not_run_sentinel_cannot_collide_with_a_real_exit_status(self) -> None:
        assert fcs.NOT_RUN not in range(256)

    def test_cli_not_run_flag_exits_zero(self, capsys) -> None:
        assert fcs.main(["--not-run"]) == 0
        assert capsys.readouterr().out.startswith("## ")

    def test_cli_rejects_both_flags_at_once(self) -> None:
        with pytest.raises(SystemExit):
            fcs.main(["--not-run", "--exit-code", "4"])

    def test_cli_requires_one_of_them(self) -> None:
        with pytest.raises(SystemExit):
            fcs.main([])


class TestCounts:
    def test_resolved_and_unresolved_counts_are_reported(self) -> None:
        out = fcs.render(4, _payload(resolved=11, unresolved=("zero_candidates",) * 3), None)
        assert "11" in out
        assert "3" in out

    def test_unresolved_rows_are_broken_down_by_reason(self) -> None:
        """Each reason has a different next action, so the aggregate count is
        not actionable on its own."""
        payload = _payload(
            resolved=1,
            unresolved=("zero_candidates", "multiple_candidates", "corrupt_candidates"),
        )
        out = fcs.render(4, payload, None)
        for reason in ("zero_candidates", "multiple_candidates", "corrupt_candidates"):
            assert reason in out

    def test_corrupt_candidates_is_called_out_as_unrecoverable(self) -> None:
        """The one reason no re-import can fix: MySQL carries U+FFFD too, so
        the original bytes are gone on both sides. Filing it as 'needs a
        closer look' would send someone hunting truth that does not exist."""
        payload = _payload(resolved=0, unresolved=("corrupt_candidates",))
        out = fcs.render(4, payload, None)
        lowered = out.lower()
        assert "tubafrenzy" in lowered
        assert "unrecoverable" in lowered or "cannot be recovered" in lowered

    def test_a_clean_capture_advertises_the_sql_it_produced(self) -> None:
        """``sql_values`` is the deliverable -- the block that gets pasted
        into Backend-Service's repair script."""
        out = fcs.render(0, _payload(resolved=14), None)
        assert "sql_values" in out or "SQL" in out

    def test_reason_counts_survive_an_unknown_reason(self) -> None:
        """A reason added to lib/fffd_pair_capture.py without touching this
        renderer must still be counted, not dropped."""
        payload = _payload(resolved=0, unresolved=("some_future_reason",))
        out = fcs.render(4, payload, None)
        assert "some_future_reason" in out


class TestUntrustedReport:
    def test_missing_file_is_a_problem_string_not_an_exception(self, tmp_path) -> None:
        payload, problem = fcs.load_payload(str(tmp_path / "absent.json"))
        assert payload is None
        assert problem and "never written" in problem

    def test_empty_file_is_reported_as_no_capture(self, tmp_path) -> None:
        path = tmp_path / "fffd-pairs.json"
        path.write_text("")
        payload, problem = fcs.load_payload(str(path))
        assert payload is None
        assert problem and "empty" in problem

    def test_truncated_json_does_not_traceback(self, tmp_path) -> None:
        path = tmp_path / "fffd-pairs.json"
        path.write_text('{"resolved": [{"legacy_rele')
        payload, problem = fcs.load_payload(str(path))
        assert payload is None
        assert problem

    def test_a_json_list_is_not_a_capture_report(self, tmp_path) -> None:
        path = tmp_path / "fffd-pairs.json"
        path.write_text("[]")
        payload, problem = fcs.load_payload(str(path))
        assert payload is None
        assert problem

    @pytest.mark.parametrize("junk", [None, "twelve", {"nested": 1}, 3.5])
    def test_non_list_row_collections_do_not_take_the_step_down(self, junk) -> None:
        out = fcs.render(4, {"resolved": junk, "unresolved": junk}, None)
        assert out.startswith("## ")

    def test_rows_that_are_not_objects_are_skipped(self) -> None:
        out = fcs.render(4, {"resolved": [], "unresolved": ["not a row", 7, None]}, None)
        assert out.startswith("## ")

    def test_a_verdict_code_with_no_report_says_so(self) -> None:
        """Exit 0/4 promised a capture; if the file cannot be read that is
        worth naming rather than rendering empty tables."""
        out = fcs.render(4, None, "`fffd-pairs.json` could not be parsed as JSON")
        assert "could not be parsed" in out


class TestAnnotation:
    @pytest.mark.parametrize("code", [0, 2, 3, 4, 137])
    def test_annotation_is_exactly_one_line(self, code) -> None:
        """GitHub truncates an annotation at its first newline."""
        line = fcs.annotation(code, _payload(resolved=1, unresolved=("zero_candidates",)))
        assert "\n" not in line
        assert line.startswith("::")

    def test_a_complete_capture_annotates_as_a_notice(self) -> None:
        assert fcs.annotation(0, _payload(resolved=14)).startswith("::notice")

    def test_an_incomplete_capture_annotates_as_an_error(self) -> None:
        payload = _payload(resolved=11, unresolved=("zero_candidates",) * 3)
        assert fcs.annotation(4, payload).startswith("::error")

    def test_the_incomplete_annotation_carries_the_reason_breakdown(self) -> None:
        """Which reasons fired is the one detail worth seeing without opening
        the run."""
        payload = _payload(resolved=11, unresolved=("corrupt_candidates",) * 2)
        assert "corrupt_candidates" in fcs.annotation(4, payload)

    def test_annotation_carries_no_markdown(self) -> None:
        payload = _payload(resolved=1, unresolved=("multiple_candidates",))
        line = fcs.annotation(4, payload)
        assert "`" not in line
        assert "**" not in line


class TestCli:
    def test_main_re_exits_with_the_capture_code(self, tmp_path, capsys) -> None:
        path = _write(tmp_path, _payload(resolved=11, unresolved=("zero_candidates",)))
        assert fcs.main(["--exit-code", "4", "--json", path]) == 4
        captured = capsys.readouterr()
        assert captured.out.startswith("## ")
        assert captured.err.startswith("::error")

    def test_main_tolerates_a_missing_report(self, tmp_path, capsys) -> None:
        assert fcs.main(["--exit-code", "3", "--json", str(tmp_path / "gone.json")]) == 3
        assert capsys.readouterr().out.startswith("## ")

    def test_json_is_optional(self, capsys) -> None:
        assert fcs.main(["--exit-code", "3"]) == 3
        assert capsys.readouterr().out.startswith("## ")
