"""Unit tests for ``lib/run_summary.py`` -- the plumbing the run-summary
renderers share (discogs-etl#384).

``parity_run_summary.py`` (#378) and ``fffd_capture_summary.py`` (#382) render
two deliberately separate verdicts, and that separation is worth keeping: the
soak's exit 4 and the capture's exit 4 mean opposite things. But the *reading*
of the report is not what differs between them. Both are handed a ``--json``
path written by a shell redirect that creates the file before the producer
runs, so both face the same degraded inputs -- zero-byte on exits 2 and 3,
truncated on a crash mid-write -- and both run in the step whose job is to
explain other failures, so neither may traceback.

``sam_deploy_summary.py`` (#396) is the third, and the one that split the
reader in two: its producer is ``sam deploy`` and its output is a captured
console log, so it binds :func:`load_text` where the other two bind
:func:`load_payload`. The degraded population is identical, which is why the
JSON path now sits on top of the text path rather than beside it.

Kept in separate copies, a hardening fix to one would silently not reach the
others. These tests pin the shared loader's contract and that the renderers
actually route through it.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from lib.run_summary import load_payload, load_text, plain  # noqa: E402


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


class TestLoadPayload:
    def test_a_valid_object_comes_back_with_no_problem(self, tmp_path) -> None:
        path = tmp_path / "report.json"
        path.write_text('{"clean": true}')
        payload, problem = load_payload(str(path), noun="parity report")
        assert payload == {"clean": True}
        assert problem is None

    def test_no_path_is_a_problem_not_an_exception(self) -> None:
        payload, problem = load_payload(None, noun="parity report")
        assert payload is None
        assert problem == "no parity report path was given"

    def test_absent_file(self, tmp_path) -> None:
        payload, problem = load_payload(str(tmp_path / "gone.json"), noun="capture report")
        assert payload is None
        assert problem and "never written" in problem

    def test_zero_byte_file_names_the_producer_noun(self, tmp_path) -> None:
        """The redirect creates the file before the producer runs, so this is
        the shape of every exit-2 and exit-3 run."""
        path = tmp_path / "report.json"
        path.write_text("")
        _, problem = load_payload(str(path), noun="capture report")
        assert problem and "empty" in problem and "capture report" in problem

    def test_whitespace_only_file_counts_as_empty(self, tmp_path) -> None:
        path = tmp_path / "report.json"
        path.write_text("   \n\t\n")
        _, problem = load_payload(str(path), noun="parity report")
        assert problem and "empty" in problem

    def test_truncated_json_does_not_traceback(self, tmp_path) -> None:
        path = tmp_path / "report.json"
        path.write_text('{"resolved": [{"legacy_rele')
        payload, problem = load_payload(str(path), noun="capture report")
        assert payload is None
        assert problem and "could not be parsed" in problem

    @pytest.mark.parametrize("raw", ["[]", '"a string"', "7", "null", "true"])
    def test_valid_json_that_is_not_an_object_is_rejected(self, tmp_path, raw) -> None:
        path = tmp_path / "report.json"
        path.write_text(raw)
        payload, problem = load_payload(str(path), noun="parity report")
        assert payload is None
        assert problem and "parity report" in problem

    def test_undecodable_bytes_do_not_traceback(self, tmp_path) -> None:
        """A report truncated mid-UTF-8-sequence is exactly the kind of file
        this population produces -- the capture's own subject matter is
        mis-decoded bytes."""
        path = tmp_path / "report.json"
        path.write_bytes(b'{"true_artist_name": "Csillagrabl\xc3')
        payload, problem = load_payload(str(path), noun="capture report")
        assert payload is None
        assert problem


class TestLoadText:
    """``load_payload``'s reader, reachable on its own for the producer whose
    output is a console log rather than JSON (#396's ``sam deploy`` capture)."""

    def test_contents_come_back_stripped(self, tmp_path) -> None:
        path = tmp_path / "sam-deploy.log"
        path.write_text("\nNo changes to deploy. Stack wxyc-discogs-rebuild is up to date\n\n")
        text, problem = load_text(str(path), noun="deploy log")
        assert text == "No changes to deploy. Stack wxyc-discogs-rebuild is up to date"
        assert problem is None

    def test_no_path_names_the_noun(self, tmp_path) -> None:
        _, problem = load_text(None, noun="deploy log")
        assert problem == "no deploy log path was given"

    @pytest.mark.parametrize(
        ("write", "expected"),
        [
            (None, "never written"),
            ("", "empty"),
            ("   \n\t\n", "empty"),
        ],
    )
    def test_degraded_files_are_problems_not_exceptions(self, tmp_path, write, expected) -> None:
        path = tmp_path / "sam-deploy.log"
        if write is not None:
            path.write_text(write)
        text, problem = load_text(str(path), noun="deploy log")
        assert text is None
        assert problem and expected in problem

    def test_undecodable_bytes_do_not_traceback(self, tmp_path) -> None:
        path = tmp_path / "sam-deploy.log"
        path.write_bytes(b"Successfully created/updated stack - Csillagrabl\xc3")
        text, problem = load_text(str(path), noun="deploy log")
        assert text is None
        assert problem and "UTF-8" in problem

    def test_load_payload_reports_the_same_problems(self, tmp_path) -> None:
        """The refactor's contract: JSON parsing sits on top of this, it does
        not re-implement the read."""
        absent = str(tmp_path / "gone.json")
        assert (
            load_payload(absent, noun="parity report")[1]
            == (load_text(absent, noun="parity report")[1])
        )


class TestPlain:
    def test_strips_backticks_and_bold(self) -> None:
        assert plain("**bold** and `code`") == "bold and code"

    def test_leaves_ordinary_text_alone(self) -> None:
        assert plain("Csillagrablók, Bête, µ-Ziq") == "Csillagrablók, Bête, µ-Ziq"


class TestEveryRendererUsesIt:
    """The point of the module. A renderer that kept its own copy would drift
    the moment any of them was hardened.

    Asserted behaviourally rather than by identity: each binds the loader to
    its own noun, so ``is`` comparisons would pin the binding mechanism instead
    of the thing that matters. A forked copy that gained its own handling shows
    up here as a divergent result.

    ``sam_deploy_summary`` binds :func:`load_text` where the other two bind
    :func:`load_payload`, so the sweep is keyed by which reader each exposes --
    the alternative, a private two-test class in that module's own file, is how
    the third renderer would end up untested by the file whose stated job is to
    test all of them.
    """

    # module name -> the loader attribute it exposes
    RENDERERS = {
        "parity_run_summary": "load_payload",
        "fffd_capture_summary": "load_payload",
        "sam_deploy_summary": "load_log",
    }

    def _degraded(self, tmp_path: Path) -> dict[str, str]:
        """Paths every reader must reject, keyed by how they are degraded.

        ``truncated`` and ``not_object`` are degraded *as JSON only* -- both are
        perfectly good text -- so the text binder is not asked about them.
        """
        empty = tmp_path / "empty.json"
        empty.write_text("")
        truncated = tmp_path / "truncated.json"
        truncated.write_text('{"a": [')
        not_object = tmp_path / "list.json"
        not_object.write_text("[]")
        undecodable = tmp_path / "undecodable.json"
        undecodable.write_bytes(b'{"x": "Csillagrabl\xc3')
        return {
            "absent": str(tmp_path / "gone.json"),
            "empty": str(empty),
            "truncated": str(truncated),
            "not_object": str(not_object),
            "undecodable": str(undecodable),
        }

    # Degraded as text, not merely as JSON -- the subset every reader owns.
    TEXT_DEGRADED = ("absent", "empty", "undecodable")

    def _reader(self, name: str):
        mod = _load(name)
        return getattr(mod, self.RENDERERS[name])

    def _applicable(self, name: str) -> tuple[str, ...]:
        json_binder = self.RENDERERS[name] == "load_payload"
        return tuple(self._degraded_keys) if json_binder else self.TEXT_DEGRADED

    _degraded_keys = ("absent", "empty", "truncated", "not_object", "undecodable")

    @pytest.mark.parametrize("name", RENDERERS)
    def test_no_renderer_tracebacks_on_any_degraded_report(self, tmp_path, name) -> None:
        read = self._reader(name)
        paths = self._degraded(tmp_path)
        for key in self._applicable(name):
            value, problem = read(paths[key])
            assert value is None, f"{name} accepted a {key} report"
            assert problem, f"{name} gave no problem string for a {key} report"

    def test_the_noun_free_problem_strings_are_identical(self, tmp_path) -> None:
        """An absent or undecodable file reads the same whichever producer
        wrote it -- so these messages must not have been forked."""
        paths = self._degraded(tmp_path)
        readers = [self._reader(name) for name in self.RENDERERS]
        for key in ("absent", "undecodable"):
            problems = {read(paths[key])[1] for read in readers}
            assert len(problems) == 1, f"renderers disagree on a {key} report: {problems}"

    def test_each_renderer_names_its_own_producer_where_it_matters(self, tmp_path) -> None:
        """The one place they *should* differ: an empty file says which kind
        of output was never produced."""
        empty = self._degraded(tmp_path)["empty"]
        expected = {
            "parity_run_summary": "parity report",
            "fffd_capture_summary": "capture report",
            "sam_deploy_summary": "deploy log",
        }
        for name, noun in expected.items():
            assert noun in self._reader(name)(empty)[1]

    @pytest.mark.parametrize("name", RENDERERS)
    def test_renderer_annotations_are_plain_text(self, name) -> None:
        """Behaviourally, not ``mod._plain is plain``.

        The identity form is what let the invariant rot: it asserted that the
        symbol had been *imported*, which stayed true while
        ``parity_run_summary`` formatted its annotation without calling it.
        Every stored body is checked through the module's own ``annotation``.
        """
        mod = _load(name)
        for record in mod._OUTCOMES.values():
            assert "`" not in plain(record.annotation_body)
            assert "**" not in plain(record.annotation_body)

    @pytest.mark.parametrize("name", RENDERERS)
    def test_no_renderer_formats_its_own_annotation_line(self, name) -> None:
        """The format string is ``lib.run_summary.annotation_line``'s. Three
        copies is how the plain-text half of the contract drifted."""
        source = (REPO_ROOT / "scripts" / f"{name}.py").read_text()
        assert "::{" not in source, f"{name} builds an annotation line itself"
