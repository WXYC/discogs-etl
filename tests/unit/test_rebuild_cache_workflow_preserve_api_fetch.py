"""``rebuild-cache.yml``'s ``workflow_dispatch`` path preserves ``api_fetch`` releases (#460).

``run_pipeline.py --preserve-api-fetch`` is off by default, and only
``scripts/rebuild-cache.sh`` turned it on, so a manual dispatch of this workflow
silently dropped every ``api_fetch`` release the dump lacks. These are static
assertions: they pin that the flag stays wired to a default-true boolean input
inside the ``set +e`` / ``PY_RC`` region, not that the run behaves.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import pytest

from tests.workflow_yaml import load_workflow, run_pipeline_tokens, step_named, triggers

WORKFLOW_PATH = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "rebuild-cache.yml"
STEP_NAME = "Run pipeline (with streamed dump)"


def _run_script() -> str:
    return step_named(load_workflow(WORKFLOW_PATH), "rebuild", STEP_NAME)["run"]


def test_input_is_a_boolean_defaulting_to_true() -> None:
    inputs = triggers(load_workflow(WORKFLOW_PATH))["workflow_dispatch"]["inputs"]
    assert inputs["preserve_api_fetch"]["type"] == "boolean"
    assert inputs["preserve_api_fetch"]["default"] is True


def test_step_env_carries_the_input() -> None:
    step = step_named(load_workflow(WORKFLOW_PATH), "rebuild", STEP_NAME)
    assert "inputs.preserve_api_fetch" in str(step["env"]["PRESERVE_API_FETCH"])


def _run_pipeline_invocation_tokens() -> list[str]:
    return run_pipeline_tokens(_run_script())


def test_flag_is_passed_unless_the_input_is_false_inside_the_set_plus_e_block() -> None:
    script = _run_script()
    assert '[ "$PRESERVE_API_FETCH" != "false" ]' in script
    assert "PRESERVE_ARGS=(--preserve-api-fetch)" in script
    region = script[script.index("set +e") : script.index("PY_RC=$?")]
    assert '"${PRESERVE_ARGS[@]}"' in region
    assert "${PRESERVE_ARGS[@]}" in _run_pipeline_invocation_tokens(), (
        "the conditional flag must be a token of the run_pipeline.py command itself, "
        "not detached from it by a dropped continuation backslash"
    )


def test_any_nonzero_exit_reaps_curl_before_waiting() -> None:
    script = _run_script()
    wait_idx = script.index('wait "$CURL_PID" || true')
    reap_idx = script.index('[ "$PY_RC" -ne 0 ] && [ ! -e data/curl.status ]')
    assert reap_idx < wait_idx
    block = script[reap_idx:wait_idx]
    assert 'kill "$CURL_PID"' in block
    assert 'exit "$PY_RC"' in block


_STUB_CURL = """#!/bin/bash
# Stub curl: record our pid, then behave per STUB_CURL.
echo $$ > "$STUB_DIR/curl.pid"
out=""
while [ $# -gt 0 ]; do
  if [ "$1" = "-o" ]; then out="$2"; shift; fi
  shift
done
case "$STUB_CURL" in
  fail) exit 22 ;;
  ok) echo data > "$out" ;;
  *) echo data > "$out" ;;  # blocks in open() until a reader arrives
esac
"""

_STUB_PYTHON = """#!/bin/bash
# Stub python: find --xml, then behave per STUB_PY.
xml=""
while [ $# -gt 0 ]; do
  if [ "$1" = "--xml" ]; then xml="$2"; shift; fi
  shift
done
case "$STUB_PY" in
  fail_early) exit 1 ;;
  fail_late) sleep 1; exit 1 ;;
  read_fifo) cat "$xml" > /dev/null; exit 1 ;;  # blocks in open() until a writer arrives
  bow_out) exit 75 ;;
  ok) cat "$xml" > /dev/null; exit 0 ;;
esac
"""


def _run_step(tmp_path: Path, curl_mode: str, py_mode: str) -> tuple[int, str, Path]:
    """Run the real step script under bash with stub curl/python first on PATH."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("curl", _STUB_CURL), ("python", _STUB_PYTHON)):
        stub = bin_dir / name
        stub.write_text(body)
        stub.chmod(0o755)
    work = tmp_path / "work"
    work.mkdir()
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "STUB_DIR": str(tmp_path),
        "STUB_CURL": curl_mode,
        "STUB_PY": py_mode,
        "DUMP_URL": "https://example.invalid/releases.xml.gz",
        "PRESERVE_API_FETCH": "false",
    }
    # Output goes to a file, not a pipe: an orphaned curl would otherwise hold
    # the pipe open and turn a leak into a hang that hides the real assertion.
    log = tmp_path / "step.log"
    # Own session so a timeout can kill every stub (curl, a python parked on
    # the FIFO) instead of orphaning them.
    with log.open("w") as out:
        proc = subprocess.Popen(
            ["bash", "-e", "-c", _run_script()],
            cwd=work,
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
            pytest.fail(f"step did not exit within 20s\n{log.read_text()}")
    return proc.returncode, log.read_text(), tmp_path / "curl.pid"


def _alive(pid_file: Path) -> bool:
    if not pid_file.exists():
        return False
    pid = int(pid_file.read_text().strip())
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.parametrize(
    ("curl_mode", "py_mode", "expected_rc", "expected_text"),
    [
        ("fail", "fail_late", 1, "::error::curl exited 22 while streaming"),
        ("fail", "read_fifo", 1, "::error::curl exited 22 while streaming"),
        ("block", "fail_early", 1, None),
        ("block", "bow_out", 75, "::warning::bowed out"),
        ("ok", "ok", 0, None),
    ],
    ids=[
        "curl-fails-first",
        "curl-fails-reader-blocked",
        "python-fails-early",
        "python-bows-out",
        "both-ok",
    ],
)
@pytest.mark.skipif(
    shutil.which("bash") is None
    or subprocess.run(["bash", "-c", "((BASH_VERSINFO[0] >= 4))"], check=False).returncode != 0,
    reason="step uses empty-array expansion under set -u, which needs bash >= 4",
)
def test_step_behavior(
    tmp_path: Path, curl_mode: str, py_mode: str, expected_rc: int, expected_text: str | None
) -> None:
    rc, log, pid_file = _run_step(tmp_path, curl_mode, py_mode)
    try:
        assert rc == expected_rc, log
        if expected_text is not None:
            assert expected_text in log
        else:
            assert "::error::curl exited" not in log
        # No stub curl may outlive the step: an orphan parked on the FIFO's
        # write end is the #482 hang.
        deadline = time.monotonic() + 2
        while _alive(pid_file) and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not _alive(pid_file), f"stub curl survived the step\n{log}"
    finally:
        if _alive(pid_file):
            os.kill(int(pid_file.read_text().strip()), 9)


def test_fifo_input_names_its_xml_type() -> None:
    """#510: --xml points at a FIFO, so the converter must be told the XML type.
    Root-element auto-detection opens and closes the FIFO, which breaks curl's
    stream before the real scan starts."""
    tokens = _run_pipeline_invocation_tokens()
    assert "--xml-type" in tokens, tokens
    assert tokens[tokens.index("--xml-type") + 1] == "releases"
