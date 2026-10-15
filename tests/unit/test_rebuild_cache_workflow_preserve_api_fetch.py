"""``rebuild-cache.yml``'s ``workflow_dispatch`` path preserves ``api_fetch`` releases (#460).

``run_pipeline.py --preserve-api-fetch`` is off by default, and only
``scripts/rebuild-cache.sh`` turned it on, so a manual dispatch of this workflow
silently dropped every ``api_fetch`` release the dump lacks. These are static
assertions: they pin that the flag stays wired to a default-true boolean input
inside the ``set +e`` / ``PY_RC`` region, not that the run behaves.
"""

from __future__ import annotations

import shlex
from pathlib import Path

from tests.workflow_yaml import load_workflow, step_named, triggers

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
    """Tokenize the ``run_pipeline.py`` call, joining ``\\`` continuations like bash."""
    joined = _run_script().replace("\\\n", " ")
    for line in joined.splitlines():
        stripped = line.lstrip()
        if "scripts/run_pipeline.py" in stripped and stripped.startswith("python"):
            return shlex.split(stripped)
    raise AssertionError("Could not find run_pipeline.py invocation in the workflow step")


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
