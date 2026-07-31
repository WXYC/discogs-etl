"""Pin the prune-audit dump wiring in ``scripts/rebuild-cache-bootstrap.sh``.

discogs-etl#217 Phase 0 taught ``run_pipeline.py`` to honor an opt-in
``PRUNE_AUDIT_DUMP_DIR`` env var, but the production monthly rebuild runs on
an ephemeral EC2 (`infra/ephemeral-rebuild/`) whose bootstrap only ever read
a fixed set of SSM parameters — there was no way for an operator to set
``PRUNE_AUDIT_DUMP_DIR`` on a scheduled run, and even if there were, the
instance's local disk is destroyed on termination so a dump written to an
arbitrary path would never survive. Phase 1 closes that gap: the bootstrap
reads an optional ``${SSM_PREFIX}/PRUNE_AUDIT_ENABLED`` parameter and, when
truthy, points ``PRUNE_AUDIT_DUMP_DIR`` at ``$LOG_DIR/prune-audit`` — the one
directory the bootstrap's ``trap EXIT`` already syncs to S3.

These tests are static-structural, matching the convention in
``test_bootstrap_ordering.py``: they parse the script text and assert that
the relevant fragments exist and appear in the required order, without
executing the script.

The critical safety property under test is that a normal rebuild (the SSM
param absent) is unaffected: ``PRUNE_AUDIT_ENABLED`` must be read the same
tolerant way as the other optional params (``SENTRY_DSN``) and must NOT gain
a required-param check that could abort the rebuild.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache-bootstrap.sh"


@pytest.fixture(scope="module")
def script_text() -> str:
    return SCRIPT_PATH.read_text()


@pytest.fixture(scope="module")
def script_lines(script_text: str) -> list[str]:
    return script_text.splitlines()


def first_line_index(lines: list[str], needle: str) -> int:
    """Return the index of the first non-comment line containing ``needle``."""
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if needle in line:
            return i
    raise AssertionError(f"{needle!r} not found in non-comment lines of {SCRIPT_PATH}")


def guard_line_index(lines: list[str]) -> int:
    """Return the index of the ``if ... PRUNE_AUDIT_ENABLED ... then`` guard line.

    Located structurally (an ``if`` line mentioning the flag and ending in
    ``then``) rather than by a literal string so the test survives changes to
    the exact comparison expression — e.g. lowercasing the value.
    """
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if (
            stripped.startswith("if ")
            and "PRUNE_AUDIT_ENABLED" in line
            and line.rstrip().endswith("then")
        ):
            return i
    raise AssertionError(f"prune-audit guard `if` line not found in {SCRIPT_PATH}")


def test_prune_audit_enabled_read_via_ssm_param_helper(script_lines: list[str]) -> None:
    """The new param must be read through the same tolerant ``ssm_param`` helper
    used for ``SENTRY_DSN``, not a bespoke ``aws ssm get-parameter`` call that
    could have different failure semantics.
    """
    line = first_line_index(script_lines, 'PRUNE_AUDIT_ENABLED="$(ssm_param')
    assert "${SSM_PREFIX}/PRUNE_AUDIT_ENABLED" in script_lines[line], (
        "PRUNE_AUDIT_ENABLED must be read from ${SSM_PREFIX}/PRUNE_AUDIT_ENABLED "
        f"via ssm_param; got: {script_lines[line]!r}"
    )


def test_prune_audit_enabled_read_after_sentry_dsn(script_lines: list[str]) -> None:
    """Mirror the documented anchor: the read should sit alongside the other
    optional (non-aborting) SSM reads, immediately after SENTRY_DSN.
    """
    sentry_line = first_line_index(script_lines, 'SENTRY_DSN="$(ssm_param')
    prune_line = first_line_index(script_lines, 'PRUNE_AUDIT_ENABLED="$(ssm_param')
    assert sentry_line < prune_line, (
        "PRUNE_AUDIT_ENABLED should be read after SENTRY_DSN, matching the "
        "optional-param grouping in the SSM-fetch block."
    )


def test_prune_audit_enabled_has_no_required_param_check(script_text: str) -> None:
    """Safety property: a missing PRUNE_AUDIT_ENABLED must never abort the
    rebuild. DATABASE_URL_DISCOGS and GH_TOKEN each get an
    ``if [ -z "$VAR" ]; then ... exit 2; fi`` required-param guard right
    after the SSM reads — PRUNE_AUDIT_ENABLED must NOT gain the same
    treatment, or an operator who never touches the flag would still be at
    the mercy of the SSM call succeeding.
    """
    assert 'if [ -z "$PRUNE_AUDIT_ENABLED" ]' not in script_text, (
        "PRUNE_AUDIT_ENABLED must stay optional — do not add a required-param "
        "check (missing/empty must silently mean disabled, not abort the "
        "rebuild)."
    )


def test_prune_audit_dump_dir_export_is_conditional(script_lines: list[str]) -> None:
    """``PRUNE_AUDIT_DUMP_DIR`` must only be exported inside a guard on
    ``PRUNE_AUDIT_ENABLED`` — never unconditionally, which would turn every
    rebuild into an audit-dump run.
    """
    guard_line = guard_line_index(script_lines)
    export_line = first_line_index(script_lines, "export PRUNE_AUDIT_DUMP_DIR=")
    assert guard_line < export_line, (
        "the PRUNE_AUDIT_ENABLED guard must precede the PRUNE_AUDIT_DUMP_DIR "
        "export so the export only runs when the flag is truthy."
    )
    # The export must be indented as the guard's body (not a sibling at the
    # guard's own level), and the if-block must be closed by a matching `fi`
    # somewhere after it. This is a structural check — a bare ``"fi" in ...``
    # substring test would false-positive on ordinary words (config, verify,
    # prefix) if lines were ever inserted between the guard and the export.
    guard_indent = len(script_lines[guard_line]) - len(script_lines[guard_line].lstrip())
    export_indent = len(script_lines[export_line]) - len(script_lines[export_line].lstrip())
    assert export_indent > guard_indent, (
        "export PRUNE_AUDIT_DUMP_DIR must be indented inside the if-block body; "
        f"got guard_indent={guard_indent}, export_indent={export_indent}"
    )
    assert any(line.strip() == "fi" for line in script_lines[export_line + 1 :]), (
        "the if-block guarding the export must be closed by a matching `fi`."
    )


def test_prune_audit_dump_dir_points_under_log_dir(script_lines: list[str]) -> None:
    """The dump must land under ``$LOG_DIR`` specifically — that's the only
    directory the ``trap EXIT`` handler syncs to S3, so anywhere else would
    be destroyed with the instance on termination.
    """
    export_line = first_line_index(script_lines, "export PRUNE_AUDIT_DUMP_DIR=")
    assert '"$LOG_DIR/prune-audit"' in script_lines[export_line], (
        "PRUNE_AUDIT_DUMP_DIR must be set to $LOG_DIR/prune-audit so it rides "
        f"the existing S3 log sync; got: {script_lines[export_line]!r}"
    )


def test_prune_audit_wiring_runs_before_rebuild_handoff(script_lines: list[str]) -> None:
    """The conditional export must happen before the ``rebuild-cache.sh``
    handoff so the exported env is visible to the child process (and, in
    turn, to ``run_pipeline.py``'s ``os.environ.get("PRUNE_AUDIT_DUMP_DIR")``
    read).
    """
    export_line = first_line_index(script_lines, "export PRUNE_AUDIT_DUMP_DIR=")
    handoff_line = first_line_index(script_lines, '"$REPO_DIR/scripts/rebuild-cache.sh"')
    assert export_line < handoff_line, (
        f"PRUNE_AUDIT_DUMP_DIR export (line {export_line + 1}) must precede the "
        f"rebuild-cache.sh handoff (line {handoff_line + 1}) for the env var to "
        f"reach run_pipeline.py."
    )


def test_prune_audit_wiring_runs_after_collision_guard(script_lines: list[str]) -> None:
    """Keep the new block after ``abort_if_not_winning_rebuild`` so a losing
    instance never bothers touching PRUNE_AUDIT_DUMP_DIR before it bows out.
    """
    guard_call_line = first_line_index(script_lines, "abort_if_not_winning_rebuild")
    export_line = first_line_index(script_lines, "export PRUNE_AUDIT_DUMP_DIR=")
    assert guard_call_line < export_line, (
        "prune-audit wiring should run after the concurrent-rebuild guard call."
    )


def test_prune_audit_enabled_logs_when_enabled(script_lines: list[str]) -> None:
    """A clear log line should announce the dump is active, for operators
    tailing the bootstrap log mid-run or reading it back from S3.
    """
    guard_line = guard_line_index(script_lines)
    export_line = first_line_index(script_lines, "export PRUNE_AUDIT_DUMP_DIR=")
    window = "\n".join(script_lines[guard_line : export_line + 3])
    assert 'log "prune-audit dump ENABLED' in window, (
        f"expected a log() call announcing the enabled dump near the export; window={window!r}"
    )


def test_prune_audit_truthy_check_is_case_insensitive(script_lines: list[str]) -> None:
    """``True``/``TRUE`` (case variants of the documented ``true``) must still
    enable the dump. The SSM value is written by hand by an operator, so a case
    slip should not silently cost a whole monthly rebuild's audit — the guard
    lowercases the value before comparing.
    """
    guard = script_lines[guard_line_index(script_lines)]
    assert "${PRUNE_AUDIT_ENABLED,,}" in guard, (
        "the PRUNE_AUDIT_ENABLED truthy test must lowercase the value "
        f"(e.g. ${{PRUNE_AUDIT_ENABLED,,}}) so True/TRUE match 'true'; got: {guard!r}"
    )


def test_prune_audit_set_but_not_truthy_is_logged(script_text: str) -> None:
    """A set-but-non-truthy value (a typo, ``false``, ``0``, ``yes``) must be
    logged, not silently ignored. Otherwise an operator who fat-fingers the flag
    only discovers the miss after the multi-hour rebuild finishes and leaves no
    dump in S3 — an expensive round trip to redo.
    """
    assert 'elif [ -n "$PRUNE_AUDIT_ENABLED" ]' in script_text, (
        'expected an `elif [ -n "$PRUNE_AUDIT_ENABLED" ]` branch that fires '
        "when the flag is non-empty but not truthy, so the ignored value is logged."
    )
    assert 'log "WARN: PRUNE_AUDIT_ENABLED' in script_text, (
        "the non-truthy branch should log a WARN naming PRUNE_AUDIT_ENABLED so "
        "the dropped flag is visible in the bootstrap log."
    )
