"""Pin the ordering invariants of ``scripts/rebuild-cache-bootstrap.sh``.

A script that runs as user-data on a one-shot EC2 has one job before any
"real" work: be observable on failure. The 2026-05-09 first-manual-rebuild
attempt (instance ``i-0983db6d39958c76c``) demonstrated what happens when
the script doesn't do that — the bootstrap died early under
``set -euo pipefail``, the trap-EXIT upload-and-shutdown chain never ran,
and the instance sat idle for 3h 43min before the sweeper Lambda's failsafe
caught it. The S3 log bucket was empty; ``DeleteOnTermination=true`` on the
EBS volume erased ``/var/log/cloud-init-bootstrap.log`` on terminate. We
have zero forensic data about what the failing line was.

These tests pin two structural rules:

1. **An S3 "bootstrap started" breadcrumb is written before any
   set-e-fatal call.** Even a 0-second crash leaves a marker in
   ``s3://${REBUILD_LOG_BUCKET}/<launch-id>/00-started.txt`` so the
   operator at least knows the script began executing. (#174)

2. **``trap on_exit EXIT`` is registered before any IMDSv2 / SSM / dnf /
   git / curl call.** Any subsequent failure path triggers the
   upload-and-shutdown chain. (#173)

The tests are static-structural: they parse the script and assert that the
relevant fragments appear in the required order. They do not execute the
script — that surface is covered by manual end-to-end retest after the next
``sam deploy``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache-bootstrap.sh"


@pytest.fixture(scope="module")
def script_lines() -> list[str]:
    return SCRIPT_PATH.read_text().splitlines()


def first_line_index(lines: list[str], needle: str) -> int:
    """Return the index of the first non-comment line containing ``needle``.

    Comment-only lines are ignored so doc-string updates can describe the
    rule without dragging the test.
    """
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if needle in line:
            return i
    raise AssertionError(f"{needle!r} not found in non-comment lines of {SCRIPT_PATH}")


def last_line_index(lines: list[str], needle: str) -> int:
    """Return the index of the *last* non-comment line containing ``needle``.

    Used to locate a call site when the same symbol also names a function
    definition earlier in the file (the def is the first match, the call is
    the last).
    """
    found = None
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if needle in line:
            found = i
    if found is None:
        raise AssertionError(f"{needle!r} not found in non-comment lines of {SCRIPT_PATH}")
    return found


def function_body(lines: list[str], func_name: str) -> str:
    """Return the source text of a top-level ``func_name() {`` ... ``}`` block.

    The bootstrap's functions are all top-level: the opening line is
    unindented and the matching closing brace is a bare ``}`` at column 0
    (no nested top-level functions in this script). Slicing on those two
    anchors isolates one function's body for fragment/ordering assertions
    without dragging in sibling functions that happen to share substrings.
    """
    start = None
    for i, line in enumerate(lines):
        if line.startswith(f"{func_name}() {{"):
            start = i
            break
    if start is None:
        raise AssertionError(f"function {func_name!r} not found in {SCRIPT_PATH}")
    for j in range(start + 1, len(lines)):
        if lines[j] == "}":
            return "\n".join(lines[start : j + 1])
    raise AssertionError(f"closing brace for {func_name!r} not found in {SCRIPT_PATH}")


def test_trap_on_exit_registered_before_imds_calls(script_lines: list[str]) -> None:
    """#173: trap must be live before the script can early-exit on IMDSv2.

    If IMDS is flaky or a metadata-network blip makes ``imds_token`` exit
    non-zero, ``set -e`` kills the script. Without the trap registered,
    no S3 upload, no ``shutdown -h now`` — instance sits idle until the 3h
    sweeper.
    """
    trap_line = first_line_index(script_lines, "trap on_exit EXIT")
    imds_token_line = first_line_index(script_lines, "imds_token()")
    # The function definition is fine before the trap; what matters is the
    # *call* site, which uses the function. The earliest call is the
    # `TOKEN="$(imds_token)"` line.
    imds_call_line = first_line_index(script_lines, 'TOKEN="$(imds_token)"')
    assert trap_line < imds_call_line, (
        f"trap on_exit EXIT (line {trap_line + 1}) must precede the first "
        f"imds_token() call (line {imds_call_line + 1}). Otherwise an IMDS "
        f"failure under set -e exits without firing the upload-and-shutdown "
        f"hook. See #173."
    )
    # Sanity: trap registered after the function definition that on_exit refers to.
    on_exit_def_line = first_line_index(script_lines, "on_exit()")
    assert on_exit_def_line < trap_line, "on_exit() must be defined before `trap on_exit EXIT`."
    # imds_token function definition can sit anywhere; we don't constrain it.
    assert imds_token_line  # silence unused; kept for future expansion


def test_s3_breadcrumb_written_before_trap_registration(script_lines: list[str]) -> None:
    """#174: S3 marker must drop before the trap, so even a crash that
    prevents the trap from running still leaves a forensic trace.

    The marker key is ``<launch-id>/00-started.txt`` where ``<launch-id>``
    is the IMDS-derived instance id when available, else a
    timestamp+pid fallback.
    """
    marker_line = first_line_index(script_lines, "00-started.txt")
    trap_line = first_line_index(script_lines, "trap on_exit EXIT")
    assert marker_line < trap_line, (
        f"S3 breadcrumb write (line {marker_line + 1}) must precede the trap "
        f"(line {trap_line + 1}). Otherwise a crash before the trap leaves no "
        f"S3 record. See #174."
    )


def test_home_env_defaulted_before_any_home_reference(script_lines: list[str]) -> None:
    """#176: cloud-init strips HOME/USER/LOGNAME from user-data's env.

    Under ``set -u`` the bootstrap's first reference to ``$HOME`` (the
    Rust-install ``"$HOME/.cargo/bin/cargo"`` block) trips with
    'unbound variable' and exits before doing any real work. Confirmed
    live on the 2026-05-10 run #2 attempt at instance
    ``i-08acdffcd38db4906`` — caught precisely because the post-#175
    trap+S3-archive chain landed the failing log in S3 within 80
    seconds of launch.

    Same applies to ``$USER`` (``sudo chown "$USER:$USER" ...``) — both
    must be defaulted up-front.
    """
    home_default_line = first_line_index(script_lines, 'HOME="${HOME:-')
    home_use_line = first_line_index(script_lines, '"$HOME/')
    assert home_default_line < home_use_line, (
        f"HOME default (line {home_default_line + 1}) must precede the first "
        f'"$HOME/..." use (line {home_use_line + 1}). cloud-init starts user-'
        f"data with HOME unset; under set -u the first reference dies. See #176."
    )

    user_default_line = first_line_index(script_lines, 'USER="${USER:-')
    user_use_line = first_line_index(script_lines, '"$USER:$USER"')
    assert user_default_line < user_use_line, (
        f"USER default (line {user_default_line + 1}) must precede the first "
        f'"$USER:$USER" use (line {user_use_line + 1}). Same cloud-init env-'
        f"strip story as HOME. See #176."
    )


def test_s3_breadcrumb_uses_aws_s3_cp_with_or_true(script_lines: list[str]) -> None:
    """#174: marker write must not be set-e-fatal itself.

    If the breadcrumb write fails (creds not yet available, network blip),
    the script must still proceed — the breadcrumb is best-effort
    observability, not a precondition.
    """
    marker_line = first_line_index(script_lines, "00-started.txt")
    # Find the closing aws s3 cp invocation in a small window after the marker
    # line. ``aws s3 cp ... || true`` is the canonical shape; tolerate it
    # spread across continuation lines.
    window = "\n".join(script_lines[marker_line : marker_line + 8])
    assert "aws s3 cp" in window, (
        f"breadcrumb block near line {marker_line + 1} must use 'aws s3 cp' to "
        f"write the marker. See #174 for the rationale (CloudWatch metric on "
        f"the bucket gives a flat-zero signal if even this fails)."
    )
    assert "|| true" in window, (
        f"breadcrumb 'aws s3 cp' near line {marker_line + 1} must end with "
        f"'|| true' so a creds/network failure on the breadcrumb itself "
        f"doesn't kill the script. See #174."
    )


def test_collision_guard_runs_after_imds_id_and_before_rebuild_handoff(
    script_lines: list[str],
) -> None:
    """#311: the concurrent-rebuild guard must sit between IMDS instance-id
    resolution and the ``rebuild-cache.sh`` handoff.

    The guard queries EC2 for peer rebuild instances and self-terminates if
    it is not the winning (earliest-launched) one. It needs the real
    ``INSTANCE_ID`` (so it must run *after* the IMDS ``instance-id`` read),
    and it must bow out *before* any write to the shared cache (so it must
    run *before* the ``rebuild-cache.sh`` handoff). Two instances that both
    booted must never both reach the pipeline — that's the 2026-07-06 #298
    deadlock this closes.
    """
    imds_id_line = first_line_index(script_lines, 'INSTANCE_ID="$(imds_get')
    guard_call_line = last_line_index(script_lines, "abort_if_not_winning_rebuild")
    handoff_line = first_line_index(script_lines, '"$REPO_DIR/scripts/rebuild-cache.sh"')
    assert imds_id_line < guard_call_line, (
        f"the abort_if_not_winning_rebuild guard (line {guard_call_line + 1}) must run "
        f"after IMDS instance-id resolution (line {imds_id_line + 1}); it needs the real "
        f"INSTANCE_ID to exclude itself from the peer query. See #311."
    )
    assert guard_call_line < handoff_line, (
        f"the abort_if_not_winning_rebuild guard (line {guard_call_line + 1}) must run "
        f"before the rebuild-cache.sh handoff (line {handoff_line + 1}); a bowing-out "
        f"instance must never write to the shared cache. See #311."
    )


def test_collision_guard_uses_same_tag_and_state_filter(script_lines: list[str]) -> None:
    """#311: the bootstrap peer check must mirror the launcher/sweeper filter
    (``tag:Project=discogs-rebuild`` + ``pending``/``running``).

    The bootstrap is bash so it can't import the Python helper; the filter is
    duplicated and must stay in sync. Pin the filter fragments so a drift on
    either side is caught.
    """
    src = "\n".join(script_lines)
    assert "tag:Project,Values=discogs-rebuild" in src, (
        "bootstrap peer check must filter on tag:Project=discogs-rebuild, matching "
        "list_active_rebuild_instances in the launcher/sweeper. See #311."
    )
    assert "instance-state-name,Values=pending,running" in src, (
        "bootstrap peer check must filter on pending/running instance state, matching "
        "list_active_rebuild_instances in the launcher/sweeper. See #311."
    )


def test_peer_query_does_not_discard_stderr(script_lines: list[str]) -> None:
    """#355: the peer query must capture stderr, not swallow it.

    The pre-#355 shape was
    ``... --output text 2>/dev/null)" || instances=""`` — a failed query
    (e.g. AccessDenied) discards its error text and collapses into the same
    empty ``instances`` value a legitimately-empty result produces. On
    2026-08-04 that hid an AccessDenied peer-check failure inside a
    345 KB bootstrap log with zero trace of the authorization error. The
    query must instead redirect stderr somewhere it can be logged.
    """
    body = function_body(script_lines, "abort_if_not_winning_rebuild")
    assert "describe-instances" in body, (
        "abort_if_not_winning_rebuild() must still call `aws ec2 describe-instances`."
    )
    assert "2>/dev/null" not in body, (
        "the peer query must not discard stderr with `2>/dev/null` -- a failed query "
        "(AccessDenied, throttling, etc.) must leave a trace to log and alert on. See #355 "
        "and the 2026-08-04 incident (#352) it fixes."
    )


def test_peer_query_failure_and_empty_result_are_distinguishable_branches(
    script_lines: list[str],
) -> None:
    """#355: a failed peer query must take a different branch than a
    legitimately empty one, with a different, greppable log-message prefix.

    Before this fix, ``2>/dev/null || instances=""`` meant "the query errored"
    and "the query succeeded with zero rows" were literally the same code
    path, both landing on the WARN "found no active rebuild instances"
    message. That message blames eventual consistency, which is a true
    explanation for the empty-result case and a false one for the errored
    case. The two must now be distinguishable both in control flow and in
    the text an operator (or an alert) would grep for.
    """
    body = function_body(script_lines, "abort_if_not_winning_rebuild")
    lines = body.splitlines()

    empty_result_line = first_line_index(lines, "no active rebuild instances")
    failure_line = first_line_index(lines, "BootstrapPeerQueryFailed")
    assert empty_result_line != failure_line, (
        "the empty-result WARN and the query-failure log line must be distinct lines "
        "with distinct messages, not a single shared fail-open path. See #355."
    )

    # The two messages must not share a common "found no active rebuild
    # instances" framing -- that framing is specifically what mis-described
    # the 2026-08-04 AccessDenied failure as a benign empty result.
    assert "no active rebuild instances" not in lines[failure_line], (
        "the query-failure log line must not reuse the empty-result phrasing "
        "('found no active rebuild instances') -- that phrasing is what made an "
        "AccessDenied failure look like a benign eventual-consistency gap on "
        "2026-08-04. See #355."
    )


def test_peer_query_failure_captures_and_logs_stderr(script_lines: list[str]) -> None:
    """#355: the captured stderr from a failed query must appear in the log."""
    body = function_body(script_lines, "abort_if_not_winning_rebuild")
    lines = body.splitlines()

    # The query must be captured under a name we can find again in the ERROR
    # log line and the Slack notification -- pin that a stderr variable is
    # both assigned and later referenced, rather than merely captured and
    # discarded.
    assert "stderr" in body, (
        "abort_if_not_winning_rebuild() must capture the peer query's stderr into a "
        "variable (not `2>/dev/null`) so a failure has forensic content. See #355."
    )
    failure_line = first_line_index(lines, "BootstrapPeerQueryFailed")
    # The captured-stderr variable name must be referenced on (or immediately
    # after) the ERROR line that reports the failure, so the log actually
    # contains the captured text rather than just announcing the failure.
    window = "\n".join(lines[failure_line : failure_line + 3])
    assert "stderr" in window, (
        "the ERROR log line for a failed peer query must include the captured stderr "
        "text, not just announce that the query failed. See #355 acceptance criteria: "
        "'Captured stderr from a failed query appears in the log.'"
    )


def test_peer_query_failure_aborts_rather_than_returns(script_lines: list[str]) -> None:
    """#355: a failed peer query must abort, not fail open.

    The empty-result and self-not-visible branches both ``return 0`` so the
    bootstrap proceeds -- that fail-open is intentional and kept (see the
    module-level docstring / #311's eventual-consistency rationale). A
    *failed* query is a different condition: the guard could not run at all,
    so "proceed anyway" is exactly the 2026-08-04 incident. This must be an
    ``exit`` (which the top-level ``trap on_exit EXIT`` turns into an
    upload-and-shutdown, per #352's post-mortem), not a ``return 0``.
    """
    body = function_body(script_lines, "abort_if_not_winning_rebuild")
    lines = body.splitlines()
    failure_line = first_line_index(lines, "BootstrapPeerQueryFailed")

    # Walk forward from the failure log line to the next `fi` that closes its
    # `if` block, and assert the block aborts via `exit` with a non-zero
    # status rather than returning 0 (the fail-open shape used by the other
    # two branches).
    block = None
    for end in range(failure_line, len(lines)):
        if lines[end].strip() == "fi":
            block = "\n".join(lines[failure_line : end + 1])
            break
    assert block is not None, "could not find the closing `fi` for the query-failure branch"
    assert "exit 0" not in block, (
        "a failed peer query must not fail open via `return 0` / `exit 0` -- that is "
        "the exact 2026-08-04 incident this ticket closes. See #355."
    )
    assert "return 0" not in block, (
        "a failed peer query must not fail open via `return 0` -- that is the exact "
        "2026-08-04 incident this ticket closes. See #355."
    )
    assert "exit " in block, (
        "a failed peer query must abort via `exit <nonzero>` so the top-level "
        "`trap on_exit EXIT` uploads the log and shuts the instance down. See #355."
    )


def test_peer_query_failure_notifies_slack(script_lines: list[str]) -> None:
    """#355: a failed peer query must page out via Slack, not just log quietly.

    345 KB of bootstrap log with no authorization error anywhere is exactly
    what happened on 2026-08-04 -- nobody was reading that log. The failure
    must reach ``notify_slack`` so it surfaces without anyone tailing logs.
    """
    body = function_body(script_lines, "abort_if_not_winning_rebuild")
    lines = body.splitlines()
    failure_line = first_line_index(lines, "BootstrapPeerQueryFailed")
    tail = "\n".join(lines[failure_line : failure_line + 6])
    assert "notify_slack" in tail, (
        "the query-failure branch must call notify_slack so the failure surfaces "
        "without anyone reading logs. See #355."
    )


def test_empty_result_fail_open_message_unchanged(script_lines: list[str]) -> None:
    """#355 must not touch the genuinely-empty-result fail-open kept from #311.

    ``DescribeInstances`` really is eventually consistent, the launcher
    precheck already ran, and the >3h sweeper is a backstop -- a *successful*
    query that legitimately finds zero peers must still proceed with its
    original message unchanged.
    """
    body = function_body(script_lines, "abort_if_not_winning_rebuild")
    assert (
        "WARN: peer check found no active rebuild instances "
        "(self not yet visible?); proceeding" in body
    ), "the empty-result fail-open message from #311 must be preserved verbatim. See #355."
    assert "return 0" in body, (
        "the empty-result branch must still fail open via `return 0`. See #355."
    )
