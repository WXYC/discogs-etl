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

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache-bootstrap.sh"

# The greppable marker the peer-query-failure branch logs (#355). It is a
# contract with operators and with the runbook's troubleshooting entry, not
# an implementation detail -- renaming it means updating both.
FAILURE_MARKER = "BootstrapPeerQueryFailed"


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


def non_comment_lines(lines: list[str]) -> list[str]:
    """Drop whole-line comments, matching ``test_rebuild_cache_curl_resilience``.

    Every assertion about what the *code* does must run against this, not
    the raw text. Otherwise an explanatory comment that quotes the shape it
    is warning against -- and the comments in this script do exactly that,
    e.g. the ``2>/dev/null || instances=""`` post-mortem note above
    ``abort_if_not_winning_rebuild`` -- fails a test that is looking for
    that shape in the code.
    """
    return [line for line in lines if not line.lstrip().startswith("#")]


FUNC_DEF_RE = re.compile(r"^(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(\)\s*\{")


def function_body(lines: list[str], func_name: str) -> list[str]:
    """Return the lines of a top-level ``func_name() {`` ... ``}`` block.

    The bootstrap's functions are all top-level: the opening line is
    unindented and the matching closing brace is a bare ``}`` at column 0.
    Slicing on those two anchors isolates one function's body for
    fragment/ordering assertions without dragging in sibling functions that
    happen to share substrings.

    Anchor-matching is not brace-counting, so it would silently over-capture
    if the closing anchor were ever missed -- a heredoc or string holding a
    bare ``}`` at column 0 would end the slice early, and a reindented
    closing brace would run the slice on into the next function. Both
    failure modes are caught here rather than surfacing as a confusing
    assertion further downstream: encountering another top-level function
    definition before the closing brace is an error, and so is running off
    the end of the file.
    """
    start = None
    for i, line in enumerate(lines):
        match = FUNC_DEF_RE.match(line)
        if match and match.group("name") == func_name:
            start = i
            break
    if start is None:
        raise AssertionError(f"function {func_name!r} not found in {SCRIPT_PATH}")
    for j in range(start + 1, len(lines)):
        if lines[j] == "}":
            return lines[start : j + 1]
        if FUNC_DEF_RE.match(lines[j]):
            raise AssertionError(
                f"while slicing {func_name!r} in {SCRIPT_PATH}, hit the definition of "
                f"{lines[j]!r} at line {j + 1} before a closing bare `}}` at column 0. "
                f"Either {func_name!r}'s closing brace is indented, or a heredoc/string "
                f"inside it holds a bare `}}` at column 0 -- this helper anchors on text, "
                f"not brace depth, so it cannot slice that shape. Restore the convention "
                f"or teach this helper to count braces."
            )
    raise AssertionError(f"closing brace for {func_name!r} not found in {SCRIPT_PATH}")


def guard_body(lines: list[str]) -> list[str]:
    """The code lines (comments stripped) of ``abort_if_not_winning_rebuild``."""
    return non_comment_lines(function_body(lines, "abort_if_not_winning_rebuild"))


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
    ``... --output text 2>/dev/null)" || instances=""`` -- a failed query
    (e.g. AccessDenied) discards its error text and collapses into the same
    empty ``instances`` value a legitimately-empty result produces. On
    2026-08-04 that hid an AccessDenied peer-check failure inside a
    345 KB bootstrap log with zero trace of the authorization error. The
    query must instead redirect stderr somewhere it can be logged.
    """
    body = guard_body(script_lines)
    code = "\n".join(body)
    assert "describe-instances" in code, (
        "abort_if_not_winning_rebuild() must still call `aws ec2 describe-instances`."
    )
    # Match any spacing -- `2>/dev/null`, `2> /dev/null`, `2>>/dev/null` all
    # discard the same forensic content.
    assert not re.search(r"2>>?\s*/dev/null", code), (
        "the peer query must not discard stderr into /dev/null -- a failed query "
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
    body = guard_body(script_lines)

    empty_result_line = first_line_index(body, "no active rebuild instances")
    failure_line = first_line_index(body, FAILURE_MARKER)
    assert empty_result_line != failure_line, (
        "the empty-result WARN and the query-failure log line must be distinct lines "
        "with distinct messages, not a single shared fail-open path. See #355."
    )

    # The two messages must not share a common "found no active rebuild
    # instances" framing -- that framing is specifically what mis-described
    # the 2026-08-04 AccessDenied failure as a benign empty result.
    assert "no active rebuild instances" not in body[failure_line], (
        "the query-failure log line must not reuse the empty-result phrasing "
        "('found no active rebuild instances') -- that phrasing is what made an "
        "AccessDenied failure look like a benign eventual-consistency gap on "
        "2026-08-04. See #355."
    )

    # ...and the branches must be selected by the query's *exit status*, not
    # by re-inspecting the (identically empty) result. Testing `$instances`
    # for emptiness is precisely the conflation #355 exists to undo.
    guard = first_line_index(body, FAILURE_MARKER)
    predicate = "\n".join(body[max(0, guard - 3) : guard])
    assert re.search(r"\$\{?query_exit\}?", predicate), (
        "the query-failure branch must be selected by the captured `aws` exit status "
        "(`query_exit`), not by re-testing whether `$instances` is empty -- that test "
        "cannot tell AccessDenied from zero rows, which is the #352 mechanism. See #355."
    )


def test_peer_query_failure_captures_and_logs_stderr(script_lines: list[str]) -> None:
    """#355: the captured stderr from a failed query must appear in the log.

    Asserting on the *expansion* (``${query_stderr}``) rather than the word
    "stderr" is deliberate: the log line contains the literal text
    ``-- stderr:`` as a label, so a substring check for "stderr" would still
    pass if someone dropped the variable expansion and left the label behind
    -- exactly the regression this test exists to catch.
    """
    body = guard_body(script_lines)
    code = "\n".join(body)

    # The variable must actually be fed from the redirected stderr file, not
    # merely declared.
    assert re.search(r"query_stderr=.*\$\{?stderr_file\}?", code), (
        "abort_if_not_winning_rebuild() must assign `query_stderr` from the file the "
        "peer query's stderr was redirected into, so a failure has forensic content. "
        "See #355."
    )
    assert re.search(r"2>\s*\"?\$\{?stderr_file\}?", code), (
        "the peer query must redirect stderr into `$stderr_file`. See #355."
    )

    failure_line = first_line_index(body, FAILURE_MARKER)
    # The captured-stderr expansion must appear on (or immediately after) the
    # ERROR line that reports the failure, so the log actually contains the
    # captured text rather than just announcing that a failure happened.
    window = "\n".join(body[failure_line : failure_line + 3])
    assert "${query_stderr}" in window, (
        "the ERROR log line for a failed peer query must interpolate the captured "
        "stderr, not just announce that the query failed. See #355 acceptance criteria: "
        "'Captured stderr from a failed query appears in the log.'"
    )


def test_peer_query_stderr_is_sanitized_before_interpolation(script_lines: list[str]) -> None:
    """#355: captured stderr must be made safe for notify_slack's JSON body.

    ``notify_slack`` hand-builds ``{"text":"..."}`` with no escaping, and
    every caller before this one passed static text. botocore's
    network-failure message embeds literal double quotes -- ``Could not
    connect to the endpoint URL: "https://ec2.us-east-1.amazonaws.com/"`` --
    which produces an invalid payload that Slack rejects, and
    ``curl ... || true`` swallows the rejection. The alert this branch
    exists to send would be silently dropped in one of the three failure
    modes it names. It must also be bounded: Slack's ``text`` limit is
    4000 chars and a botocore traceback runs well past it.
    """
    body = guard_body(script_lines)
    code = "\n".join(body)

    # The capture must run through a `tr -c` whitelist, not a blacklist: a
    # blacklist only excludes the characters we thought of, and the failure
    # mode is silent.
    capture = next(
        (line for line in body if re.match(r'\s*query_stderr="?\$\(', line)),
        None,
    )
    assert capture is not None, (
        "`query_stderr` must be assigned from a command substitution that sanitizes "
        "the captured stderr. See #355."
    )
    assert "tr -c " in capture, (
        "the captured stderr must be sanitized through a `tr -c <whitelist>` so "
        "characters we did not anticipate cannot reach notify_slack's hand-built JSON "
        "payload. See #355."
    )

    whitelist = next(
        (line for line in body if "printable_safe=" in line),
        None,
    )
    assert whitelist is not None, (
        "the `tr -c` whitelist must be a named set (`printable_safe`) so its contents "
        "are reviewable. See #355."
    )
    assert '"' not in whitelist and "\\" not in whitelist, (
        'the sanitizing whitelist must exclude `"` and `\\` -- notify_slack builds '
        '`{"text":"..."}` by hand with no escaping, and botocore\'s network-failure '
        "message embeds literal double quotes. An unescaped quote makes Slack reject "
        "the payload and `curl ... || true` swallows the rejection. See #355."
    )

    assert re.search(r"query_stderr=\"\$\{query_stderr:\d+:\d+\}\"", code), (
        "the captured stderr must be length-capped before interpolation -- an "
        "unbounded botocore traceback exceeds Slack's 4000-char `text` limit and "
        "would drop the alert. See #355."
    )


def test_peer_query_failure_aborts_rather_than_returns(script_lines: list[str]) -> None:
    """#355: a failed peer query must abort, not fail open.

    The empty-result and self-not-visible branches both ``return 0`` so the
    bootstrap proceeds -- that fail-open is intentional and kept (see the
    module-level docstring / #311's eventual-consistency rationale). A
    *failed* query is a different condition: the guard could not run at all,
    so "proceed anyway" is exactly the 2026-08-04 incident. This must be an
    ``exit`` with a non-zero status (which the top-level ``trap on_exit
    EXIT`` turns into an upload-and-shutdown, per #352's post-mortem), not a
    ``return 0``.
    """
    body = guard_body(script_lines)
    failure_line = first_line_index(body, FAILURE_MARKER)

    # Walk forward from the failure log line to the next `fi` that closes its
    # `if` block. `body` is already comment-stripped, so a future explanatory
    # comment mentioning `return 0` cannot fail this.
    block = None
    for end in range(failure_line, len(body)):
        if body[end].strip() == "fi":
            block = body[failure_line : end + 1]
            break
    assert block is not None, "could not find the closing `fi` for the query-failure branch"

    statements = [line.strip() for line in block]
    assert not any(re.fullmatch(r"(exit|return)\s+0", stmt) for stmt in statements), (
        "a failed peer query must not fail open via `return 0` / `exit 0` -- that is "
        "the exact 2026-08-04 incident this ticket closes. See #355."
    )
    assert not any(re.fullmatch(r"return\b.*", stmt) for stmt in statements), (
        "a failed peer query must `exit`, not `return` -- a bare `return` would hand "
        "control back to the caller and proceed to the cache write. See #355."
    )
    assert any(re.fullmatch(r"exit\s+[1-9][0-9]*", stmt) for stmt in statements), (
        "a failed peer query must abort via `exit <nonzero>` so the top-level "
        "`trap on_exit EXIT` uploads the log and shuts the instance down. See #355."
    )


def test_peer_query_failure_notifies_slack(script_lines: list[str]) -> None:
    """#355: a failed peer query must page out via Slack, not just log quietly.

    345 KB of bootstrap log with no authorization error anywhere is exactly
    what happened on 2026-08-04 -- nobody was reading that log. The failure
    must reach ``notify_slack`` so it surfaces without anyone tailing logs.
    """
    body = guard_body(script_lines)
    failure_line = first_line_index(body, FAILURE_MARKER)
    tail = "\n".join(body[failure_line : failure_line + 6])
    assert "notify_slack" in tail, (
        "the query-failure branch must call notify_slack so the failure surfaces "
        "without anyone reading logs. See #355."
    )


def test_peer_query_runs_after_slack_webhook_is_sourced(script_lines: list[str]) -> None:
    """#355: the guard's Slack alert is only real if the webhook is already set.

    ``notify_slack`` no-ops when ``SLACK_MONITORING_WEBHOOK`` is empty. The
    guard is called late (step 6, right before the first cache write) and the
    SSM fetch happens at step 5, so today the alert does fire -- but nothing
    else pins that ordering, and moving the guard earlier "to fail faster"
    would silently reduce it back to a log-only failure.
    """
    code = non_comment_lines(script_lines)
    webhook_assigned = first_line_index(code, 'SLACK_MONITORING_WEBHOOK="$(ssm_param')
    guard_called = last_line_index(code, "abort_if_not_winning_rebuild")
    assert webhook_assigned < guard_called, (
        "abort_if_not_winning_rebuild must run after SLACK_MONITORING_WEBHOOK is read "
        "from SSM, or its :rotating_light: alert silently no-ops and the failure is "
        "log-only again. See #355."
    )


def test_empty_result_fail_open_message_unchanged(script_lines: list[str]) -> None:
    """#355 must not touch the genuinely-empty-result fail-open kept from #311.

    ``DescribeInstances`` really is eventually consistent, the launcher
    precheck already ran, and the >3h sweeper is a backstop -- a *successful*
    query that legitimately finds zero peers must still proceed with its
    original message unchanged.
    """
    body = guard_body(script_lines)
    code = "\n".join(body)
    assert (
        "WARN: peer check found no active rebuild instances "
        "(self not yet visible?); proceeding" in code
    ), "the empty-result fail-open message from #311 must be preserved verbatim. See #355."

    empty_result_line = first_line_index(body, "no active rebuild instances")
    assert any(
        line.strip() == "return 0" for line in body[empty_result_line : empty_result_line + 2]
    ), "the empty-result branch must still fail open via `return 0`. See #355."


def test_tie_break_and_no_mutual_suicide_preserved(script_lines: list[str]) -> None:
    """#355 must leave #311's tie-break semantics exactly as they were.

    The total order is "earliest LaunchTime, ties broken by lexicographically
    smaller InstanceId", realised as a plain ``sort | head -n1`` over
    ``<LaunchTime>\\t<InstanceId>`` lines. The no-mutual-suicide property
    depends on all three of: the self-not-yet-listed branch proceeding rather
    than aborting, the winner being compared against self, and the loser
    exiting **0** (a bow-out is not a failure). A regression in any of them
    is either two concurrent writers or zero.
    """
    body = guard_body(script_lines)
    code = "\n".join(body)

    assert "LaunchTime,InstanceId" in code, (
        "the peer query must select [LaunchTime, InstanceId] in that order -- the "
        "tie-break is a lexicographic sort over those two columns. See #311."
    )
    assert re.search(r"sort\s*\|\s*head -n1", code), (
        "the winner must still be `sort | head -n1` over `<LaunchTime>\\t<InstanceId>`. See #311."
    )
    assert 'if [ "$winner" != "$INSTANCE_ID" ]; then' in code, (
        "the loser branch must still compare the computed winner against self. See #311."
    )

    # Self not yet listed -> proceed (fail-open), NOT abort. If this became an
    # abort, every instance in a same-second pair could bow out and no rebuild
    # would run at all.
    self_missing_line = first_line_index(body, "does not yet list self")
    assert any(
        line.strip() == "return 0" for line in body[self_missing_line : self_missing_line + 2]
    ), (
        "the self-not-yet-listed branch must still fail open via `return 0`; turning it "
        "into an abort risks all instances bowing out. See #311."
    )

    # The loser bows out with exit 0 -- a deliberate no-op, not a failure.
    loser_line = first_line_index(body, "BootstrapCollisionAborted")
    loser_block = [line.strip() for line in body[loser_line : loser_line + 4]]
    assert "exit 0" in loser_block, (
        "the outranked instance must still bow out with `exit 0` -- a non-zero exit "
        "would page as a failure for what is correct, intended behaviour. See #311."
    )
