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
