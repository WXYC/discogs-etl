"""Pin the ERR-trap exit-code-propagation invariants of ``scripts/rebuild-cache.sh``.

The 2026-06-07 manual jumpstart rebuild (#267) failed at the converter step,
but the wrapping script exited 0 — so the bootstrap ``on_exit`` logged a clean
success and no Slack failure ping fired. Root cause: the ERR trap installed
after ``WORK_DIR`` exists runs ``rm -rf "$WORK_DIR"`` BEFORE calling
``on_error``, which clobbers ``$?`` to the rm's exit status (~always 0).
``on_error`` then reads ``$?`` as 0, posts a misleading "exit 0" failure
warning, and exits 0 — masking the real failure.

The first ERR trap (``trap 'on_error $LINENO' ERR``) is safe today because
nothing runs between the failing command and ``on_error``. But the same
regression could re-enter if a future change adds cleanup in front of the
on_error call.

These tests pin both shapes by asserting every ERR trap either:
  (a) calls ``on_error`` as its only command (preserving ``$?``), OR
  (b) captures ``$?`` into a variable as its first command and threads the
      captured value through to ``on_error`` as a positional arg.

The Python-layer half of the same silent-failure symptom was fixed in
#180; this is the bash-layer half. See #269.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache.sh"

# Match ``trap '<body>' ERR``. The script uses single-quoted trap bodies;
# if that convention ever changes, this regex needs to grow.
TRAP_ERR_RE = re.compile(r"""trap\s+'([^']+)'\s+ERR""")

# Match a leading ``var=$?`` assignment (optionally prefixed with
# ``local`` / ``declare``). This is the safe pattern that snapshots the
# exit code before any cleanup command can overwrite it.
EXIT_CODE_CAPTURE_RE = re.compile(
    r"""^\s*(?:local\s+|declare\s+)?[A-Za-z_][A-Za-z0-9_]*=\$\?\s*$"""
)


@pytest.fixture(scope="module")
def script_text() -> str:
    return SCRIPT_PATH.read_text()


def _split_trap_commands(body: str) -> list[str]:
    # Bash trap bodies can chain with ``;``. We only care about the
    # FIRST top-level command, so a simple split is sufficient to
    # surface the #269 bug pattern.
    return [c.strip() for c in body.split(";") if c.strip()]


def test_err_traps_propagate_real_exit_code(script_text: str) -> None:
    """#269: every ERR trap must preserve the failing command's ``$?``.

    Each ERR trap is either:
      - A single-command trap that calls ``on_error`` directly (safe by
        construction: ``$?`` is unchanged when on_error runs).
      - A multi-command trap whose FIRST command captures ``$?`` (so the
        subsequent commands cannot clobber the exit code that on_error
        ultimately propagates).

    A multi-command trap that runs any non-capturing command before
    on_error is the bug #269 fixed: the intervening command's exit
    status (usually 0 from a successful ``rm -rf``) becomes what
    on_error sees, and the real failure exits 0 silently.
    """
    traps = TRAP_ERR_RE.findall(script_text)
    assert traps, "no ERR trap found in rebuild-cache.sh — test setup is wrong"

    for body in traps:
        cmds = _split_trap_commands(body)
        assert cmds, f"empty trap body: {body!r}"
        first = cmds[0]
        if len(cmds) == 1:
            assert "on_error" in first, (
                f"single-command ERR trap must call on_error; got: {first!r}"
            )
        else:
            assert EXIT_CODE_CAPTURE_RE.match(first), (
                f"multi-command ERR trap must capture $? as its first "
                f"command, otherwise on_error reads $? as the exit code of "
                f"whatever ran between the failure and on_error. "
                f"Got first command: {first!r}; full trap body: {body!r}. "
                f"See #269."
            )


def test_on_error_accepts_explicit_exit_code(script_text: str) -> None:
    """#269: on_error must accept an explicit exit_code arg.

    The fix for the multi-command trap depends on threading the captured
    ``$?`` through to ``on_error`` as a positional arg (since ``$?`` has
    already been clobbered by the cleanup command). Pin that on_error's
    signature reads ``exit_code`` from ``$2`` with a ``$?`` fallback, so
    both styles of caller — single-command trap (no arg) and
    multi-command trap (explicit arg) — work correctly.
    """
    fn_match = re.search(
        r"^on_error\s*\(\s*\)\s*\{(.*?)^\}",
        script_text,
        re.DOTALL | re.MULTILINE,
    )
    assert fn_match, "could not locate on_error function in rebuild-cache.sh"
    body = fn_match.group(1)

    # The exit_code assignment must be the FIRST statement in the body
    # (any prior ``local foo=...`` would clobber $? before the fallback
    # could read it). And it must use the ``${2:-$?}`` shape so an
    # explicit arg overrides the fallback.
    first_non_blank = next(
        (line.strip() for line in body.splitlines() if line.strip()),
        "",
    )
    assert re.match(
        r"""^local\s+exit_code\s*=\s*"?\$\{2:-\$\?\}"?\s*$""",
        first_non_blank,
    ), (
        "on_error's FIRST statement must be `local exit_code=${2:-$?}` "
        "so multi-command ERR traps can thread the original exit code "
        "through cleanly without losing it to a preceding cleanup "
        f"command. Got first statement: {first_non_blank!r}. See #269."
    )
