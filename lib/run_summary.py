"""Plumbing shared by the GitHub Actions run-summary renderers (#384).

``scripts/parity_run_summary.py`` (#378) and ``scripts/fffd_capture_summary.py``
(#382) render two deliberately separate verdicts -- the catalog-parity soak's
exit 4 and the U+FFFD capture's exit 4 mean opposite things, and collapsing
them into one renderer is what both exist to prevent.

What does *not* differ between them is reading the report. Both are handed a
``--json`` path written by a shell redirect that creates the file before the
producer runs, so both see the same degraded inputs -- zero-byte on exits 2 and
3, truncated on a crash mid-write -- and both run in the step whose job is to
explain other failures, where a traceback turns one legible failure into two.
Two copies of that logic would drift the moment either was hardened, so it
lives here once.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_text(path: str | None, *, noun: str) -> tuple[str | None, str | None]:
    """Read a producer's output file as text.

    Returns ``(text, problem)`` -- exactly one of which is None. Every failure
    mode collapses into a human-readable ``problem`` string rather than an
    exception.

    Split out of :func:`load_payload` for ``scripts/sam_deploy_summary.py``
    (#396), whose producer is ``sam deploy`` and whose output is a captured
    console log rather than JSON. The degraded population is identical --
    absent because an earlier step died, zero-byte because the redirect
    created the file before the producer ran, truncated or undecodable because
    the producer was killed mid-write -- so the handling is shared rather than
    copied a third time.

    Args:
        path: The reported path, or None when the caller was given no path
            argument at all.
        noun: What the file is, for the problem strings -- e.g.
            ``"parity report"``, ``"capture report"``, ``"deploy log"``.

    Returns:
        The file's stripped contents and None, or None and a problem
        description.
    """
    if not path:
        return None, f"no {noun} path was given"
    file = Path(path)
    if not file.exists():
        return None, f"`{file.name}` was never written"
    try:
        raw = file.read_text(encoding="utf-8").strip()
    except UnicodeDecodeError as exc:
        # Deliberately not errors="replace": the capture's entire subject
        # matter is bytes that were mis-decoded into U+FFFD, and silently
        # doing the same thing to its own report would be the one failure
        # mode nobody would think to look for.
        return None, f"`{file.name}` is not valid UTF-8 ({exc.reason})"
    except OSError as exc:
        return None, f"`{file.name}` could not be read ({exc})"
    if not raw:
        return None, f"`{file.name}` is empty -- the producer wrote no {noun}"
    return raw, None


def load_payload(path: str | None, *, noun: str) -> tuple[dict[str, Any] | None, str | None]:
    """Read a producer's ``--json`` object.

    Returns ``(payload, problem)`` -- exactly one of which is None. Every
    failure mode collapses into a human-readable ``problem`` string rather
    than an exception.

    Args:
        path: The reported path, or None when the caller was given no
            ``--json`` argument at all.
        noun: What the file is, for the problem strings -- e.g.
            ``"parity report"`` or ``"capture report"``.

    Returns:
        The parsed object and None, or None and a problem description.
    """
    raw, problem = load_text(path, noun=noun)
    if raw is None:
        return None, problem
    # `path` is necessarily truthy here -- load_text returns text only for a
    # path it could open. Not defended with `or ""`: that would turn a future
    # contract change into messages naming an empty file, and quietly-wrong
    # output about a producer's run is the failure mode this module exists to
    # avoid. A TypeError is the better answer.
    file = Path(path)  # type: ignore[arg-type]
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"`{file.name}` could not be parsed as JSON ({exc})"
    if not isinstance(parsed, dict):
        return None, f"`{file.name}` is {type(parsed).__name__}, not a {noun} object"
    return parsed, None


def plain(markdown: str) -> str:
    """Strip the Markdown emphasis a summary line carries.

    GitHub annotations render as plain text, where backticks and asterisks are
    noise rather than formatting.
    """
    return markdown.replace("`", "").replace("**", "")


def annotation_line(kind: str, *, title: str, body: str) -> str:
    """One GitHub workflow command, on one line.

    Two invariants that are easy to state and easy to lose, which is why the
    three renderers call this rather than each formatting the string:

    * **One line.** GitHub truncates an annotation at its first newline, so a
      multi-line body silently loses everything after the first.
    * **Plain text.** Annotations are not rendered as Markdown, so the
      backticks and asterisks a summary line carries show up as literal
      punctuation. Every body goes through :func:`plain`.

    The second one had already drifted while this lived in three places:
    ``parity_run_summary.py`` stripped its appended causes but not its stored
    ``annotation_body``, and nothing caught it because the test asserted that
    the module had *imported* ``plain``, not that its output was stripped. It
    was latent only because parity's bodies happened to carry no backticks.

    Args:
        kind: The workflow-command level -- ``notice``, ``warning``, ``error``.
        title: The annotation title, shown in bold in the run UI.
        body: The message, in the summary's Markdown register; stripped here.

    Returns:
        The complete ``::kind title=...::body`` line, without a trailing
        newline.
    """
    return f"::{kind} title={title}::{plain(body)}"
