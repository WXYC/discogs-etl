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
    if not path:
        return None, "no report path was given"
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
