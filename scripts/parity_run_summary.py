#!/usr/bin/env python3
"""Render a GitHub Actions run summary for a catalog-parity soak run (#378).

``catalog_parity_diff.py --fail-on-drift`` has five meaningful exits, and the
one that matters most is the one that looks least like itself:

===== ==========================================================================
 0     the two catalogs agree -- a clean day, countable toward wiki#89 AC#4's
       seven-consecutive-clean-days gate
 4     the harness ran correctly and the data does not agree. A **verdict**,
       not a malfunction. Expected on every run until BS#2108's pending
       ``/wxycdb`` delete and the 28 residual field mismatches clear
 3     a producer could not build its input -- the Kattare SSH tunnel, MySQL,
       or the Backend service account. No verdict was computed
 2     the harness rejected its arguments -- a defect in the workflow file,
       nothing to do with the data
 1     an uncaught crash (or any other code: a segfaulting mysql client
       exits 139)
===== ==========================================================================

Read the wrong way round, exit 4 looks like a broken soak and gets muted,
while exit 3 looks like a parity failure and sends someone hunting a data
problem that does not exist. The exit status alone cannot tell them apart,
which is why this renderer exists instead of a line of inline bash.

It prints Markdown on stdout (the workflow appends it to
``$GITHUB_STEP_SUMMARY``) and exactly one GitHub workflow-command annotation
on stderr, then re-exits with the harness's own code so the job's failing step
carries that number into the log. Deliberately no ``init_logger``: this
script's output *is* its log, and JSON log lines on stderr would corrupt the
annotation.

The ``--json`` report is treated as untrusted. On exits 2 and 3 the harness
writes nothing to stdout, so the workflow's redirect leaves a zero-byte file;
a crash mid-write leaves a truncated one. Neither may traceback -- a summary
step that dies turns one legible failure into two.

Usage::

    python scripts/parity_run_summary.py --exit-code 4 --json parity.json \\
        >> "$GITHUB_STEP_SUMMARY"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

CLEAN = 0
BAD_ARGS = 2
SOURCE_ERROR = 3
DRIFT = 4

# Row-set and CTA counters, in report order. `field_mismatches` renders
# separately (it is a per-column dict) and the *_expected/*_unexplained splits
# annotate their parent count rather than getting rows of their own.
_ROW_SET_METRICS = ("matched", "missing_in_backend", "extra_in_backend")


def load_payload(path: str | None) -> tuple[dict[str, Any] | None, str | None]:
    """Read the harness's ``--json`` object.

    Returns ``(payload, problem)`` -- exactly one of which is None. Every
    failure mode collapses into a human-readable ``problem`` string rather
    than an exception, because this runs in the step that explains other
    failures.
    """
    if not path:
        return None, "no report path was given"
    file = Path(path)
    if not file.exists():
        return None, f"`{file.name}` was never written"
    raw = file.read_text().strip()
    if not raw:
        return None, f"`{file.name}` is empty -- the harness produced no verdict"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"`{file.name}` could not be parsed as JSON ({exc})"
    if not isinstance(parsed, dict):
        return None, f"`{file.name}` is {type(parsed).__name__}, not a parity report object"
    return parsed, None


def verdict_causes(payload: dict[str, Any] | None) -> list[str]:
    """The conditions in the ``clean`` conjunction that this run failed.

    ``clean`` is ``missing_unexplained == 0 and extra_unexplained == 0 and
    sum(field_mismatches) == 0 and cta_within_baseline``. Only the first three
    are recoverable from the JSON -- ``cta_within_baseline`` compares against
    the vendored ledger's baselines, which the report does not carry -- so the
    fourth is named by elimination when nothing else explains a false verdict.

    Expected residue is deliberately absent: 609 ledger-explained missing rows
    are the ledger doing its job, and listing them daily would bury the counts
    that actually changed.
    """
    if not isinstance(payload, dict):
        return []

    causes: list[str] = []
    for key, gloss in (
        (
            "missing_unexplained",
            "MySQL rows absent from Backend that the residue ledger does not explain",
        ),
        ("extra_unexplained", "Backend rows absent from MySQL that Rule A does not explain"),
    ):
        count = _as_int(payload.get(key))
        if count:
            causes.append(f"`{key}` = **{count}** -- {gloss}")

    mismatches = payload.get("field_mismatches")
    if isinstance(mismatches, dict):
        nonzero = {col: _as_int(n) for col, n in mismatches.items() if _as_int(n)}
        if nonzero:
            ranked = sorted(nonzero.items(), key=lambda kv: (-kv[1], kv[0]))
            detail = ", ".join(f"`{col}` {count}" for col, count in ranked)
            causes.append(
                f"`field_mismatches` = **{sum(nonzero.values())}** across "
                f"{len(nonzero)} column(s) -- {detail}"
            )

    if not causes and payload.get("clean") is False:
        causes.append(
            "the `compilation_track_artist` counts exceed the vendored CTA baseline -- "
            "the only remaining condition in the `clean` conjunction, named here by "
            "elimination because the report does not carry the baseline itself"
        )
    return causes


def _as_int(value: object) -> int:
    """Coerce a report field to int, treating anything unusable as 0.

    The report is untrusted (see the module docstring); a string or None where
    a count belongs must not take the summary step down.
    """
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _taxonomy(exit_code: int) -> tuple[str, str, bool]:
    """``(heading, lead paragraph, whether a verdict was computed)``."""
    if exit_code == CLEAN:
        return (
            "Catalog parity: clean",
            "**Exit 0 -- the catalogs agree.** This run counts as one clean day toward "
            "[wiki#89](https://github.com/WXYC/wiki/issues/89) AC#4's seven-consecutive-day "
            "streak. The streak is a property of consecutive *runs*, so a skipped or "
            "cancelled day resets it.",
            True,
        )
    if exit_code == DRIFT:
        return (
            "Catalog parity: drift verdict",
            "**Exit 4 -- this is a verdict, not a malfunction.** The harness ran correctly "
            "end to end; the MySQL-sourced and Backend-sourced catalogs do not agree. "
            "Expect this on every run until [BS#2108](https://github.com/WXYC/Backend-Service/issues/2108)'s "
            "pending `/wxycdb` delete and the residual field mismatches clear -- see the "
            "causes below for which condition actually fired today.",
            True,
        )
    if exit_code == SOURCE_ERROR:
        return (
            "Catalog parity: producer failure",
            "**Exit 3 -- infrastructure, not data.** One of the two `library.db` inputs "
            "could not be built: the Kattare SSH tunnel, the MySQL export, or the Backend "
            "service-account sign-in. **No verdict was computed** -- this run says nothing "
            "about parity, and neither breaks nor extends the clean-day streak. Check the "
            "producer error on stderr in the harness step above.",
            False,
        )
    if exit_code == BAD_ARGS:
        return (
            "Catalog parity: workflow defect",
            "**Exit 2 -- the harness rejected its own arguments.** This is a bug in "
            "`.github/workflows/catalog-parity.yml`, not in the catalogs. The usual cause "
            "is `--fail-on-drift` combined with `--residue-ledger none`, which asks the "
            "harness to fail on drift while refusing the definition of expected drift. "
            "**No verdict was computed.**",
            False,
        )
    return (
        "Catalog parity: unexpected failure",
        f"**Exit {exit_code} -- unexpected.** This is outside the harness's documented exit "
        "taxonomy (0, 2, 3, 4), so it is an uncaught crash or a signal from a child process "
        "-- exit 139 is the MySQL client segfaulting, which means the runner picked up a "
        "non-MariaDB client. **No verdict was computed.**",
        False,
    )


def _render_report(payload: dict[str, Any]) -> list[str]:
    """The metrics tables. Every lookup tolerates an absent key."""
    lines: list[str] = []

    verdict = payload.get("clean")
    if verdict is None:
        rendered = "`null` -- **unavailable**, the run had no residue ledger"
    else:
        rendered = f"`{str(bool(verdict)).lower()}`"
    lines.append(f"**Verdict:** {rendered}")
    lines.append("")

    lines.append("| Metric | Count |")
    lines.append("| --- | ---: |")
    for key in _ROW_SET_METRICS:
        count = _as_int(payload.get(key))
        note = ""
        if key != "matched":
            side = key.split("_")[0]
            expected = _as_int(payload.get(f"{side}_expected"))
            unexplained = _as_int(payload.get(f"{side}_unexplained"))
            note = f" ({expected} expected, {unexplained} unexplained)"
        lines.append(f"| `{key}` | {count}{note} |")

    mismatches = payload.get("field_mismatches")
    total = sum(_as_int(n) for n in mismatches.values()) if isinstance(mismatches, dict) else 0
    lines.append(f"| `field_mismatches` | {total} |")
    for key in ("cta_missing", "cta_extra"):
        lines.append(f"| `{key}` | {_as_int(payload.get(key))} |")
    lines.append("")

    normalizations = payload.get("normalizations")
    if isinstance(normalizations, dict) and normalizations:
        lines.append("<details><summary>Normalizations (reported, never gating)</summary>")
        lines.append("")
        lines.append("| Column | Class | Count |")
        lines.append("| --- | --- | ---: |")
        for column in sorted(normalizations):
            classes = normalizations[column]
            if not isinstance(classes, dict):
                continue
            for cls in sorted(classes):
                lines.append(f"| `{column}` | `{cls}` | {_as_int(classes[cls])} |")
        lines.append("")
        lines.append("</details>")
        lines.append("")
    return lines


def render(exit_code: int, payload: dict[str, Any] | None, problem: str | None) -> str:
    heading, lead, verdict_computed = _taxonomy(exit_code)
    lines = [f"## {heading}", "", lead, ""]

    if payload is not None:
        lines.extend(_render_report(payload))
        causes = verdict_causes(payload)
        if causes:
            lines.append("**Why the verdict is not clean:**")
            lines.append("")
            lines.extend(f"- {cause}" for cause in causes)
            lines.append("")
    elif verdict_computed:
        # The exit code promised a verdict and the report cannot supply it.
        lines.append(f"> The `--json` report could not be read: {problem}.")
        lines.append("")
    else:
        lines.append(f"_No report to summarise: {problem}._")
        lines.append("")

    lines.append(
        "The full `--json` report is attached to this run as an artifact. "
        "Harness: `scripts/catalog_parity_diff.py` "
        "([#370](https://github.com/WXYC/discogs-etl/issues/370)); "
        "schedule: [#378](https://github.com/WXYC/discogs-etl/issues/378)."
    )
    return "\n".join(lines) + "\n"


def annotation(exit_code: int, payload: dict[str, Any] | None) -> str:
    """One GitHub workflow command, on one line.

    GitHub truncates an annotation at its first newline, so a multi-line
    message silently loses everything after the first line.
    """
    if exit_code == CLEAN:
        return (
            "::notice title=Catalog parity clean::"
            "The MySQL-sourced and Backend-sourced catalogs agree; this run counts "
            "toward the wiki#89 seven-day streak."
        )
    if exit_code == DRIFT:
        causes = verdict_causes(payload)
        detail = "; ".join(_plain(cause) for cause in causes) if causes else "see the run summary"
        return (
            "::error title=Catalog parity drift::"
            f"Verdict (exit 4), not a broken run -- the harness completed and the catalogs "
            f"disagree. {detail}."
        )
    if exit_code == SOURCE_ERROR:
        return (
            "::error title=Catalog parity producer failure::"
            "Exit 3: a library.db input could not be built (SSH tunnel, MySQL export, or "
            "Backend sign-in). No parity verdict was computed."
        )
    if exit_code == BAD_ARGS:
        return (
            "::error title=Catalog parity workflow defect::"
            "Exit 2: the harness rejected its arguments -- a bug in catalog-parity.yml, "
            "not in the catalogs. No parity verdict was computed."
        )
    return (
        "::error title=Catalog parity crashed::"
        f"Exit {exit_code} is outside the harness's exit taxonomy (0/2/3/4). No parity "
        "verdict was computed."
    )


def _plain(markdown: str) -> str:
    """Strip the Markdown emphasis a cause carries for the step summary.

    Annotations render as plain text, where backticks and asterisks are noise.
    """
    return markdown.replace("`", "").replace("**", "")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Render a GitHub Actions step summary for a catalog_parity_diff.py run, "
            "distinguishing the drift verdict (exit 4) from infrastructure and harness "
            "failures (exits 1/2/3). Markdown on stdout, one annotation on stderr; "
            "re-exits with the harness's own code."
        ),
    )
    p.add_argument(
        "--exit-code",
        type=int,
        required=True,
        help="The exit code catalog_parity_diff.py returned.",
    )
    p.add_argument(
        "--json",
        default=None,
        metavar="PATH",
        help=(
            "Path to the harness's --json report. Optional and untrusted: a missing, "
            "empty, or truncated file degrades the summary rather than failing it."
        ),
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    payload, problem = load_payload(args.json)
    sys.stdout.write(render(args.exit_code, payload, problem))
    sys.stderr.write(annotation(args.exit_code, payload) + "\n")
    return args.exit_code


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
