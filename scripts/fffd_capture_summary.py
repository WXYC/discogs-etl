#!/usr/bin/env python3
"""Render a GitHub Actions run summary for a U+FFFD CTA pair capture (#382).

``catalog_parity_diff.py --capture-fffd-cta-pairs`` runs as an opt-in step of
the catalog-parity soak, so its verdict lands in the same run summary as the
soak's own -- and the two share an exit code that means opposite things:

    --fail-on-drift        4 -> the two catalogs do not agree. The expected
                                daily state; no action.
    --capture-fffd-...     4 -> the capture is INCOMPLETE. Some corrupt rows
                                could not be paired to a MySQL truth, so the
                                repair set handed to BS#2152 is short.

Nothing in a red X distinguishes them, which is why this renderer exists
alongside ``parity_run_summary.py`` rather than as a second branch inside it:
two verdicts, two meanings, two steps that fail independently.

The unresolved *reason* is the actionable part, and the three do not call for
the same response:

``corrupt_candidates``
    The only MySQL rows that fit carry U+FFFD themselves -- tubafrenzy lost
    the bytes too. **Unrecoverable**: no re-import can supply what neither
    side still holds.
``multiple_candidates``
    More than one MySQL row fits the mask. The harness refuses to guess; a
    human picks, using ``track_position``.
``zero_candidates``
    No MySQL row on that release agrees at every non-U+FFFD position. Either
    the row changed in tubafrenzy since, or the corruption did not preserve
    the character count (a truncated multi-byte sequence decodes to one
    U+FFFD, not one per byte).

The ``--json`` report is untrusted, exactly as in ``parity_run_summary.py``:
on exits 2 and 3 the harness writes nothing to stdout, so the workflow's
redirect leaves a zero-byte file, and a crash mid-write leaves a truncated
one. Neither may traceback -- a summary step that dies turns one legible
failure into two.

Usage::

    python scripts/fffd_capture_summary.py --exit-code 4 --json fffd-pairs.json \\
        >> "$GITHUB_STEP_SUMMARY"
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.run_summary import load_payload as _load_payload  # noqa: E402
from lib.run_summary import plain as _plain  # noqa: E402

# The untrusted-report reader, shared with parity_run_summary.py so a
# hardening fix reaches both (#384). Bound here with this tool's noun.
load_payload = partial(_load_payload, noun="capture report")

COMPLETE = 0
BAD_ARGS = 2
SOURCE_ERROR = 3
INCOMPLETE = 4
# Not a process exit status -- the capture step was skipped and never returned
# one. Kept outside 0-255 so it can never collide with a code the harness
# actually produced.
NOT_RUN = -1

ISSUE = "[BS#2152](https://github.com/WXYC/Backend-Service/issues/2152)"

# Only the reasons that actually fired get a gloss -- see the module docstring
# for why each one means something different to whoever reads this.
_REASON_GLOSS: dict[str, str] = {
    "corrupt_candidates": (
        "the matching MySQL rows carry U+FFFD themselves, so **tubafrenzy lost the bytes "
        "too**. This is the one reason that is **unrecoverable**: a re-import cannot supply "
        "what neither side still holds. Do not send these back for another capture"
    ),
    "multiple_candidates": (
        "more than one MySQL row fits the mask equally well, and the harness refuses to "
        "guess. Resolvable by hand from `track_position` and the candidate list in the "
        "report"
    ),
    "zero_candidates": (
        "no MySQL row on that release agrees at every non-U+FFFD position. Either the row "
        "changed in tubafrenzy after the corruption, or the character count was not "
        "preserved -- a truncated multi-byte sequence decodes to a single U+FFFD rather "
        "than one per byte"
    ),
}


@dataclass(frozen=True)
class _Outcome:
    """Everything one exit code means, in one place.

    Same rationale as ``parity_run_summary._Outcome``: the summary and the
    annotation say the same thing in two registers, and branching on the exit
    code separately in each is how they drift apart.
    """

    heading: str
    lead: str
    annotation_title: str
    annotation_body: str
    capture_taken: bool


_OUTCOMES: dict[int, _Outcome] = {
    COMPLETE: _Outcome(
        heading="U+FFFD capture: complete",
        lead=(
            "**Exit 0 -- every corrupt row paired.** Each Backend "
            "`compilation_track_artist` value containing U+FFFD was matched to exactly one "
            "MySQL row, and the `sql_values` block in the attached report is the complete "
            f"repair set for {ISSUE}."
        ),
        annotation_title="U+FFFD capture complete",
        annotation_body=(
            "Every corrupt compilation_track_artist row paired to a MySQL truth; the "
            "attached report carries the full BS#2152 repair set."
        ),
        capture_taken=True,
    ),
    INCOMPLETE: _Outcome(
        heading="U+FFFD capture: incomplete",
        lead=(
            "**Exit 4 -- the capture ran, and some rows are unresolved.** This is *not* the "
            "parity verdict reported separately in this run: the harness paired what it "
            "could and refused to guess the rest, so the repair set below is correct but "
            "short. The resolved rows are usable as they stand -- the reason breakdown says "
            "what, if anything, can be done about the others."
        ),
        annotation_title="U+FFFD capture incomplete",
        annotation_body=(
            "Exit 4: rows were left unresolved (not a parity verdict). Resolved rows are "
            "still usable."
        ),
        capture_taken=True,
    ),
    SOURCE_ERROR: _Outcome(
        heading="U+FFFD capture: producer failure",
        lead=(
            "**Exit 3 -- no capture was taken.** A source could not be read or the report "
            "could not be written: the pre-built `library.db`, the Backend service-account "
            "sign-in, the per-release `GET /library/{id}/compilation-tracks` follow-up, or "
            "the destination path. **No rows were captured**, so this run says nothing about "
            f"how many of {ISSUE}'s rows are repairable. If the write itself failed, the "
            "capture was still emitted on stdout in the step log -- recover it from there "
            "rather than re-running."
        ),
        annotation_title="U+FFFD capture producer failure",
        annotation_body=(
            "Exit 3: no capture was taken (source read, Backend sign-in, or report write failed)."
        ),
        capture_taken=False,
    ),
    NOT_RUN: _Outcome(
        heading="U+FFFD capture: not run",
        lead=(
            "**The capture was requested but never ran.** It is gated on the parity harness "
            "having completed -- exits 0 and 4 -- because those are the only codes that mean "
            "the MySQL export finished and the `library.db` it pairs against is whole. The "
            "harness did not complete on this run, so there was nothing to pair against and "
            "the step was skipped. **Nothing failed here**: the parity verdict reported "
            "separately in this run carries the actual cause, and re-dispatching once that is "
            "fixed will take the capture."
        ),
        annotation_title="U+FFFD capture not run",
        annotation_body=(
            "Skipped: the parity harness did not complete, so there was no library.db to "
            "pair against. See the parity verdict for the cause."
        ),
        capture_taken=False,
    ),
    BAD_ARGS: _Outcome(
        heading="U+FFFD capture: workflow defect",
        lead=(
            "**Exit 2 -- the harness rejected its own arguments.** A bug in "
            "`.github/workflows/catalog-parity.yml`, not in the data. This mode requires "
            "`--backend-source` (the bulk CTA export omits `track_position`), either "
            "`--mysql-source` or a pre-built `--mysql-db`, and an output directory that "
            "already exists. **No capture was taken.**"
        ),
        annotation_title="U+FFFD capture workflow defect",
        annotation_body=(
            "Exit 2: the harness rejected its arguments -- a bug in catalog-parity.yml. No "
            "capture was taken."
        ),
        capture_taken=False,
    ),
}


def _outcome(exit_code: int) -> _Outcome:
    """The record for this exit code, synthesising one for the catch-all."""
    known = _OUTCOMES.get(exit_code)
    if known is not None:
        return known
    return _Outcome(
        heading="U+FFFD capture: unexpected failure",
        lead=(
            f"**Exit {exit_code} -- unexpected.** This is outside the capture mode's "
            "documented exit taxonomy (0, 2, 3, 4), so it is the harness crashing or the "
            "step being killed from outside (the job timeout, a cancellation, the runner "
            "going away). **No capture was taken.**"
        ),
        annotation_title="U+FFFD capture crashed",
        annotation_body=(
            f"Exit {exit_code} is outside the capture mode's taxonomy (0/2/3/4). No capture "
            "was taken."
        ),
        capture_taken=False,
    )


def _rows(payload: dict[str, Any] | None, key: str) -> list[dict[str, Any]]:
    """The ``resolved``/``unresolved`` list, with every non-object dropped.

    The report is untrusted: a string is iterable and a dict is not a list of
    rows, so both have to be rejected explicitly rather than iterated.
    """
    if not isinstance(payload, dict):
        return []
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def reason_counts(payload: dict[str, Any] | None) -> Counter[str]:
    """How many unresolved rows fell to each reason.

    Counts whatever string the report carries rather than a fixed set, so a
    reason added to ``lib/fffd_pair_capture.py`` still shows up here instead
    of vanishing from the totals.
    """
    counts: Counter[str] = Counter()
    for row in _rows(payload, "unresolved"):
        reason = row.get("reason")
        counts[reason if isinstance(reason, str) and reason else "(unlabelled)"] += 1
    return counts


def _render_report(payload: dict[str, Any]) -> list[str]:
    """The counts table and the reason breakdown."""
    resolved = _rows(payload, "resolved")
    unresolved = _rows(payload, "unresolved")

    lines = [
        "| Rows | Count |",
        "| --- | ---: |",
        f"| resolved | {len(resolved)} |",
        f"| unresolved | {len(unresolved)} |",
        "",
    ]

    counts = reason_counts(payload)
    if counts:
        lines.append("**Why rows were left unresolved:**")
        lines.append("")
        for reason, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
            gloss = _REASON_GLOSS.get(reason)
            if gloss:
                lines.append(f"- `{reason}` = **{count}** -- {gloss}.")
            else:
                lines.append(
                    f"- `{reason}` = **{count}** -- not a reason this renderer knows; see "
                    "`lib/fffd_pair_capture.py`."
                )
        lines.append("")

    sql = payload.get("sql_values")
    if resolved and isinstance(sql, str) and sql.strip():
        lines.append(
            f"The report's `sql_values` block holds {len(resolved)} `pending_cta_repair` "
            "VALUES tuple(s), ready to paste into Backend-Service's "
            "`scripts/audit/bs_replacement_char_cta.sql` in place of its placeholder row."
        )
        lines.append("")
    return lines


def render(exit_code: int, payload: dict[str, Any] | None, problem: str | None) -> str:
    outcome = _outcome(exit_code)
    lines = [f"## {outcome.heading}", "", outcome.lead, ""]

    if payload is not None:
        lines.extend(_render_report(payload))
    elif outcome.capture_taken:
        # The exit code promised a capture and the report cannot supply it.
        lines.append(f"> The `--json` report could not be read: {problem}.")
        lines.append("")
    elif exit_code != NOT_RUN:
        # Why the report is missing is worth naming for every outcome where
        # one should have existed. NOT_RUN is the exception: nothing was ever
        # going to write it, and the lead already says why.
        lines.append(f"_No report to summarise: {problem}._")
        lines.append("")

    artifact = (
        "The full capture is attached to this run as an artifact. " if outcome.capture_taken else ""
    )
    lines.append(
        f"{artifact}Mode: `catalog_parity_diff.py --capture-fffd-cta-pairs` "
        "([#382](https://github.com/WXYC/discogs-etl/issues/382)); consumer: "
        f"{ISSUE}."
    )
    return "\n".join(lines) + "\n"


def annotation(exit_code: int, payload: dict[str, Any] | None) -> str:
    """One GitHub workflow command, on one line.

    GitHub truncates an annotation at its first newline, so a multi-line
    message silently loses everything after the first line.
    """
    outcome = _outcome(exit_code)
    # NOT_RUN is a notice, not an error: the parity verdict step already fails
    # the job for the exit 2/3 that caused the skip, and a second error
    # annotation for one cause reads as two independent failures.
    kind = "notice" if exit_code in (COMPLETE, NOT_RUN) else "error"
    body = outcome.annotation_body
    if exit_code == INCOMPLETE:
        # The one code whose annotation earns per-run detail: which reasons
        # fired decides what happens next, and `corrupt_candidates` in
        # particular means nobody should re-run this hoping for more.
        counts = reason_counts(payload)
        detail = (
            ", ".join(f"{reason} {count}" for reason, count in sorted(counts.items()))
            if counts
            else "see the run summary"
        )
        body = f"{body} Reasons: {detail}."
    return f"::{kind} title={outcome.annotation_title}::{_plain(body)}"


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Render a GitHub Actions step summary for a catalog_parity_diff.py "
            "--capture-fffd-cta-pairs run, distinguishing an incomplete capture (exit 4) "
            "from the parity soak's drift verdict, which shares that code. Markdown on "
            "stdout, one annotation on stderr; re-exits with the capture's own code."
        ),
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--exit-code",
        type=int,
        help="The exit code the capture mode returned.",
    )
    mode.add_argument(
        "--not-run",
        action="store_true",
        help=(
            "The capture step was skipped -- the parity harness did not complete, so there "
            "was no library.db to pair against. Renders as a notice and exits 0: nothing "
            "failed here, and the parity verdict carries the real cause. Distinct from "
            "--exit-code 3, which means a capture was attempted and its source or write "
            "failed."
        ),
    )
    p.add_argument(
        "--json",
        default=None,
        metavar="PATH",
        help=(
            "Path to the capture's report. Optional and untrusted: a missing, empty, or "
            "truncated file degrades the summary rather than failing it."
        ),
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    code = NOT_RUN if args.not_run else args.exit_code
    payload, problem = load_payload(None if args.not_run else args.json)
    sys.stdout.write(render(code, payload, problem))
    sys.stderr.write(annotation(code, payload) + "\n")
    # A skipped capture is not a failure of its own -- the parity verdict step
    # fails the job for the cause. Every real code is re-raised as itself.
    return 0 if args.not_run else args.exit_code


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
