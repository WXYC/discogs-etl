"""Loading and walking a GitHub Actions workflow under test.

Shared rather than duplicated, on the same reasoning as ``tests/cfn_yaml.py``:
this is the second workflow with a wiring-test module of its own, and a
per-file copy is how a repo ends up with several subtly different loaders.
Two copies had already disagreed on what ``_step`` means -- exact name match in
one, case-insensitive substring across ``name`` *and* ``uses`` in the other --
so both are offered here under names that say which is which.

The load-bearing piece is :func:`triggers`. **YAML 1.1 -- which PyYAML
implements -- resolves an unquoted ``on`` key to the boolean ``True``**, so
``doc["on"]`` raises ``KeyError`` on every GitHub Actions file that spells the
key the normal way. That knowledge lived in exactly one of the two copies,
which is the whole problem with two copies.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_workflow(path: Path) -> dict[str, Any]:
    """Parse a workflow file, asserting it exists first.

    The assertion is not ceremony: a renamed or moved workflow otherwise
    surfaces as a ``TypeError`` on ``None`` several frames away from the cause.
    """
    assert path.exists(), f"{path} does not exist"
    return yaml.safe_load(path.read_text())


def triggers(doc: dict[str, Any]) -> dict[str, Any]:
    """The ``on:`` block, whichever key PyYAML resolved it to.

    See the module docstring: unquoted ``on`` becomes ``True`` under YAML 1.1.
    """
    return doc.get("on", doc.get(True))  # type: ignore[arg-type]


def steps(doc: dict[str, Any], job: str) -> list[dict[str, Any]]:
    """Every step of one job."""
    return doc["jobs"][job]["steps"]


def step_named(doc: dict[str, Any], job: str, name: str) -> dict[str, Any]:
    """The step whose ``name`` matches exactly.

    Prefer this when the workflow's step names are themselves part of what is
    being pinned; a substring match would keep passing through a rename.
    """
    for step in steps(doc, job):
        if step.get("name") == name:
            return step
    raise AssertionError(
        f"no step named {name!r} in job {job!r}; have {[s.get('name') for s in steps(doc, job)]}"
    )


def step_matching(doc: dict[str, Any], job: str, needle: str) -> dict[str, Any]:
    """The first step whose ``name`` or ``uses`` contains ``needle``.

    Case-insensitive. Use when the step is identified by the action it runs
    rather than by a name the test cares about.
    """
    lowered = needle.lower()
    for step in steps(doc, job):
        if lowered in str(step.get("name", "")).lower():
            return step
        if lowered in str(step.get("uses", "")).lower():
            return step
    raise AssertionError(
        f"no step matching {needle!r} in job {job!r}; "
        f"have {[s.get('name') for s in steps(doc, job)]}"
    )


def working_directory(doc: dict[str, Any], job: str, name: str) -> str:
    """Where a step's ``run:`` executes, relative to the repo root.

    GitHub resolves ``working-directory`` step-first, then the job's
    ``defaults.run``, then the workflow's. Reading only the step's own key
    misses a job-level default entirely -- the realistic way this breaks, since
    a value repeated across sibling steps invites hoisting.
    """
    for source in (
        step_named(doc, job, name),
        doc["jobs"][job].get("defaults", {}).get("run", {}),
        doc.get("defaults", {}).get("run", {}),
    ):
        if source.get("working-directory"):
            return str(source["working-directory"])
    return "."
