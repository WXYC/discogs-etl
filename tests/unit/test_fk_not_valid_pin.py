"""Cross-file pin: every FK on release(id) is added with NOT VALID.

The dedup + verify + run_pipeline paths all DROP CASCADE and re-add FK
constraints. Each ADD CONSTRAINT validates the existing rows in the child
table against the new parent — which races with LML's runtime cache writes
inserting orphans. The fix is uniform: add the FK with NOT VALID, which
skips re-validation of existing rows but still enforces the FK on new
INSERTs.

History of the rolling rediscoveries:
  - #211: dedup_releases.py + schema/create_track_indexes.sql
  - #188 / 2026-05-14 02:20 UTC: surfaced two more sites in run_pipeline.py
    and one more in dedup_releases.py (track-side function) +
    verify_cache.py (verify-and-prune path).

This test is the durable defense: it greps every script for any
``ADD CONSTRAINT fk_*_release FOREIGN KEY ... REFERENCES release(id)``
and asserts ``NOT VALID`` follows within a few tokens. Catches future
regressions at unit-test time instead of at the next rebuild.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"

# Files that own FK constraint creation. The schema/*.sql files are
# applied via run_sql_file / alembic; the python scripts execute their
# own ALTER TABLE statements.
TARGET_FILES = [
    SCRIPTS_DIR / "run_pipeline.py",
    SCRIPTS_DIR / "dedup_releases.py",
    SCRIPTS_DIR / "verify_cache.py",
    REPO_ROOT / "schema" / "create_track_indexes.sql",
]


def _collapse_python_string_concatenation(text: str) -> str:
    """Join adjacent Python string literals into one logical line.

    Python's implicit concat (``"foo " "bar"``) lets the FK SQL be split
    across multiple source lines. To grep for ``ADD CONSTRAINT ... NOT
    VALID`` we need to join those literals into a single string so the
    pattern matches across what's logically one statement.
    """
    # Collapse ``"...end-of-line\n        "...`` into a single ``"...end-of-line...``.
    return re.sub(r'"\s*\n\s*"', " ", text)


def test_every_release_fk_constraint_uses_not_valid() -> None:
    """For every ``ALTER TABLE ... ADD CONSTRAINT fk_*_release FOREIGN KEY
    (release_id) REFERENCES release(id) ON DELETE CASCADE``, the immediately-
    following tokens must include ``NOT VALID``. Otherwise the next rebuild
    will hit a ForeignKeyViolation on the orphans LML inserts during the
    dedup swap window — exactly the failure mode of #188 / 2026-05-14
    02:20 UTC.
    """
    pattern = re.compile(
        r"ADD CONSTRAINT (fk_\w+_release) "
        r"FOREIGN KEY \(release_id\) REFERENCES release\(id\) "
        r"ON DELETE CASCADE(?P<tail>[^,;]*)",
    )

    failures: list[str] = []
    for path in TARGET_FILES:
        text = _collapse_python_string_concatenation(path.read_text())
        for m in pattern.finditer(text):
            constraint = m.group(1)
            tail = m.group("tail")
            if "NOT VALID" not in tail:
                # Show line number in the original (pre-collapse) text for usefulness.
                line = text[: m.start()].count("\n") + 1
                failures.append(
                    f"{path.relative_to(REPO_ROOT)}:~{line}  {constraint}  (tail: {tail!r})"
                )

    assert not failures, (
        "FK constraint(s) missing NOT VALID — these will race-fail the next rebuild:\n  "
        + "\n  ".join(failures)
        + "\n\nSee #211 + #188 for the failure mode. The fix is to add NOT VALID "
        "after ON DELETE CASCADE."
    )


def test_target_files_exist() -> None:
    """Sanity: if any of the TARGET_FILES disappears (refactor, rename),
    the pin above silently passes. Catch that here."""
    for path in TARGET_FILES:
        assert path.exists(), f"{path} disappeared — update TARGET_FILES"
