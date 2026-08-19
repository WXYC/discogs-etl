"""Every alembic revision id must fit ``alembic_version.version_num``.

Alembic creates that column as ``VARCHAR(32)``. A revision id longer than 32
characters parses, imports, and passes every offline check — then fails at the
very end of ``alembic upgrade``, when the version table is stamped:

    psycopg.errors.StringDataRightTruncation:
    value too long for type character varying(32)

The migration's DDL has already been applied at that point, so the upgrade
aborts *after* mutating the schema but *before* recording that it did. This
repo came within one character of shipping that: 0008 is 31 characters, and
0014 was originally drafted at 36.

A cheap length assertion catches it at authoring time instead of against a
production database mid-rebuild.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
VERSIONS_DIR = REPO_ROOT / "alembic" / "versions"

# Alembic's own DDL for the version table.
_VERSION_NUM_MAX_LENGTH = 32

_REVISION_RE = re.compile(r'^revision: str = "(.+?)"', re.MULTILINE)


def _revision_ids() -> dict[str, str]:
    """Map revision id -> defining filename, for every versions/*.py."""
    found: dict[str, str] = {}
    for path in sorted(VERSIONS_DIR.glob("[0-9]*.py")):
        match = _REVISION_RE.search(path.read_text())
        assert match is not None, f'{path.name} has no `revision: str = "..."` assignment'
        found[match.group(1)] = path.name
    return found


def test_revision_ids_were_actually_found() -> None:
    """Vacuity guard — an empty glob would make the length test unfalsifiable."""
    revisions = _revision_ids()
    assert len(revisions) >= 14, (
        f"Only found {len(revisions)} revision ids in {VERSIONS_DIR}. The glob or the "
        f"`revision: str = ` convention changed; the length test below is vacuous."
    )


def test_every_revision_id_fits_the_version_column() -> None:
    too_long = {
        rev: (len(rev), filename)
        for rev, filename in _revision_ids().items()
        if len(rev) > _VERSION_NUM_MAX_LENGTH
    }
    assert not too_long, (
        f"Revision ids exceed alembic_version.version_num's VARCHAR({_VERSION_NUM_MAX_LENGTH}): "
        f"{too_long}. `alembic upgrade` applies the migration's DDL and only then stamps the "
        f"version table, so an over-long id fails AFTER mutating the schema. Shorten the id "
        f"(and rename the file to match)."
    )


def test_revision_id_matches_its_filename() -> None:
    """The repo's convention: versions/<revision>.py. Keeps `alembic history` greppable."""
    mismatched = {
        rev: filename for rev, filename in _revision_ids().items() if filename != f"{rev}.py"
    }
    assert not mismatched, (
        f"Revision ids disagree with their filenames: {mismatched}. Rename the file to "
        f"<revision>.py so the id is discoverable from the directory listing."
    )
