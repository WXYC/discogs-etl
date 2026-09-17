"""Primitives shared by both ``library.db`` producers.

Extracted verbatim from ``scripts/catalog_parity_diff.py`` (WXYC/discogs-etl#346)
so the Backend producer can be imported by the daily sync without dragging in
the whole parity harness. The MySQL producer still lives in that script and
imports these back, so both sides keep building through exactly one
implementation of "refuse to clobber, build atomically, publish on success".

Names keep their original spelling, leading underscores included, so the
extraction reads as a move rather than a rewrite and the harness's existing
tests exercise this code unchanged through the re-exports.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

from lib.library_db import build_library_db


class SourceError(RuntimeError):
    """A library.db side is unusable.

    Either an input file (missing, unreadable, missing a required table, or
    malformed -- e.g. a duplicate id) or a producer that could not build one
    (bad DSN/URL, missing credentials, a refused overwrite, an unreachable
    source, or an inconsistent snapshot).
    """


# --- Producers: build a library.db from a live source (#351) --------------

# One row of either output table, already mapped out of its wire dict: a
# `library` row in LIBRARY_INSERT_COLUMNS order, or a (library_release_id,
# artist_name, track_title) triple. Element 0 is the library release id in
# both, which is what the snapshot-consistency check keys on.
_Row = Sequence[object]


def _require_absent(output_path: str, label: str) -> None:
    """Refuse to build over an existing file.

    The harness builds scratch copies; the one thing it must never do is
    overwrite a real ``library.db`` (or the prebuilt file the operator passed
    as the other side of the diff).
    """
    if Path(output_path).exists():
        raise SourceError(
            f"refusing to build the {label} library.db at {output_path}: the path "
            f"already exists. Producers only ever write to a fresh path -- point the "
            f"output at a new one (or delete that one deliberately)."
        )
    parent = Path(output_path).parent
    if not parent.is_dir():
        raise SourceError(
            f"cannot build the {label} library.db at {output_path}: its parent "
            f"directory {str(parent)!r} does not exist"
        )


@contextmanager
def _atomic_output(output_path: str, label: str) -> Iterator[str]:
    """Yield a scratch path to build into, published only on success.

    ``_require_absent`` refuses any path that already exists, so a build that
    dies partway -- a duplicate id, a full disk, a killed process -- must not
    leave a stub behind: it would refuse every subsequent run of a
    seven-clean-day parity soak until an operator deleted it by hand. Build
    beside the target (same filesystem, so the publish is a rename) and
    ``os.replace`` it into place at the end.
    """
    _require_absent(output_path, label)
    target = Path(output_path)
    fd, scratch = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".partial", dir=target.parent)
    os.close(fd)
    try:
        yield scratch
    except BaseException:
        Path(scratch).unlink(missing_ok=True)
        raise
    os.replace(scratch, output_path)


def _build_into(
    output_path: str,
    label: str,
    library_rows: Iterable[_Row],
    cta_rows: Iterable[_Row] | None,
) -> int:
    """Build a library.db at ``output_path`` atomically, as a ``SourceError`` seam.

    SQLite write failures (a duplicate id from a malformed export, a full
    disk) are producer failures like any other, so they surface as
    ``SourceError`` -- exit 3 -- rather than a raw traceback.
    """
    try:
        with _atomic_output(output_path, label) as scratch:
            return build_library_db(scratch, library_rows, cta_rows, report=_report)
    except sqlite3.Error as exc:
        raise SourceError(
            f"failed to write the {label} library.db at {output_path}: {exc}"
        ) from exc


def _report(message: str) -> None:
    """Emit a producer progress line on stderr.

    Deliberately not stdout: under ``--json`` stdout carries exactly one JSON
    object, and a progress line there would break every machine consumer of
    this harness. (``tsv_to_sqlite.py``, whose stdout is a human log, keeps
    the default.)
    """
    print(message, file=sys.stderr)
