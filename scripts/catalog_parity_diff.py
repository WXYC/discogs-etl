"""Diff-two-files core of the discogs-etl#346 catalog-parity harness.

discogs-etl builds the production ``library.db`` nightly from tubafrenzy's
MySQL database (``scripts/sync-library.sh``). To retire that MySQL
dependency ahead of the 2026-08-31 tubafrenzy turndown, the daily build must
move to Backend-Service as its catalog source -- but only once a
Backend-sourced ``library.db`` is proven equivalent to the MySQL-sourced one.

This script is the comparison core of that proof: given two already-built
``library.db`` SQLite files, it diffs them field-by-field and reports where
they diverge, so an operator can drive the migration to zero drift (or an
explicitly accepted residue).

**Out of scope**: building a fresh ``library.db`` from a live source (direct
Postgres vs. an HTTP call to Backend-Service) is a separate, not-yet-decided
piece of work -- see ``_build_library_db_from_mysql`` /
``_build_library_db_from_backend`` below and WXYC/discogs-etl#346. The
``--mysql-source`` / ``--backend-source`` flags are reserved for that future
producer step; passing them today is a CLI usage error (exit 2), not a
partial implementation.

Schema note: the ``library`` table's 12 columns (``id, title, artist,
call_letters, artist_call_number, release_call_number, genre, format,
alternate_artist_name, album_artist, label, cross_reference_names``) are
derived from ``scripts/tsv_to_sqlite.py``'s ``CREATE TABLE library`` -- the
authoritative daily-sync shape. ``label`` is always NULL in production (the
TSV INSERT never includes it -- see that script's module docstring), so it
is excluded from the diffed column set below rather than compared as a
trivially-always-equal no-op.

Usage::

    python scripts/catalog_parity_diff.py \\
        --mysql-db /path/to/mysql-sourced/library.db \\
        --backend-db /path/to/backend-sourced/library.db \\
        --json

Exit codes:

- ``0`` -- ran successfully. A nonzero diff count is still exit 0; the
  operator reads the counts (and, in ``--json`` mode, the id lists) to judge
  parity.
- ``2`` -- bad arguments (missing required flags, or the reserved
  build-from-source flags).
- ``3`` -- source/read error: missing file, unreadable database, or a
  required table (``library``) absent from one of the inputs.

Never writes to either input: both files are opened as read-only SQLite
connections (``mode=ro``).
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)


class SourceError(RuntimeError):
    """A library.db input could not be read: missing file, unreadable, or missing a table."""


# The exact `library` table column list, derived from scripts/tsv_to_sqlite.py's
# `CREATE TABLE library (...)` statement -- the authoritative daily-sync shape
# (also reflected in sync-library.sh's MySQL SELECT).
LIBRARY_COLUMNS = (
    "id",
    "title",
    "artist",
    "call_letters",
    "artist_call_number",
    "release_call_number",
    "genre",
    "format",
    "alternate_artist_name",
    "album_artist",
    "label",
    "cross_reference_names",
)

# Columns actually diffed field-by-field: every library column except `id`
# (the join key, not a diffable field) and `label` (always NULL in prod --
# the TSV insert in tsv_to_sqlite.py omits it entirely -- so it is trivially
# NULL==NULL on both sides and carries no signal; excluded rather than
# reported as a permanently-zero mismatch bucket).
DIFF_COLUMNS = tuple(c for c in LIBRARY_COLUMNS if c not in ("id", "label"))

# compilation_track_artist has no primary key of its own; it is compared as
# a (library_release_id, artist_name, track_title) multiset.
CTA_COLUMNS = ("library_release_id", "artist_name", "track_title")


@dataclass(frozen=True)
class ParityDiff:
    """Outcome of diffing two library.db files.

    Field order matches the CLI's ``--json`` contract (``dataclasses.asdict``
    preserves declaration order), with the two id lists appended at the end.
    """

    matched: int
    missing_in_backend: int
    extra_in_backend: int
    field_mismatches: dict[str, int]
    cta_missing: int
    cta_extra: int
    missing_in_backend_ids: list[int] = field(default_factory=list)
    extra_in_backend_ids: list[int] = field(default_factory=list)


def _normalize(value: object) -> object:
    """Normalize a single field value for comparison.

    SQL NULL, the empty string, and the literal string ``"NULL"`` (a known
    transient artifact of the MySQL export pipeline, being fixed at the
    source separately -- see WXYC/discogs-etl#346) are all treated as equal,
    and collapse to ``None``. Surrounding whitespace is stripped. No other
    transform is applied: no case folding, no accent folding, no internal
    whitespace collapsing.
    """
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "" or stripped == "NULL":
            return None
        return stripped
    return value


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _require_table(conn: sqlite3.Connection, table: str, label: str) -> None:
    if not _table_exists(conn, table):
        raise SourceError(f"{label} database is missing required table '{table}'")


def _open_readonly(path: str, label: str) -> sqlite3.Connection:
    """Open ``path`` as a read-only SQLite connection.

    Raises ``SourceError`` (never writes, never creates) when the file is
    missing or is not a readable SQLite database.
    """
    p = Path(path)
    if not p.is_file():
        raise SourceError(f"{label} database not found: {path}")
    uri = f"file:{p.resolve().as_posix()}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("SELECT 1")  # surface "file is not a database" errors immediately
    except sqlite3.Error as exc:
        raise SourceError(f"{label} database unreadable: {path} ({exc})") from exc
    return conn


def _load_library_rows(conn: sqlite3.Connection) -> dict[int, dict[str, object]]:
    """Read every ``library`` row, keyed by ``id``."""
    cols = ", ".join(LIBRARY_COLUMNS)
    rows = conn.execute(f"SELECT {cols} FROM library").fetchall()
    result: dict[int, dict[str, object]] = {}
    for row in rows:
        record = dict(zip(LIBRARY_COLUMNS, row, strict=True))
        result[int(record["id"])] = record
    return result


def _load_cta_counts(conn: sqlite3.Connection) -> Counter[tuple[object, ...]]:
    """Read compilation_track_artist as a normalized multiset.

    Returns an empty Counter (never raises) when the table is absent --
    the table is optional, matching tsv_to_sqlite.py's graceful-degradation
    handling of pre-V008 fixtures / Backend-Service-sourced catalogs.
    """
    if not _table_exists(conn, "compilation_track_artist"):
        return Counter()
    cols = ", ".join(CTA_COLUMNS)
    rows = conn.execute(f"SELECT {cols} FROM compilation_track_artist").fetchall()
    return Counter(tuple(_normalize(v) for v in row) for row in rows)


def diff_library_dbs(
    mysql_conn: sqlite3.Connection, backend_conn: sqlite3.Connection
) -> ParityDiff:
    """Compute the full parity diff between two already-open library.db connections.

    Assumes both connections have a ``library`` table (callers -- ``run_diff``
    -- are responsible for validating that up front via ``_require_table``).
    """
    mysql_rows = _load_library_rows(mysql_conn)
    backend_rows = _load_library_rows(backend_conn)

    mysql_ids = set(mysql_rows)
    backend_ids = set(backend_rows)

    matched_ids = mysql_ids & backend_ids
    missing_ids = sorted(mysql_ids - backend_ids)
    extra_ids = sorted(backend_ids - mysql_ids)

    field_mismatches: dict[str, int] = dict.fromkeys(DIFF_COLUMNS, 0)
    for id_ in matched_ids:
        mrow = mysql_rows[id_]
        brow = backend_rows[id_]
        for col in DIFF_COLUMNS:
            if _normalize(mrow[col]) != _normalize(brow[col]):
                field_mismatches[col] += 1

    mysql_cta = _load_cta_counts(mysql_conn)
    backend_cta = _load_cta_counts(backend_conn)
    cta_missing = sum((mysql_cta - backend_cta).values())
    cta_extra = sum((backend_cta - mysql_cta).values())

    return ParityDiff(
        matched=len(matched_ids),
        missing_in_backend=len(missing_ids),
        extra_in_backend=len(extra_ids),
        field_mismatches=field_mismatches,
        cta_missing=cta_missing,
        cta_extra=cta_extra,
        missing_in_backend_ids=missing_ids,
        extra_in_backend_ids=extra_ids,
    )


def run_diff(mysql_db: str, backend_db: str) -> ParityDiff:
    """Open both library.db files read-only, validate schema, and diff them.

    Raises ``SourceError`` for any file/table problem on either side. Never
    writes to either input.
    """
    mysql_conn = _open_readonly(mysql_db, "mysql")
    try:
        _require_table(mysql_conn, "library", "mysql")
        backend_conn = _open_readonly(backend_db, "backend")
        try:
            _require_table(backend_conn, "library", "backend")
            return diff_library_dbs(mysql_conn, backend_conn)
        finally:
            backend_conn.close()
    finally:
        mysql_conn.close()


# --- Producer stubs (out of scope; see the module docstring and #346) -----
#
# Building a fresh library.db from a live source is a separate, not-yet-
# decided piece of work (direct-PG vs. HTTP producer). These functions exist
# only to reserve the CLI surface (--mysql-source / --backend-source) so the
# real producer can be wired in later without a breaking CLI change. They
# must not be implemented here.


def _build_library_db_from_mysql(source: str, output_path: str) -> None:
    """STUB -- build a library.db from a live MySQL (tubafrenzy) source.

    Not implemented: the producer half of #346 is blocked on an unmade
    direct-PG-vs-HTTP design decision. Do not implement without that
    decision; pass an already-built ``--mysql-db`` file instead.
    """
    raise NotImplementedError(
        "the producer (build-from-source) half of #346 is out of scope for "
        "this harness -- pass --mysql-db pointing at an already-built "
        "library.db instead"
    )


def _build_library_db_from_backend(source: str, output_path: str) -> None:
    """STUB -- build a library.db from Backend-Service.

    Not implemented: see ``_build_library_db_from_mysql`` above and #346.
    """
    raise NotImplementedError(
        "the producer (build-from-source) half of #346 is out of scope for "
        "this harness -- pass --backend-db pointing at an already-built "
        "library.db instead"
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Diff two already-built library.db SQLite files (a MySQL-sourced "
            "daily build vs. a Backend-sourced build) for the discogs-etl#346 "
            "catalog-parity harness. Compares library rows (keyed by id), the "
            "compilation_track_artist table, and row-set membership. "
            "Read-only -- never writes to either input."
        ),
    )
    p.add_argument(
        "--mysql-db",
        default=None,
        help="Path to the MySQL-sourced (daily-sync) library.db.",
    )
    p.add_argument(
        "--backend-db",
        default=None,
        help="Path to the Backend-sourced library.db.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON object on stdout (machine-readable).",
    )
    # Reserved for the not-yet-built producer half of #346 (build library.db
    # from a live source). Deliberately NOT implemented -- see the module
    # docstring. Passing either flag is a CLI usage error (exit 2).
    p.add_argument(
        "--mysql-source",
        default=None,
        metavar="DSN",
        help="Reserved for a future producer step (NOT IMPLEMENTED).",
    )
    p.add_argument(
        "--backend-source",
        default=None,
        metavar="URL",
        help="Reserved for a future producer step (NOT IMPLEMENTED).",
    )
    return p


def _print_human(result: ParityDiff) -> None:
    print(f"matched:            {result.matched:>10}")
    print(f"missing_in_backend: {result.missing_in_backend:>10}")
    if result.missing_in_backend_ids:
        print(f"  ids: {result.missing_in_backend_ids}")
    print(f"extra_in_backend:   {result.extra_in_backend:>10}")
    if result.extra_in_backend_ids:
        print(f"  ids: {result.extra_in_backend_ids}")
    print("field_mismatches:")
    for col in DIFF_COLUMNS:
        print(f"  {col:<24} {result.field_mismatches[col]:>6}")
    print(f"cta_missing:        {result.cta_missing:>10}")
    print(f"cta_extra:          {result.cta_extra:>10}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    init_logger(repo="discogs-etl", tool="discogs-etl catalog_parity_diff")

    if args.mysql_source is not None or args.backend_source is not None:
        try:
            if args.mysql_source is not None:
                _build_library_db_from_mysql(args.mysql_source, args.mysql_db or "")
            if args.backend_source is not None:
                _build_library_db_from_backend(args.backend_source, args.backend_db or "")
        except NotImplementedError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

    if not args.mysql_db or not args.backend_db:
        print("error: --mysql-db and --backend-db are both required.", file=sys.stderr)
        parser.print_usage(sys.stderr)
        return 2

    try:
        result = run_diff(args.mysql_db, args.backend_db)
    except SourceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:  # pragma: no cover - unexpected read-time failure
        logger.exception("catalog parity diff failed")
        print(f"error: {exc}", file=sys.stderr)
        return 3

    if args.json:
        print(json.dumps(asdict(result)))
    else:
        _print_human(result)

    logger.info(
        "catalog parity diff complete",
        extra={
            "step": "catalog_parity_diff",
            "matched": result.matched,
            "missing_in_backend": result.missing_in_backend,
            "extra_in_backend": result.extra_in_backend,
            "cta_missing": result.cta_missing,
            "cta_extra": result.cta_extra,
        },
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
