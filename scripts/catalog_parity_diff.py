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

It also carries the **producer** half (WXYC/discogs-etl#351): given a live
source instead of a prebuilt file, ``--mysql-source`` / ``--backend-source``
build the corresponding ``library.db`` first, so a single invocation can
build both sides and diff them.

- ``--mysql-source mysql://user@host:port/dbname`` reproduces the daily
  build's own read path -- the ``mysql`` CLI in batch/raw mode, running the
  exact SELECTs from ``scripts/sync-library.sh`` (a source-grep test pins
  them together), parsed by the same TSV parser production uses. The CLI, not
  a Python driver, because tubafrenzy's MySQL 4.1 auth breaks those drivers.
  The password comes from ``$LIBRARY_DB_PASSWORD``: put in the DSN it would
  sit in this process's own argv, visible to ``ps`` for the whole run.
- ``--backend-source https://api.wxyc.org`` is the migration target
  (decision D3 / Option B, 2026-08-03): the gzipped-NDJSON exports ``GET
  /library/catalog`` + ``GET /library/catalog/compilation-tracks``
  (WXYC/Backend-Service#1965) read over HTTPS with a service-account bearer
  token from ``$BACKEND_CATALOG_TOKEN`` -- no prod-DB credentials, which is
  the whole point of Option B over a direct-Postgres producer. Plain http to
  anything but a loopback address is refused, and so is any cross-origin
  redirect, which urllib would otherwise follow *carrying that token*.

Both producers write **only** to the path named by the matching ``--*-db``
flag, and refuse to write to a path that already exists: this harness must
never be able to clobber a real ``library.db``. Each builds into a scratch
file beside its target and renames it into place at the end, so a build that
dies partway leaves nothing behind to refuse on the next parity day.

**Still out of scope**: the operational cutover -- running the 7 clean parity
days, flipping ``sync-library.sh``'s source, and taking ``/wxycdb`` dark --
is WXYC/discogs-etl#346, deliberately human-gated.

Schema note: the ``library`` table's 12 columns (``id, title, artist,
call_letters, artist_call_number, release_call_number, genre, format,
alternate_artist_name, album_artist, label, cross_reference_names``) are
**imported** from ``lib/library_db.py`` -- the authoritative daily-sync
shape, shared with ``scripts/tsv_to_sqlite.py`` so both producers build the
same database. Imported rather than restated, so a column added there widens
this diff automatically instead of becoming an undiffed blind spot.
``label`` is always NULL in production (nothing ever inserts it -- see that
module's docstring), so it is excluded from the diffed column set below
rather than compared as a trivially-always-equal no-op.

Usage::

    python scripts/catalog_parity_diff.py \\
        --mysql-db /path/to/mysql-sourced/library.db \\
        --backend-db /path/to/backend-sourced/library.db \\
        --json

Exit codes:

- ``0`` -- ran successfully. A nonzero diff count is still exit 0; the
  operator reads the counts (and, in ``--json`` mode, the id lists) to judge
  parity.
- ``2`` -- bad arguments (missing required flags).
- ``3`` -- source/read error: missing file, unreadable database, a required
  table (``library``) absent from one of the inputs, a malformed input
  (duplicate ``library.id``, which a valid library.db's primary key forbids),
  or **any** producer failure (unreachable source, missing credentials, a
  refused overwrite, an inconsistent snapshot, an empty catalog export, a
  contract-violating row, a missing ``mysql`` binary, a malformed DSN).

Never writes to either input: both files are opened as read-only SQLite
connections (``mode=ro``).
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import zlib
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import unquote, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    Request,
    build_opener,
    pathname2url,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.library_db import (  # noqa: E402
    CROSS_REFERENCE_SEPARATOR,
    LIBRARY_COLUMNS,
    build_library_db,
    parse_compilation_track_tsv,
    parse_library_tsv,
)
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)


class SourceError(RuntimeError):
    """A library.db side is unusable.

    Either an input file (missing, unreadable, missing a required table, or
    malformed -- e.g. a duplicate id) or a producer that could not build one
    (bad DSN/URL, missing credentials, a refused overwrite, an unreachable
    source, or an inconsistent snapshot).
    """


# Columns actually diffed field-by-field: every library column except `id`
# (the join key, not a diffable field) and `label` (always NULL in prod --
# `lib/library_db.py`'s insert omits it entirely -- so it is trivially
# NULL==NULL on both sides and carries no signal; excluded rather than
# reported as a permanently-zero mismatch bucket).
#
# Derived from the imported `LIBRARY_COLUMNS` rather than a local copy: a
# column added to `lib/library_db.py` must widen the diff automatically, or
# it becomes a permanently-undiffed blind spot in the tool that certifies
# the cutover.
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
    # Percent-encode the path before splicing it into the file: URI. A raw path
    # containing a URI-significant character (?, #, or a space) would otherwise
    # be misparsed -- e.g. a `?` in the path prematurely starts the query string,
    # silently dropping the `?mode=ro` read-only guard and/or opening the wrong
    # file. pathname2url encodes those characters while leaving `/` intact.
    uri = f"file:{pathname2url(str(p.resolve()))}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        # Force a read of the database header/schema so a "file is not a
        # database" error surfaces here (and is wrapped as SourceError) rather
        # than leaking as a raw sqlite3.DatabaseError from a later query. A bare
        # `SELECT 1` is a constant expression that never touches the file, so it
        # does NOT validate the header on every SQLite build (it passes on macOS
        # but not on the Linux CI build); querying sqlite_master does.
        conn.execute("SELECT count(*) FROM sqlite_master")
    except sqlite3.Error as exc:
        raise SourceError(f"{label} database unreadable: {path} ({exc})") from exc
    return conn


def _load_library_rows(conn: sqlite3.Connection, label: str) -> dict[int, dict[str, object]]:
    """Read every ``library`` row, keyed by ``id``.

    A valid daily-sync ``library.db`` has ``id INTEGER PRIMARY KEY`` (see
    ``lib/library_db.py``), so ids are unique. If two rows share an id
    the input is malformed; keying into a dict would silently keep only the
    last (hiding a row-count divergence this parity harness exists to catch),
    so we raise ``SourceError`` instead of under-counting.
    """
    cols = ", ".join(LIBRARY_COLUMNS)
    rows = conn.execute(f"SELECT {cols} FROM library").fetchall()
    result: dict[int, dict[str, object]] = {}
    for row in rows:
        record = dict(zip(LIBRARY_COLUMNS, row, strict=True))
        row_id = int(record["id"])
        if row_id in result:
            raise SourceError(
                f"{label} database has a duplicate library.id ({row_id}); "
                "a valid library.db has a unique id primary key"
            )
        result[row_id] = record
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
    mysql_rows = _load_library_rows(mysql_conn, "mysql")
    backend_rows = _load_library_rows(backend_conn, "backend")

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


# --- Producers: build a library.db from a live source (#351) --------------

# One row of either output table, already mapped out of its wire dict: a
# `library` row in LIBRARY_INSERT_COLUMNS order, or a (library_release_id,
# artist_name, track_title) triple. Element 0 is the library release id in
# both, which is what the snapshot-consistency check keys on.
_Row = Sequence[object]

# Env var holding the Backend-Service service-account bearer JWT. Decision D3
# (Option B) deliberately picked an HTTP/contract-governed producer over a
# direct-Postgres one precisely so that no prod-DB credential has to exist in
# GitHub Actions -- this token is the only secret the Backend producer needs.
BACKEND_TOKEN_ENV = "BACKEND_CATALOG_TOKEN"

# Env var holding the tubafrenzy MySQL password. It is read from the
# environment rather than from the ``--mysql-source`` DSN because a DSN
# password sits in *this* process's argv for the whole run -- readable by any
# `ps`, and echoed by a `set -x` or a GitHub Actions command trace. (It is
# still accepted inside the DSN for local one-offs; the env var wins.)
MYSQL_PASSWORD_ENV = "LIBRARY_DB_PASSWORD"

_CATALOG_PATH = "/library/catalog"
_COMPILATION_TRACKS_PATH = "/library/catalog/compilation-tracks"

# The full catalog is ~2.6 MB gzipped and the CTA export ~3.2 MB (measured
# against the 2026-07-19 prod snapshot, api.yaml BS#1965 notes), both served
# from a per-watermark in-memory cache -- but a cold cache has to build the
# whole body first, so the budget is generous.
_HTTP_TIMEOUT_SECONDS = 300

# GET /library/catalog and GET /library/catalog/compilation-tracks are two
# requests, not one transaction. They form a consistent snapshot only while
# the shared library_watermark holds still across both; api.yaml's
# CROSS-ENDPOINT CONSISTENCY note says to treat a change as "re-fetch both".
# A catalog write mid-fetch is rare and self-healing, so a couple of retries
# is plenty -- and failing after them is right, because the alternative is
# diffing a torn snapshot and calling the tear a parity defect.
_SNAPSHOT_ATTEMPTS = 3

# Loopback only: anywhere else, plain HTTP would put the service-account
# bearer token on the wire in the clear.
_PLAINTEXT_OK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

_DEFAULT_PORTS = {"http": 80, "https": 443}


def _origin(url: str) -> tuple[str, str, int]:
    """(scheme, host, port) with the scheme's default port made explicit."""
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    return (scheme, (parts.hostname or "").lower(), parts.port or _DEFAULT_PORTS.get(scheme, 0))


class _SameOriginRedirectHandler(HTTPRedirectHandler):
    """Refuse any redirect that would carry the bearer token to a new origin.

    ``urllib``'s default handler copies every header except Content-Length /
    Content-Type into the redirected request -- ``Authorization`` included.
    So a 302 from a proxy, a misconfigured CDN, or hijacked DNS would replay
    the service-account JWT to a foreign host, and to a plaintext one, which
    is exactly what ``_resolve_backend_base_url``'s https check exists to
    prevent. That check only ever sees the *first* URL, so the guard has to
    live here as well.

    Stopping (rather than stripping the header and following the hop) keeps
    the failure diagnosable: a silently-unauthenticated request would come
    back as a bare 401 from an origin the operator never named.
    """

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Request | None:
        if _origin(req.full_url) != _origin(newurl):
            raise SourceError(
                f"refusing to follow the {code} redirect from {req.full_url} to {newurl}: "
                f"it crosses origins, and urllib would replay the {BACKEND_TOKEN_ENV} "
                "bearer token to the new host. Point --backend-source at the origin that "
                "actually serves the catalog exports."
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


# Module-level opener so every catalog fetch goes through the redirect guard
# above; the bare `urllib.request.urlopen` would use the permissive default.
_opener = build_opener(_SameOriginRedirectHandler())

# The library SELECT production runs every day, copied verbatim from
# scripts/sync-library.sh. tests/unit/test_catalog_parity_diff.py lifts every
# `-e "SELECT ..."` out of that script and asserts (whitespace-insensitively)
# that the set is exactly these two -- equality, not containment, so neither
# an appended `ORDER BY`/`LIMIT` on the shell side nor a third divergent
# query can slip past. A baseline built from a *different* query would make
# the parity diff measure the harness rather than the migration. Change one,
# change both.
LIBRARY_SELECT_SQL = (
    "SELECT r.ID, r.TITLE, lc.PRESENTATION_NAME, lc.CALL_LETTERS, lc.CALL_NUMBERS,"
    " r.CALL_NUMBERS, g.REFERENCE_NAME, f.REFERENCE_NAME,"
    " IFNULL(r.ALTERNATE_ARTIST_NAME, ''), IFNULL(r.ALBUM_ARTIST, ''),"
    " IFNULL((SELECT GROUP_CONCAT(DISTINCT xlc.PRESENTATION_NAME SEPARATOR ' | ')"
    " FROM LIBRARY_CODE_CROSS_REFERENCE xcr, LIBRARY_CODE xlc"
    " WHERE xlc.ID = CASE WHEN xcr.CROSS_REFERENCING_ARTIST_ID = lc.ID"
    " THEN xcr.CROSS_REFERENCED_LIBRARY_CODE_ID"
    " WHEN xcr.CROSS_REFERENCED_LIBRARY_CODE_ID = lc.ID"
    " THEN xcr.CROSS_REFERENCING_ARTIST_ID ELSE NULL END"
    " AND (xcr.CROSS_REFERENCING_ARTIST_ID = lc.ID"
    " OR xcr.CROSS_REFERENCED_LIBRARY_CODE_ID = lc.ID)"
    " AND xlc.ID != lc.ID), '')"
    " FROM LIBRARY_RELEASE r JOIN LIBRARY_CODE lc ON r.LIBRARY_CODE_ID = lc.ID"
    " JOIN FORMAT f ON r.FORMAT_ID = f.ID JOIN GENRE g ON lc.GENRE_ID = g.ID"
)

# Likewise the compilation-track SELECT. Supplementary to LIBRARY_RELEASE: on
# a source with no COMPILATION_TRACK_ARTIST table this query fails and the
# build continues without the table, exactly as sync-library.sh does.
COMPILATION_TRACK_SELECT_SQL = (
    "SELECT LIBRARY_RELEASE_ID, ARTIST_NAME, IFNULL(TRACK_TITLE, '')"
    " FROM COMPILATION_TRACK_ARTIST ORDER BY LIBRARY_RELEASE_ID"
)


def _require_absent(output_path: str, label: str) -> None:
    """Refuse to build over an existing file.

    The harness builds scratch copies; the one thing it must never do is
    overwrite a real ``library.db`` (or the prebuilt file the operator passed
    as the other side of the diff).
    """
    if Path(output_path).exists():
        raise SourceError(
            f"refusing to build the {label} library.db at {output_path}: the path "
            f"already exists. This harness only ever writes fresh scratch copies -- "
            f"point --{label}-db at a new path (or delete that one deliberately)."
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


def _empty_if_none(value: object) -> object:
    """Map a null wire value to ''.

    sync-library.sh wraps alternate_artist_name / album_artist /
    cross_reference_names in ``IFNULL(<col>, '')``, so production's
    library.db holds '' -- never NULL -- for those three. The Backend build
    must match: after the cutover this output *is* library.db, and the
    difference is observable in the FTS content and in LML's pipe-split of
    cross_reference_names, not just in the diff (which normalizes them equal).
    """
    return "" if value is None else value


# The CatalogExportRow fields api.yaml marks `required` and this producer
# writes straight into a library column. A bare `.get()` on any of them would
# turn a field rename or a partial server-side regression into a SQL NULL for
# every row: after the cutover that empties the row's FTS content and breaks
# LML search, and before it, it reads as an ordinary field mismatch --
# indistinguishable from real catalog drift. (`id`, also required, is
# deliberately absent: it is never written, only quoted in diagnostics.)
_REQUIRED_CATALOG_FIELDS = (
    "album_title",
    "artist_name",
    "code_letters",
    "code_artist_number",
    "code_number",
    "genre_name",
    "format_name",
)


def _require_legacy_release_id(row: dict[str, Any], what: str) -> int:
    """Read ``legacy_release_id`` as an int, or raise ``SourceError``."""
    legacy_release_id = row.get("legacy_release_id")
    if legacy_release_id is None:
        raise SourceError(
            f"{what} export row has no legacy_release_id "
            f"(Backend serial id {row.get('id')!r}): either this Backend predates "
            "WXYC/Backend-Service#1965 or the BS#1963 mint/backfill is incomplete. "
            "Refusing to write a library.db row with a null id."
        )
    try:
        return int(legacy_release_id)
    except (TypeError, ValueError) as exc:
        raise SourceError(
            f"{what} export row has a non-integer legacy_release_id "
            f"({legacy_release_id!r}); library.db's id is an INTEGER PRIMARY KEY"
        ) from exc


def _catalog_row_to_library_row(row: dict[str, Any]) -> list[object]:
    """Map one CatalogExportRow onto a daily-sync ``library`` row tuple."""
    legacy_release_id = _require_legacy_release_id(row, "catalog")
    for name in _REQUIRED_CATALOG_FIELDS:
        if row.get(name) is None:
            raise SourceError(
                f"catalog export row for legacy_release_id {legacy_release_id} is missing "
                f"the required field {name!r}; api.yaml's CatalogExportRow marks it required, "
                "and writing it as NULL would corrupt library.db rather than show up as drift"
            )

    cross_reference_names = row.get("cross_reference_names") or []
    if not isinstance(cross_reference_names, list):
        # A string is truthy and joinable, so without this check a scalar
        # would be split character-by-character into phantom aliases
        # ('S | t | e | r | e | o | l | a | b') that LML then pipe-splits
        # straight into the live search index.
        raise SourceError(
            f"catalog export row for legacy_release_id {legacy_release_id} has "
            f"cross_reference_names of type {type(cross_reference_names).__name__}, "
            "not the array api.yaml specifies"
        )

    return [
        legacy_release_id,
        row["album_title"],
        row["artist_name"],
        row["code_letters"],
        row["code_artist_number"],
        row["code_number"],
        row["genre_name"],
        row["format_name"],
        _empty_if_none(row.get("alternate_artist_name")),
        _empty_if_none(row.get("album_artist")),
        # The wire carries an array precisely so no artist name can be split
        # into phantom aliases by the delimiter; the join happens here, at the
        # one place that writes the SQLite column.
        CROSS_REFERENCE_SEPARATOR.join(cross_reference_names),
    ]


def _catalog_cta_row_to_library_row(row: dict[str, Any]) -> tuple[object, ...]:
    """Map one CatalogCompilationTrackRow onto a ``compilation_track_artist`` row."""
    legacy_release_id = _require_legacy_release_id(row, "compilation-track")
    artist_name = row.get("artist_name")
    if artist_name is None:
        # `required` in the api.yaml schema and NOT NULL in the column
        # beneath it, so this is a contract violation, not a data gap.
        raise SourceError(
            "compilation-track export row is missing the required field 'artist_name' "
            f"(legacy_release_id={legacy_release_id})"
        )
    return (
        legacy_release_id,
        artist_name,
        _empty_if_none(row.get("track_title")),
    )


def _resolve_backend_base_url(source: str) -> str:
    """Validate and normalize the Backend base URL."""
    parts = urlsplit(source)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise SourceError(
            "--backend-source must be an http(s) base URL "
            f"(e.g. https://api.wxyc.org), got {source!r}"
        )
    if parts.scheme == "http" and (parts.hostname or "") not in _PLAINTEXT_OK_HOSTS:
        raise SourceError(
            f"refusing to send the {BACKEND_TOKEN_ENV} bearer token over plaintext http "
            f"to {parts.hostname!r} -- use https (plain http is allowed only for a "
            "loopback address, for local testing)"
        )
    return f"{parts.scheme}://{parts.netloc}{parts.path.rstrip('/')}"


def _map_ndjson_lines(
    url: str, stream: Any, mapper: Callable[[dict[str, Any]], _Row]
) -> list[_Row]:
    """Parse an NDJSON byte stream line by line, mapping each row as it arrives.

    Deliberately incremental: the two prod exports are 28.5 MB + 13.1 MB
    across 64,815 + 144,778 rows, and reading the whole body, decoding it,
    splitting it into a line list, and keeping every raw dict alive would
    hold four copies at once for a job whose logical working set is one row.
    Mapping here also drops each parsed dict as soon as its library row is
    built.
    """
    rows: list[_Row] = []
    for lineno, raw in enumerate(stream, start=1):
        line = raw.decode("utf-8").strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SourceError(f"{url} line {lineno} is not valid JSON: {exc}") from exc
        rows.append(mapper(payload))
    return rows


def _fetch_ndjson(
    url: str, token: str, mapper: Callable[[dict[str, Any]], _Row]
) -> tuple[list[_Row], str]:
    """GET a gzipped-NDJSON export; return its mapped rows and the watermark."""
    # Scheme is validated to http/https in _resolve_backend_base_url, so this
    # cannot be tricked into a file:// or other local-handler fetch, and
    # _opener refuses any cross-origin redirect that would carry the token on.
    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/x-ndjson",
            "Accept-Encoding": "gzip",
        },
    )
    try:
        with _opener.open(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
            watermark = response.headers.get("Last-Modified")
            if not watermark:
                # Without it the cross-endpoint consistency rule below is
                # unenforceable, and silently pairing two unrelated snapshots
                # is worse than stopping.
                raise SourceError(
                    f"{url} returned no Last-Modified header; the catalog exports must carry "
                    "the library_watermark for the two-request snapshot to be checkable"
                )
            encoding = (response.headers.get("Content-Encoding") or "").lower()
            stream = gzip.GzipFile(fileobj=response) if encoding == "gzip" else response
            rows = _map_ndjson_lines(url, stream, mapper)
    except SourceError:
        raise
    except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
        raise SourceError(
            f"{url} declared Content-Encoding: gzip but did not decompress: {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise SourceError(f"{url} returned a body that is not valid UTF-8: {exc}") from exc
    except (URLError, OSError) as exc:
        raise SourceError(f"failed to fetch {url}: {exc}") from exc
    return rows, watermark


def _fetch_consistent_snapshot(base_url: str, token: str) -> tuple[list[_Row], list[_Row]]:
    """Fetch both exports, retrying until they describe the same catalog snapshot.

    Returns rows already mapped into ``library`` / ``compilation_track_artist``
    shape, so both id sets are plain ints and nothing raw survives the fetch.

    Note that ``Last-Modified`` is an HTTP-date, i.e. whole seconds: two
    distinct watermarks inside the same second compare equal, so a write
    landing between the two GETs in that window slips through. The
    dangling-id check below catches the CTA->catalog direction of that tear;
    the other direction (a catalog row added mid-pair) would show up as a
    single spurious ``extra_in_backend`` and self-heals on the next run,
    because the exports are served from a per-watermark cache.
    """
    reason = "no attempt was made"
    for attempt in range(1, _SNAPSHOT_ATTEMPTS + 1):
        catalog_rows, catalog_watermark = _fetch_ndjson(
            base_url + _CATALOG_PATH, token, _catalog_row_to_library_row
        )
        cta_rows, cta_watermark = _fetch_ndjson(
            base_url + _COMPILATION_TRACKS_PATH, token, _catalog_cta_row_to_library_row
        )

        if catalog_watermark != cta_watermark:
            reason = (
                "the catalog watermark advanced between the two exports "
                f"({catalog_watermark!r} -> {cta_watermark!r})"
            )
        else:
            catalog_ids = {row[0] for row in catalog_rows}
            dangling = sorted({row[0] for row in cta_rows} - catalog_ids)
            if dangling:
                # Server-side row eligibility should make this impossible within
                # one snapshot, so seeing it means the pair is torn in a way the
                # watermark didn't reveal. Same remedy: re-fetch.
                reason = (
                    "the compilation-track export references legacy_release_id(s) "
                    f"{dangling[:5]} with no row in the catalog export "
                    f"({len(dangling)} total)"
                )
            else:
                if attempt > 1:
                    logger.info(
                        "catalog snapshot settled",
                        extra={"step": "backend_producer", "attempt": attempt},
                    )
                return catalog_rows, cta_rows

        logger.warning(
            "re-fetching the catalog snapshot",
            extra={"step": "backend_producer", "attempt": attempt, "reason": reason},
        )

    raise SourceError(
        f"could not read a consistent catalog snapshot from {base_url} in "
        f"{_SNAPSHOT_ATTEMPTS} attempts: {reason}"
    )


def _build_library_db_from_backend(source: str, output_path: str) -> None:
    """Build a daily-sync-shaped library.db from Backend-Service over HTTP.

    Args:
        source: Backend base URL, e.g. ``https://api.wxyc.org``.
        output_path: Where to write the SQLite database. Must not exist.

    Raises:
        SourceError: on a refused overwrite, a bad/plaintext URL, a
            cross-origin redirect, a missing ``$BACKEND_CATALOG_TOKEN``, a
            fetch or decode failure, a torn snapshot, an empty catalog, or a
            catalog row that violates the api.yaml contract.
    """
    _require_absent(output_path, "backend")
    base_url = _resolve_backend_base_url(source)
    token = os.environ.get(BACKEND_TOKEN_ENV)
    if not token:
        raise SourceError(
            f"{BACKEND_TOKEN_ENV} is not set: the Backend catalog exports require a "
            "service-account bearer token with catalog:read"
        )

    library_rows, compilation_rows = _fetch_consistent_snapshot(base_url, token)
    if not library_rows:
        # A broken export query, an over-narrow token scope, or a truncated
        # cached buffer all surface as a 200 with no rows. Building from it
        # would report the entire catalog as missing_in_backend -- an
        # operator reading that sees catastrophic drift, not a producer that
        # read nothing. And post-cutover this producer IS the daily build.
        raise SourceError(
            f"{base_url}{_CATALOG_PATH} returned no rows; a catalog export is never "
            "legitimately empty, so this is a producer failure (check the token's scope "
            "and the export query) rather than total drift"
        )
    if not compilation_rows:
        # Supplementary, and genuinely absent on some sources -- so a warning
        # rather than a failure, matching the MySQL side's graceful
        # degradation. Loud, because in prod it is ~144k rows and its absence
        # would otherwise land in the report as cta_missing.
        logger.warning(
            "the compilation-track export returned no rows; building without the table",
            extra={"step": "backend_producer", "source": base_url},
        )
        _report(
            f"WARNING: {base_url}{_COMPILATION_TRACKS_PATH} returned no rows; "
            "building without a compilation_track_artist table"
        )

    count = _build_into(output_path, "backend", library_rows, compilation_rows or None)
    logger.info(
        "built Backend-sourced library.db",
        extra={
            "step": "backend_producer",
            "rows": count,
            "compilation_track_rows": len(compilation_rows),
            "output": output_path,
        },
    )
    _report(f"Exported {count} rows to {output_path} (source: {base_url})")


def _default_mysql_runner(argv: Sequence[str], env: dict[str, str], stdout_path: str) -> bool:
    """Run the ``mysql`` CLI, capturing stdout to ``stdout_path``.

    Returns True on exit 0. stderr is echoed so a failure is diagnosable
    (sync-library.sh does the same, appending it to the ETL log).
    """
    with open(stdout_path, "wb") as stdout:
        # argv is built by _mysql_invocation; no shell, no interpolation.
        completed = subprocess.run(
            list(argv),
            env={**os.environ, **env},
            stdout=stdout,
            stderr=subprocess.PIPE,
            check=False,
        )
    if completed.stderr:
        sys.stderr.write(completed.stderr.decode("utf-8", errors="replace"))
    return completed.returncode == 0


# Module-level seam so tests can substitute the CLI. Rebound, not wrapped, so
# `_build_library_db_from_mysql` picks it up at call time.
_mysql_runner = _default_mysql_runner


def _mysql_invocation(source: str) -> tuple[list[str], dict[str, str]]:
    """Build the ``mysql`` argv + env from a ``mysql://`` DSN.

    Mirrors sync-library.sh: batch (``-B``) + raw (``-N``) mode over the CLI
    rather than a Python driver, because tubafrenzy runs a MySQL old enough
    that the drivers can't authenticate against it. The password reaches the
    CLI through ``MYSQL_PWD``, never its argv.

    Prefer ``$LIBRARY_DB_PASSWORD`` over a password embedded in the DSN. The
    DSN reaches *this* process on the command line, so an embedded password
    is visible to `ps` for the whole run and echoed by any ``set -x`` or
    GitHub Actions command trace -- and a password containing ``/``, ``#``,
    ``?`` or ``%`` has to be percent-encoded or ``urlsplit`` silently
    mis-slices the DSN around it.
    """
    parts = urlsplit(source)
    if parts.scheme not in ("mysql", "mysql+pymysql"):
        raise SourceError(
            f"--mysql-source must be a mysql:// DSN (mysql://user@host:port/dbname), got {source!r}"
        )
    database = parts.path.lstrip("/")
    if not parts.hostname or not database:
        raise SourceError(f"--mysql-source is missing a host and/or database name: {source!r}")
    try:
        port = parts.port or 3306
    except ValueError as exc:
        # Reached whenever the netloc's `:`-suffix isn't numeric -- most often
        # an un-encoded `/` in the password, which moves the real host:port
        # into the path and leaves the password fragment where the port
        # belongs. Percent-encode it, or (better) use $LIBRARY_DB_PASSWORD.
        raise SourceError(
            f"--mysql-source has a malformed port in {source!r} ({exc}). If the password "
            f"contains '/', '#', '?' or '%' it must be percent-encoded -- or supply it via "
            f"${MYSQL_PASSWORD_ENV} and leave it out of the DSN entirely."
        ) from exc

    argv = ["mysql", "-h", parts.hostname, "-P", str(port)]
    if parts.username:
        argv += ["-u", unquote(parts.username)]
    argv += ["--default-character-set=utf8", "-B", "-N", database]
    password = os.environ.get(MYSQL_PASSWORD_ENV) or unquote(parts.password or "")
    return argv, {"MYSQL_PWD": password}


def _build_library_db_from_mysql(source: str, output_path: str) -> None:
    """Build the baseline library.db from tubafrenzy MySQL, the way prod does.

    Args:
        source: ``mysql://user@host:port/dbname``, with the password in
            ``$LIBRARY_DB_PASSWORD``. Point it at a local port when
            tunnelling, as sync-library.sh does.
        output_path: Where to write the SQLite database. Must not exist.

    Raises:
        SourceError: on a refused overwrite, a malformed DSN, or a failed
            library export query. A failed *compilation-track* query is
            tolerated (that table is supplementary and absent on some
            sources), matching sync-library.sh.
    """
    _require_absent(output_path, "mysql")
    argv, env = _mysql_invocation(source)

    scratch = tempfile.mkdtemp(prefix="catalog-parity-mysql-")
    try:
        library_tsv = os.path.join(scratch, "library.tsv")
        if not _mysql_runner([*argv, "-e", LIBRARY_SELECT_SQL], env, library_tsv):
            raise SourceError(
                "the MySQL library export query failed (see stderr above); "
                "no library.db was written"
            )

        cta_tsv = os.path.join(scratch, "compilation_track_artist.tsv")
        cta_rows = None
        if _mysql_runner([*argv, "-e", COMPILATION_TRACK_SELECT_SQL], env, cta_tsv):
            if os.path.getsize(cta_tsv) > 0:
                cta_rows = parse_compilation_track_tsv(cta_tsv)
        else:
            logger.warning(
                "compilation-track export unavailable; building without it",
                extra={"step": "mysql_producer"},
            )

        count = _build_into(output_path, "mysql", parse_library_tsv(library_tsv), cta_rows)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    logger.info(
        "built MySQL-sourced library.db",
        extra={"step": "mysql_producer", "rows": count, "output": output_path},
    )
    _report(f"Exported {count} rows to {output_path} (source: MySQL)")


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
    p.add_argument(
        "--mysql-source",
        default=None,
        metavar="DSN",
        help=(
            "Build the MySQL-sourced library.db first, from this "
            "mysql://user@host:port/dbname DSN, writing it to --mysql-db "
            f"(which must not already exist). Password via ${MYSQL_PASSWORD_ENV} -- "
            "putting it in the DSN leaves it in this process's argv."
        ),
    )
    p.add_argument(
        "--backend-source",
        default=None,
        metavar="URL",
        help=(
            "Build the Backend-sourced library.db first, from this base URL "
            f"(e.g. https://api.wxyc.org), writing it to --backend-db (which must "
            f"not already exist). Requires a service-account token in ${BACKEND_TOKEN_ENV}."
        ),
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

    # Each --*-source needs its matching --*-db as the output path, so the
    # required-flags check runs before any build.
    for source, output, flag in (
        (args.mysql_source, args.mysql_db, "mysql"),
        (args.backend_source, args.backend_db, "backend"),
    ):
        if source is not None and not output:
            print(
                f"error: --{flag}-source requires --{flag}-db, the path to write the "
                "built library.db to.",
                file=sys.stderr,
            )
            parser.print_usage(sys.stderr)
            return 2

    if not args.mysql_db or not args.backend_db:
        print("error: --mysql-db and --backend-db are both required.", file=sys.stderr)
        parser.print_usage(sys.stderr)
        return 2

    try:
        # Validate BOTH output paths before either build runs: checking the
        # backend path only when its turn comes would burn the entire MySQL
        # export before refusing a path the operator already had on disk.
        if args.mysql_source is not None:
            _require_absent(args.mysql_db, "mysql")
        if args.backend_source is not None:
            _require_absent(args.backend_db, "backend")

        if args.mysql_source is not None:
            _build_library_db_from_mysql(args.mysql_source, args.mysql_db)
        if args.backend_source is not None:
            _build_library_db_from_backend(args.backend_source, args.backend_db)
    except SourceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:
        # Exit 3 is the documented contract for *any* producer failure. Without
        # this, a `mysql` binary that isn't on PATH, an unreadable output
        # directory, or a malformed DSN escapes as a raw traceback and exit 1.
        logger.exception("catalog parity producer failed")
        print(f"error: {exc}", file=sys.stderr)
        return 3

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
