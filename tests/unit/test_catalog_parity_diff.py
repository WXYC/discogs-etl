"""Unit tests for ``scripts/catalog_parity_diff.py``.

Two halves of the catalog-parity harness live in that script:

- The **diff core** (discogs-etl#346): given two already-built ``library.db``
  SQLite files (one from the daily MySQL-sourced build, one Backend-sourced),
  report where they diverge -- per-column field mismatches, row-set
  membership (ids present in only one side), and ``compilation_track_artist``
  (CTA) drift.
- The **producers** (discogs-etl#351): build either side from a live source
  -- ``TestMysqlProducer`` (the daily build's own ``mysql`` CLI read path) and
  ``TestBackendProducer`` (the BS#1965 NDJSON exports over HTTP, exercised
  against a real local HTTP server rather than a mocked ``urlopen``, so the
  gzip/header/framing handling is actually covered).
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import re
import sqlite3
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "catalog_parity_diff.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("catalog_parity_diff", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["catalog_parity_diff"] = mod
    spec.loader.exec_module(mod)
    return mod


# The exact daily-sync `library` table shape. Spelled out literally rather
# than imported from lib/library_db.py: these tests assert what the schema IS,
# so importing the constant under test would make them tautological.
_LIBRARY_COLUMNS = (
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

_DEFAULT_ROW = {
    "title": "Aluminum Tunes",
    "artist": "Stereolab",
    "call_letters": "ST",
    "artist_call_number": 100,
    "release_call_number": 1,
    "genre": "Rock",
    "format": "CD",
    "alternate_artist_name": None,
    "album_artist": None,
    "label": None,
    "cross_reference_names": None,
}


def _make_library_db(
    path: Path,
    rows: list[dict],
    cta_rows: list[tuple[int, str, str | None]] | None = None,
) -> None:
    """Build a real library.db (matching the daily-sync schema) at ``path``.

    Each entry in ``rows`` is a dict with at least an "id" key; any column
    left unset falls back to ``_DEFAULT_ROW``. Pass ``cta_rows`` (id,
    artist_name, track_title tuples) to also create the optional
    compilation_track_artist table.
    """
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE library (
        id INTEGER PRIMARY KEY, title TEXT, artist TEXT, call_letters TEXT,
        artist_call_number INTEGER, release_call_number INTEGER,
        genre TEXT, format TEXT, alternate_artist_name TEXT,
        album_artist TEXT, label TEXT, cross_reference_names TEXT
    )"""
    )
    for row in rows:
        merged = {**_DEFAULT_ROW, **row}
        values = [merged["id"]] + [merged[c] for c in _LIBRARY_COLUMNS if c != "id"]
        cur.execute(
            f"INSERT INTO library ({', '.join(_LIBRARY_COLUMNS)}) "
            f"VALUES ({', '.join('?' for _ in _LIBRARY_COLUMNS)})",
            values,
        )
    if cta_rows:
        cur.execute(
            """CREATE TABLE compilation_track_artist (
            library_release_id INTEGER NOT NULL,
            artist_name TEXT NOT NULL,
            track_title TEXT
        )"""
        )
        cur.executemany(
            "INSERT INTO compilation_track_artist"
            " (library_release_id, artist_name, track_title) VALUES (?,?,?)",
            cta_rows,
        )
    conn.commit()
    conn.close()


# --- Producer fixtures (#351) --------------------------------------------

_LM_A = "Sat, 09 Aug 2026 12:00:00 GMT"
_LM_B = "Sat, 09 Aug 2026 12:30:00 GMT"


def _squash(text: str) -> str:
    """Collapse all whitespace runs to single spaces, for SQL source comparison."""
    return re.sub(r"\s+", " ", text).strip()


def _sync_library_selects(script: str) -> set[str]:
    """Every SELECT ``sync-library.sh`` hands to the ``mysql`` CLI, squashed.

    The script passes each query as a double-quoted ``-e`` argument and none
    of them contain a double quote, so this lifts them out exactly.
    """
    return {_squash(sql) for sql in re.findall(r'-e "(SELECT [^"]*)"', script)}


def _catalog_row(**overrides: Any) -> dict[str, Any]:
    """One ``CatalogExportRow`` as GET /library/catalog serves it (BS#1965).

    Defaults mirror ``_DEFAULT_ROW`` above so a Backend-sourced build and the
    MySQL-sourced fixtures diff to zero. ``id`` is the BS serial (deliberately
    different from ``legacy_release_id``, to prove the producer emits the
    latter).
    """
    row: dict[str, Any] = {
        "id": 5_001,
        "legacy_release_id": 72_101,
        "album_title": "Aluminum Tunes",
        "artist_name": "Stereolab",
        "code_letters": "ST",
        "code_artist_number": 100,
        "code_number": 1,
        "genre_name": "Rock",
        "format_name": "CD",
        "alternate_artist_name": None,
        "album_artist": None,
        "cross_reference_names": [],
        "label": "Duophonic",
        "on_streaming": True,
        "rotation_bin": None,
        "rotation_kill_date": None,
    }
    row.update(overrides)
    return row


@dataclass
class _RecordedRequest:
    path: str
    authorization: str | None


class _BackendStub:
    """A stand-in for Backend-Service's two catalog NDJSON exports.

    Serves gzipped NDJSON with a ``Last-Modified`` watermark, records every
    request, and exposes ``on_catalog_fetch`` so a test can advance the
    watermark mid-pair to exercise the torn-snapshot re-fetch rule.
    """

    def __init__(
        self,
        catalog_rows: list[dict[str, Any]],
        cta_rows: list[dict[str, Any]],
        gzip_body: bool = True,
        last_modified: str = _LM_A,
    ) -> None:
        self.catalog_rows = catalog_rows
        self.cta_rows = cta_rows
        self.gzip_body = gzip_body
        self.last_modified = last_modified
        self.requests: list[_RecordedRequest] = []
        self.on_catalog_fetch: Callable[[], None] | None = None
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler_class())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self) -> _BackendStub:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def _handler_class(self) -> type[BaseHTTPRequestHandler]:
        stub = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *args: object) -> None:  # keep pytest output clean
                pass

            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                stub.requests.append(_RecordedRequest(self.path, self.headers.get("Authorization")))
                if self.path == "/library/catalog":
                    rows = stub.catalog_rows
                elif self.path == "/library/catalog/compilation-tracks":
                    rows = stub.cta_rows
                else:
                    self.send_error(404)
                    return
                body = "".join(json.dumps(r) + "\n" for r in rows).encode("utf-8")
                if stub.gzip_body:
                    body = gzip.compress(body)
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.send_header("Last-Modified", stub.last_modified)
                if stub.gzip_body:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                if self.path == "/library/catalog" and stub.on_catalog_fetch is not None:
                    stub.on_catalog_fetch()

        return _Handler


class _RedirectingStub:
    """A server that answers every GET with a 302 to another origin.

    Stands in for a proxy, a misconfigured CDN, or hijacked DNS: the hop the
    producer must not follow while still carrying the service-account bearer
    token.
    """

    def __init__(self, target_base: str, status: int = 302) -> None:
        self.target_base = target_base
        self.status = status
        self.requests: list[_RecordedRequest] = []
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler_class())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self) -> _RedirectingStub:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def _handler_class(self) -> type[BaseHTTPRequestHandler]:
        stub = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *args: object) -> None:
                pass

            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                stub.requests.append(_RecordedRequest(self.path, self.headers.get("Authorization")))
                self.send_response(stub.status)
                self.send_header("Location", stub.target_base + self.path)
                self.send_header("Content-Length", "0")
                self.end_headers()

        return _Handler


@dataclass
class _MysqlCall:
    argv: list[str]
    env: dict[str, str]


@dataclass
class _FakeMysqlRunner:
    """Stands in for the ``mysql`` CLI, writing canned TSV to the capture file.

    ``cta_tsv=None`` simulates a source whose ``COMPILATION_TRACK_ARTIST``
    table does not exist -- the query fails and the build must continue.
    """

    library_tsv: str
    cta_tsv: str | None
    calls: list[_MysqlCall] = field(default_factory=list)

    def __call__(self, argv: list[str], env: dict[str, str], stdout_path: str) -> bool:
        self.calls.append(_MysqlCall(list(argv), dict(env)))
        payload = self.library_tsv if len(self.calls) == 1 else self.cta_tsv
        if payload is None:
            return False
        Path(stdout_path).write_text(payload, encoding="utf-8")
        return True


class TestNormalize:
    """Pure normalization rule: NULL / '' / 'NULL' / whitespace-padding collapse to equal."""

    def test_none_normalizes_to_none(self) -> None:
        mod = _load_module()
        assert mod._normalize(None) is None

    def test_empty_string_normalizes_to_none(self) -> None:
        mod = _load_module()
        assert mod._normalize("") is None

    def test_literal_null_string_normalizes_to_none(self) -> None:
        mod = _load_module()
        assert mod._normalize("NULL") is None

    def test_whitespace_padded_value_is_stripped(self) -> None:
        mod = _load_module()
        assert mod._normalize("  Stereolab  ") == "Stereolab"

    def test_whitespace_only_normalizes_to_none(self) -> None:
        mod = _load_module()
        assert mod._normalize("   ") is None

    def test_non_string_passthrough(self) -> None:
        mod = _load_module()
        assert mod._normalize(100) == 100
        assert mod._normalize(None) is None

    def test_case_is_not_folded(self) -> None:
        """Only the exact literal 'NULL' is treated as NULL -- no case folding."""
        mod = _load_module()
        assert mod._normalize("null") == "null"
        assert mod._normalize("Null") == "Null"

    def test_null_foo_is_not_normalized_away(self) -> None:
        """'null foo' is genuinely different data, not the transient NULL artifact."""
        mod = _load_module()
        assert mod._normalize("null foo") == "null foo"
        assert mod._normalize("null foo") != mod._normalize("")


class TestDiffLibraryDbs:
    """Row-level and field-level diff semantics via diff_library_dbs()."""

    def test_perfect_match_has_zero_diffs(self, tmp_path: Path) -> None:
        mod = _load_module()
        rows = [
            {"id": 1, "title": "Aluminum Tunes", "artist": "Stereolab"},
            {"id": 2, "title": "DOGA", "artist": "Juana Molina"},
        ]
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, rows)
        _make_library_db(backend_db, rows)

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.matched == 2
        assert result.missing_in_backend == 0
        assert result.extra_in_backend == 0
        assert result.missing_in_backend_ids == []
        assert result.extra_in_backend_ids == []
        assert all(count == 0 for count in result.field_mismatches.values())
        assert result.cta_missing == 0
        assert result.cta_extra == 0

    def test_field_mismatch_in_specific_column(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "Rock"}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Electronic"}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.matched == 1
        assert result.field_mismatches["genre"] == 1
        assert all(v == 0 for col, v in result.field_mismatches.items() if col != "genre")

    def test_id_missing_in_backend(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}, {"id": 2}])
        _make_library_db(backend_db, [{"id": 1}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.matched == 1
        assert result.missing_in_backend == 1
        assert result.missing_in_backend_ids == [2]
        assert result.extra_in_backend == 0
        assert result.extra_in_backend_ids == []

    def test_id_extra_in_backend(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}, {"id": 99}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.matched == 1
        assert result.extra_in_backend == 1
        assert result.extra_in_backend_ids == [99]
        assert result.missing_in_backend == 0
        assert result.missing_in_backend_ids == []

    def test_label_column_always_null_is_a_no_op(self, tmp_path: Path) -> None:
        """label is always NULL in prod; excluded from the diffed column set."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "label": None}])
        _make_library_db(backend_db, [{"id": 1, "label": None}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert "label" not in result.field_mismatches
        assert result.matched == 1

    def test_normalization_equivalence_null_empty_string_and_whitespace(
        self, tmp_path: Path
    ) -> None:
        """NULL, '', 'NULL', and whitespace-padded values all count as equal."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(
            mysql_db,
            [
                {"id": 1, "alternate_artist_name": None},
                {"id": 2, "alternate_artist_name": ""},
                {"id": 3, "alternate_artist_name": "NULL"},
                {"id": 4, "alternate_artist_name": "  Sessa  "},
            ],
        )
        _make_library_db(
            backend_db,
            [
                {"id": 1, "alternate_artist_name": ""},
                {"id": 2, "alternate_artist_name": "NULL"},
                {"id": 3, "alternate_artist_name": None},
                {"id": 4, "alternate_artist_name": "Sessa"},
            ],
        )

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.field_mismatches["alternate_artist_name"] == 0

    def test_genuine_difference_is_not_normalized_away(self, tmp_path: Path) -> None:
        """'null foo' vs '' must NOT be normalized to equal."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "alternate_artist_name": "null foo"}])
        _make_library_db(backend_db, [{"id": 1, "alternate_artist_name": ""}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.field_mismatches["alternate_artist_name"] == 1

    def test_cta_row_missing_in_backend(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(
            mysql_db,
            [{"id": 1}],
            cta_rows=[(1, "Duke Ellington", "In a Sentimental Mood")],
        )
        _make_library_db(backend_db, [{"id": 1}], cta_rows=[])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.cta_missing == 1
        assert result.cta_extra == 0

    def test_cta_row_extra_in_backend(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}], cta_rows=[])
        _make_library_db(
            backend_db,
            [{"id": 1}],
            cta_rows=[(1, "John Coltrane", "In a Sentimental Mood")],
        )

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.cta_missing == 0
        assert result.cta_extra == 1

    def test_cta_table_absent_on_both_sides_is_a_noop(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn)
        mysql_conn.close()
        backend_conn.close()

        assert result.cta_missing == 0
        assert result.cta_extra == 0


class TestRunDiff:
    """run_diff() opens both files read-only and raises SourceError on bad input."""

    def test_missing_mysql_file_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        backend_db = tmp_path / "backend.db"
        _make_library_db(backend_db, [{"id": 1}])

        with pytest.raises(mod.SourceError):
            mod.run_diff(str(tmp_path / "does-not-exist.db"), str(backend_db))

    def test_missing_backend_file_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        _make_library_db(mysql_db, [{"id": 1}])

        with pytest.raises(mod.SourceError):
            mod.run_diff(str(mysql_db), str(tmp_path / "does-not-exist.db"))

    def test_missing_library_table_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        # backend.db exists but has no `library` table at all.
        conn = sqlite3.connect(backend_db)
        conn.execute("CREATE TABLE unrelated (id INTEGER)")
        conn.commit()
        conn.close()

        with pytest.raises(mod.SourceError):
            mod.run_diff(str(mysql_db), str(backend_db))

    def test_unreadable_file_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        backend_db = tmp_path / "backend.db"
        _make_library_db(backend_db, [{"id": 1}])
        not_a_db = tmp_path / "not-a-db.db"
        not_a_db.write_text("this is plainly not a sqlite database", encoding="utf-8")

        with pytest.raises(mod.SourceError):
            mod.run_diff(str(not_a_db), str(backend_db))

    def test_run_diff_never_writes_to_inputs(self, tmp_path: Path) -> None:
        """Read-only connections: mtime of both input files is unchanged after run_diff()."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        mysql_mtime_before = mysql_db.stat().st_mtime_ns
        backend_mtime_before = backend_db.stat().st_mtime_ns

        mod.run_diff(str(mysql_db), str(backend_db))

        assert mysql_db.stat().st_mtime_ns == mysql_mtime_before
        assert backend_db.stat().st_mtime_ns == backend_mtime_before

    def test_run_diff_happy_path_returns_parity_diff(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "Rock"}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Rock"}])

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.matched == 1

    def test_duplicate_id_raises_source_error(self, tmp_path: Path) -> None:
        """A library.db with duplicate ids is malformed; collapsing it (last-wins)
        would silently hide row-count drift, so it must raise rather than under-count."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        # backend.db has a `library` table without the id PRIMARY KEY, so it can
        # hold two rows with the same id -- the exact malformed-producer case a
        # parity harness must surface, not swallow.
        conn = sqlite3.connect(backend_db)
        conn.execute(
            """CREATE TABLE library (
            id INTEGER, title TEXT, artist TEXT, call_letters TEXT,
            artist_call_number INTEGER, release_call_number INTEGER,
            genre TEXT, format TEXT, alternate_artist_name TEXT,
            album_artist TEXT, label TEXT, cross_reference_names TEXT
        )"""
        )
        conn.executemany("INSERT INTO library (id) VALUES (?)", [(1,), (1,)])
        conn.commit()
        conn.close()

        with pytest.raises(mod.SourceError):
            mod.run_diff(str(mysql_db), str(backend_db))

    def test_path_with_special_uri_chars_opens_read_only(self, tmp_path: Path) -> None:
        """Paths containing URI-significant characters (?, #, space) must be
        percent-encoded before building the file: URI, or the `?mode=ro` query
        gets misparsed -- opening the wrong file (or dropping read-only)."""
        mod = _load_module()
        weird_dir = tmp_path / "we ird?dir#x"
        weird_dir.mkdir()
        mysql_db = weird_dir / "mysql.db"
        backend_db = weird_dir / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "Rock"}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Rock"}])

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.matched == 1


class TestMainCli:
    """CLI wiring: argument validation, JSON contract, exit codes."""

    def test_missing_required_args_exits_2(self) -> None:
        mod = _load_module()
        assert mod.main([]) == 2

    def test_only_mysql_db_given_exits_2(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        _make_library_db(mysql_db, [{"id": 1}])
        assert mod.main(["--mysql-db", str(mysql_db)]) == 2

    def test_missing_file_exits_3(self, tmp_path: Path) -> None:
        mod = _load_module()
        backend_db = tmp_path / "backend.db"
        _make_library_db(backend_db, [{"id": 1}])
        exit_code = mod.main(
            [
                "--mysql-db",
                str(tmp_path / "does-not-exist.db"),
                "--backend-db",
                str(backend_db),
            ]
        )
        assert exit_code == 3

    def test_missing_table_exits_3(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        conn = sqlite3.connect(backend_db)
        conn.execute("CREATE TABLE unrelated (id INTEGER)")
        conn.commit()
        conn.close()

        exit_code = mod.main(["--mysql-db", str(mysql_db), "--backend-db", str(backend_db)])
        assert exit_code == 3

    def test_duplicate_id_exits_3(self, tmp_path: Path) -> None:
        """A malformed input (duplicate library.id) is a source error -> exit 3."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(backend_db, [{"id": 1}])
        conn = sqlite3.connect(mysql_db)
        conn.execute(
            """CREATE TABLE library (
            id INTEGER, title TEXT, artist TEXT, call_letters TEXT,
            artist_call_number INTEGER, release_call_number INTEGER,
            genre TEXT, format TEXT, alternate_artist_name TEXT,
            album_artist TEXT, label TEXT, cross_reference_names TEXT
        )"""
        )
        conn.executemany("INSERT INTO library (id) VALUES (?)", [(7,), (7,)])
        conn.commit()
        conn.close()

        exit_code = mod.main(["--mysql-db", str(mysql_db), "--backend-db", str(backend_db)])
        assert exit_code == 3

    def test_success_exits_0_even_with_diffs(self, tmp_path: Path) -> None:
        """Exit 0 means 'ran successfully' -- a nonzero diff count is still exit 0."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}, {"id": 2}])
        _make_library_db(backend_db, [{"id": 1}])

        exit_code = mod.main(["--mysql-db", str(mysql_db), "--backend-db", str(backend_db)])
        assert exit_code == 0

    def test_json_output_has_exact_contract_shape(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "Rock"}, {"id": 2}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Electronic"}, {"id": 99}])

        exit_code = mod.main(
            ["--mysql-db", str(mysql_db), "--backend-db", str(backend_db), "--json"]
        )
        assert exit_code == 0

        captured = capsys.readouterr()
        payload = json.loads(captured.out)

        assert payload["matched"] == 1
        assert payload["missing_in_backend"] == 1
        assert payload["extra_in_backend"] == 1
        assert payload["field_mismatches"]["genre"] == 1
        assert payload["cta_missing"] == 0
        assert payload["cta_extra"] == 0
        assert payload["missing_in_backend_ids"] == [2]
        assert payload["extra_in_backend_ids"] == [99]

    def test_human_output_is_not_json(self, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        exit_code = mod.main(["--mysql-db", str(mysql_db), "--backend-db", str(backend_db)])
        assert exit_code == 0

        captured = capsys.readouterr()
        with pytest.raises(json.JSONDecodeError):
            json.loads(captured.out)
        assert "matched" in captured.out

    def test_cli_subprocess_invocation(self, tmp_path: Path) -> None:
        """Running as a subprocess exercises the real __main__ entrypoint."""
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--mysql-db",
                str(mysql_db),
                "--backend-db",
                str(backend_db),
                "--json",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload["matched"] == 1


class TestBackendProducer:
    """``--backend-source``: build a daily-sync-shaped library.db over HTTP (#351).

    Decision D3 / Option B (2026-08-03): the Backend-sourced build reads the
    extended ``GET /library/catalog`` + ``GET /library/catalog/compilation-tracks``
    NDJSON exports (WXYC/Backend-Service#1965) with a service-account bearer
    token -- no prod-DB credentials.
    """

    def test_builds_the_daily_sync_library_shape(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[
                {
                    "legacy_release_id": 72_101,
                    "artist_name": "Juana Molina",
                    "track_title": "la paradoja",
                }
            ],
        ) as stub:
            mod._build_library_db_from_backend(stub.base_url, str(out))

        conn = sqlite3.connect(out)
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(library)")]
            assert tuple(cols) == _LIBRARY_COLUMNS
            row = conn.execute(f"SELECT {', '.join(_LIBRARY_COLUMNS)} FROM library").fetchone()
            # id is legacy_release_id, NOT the BS serial id (which collides
            # with the tubafrenzy id space -- BS#1963 / decision D4).
            assert row == (
                72_101,
                "Aluminum Tunes",
                "Stereolab",
                "ST",
                100,
                1,
                "Rock",
                "CD",
                "",
                "",
                None,
                "",
            )
            assert conn.execute(
                "SELECT library_release_id, artist_name, track_title FROM compilation_track_artist"
            ).fetchall() == [(72_101, "Juana Molina", "la paradoja")]
            # Same FTS + index furniture the MySQL-sourced daily build creates.
            objects = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')"
                )
            }
            assert {"library_fts", "idx_artist", "idx_album_artist", "idx_cta_release"} <= objects
            assert conn.execute(
                "SELECT rowid FROM library_fts WHERE library_fts MATCH 'stereolab'"
            ).fetchall() == [(72_101,)]
        finally:
            conn.close()

    def test_pipe_joins_cross_reference_names(self, tmp_path: Path, monkeypatch) -> None:
        """The wire carries an ARRAY; library.db stores the ' | '-joined string."""
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[
                _catalog_row(
                    legacy_release_id=72_102,
                    artist_name="Nilüfer Yanya",
                    cross_reference_names=["Csillagrablók", "Hermanos Gutiérrez"],
                )
            ],
            cta_rows=[],
        ) as stub:
            mod._build_library_db_from_backend(stub.base_url, str(out))

        conn = sqlite3.connect(out)
        try:
            assert conn.execute("SELECT artist, cross_reference_names FROM library").fetchone() == (
                "Nilüfer Yanya",
                "Csillagrablók | Hermanos Gutiérrez",
            )
        finally:
            conn.close()

    def test_absent_optional_text_becomes_empty_string_not_null(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Match prod byte-for-byte: sync-library.sh IFNULLs these three to ''.

        Not merely a parity nicety -- after the cutover this producer's output
        IS library.db, and a NULL where prod has '' changes what LML's
        pipe-split and the FTS content see.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[
                _catalog_row(
                    legacy_release_id=72_103,
                    alternate_artist_name=None,
                    album_artist=None,
                    cross_reference_names=[],
                )
            ],
            cta_rows=[],
        ) as stub:
            mod._build_library_db_from_backend(stub.base_url, str(out))

        conn = sqlite3.connect(out)
        try:
            assert conn.execute(
                "SELECT alternate_artist_name, album_artist, cross_reference_names FROM library"
            ).fetchone() == ("", "", "")
            assert (
                conn.execute(
                    "SELECT rowid FROM library_fts WHERE library_fts MATCH 'null'"
                ).fetchall()
                == []
            )
        finally:
            conn.close()

    def test_sends_the_service_account_bearer_token(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(catalog_rows=[_catalog_row()], cta_rows=[]) as stub:
            mod._build_library_db_from_backend(stub.base_url, str(out))

        assert [r.path for r in stub.requests] == [
            "/library/catalog",
            "/library/catalog/compilation-tracks",
        ]
        assert {r.authorization for r in stub.requests} == {"Bearer svc-token"}

    def test_reads_an_identity_encoded_body(self, tmp_path: Path, monkeypatch) -> None:
        """Content-Encoding is honoured, not assumed: a non-gzip body still parses."""
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_104)], cta_rows=[], gzip_body=False
        ) as stub:
            mod._build_library_db_from_backend(stub.base_url, str(out))

        conn = sqlite3.connect(out)
        try:
            assert conn.execute("SELECT id FROM library").fetchone() == (72_104,)
        finally:
            conn.close()

    def test_rejects_a_row_with_no_legacy_release_id(self, tmp_path: Path, monkeypatch) -> None:
        """Fail loudly rather than write a library.db row with a null id."""
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(catalog_rows=[_catalog_row(legacy_release_id=None)], cta_rows=[]) as stub:
            with pytest.raises(mod.SourceError, match="legacy_release_id"):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert not out.exists()

    def test_refetches_when_the_watermark_advances_between_the_two_gets(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A Last-Modified change across the pair means 're-fetch both' (api.yaml)."""
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_105)], cta_rows=[]
        ) as stub:
            # First catalog fetch is served at watermark A, then the watermark
            # advances before the CTA fetch -- exactly the torn-snapshot the
            # spec tells the producer to discard. It settles for attempt 2.
            def advance_once() -> None:
                stub.last_modified = _LM_B
                stub.on_catalog_fetch = None

            stub.on_catalog_fetch = advance_once
            mod._build_library_db_from_backend(stub.base_url, str(out))

        assert [r.path for r in stub.requests] == [
            "/library/catalog",
            "/library/catalog/compilation-tracks",
            "/library/catalog",
            "/library/catalog/compilation-tracks",
        ]
        conn = sqlite3.connect(out)
        try:
            assert conn.execute("SELECT id FROM library").fetchone() == (72_105,)
        finally:
            conn.close()

    def test_gives_up_when_the_watermark_never_settles(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(catalog_rows=[_catalog_row()], cta_rows=[]) as stub:
            counter = {"n": 0}

            def always_advance() -> None:
                counter["n"] += 1
                stub.last_modified = f"Sat, 09 Aug 2026 12:{counter['n']:02d}:00 GMT"

            stub.on_catalog_fetch = always_advance
            with pytest.raises(mod.SourceError, match="watermark"):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert not out.exists()

    def test_refetches_on_a_compilation_track_with_no_catalog_row(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A dangling legacy_release_id is evidence to re-fetch, then an error."""
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_106)],
            cta_rows=[
                {
                    "legacy_release_id": 999_999,
                    "artist_name": "Jessica Pratt",
                    "track_title": "Back, Baby",
                }
            ],
        ) as stub:
            with pytest.raises(mod.SourceError, match="compilation"):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert len(stub.requests) > 2  # retried the pair before giving up
        assert not out.exists()

    def test_refuses_plaintext_http_to_a_remote_host(self, tmp_path: Path, monkeypatch) -> None:
        """Never put a bearer token on the wire in the clear."""
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        with pytest.raises(mod.SourceError, match="https"):
            mod._build_library_db_from_backend("http://api.wxyc.org", str(tmp_path / "backend.db"))

    def test_requires_the_service_account_token(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.delenv(mod.BACKEND_TOKEN_ENV, raising=False)
        with pytest.raises(mod.SourceError, match=mod.BACKEND_TOKEN_ENV):
            mod._build_library_db_from_backend("https://api.wxyc.org", str(tmp_path / "backend.db"))

    def test_refuses_to_overwrite_an_existing_file(self, tmp_path: Path, monkeypatch) -> None:
        """Scratch copies only -- never clobber a prod artifact or a built input."""
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        existing = tmp_path / "library.db"
        existing.write_bytes(b"precious")
        with pytest.raises(mod.SourceError, match="exists"):
            mod._build_library_db_from_backend("https://api.wxyc.org", str(existing))
        assert existing.read_bytes() == b"precious"

    def test_refuses_to_follow_a_cross_origin_redirect(self, tmp_path: Path, monkeypatch) -> None:
        """The bearer token must not be replayed to a host the operator didn't name.

        urllib's default redirect handler copies every header (including
        Authorization) into the redirected request, so a 302 from a proxy,
        a misconfigured CDN, or hijacked DNS would hand the service-account
        JWT to a foreign origin -- and to a plaintext one, defeating the
        https-only check entirely (that check only ever sees the first URL).
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(catalog_rows=[_catalog_row()], cta_rows=[]) as elsewhere:
            with _RedirectingStub(elsewhere.base_url) as redirector:
                with pytest.raises(mod.SourceError, match="redirect"):
                    mod._build_library_db_from_backend(redirector.base_url, str(out))
            assert elsewhere.requests == []
        assert not out.exists()

    def test_rejects_a_string_cross_reference_names(self, tmp_path: Path, monkeypatch) -> None:
        """A scalar where the contract promises an array must fail, not char-split.

        ``' | '.join("Stereolab")`` yields 'S | t | e | r | e | o | l | a | b'
        -- one phantom alias per letter, which LML then pipe-splits into the
        live search index. Silent, and indistinguishable from real drift.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(cross_reference_names="Stereolab")], cta_rows=[]
        ) as stub:
            with pytest.raises(mod.SourceError, match="cross_reference_names"):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert not out.exists()

    @pytest.mark.parametrize(
        "field_name",
        [
            "album_title",
            "artist_name",
            "code_letters",
            "code_artist_number",
            "code_number",
            "genre_name",
            "format_name",
        ],
    )
    def test_rejects_a_missing_required_field(
        self, tmp_path: Path, monkeypatch, field_name: str
    ) -> None:
        """Every api.yaml-`required` CatalogExportRow field is validated, not just the id.

        A bare ``.get()`` would write SQL NULL for a renamed or regressed
        field: post-cutover that empties the row's FTS content and breaks LML
        search, and pre-cutover it looks like an ordinary field mismatch.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(catalog_rows=[_catalog_row(**{field_name: None})], cta_rows=[]) as stub:
            with pytest.raises(mod.SourceError, match=field_name):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert not out.exists()

    def test_rejects_a_zero_row_catalog_export(self, tmp_path: Path, monkeypatch) -> None:
        """An empty catalog is a producer failure, never a 64,815-row drift report.

        A broken export query, an over-narrow token scope, or a truncated
        cached buffer all surface as a 200 with no rows; building from it
        would report the whole catalog as missing_in_backend.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(catalog_rows=[], cta_rows=[]) as stub:
            with pytest.raises(mod.SourceError, match="no rows"):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert not out.exists()

    def test_leaves_no_partial_file_when_the_build_fails(self, tmp_path: Path, monkeypatch) -> None:
        """A failure mid-insert must not leave a stub file that wedges the next run.

        ``_require_absent`` refuses any path that exists, so a partial write
        here would block every subsequent parity day until an operator
        deleted it by hand.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        # Two rows sharing a legacy_release_id: the mapping succeeds, and the
        # library.id PRIMARY KEY rejects the second INSERT mid-build.
        with _BackendStub(
            catalog_rows=[_catalog_row(id=1), _catalog_row(id=2)], cta_rows=[]
        ) as stub:
            with pytest.raises(mod.SourceError):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert not out.exists()
        assert list(tmp_path.iterdir()) == []

    def test_rejects_a_non_integer_legacy_release_id(self, tmp_path: Path, monkeypatch) -> None:
        """A non-numeric id is a contract violation -- a SourceError, not a ValueError."""
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        bad_row = _catalog_row(legacy_release_id="ST-100")
        with _BackendStub(catalog_rows=[bad_row], cta_rows=[]) as stub:
            with pytest.raises(mod.SourceError, match="legacy_release_id"):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert not out.exists()

    def test_tolerates_mixed_id_types_across_the_two_exports(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A stringified bigint on one side must not crash the dangling-id check.

        Sorting ``{"72101", 72101}`` with a raw ``<`` raises TypeError inside
        the torn-snapshot error path -- the one place that has to stay
        diagnosable.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[
                {
                    "legacy_release_id": "72101",
                    "artist_name": "Juana Molina",
                    "track_title": "la paradoja",
                }
            ],
        ) as stub:
            mod._build_library_db_from_backend(stub.base_url, str(out))

        conn = sqlite3.connect(out)
        try:
            assert conn.execute(
                "SELECT library_release_id FROM compilation_track_artist"
            ).fetchall() == [(72_101,)]
        finally:
            conn.close()


class TestSharedSchemaConstants:
    """The producer must not re-declare what ``lib/library_db.py`` owns."""

    def test_library_columns_is_the_shared_constant(self) -> None:
        """A local copy would silently keep diffing the old column set.

        ``lib/library_db.py`` exists so the shape lives in exactly one place;
        a re-declaration here means a column added there becomes a
        permanently-undiffed blind spot in the tool that certifies the cutover.
        """
        mod = _load_module()
        sys.path.insert(0, str(REPO_ROOT))
        from lib.library_db import LIBRARY_COLUMNS as shared_columns

        assert mod.LIBRARY_COLUMNS is shared_columns
        assert tuple(shared_columns) == _LIBRARY_COLUMNS


class TestMysqlProducer:
    """``--mysql-source``: build the baseline library.db via the daily-sync read path."""

    def test_builds_via_the_mysql_cli_read_path(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.delenv(mod.MYSQL_PASSWORD_ENV, raising=False)
        out = tmp_path / "mysql.db"
        runner = _FakeMysqlRunner(
            library_tsv=("72101\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\t\t\n"),
            cta_tsv="72101\tJuana Molina\tla paradoja\n",
        )
        monkeypatch.setattr(mod, "_mysql_runner", runner)

        mod._build_library_db_from_mysql("mysql://wxyc:sekrit@127.0.0.1:13306/wxycmusic", str(out))

        conn = sqlite3.connect(out)
        try:
            assert conn.execute("SELECT id, artist FROM library").fetchall() == [
                (72_101, "Stereolab")
            ]
            assert conn.execute(
                "SELECT library_release_id, artist_name FROM compilation_track_artist"
            ).fetchall() == [(72_101, "Juana Molina")]
        finally:
            conn.close()

        # Same invocation shape as sync-library.sh: mysql CLI (not a Python
        # driver -- MySQL 4.1 auth), batch/raw mode, password via MYSQL_PWD.
        assert runner.calls[0].argv[0] == "mysql"
        assert "-B" in runner.calls[0].argv and "-N" in runner.calls[0].argv
        assert runner.calls[0].env["MYSQL_PWD"] == "sekrit"
        assert "sekrit" not in " ".join(runner.calls[0].argv)

    def test_continues_when_the_compilation_track_table_is_absent(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """CTA is supplementary; a missing table must not fail the build."""
        mod = _load_module()
        out = tmp_path / "mysql.db"
        runner = _FakeMysqlRunner(
            library_tsv="72101\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\t\t\n",
            cta_tsv=None,  # query fails, as it does on a source with no CTA table
        )
        monkeypatch.setattr(mod, "_mysql_runner", runner)

        mod._build_library_db_from_mysql("mysql://wxyc:sekrit@127.0.0.1/wxycmusic", str(out))

        conn = sqlite3.connect(out)
        try:
            assert conn.execute("SELECT count(*) FROM library").fetchone() == (1,)
            assert conn.execute(
                "SELECT count(*) FROM sqlite_master WHERE name = 'compilation_track_artist'"
            ).fetchone() == (0,)
        finally:
            conn.close()

    def test_refuses_to_overwrite_an_existing_file(self, tmp_path: Path) -> None:
        mod = _load_module()
        existing = tmp_path / "library.db"
        existing.write_bytes(b"precious")
        with pytest.raises(mod.SourceError, match="exists"):
            mod._build_library_db_from_mysql("mysql://u:p@h/db", str(existing))
        assert existing.read_bytes() == b"precious"

    def test_select_statements_match_sync_library_sh(self) -> None:
        """Drift guard: the baseline must be the *same* query prod runs daily.

        A producer that quietly diverges from ``scripts/sync-library.sh``
        would diff the Backend build against something production never
        builds -- parity would then measure the harness, not the migration.

        Asserted as set equality over every ``-e "SELECT ..."`` the script
        runs, not as substring containment: containment is one-directional,
        so appending ``ORDER BY``/``LIMIT`` to the shell's copy -- or adding a
        third, divergent SELECT -- would leave the guard green while the two
        producers ran different queries.
        """
        mod = _load_module()
        script = (REPO_ROOT / "scripts" / "sync-library.sh").read_text(encoding="utf-8")
        assert _sync_library_selects(script) == {
            _squash(mod.LIBRARY_SELECT_SQL),
            _squash(mod.COMPILATION_TRACK_SELECT_SQL),
        }

    def test_password_comes_from_the_environment_not_the_dsn(self, monkeypatch) -> None:
        """A DSN password sits in *this* process's argv, visible to `ps`."""
        mod = _load_module()
        monkeypatch.setenv(mod.MYSQL_PASSWORD_ENV, "sekrit")
        argv, env = mod._mysql_invocation("mysql://wxyc@127.0.0.1:13306/wxycmusic")

        assert env["MYSQL_PWD"] == "sekrit"
        assert "sekrit" not in " ".join(argv)
        assert argv[:5] == ["mysql", "-h", "127.0.0.1", "-P", "13306"]

    def test_environment_password_wins_over_a_dsn_password(self, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.MYSQL_PASSWORD_ENV, "from-env")
        _, env = mod._mysql_invocation("mysql://wxyc:from-dsn@127.0.0.1/wxycmusic")
        assert env["MYSQL_PWD"] == "from-env"

    def test_a_malformed_port_raises_source_error(self, monkeypatch) -> None:
        """A `/` in an un-encoded DSN password shifts the netloc, so the port
        parse blows up with a bare ValueError deep in urllib -- exit 1 with a
        traceback, not the documented exit 3."""
        mod = _load_module()
        monkeypatch.delenv(mod.MYSQL_PASSWORD_ENV, raising=False)
        with pytest.raises(mod.SourceError, match="port"):
            mod._mysql_invocation("mysql://wxyc:se/kr@127.0.0.1:13306/wxycmusic")


class TestProducerCli:
    """Wiring: --mysql-source / --backend-source build, then the core diffs."""

    def test_round_trip_builds_both_sides_and_diffs_to_zero(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        monkeypatch.setattr(
            mod,
            "_mysql_runner",
            _FakeMysqlRunner(
                library_tsv=(
                    "72101\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\t\t\n"
                    "72102\tOn Your Own Love Again\tJessica Pratt\tPR\t42\t7\tRock\tLP\t\t\t\n"
                ),
                cta_tsv="72101\tJuana Molina\tla paradoja\n",
            ),
        )
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[
                _catalog_row(legacy_release_id=72_101),
                _catalog_row(
                    legacy_release_id=72_102,
                    album_title="On Your Own Love Again",
                    artist_name="Jessica Pratt",
                    code_letters="PR",
                    code_artist_number=42,
                    code_number=7,
                    format_name="LP",
                ),
            ],
            cta_rows=[
                {
                    "legacy_release_id": 72_101,
                    "artist_name": "Juana Molina",
                    "track_title": "la paradoja",
                }
            ],
        ) as stub:
            exit_code = mod.main(
                [
                    "--mysql-source",
                    "mysql://wxyc:sekrit@127.0.0.1/wxycmusic",
                    "--mysql-db",
                    str(mysql_db),
                    "--backend-source",
                    stub.base_url,
                    "--backend-db",
                    str(backend_db),
                    "--json",
                ]
            )

        assert exit_code == 0
        assert mysql_db.exists() and backend_db.exists()
        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.matched == 2
        assert result.missing_in_backend == 0
        assert result.extra_in_backend == 0
        assert set(result.field_mismatches.values()) == {0}
        assert (result.cta_missing, result.cta_extra) == (0, 0)

    def test_json_stdout_stays_a_single_object_while_producing(
        self, tmp_path: Path, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        """Producer progress must not leak into the machine-readable channel.

        Both producers print an "Exported N rows" line and the CTA import
        prints its own; on stdout they would sit in front of the JSON object
        and break every consumer that parses it.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        monkeypatch.setattr(
            mod,
            "_mysql_runner",
            _FakeMysqlRunner(
                library_tsv="72101\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\t\t\n",
                cta_tsv="72101\tJuana Molina\tla paradoja\n",
            ),
        )
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[
                {
                    "legacy_release_id": 72_101,
                    "artist_name": "Juana Molina",
                    "track_title": "la paradoja",
                }
            ],
        ) as stub:
            exit_code = mod.main(
                [
                    "--mysql-source",
                    "mysql://wxyc:sekrit@127.0.0.1/wxycmusic",
                    "--mysql-db",
                    str(tmp_path / "mysql.db"),
                    "--backend-source",
                    stub.base_url,
                    "--backend-db",
                    str(tmp_path / "backend.db"),
                    "--json",
                ]
            )

        assert exit_code == 0
        captured = capsys.readouterr()
        assert json.loads(captured.out)["matched"] == 1  # the WHOLE of stdout
        assert "Exported" in captured.err

    def test_source_flag_without_an_output_path_is_a_usage_error(self) -> None:
        mod = _load_module()
        assert mod.main(["--backend-source", "https://api.wxyc.org", "--mysql-db", "x"]) == 2
        assert mod.main(["--mysql-source", "mysql://u:p@h/db", "--backend-db", "x"]) == 2

    def test_a_build_failure_exits_three(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.delenv(mod.BACKEND_TOKEN_ENV, raising=False)
        exit_code = mod.main(
            [
                "--backend-source",
                "https://api.wxyc.org",
                "--backend-db",
                str(tmp_path / "backend.db"),
                "--mysql-db",
                str(tmp_path / "mysql.db"),
            ]
        )
        assert exit_code == 3

    def test_an_unexpected_producer_failure_still_exits_three(
        self, tmp_path: Path, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        """Exit 3 is the documented contract for *any* producer failure.

        Catching only SourceError let a `mysql` binary that isn't on PATH, a
        missing output directory, or a malformed DSN port escape as a raw
        traceback and exit 1.
        """
        mod = _load_module()
        monkeypatch.delenv(mod.MYSQL_PASSWORD_ENV, raising=False)

        def explode(*args: object, **kwargs: object) -> bool:
            raise FileNotFoundError(2, "No such file or directory: 'mysql'")

        monkeypatch.setattr(mod, "_mysql_runner", explode)
        exit_code = mod.main(
            [
                "--mysql-source",
                "mysql://wxyc@127.0.0.1/wxycmusic",
                "--mysql-db",
                str(tmp_path / "mysql.db"),
                "--backend-db",
                str(tmp_path / "backend.db"),
            ]
        )
        assert exit_code == 3
        assert "error:" in capsys.readouterr().err

    def test_output_paths_are_checked_before_any_build_runs(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Refusing the backend path only *after* the MySQL export burns the whole export.

        Both --*-db paths are validated up front, so a same-path mistake
        costs nothing.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        monkeypatch.delenv(mod.MYSQL_PASSWORD_ENV, raising=False)
        runner = _FakeMysqlRunner(
            library_tsv="72101\tAluminum Tunes\tStereolab\tST\t100\t1\tRock\tCD\t\t\t\n",
            cta_tsv=None,
        )
        monkeypatch.setattr(mod, "_mysql_runner", runner)
        existing = tmp_path / "backend.db"
        existing.write_bytes(b"precious")

        exit_code = mod.main(
            [
                "--mysql-source",
                "mysql://wxyc@127.0.0.1/wxycmusic",
                "--mysql-db",
                str(tmp_path / "mysql.db"),
                "--backend-source",
                "https://api.wxyc.org",
                "--backend-db",
                str(existing),
            ]
        )

        assert exit_code == 3
        assert runner.calls == []
        assert existing.read_bytes() == b"precious"
        assert not (tmp_path / "mysql.db").exists()
