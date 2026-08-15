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

import base64
import gzip
import importlib.util
import json
import logging
import re
import sqlite3
import subprocess
import sys
import threading
import time
import unicodedata
from dataclasses import dataclass, field, fields
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


def _fake_jwt(expires_in_seconds: int = 900, *, unreadable_exp: bool = False) -> str:
    """A JWT-shaped string whose payload carries an ``exp``.

    The producer never verifies a signature -- it decodes ``exp`` locally to
    decide when to refresh -- so a real key would only make the fixture
    slower. The signature segment is deliberately garbage.

    ``unreadable_exp`` produces the shape a claim-format change upstream would
    hand us: still a JWT, still accepted by Backend-Service, but with nothing
    the local decode can turn into an epoch.
    """
    exp: Any = "not-an-epoch" if unreadable_exp else int(time.time()) + expires_in_seconds
    payload = {"exp": exp, "role": "member"}

    def seg(obj: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{seg({'alg': 'EdDSA'})}.{seg(payload)}.not-a-signature"


@dataclass
class _RecordedRequest:
    path: str
    authorization: str | None
    # better-auth's CSRF guard rejects an origin-less sign-in, so the mint
    # path has to send one -- which no test could assert on while this stub
    # recorded only (path, authorization).
    origin: str | None = None


class _BackendStub:
    """A stand-in for Backend-Service's two catalog NDJSON exports.

    Serves gzipped NDJSON with a ``Last-Modified`` watermark, records every
    request, and exposes ``on_catalog_fetch`` so a test can advance the
    watermark mid-pair to exercise the torn-snapshot re-fetch rule.

    It also stands in for the two auth-service endpoints the producer mints
    through (``POST /auth/sign-in/email`` -> session, ``GET /auth/token`` ->
    JWT). Passing ``credentials`` turns on bearer enforcement: the exports
    then 401 anything but a JWT this stub actually issued, which is what
    makes the refresh path testable. Left at ``None`` (the default), any
    bearer is accepted and the auth endpoints simply go unused -- the regime
    every pre-#365 test in this file runs under.
    """

    def __init__(
        self,
        catalog_rows: list[dict[str, Any]],
        cta_rows: list[dict[str, Any]],
        gzip_body: bool = True,
        last_modified: str = _LM_A,
        credentials: tuple[str, str] | None = None,
        jwt_ttl_seconds: int = 900,
        unreadable_jwt_exp: bool = False,
        compilation_tracks_by_id: dict[int, list[dict[str, Any]]] | None = None,
    ) -> None:
        self.catalog_rows = catalog_rows
        self.cta_rows = cta_rows
        self.gzip_body = gzip_body
        self.last_modified = last_modified
        self.credentials = credentials
        self.jwt_ttl_seconds = jwt_ttl_seconds
        self.unreadable_jwt_exp = unreadable_jwt_exp
        # GET /library/{id}/compilation-tracks (BS#1964), keyed on the
        # Backend serial id -- distinct from the bulk
        # /library/catalog/compilation-tracks export above, and the only
        # source of `track_position` (WXYC/Backend-Service#2152's
        # --capture-fffd-cta-pairs mode). A missing key answers 404, an
        # empty list answers 200 with `tracks: []`.
        self.compilation_tracks_by_id = compilation_tracks_by_id or {}
        # What a forced 429 puts in its retry hint. The express limiter's
        # window is 15 minutes, so a hint far longer than any retry the
        # producer is willing to wait out is the realistic case, not an
        # exotic one.
        self.sign_in_retry_after: str | None = "1"
        self.requests: list[_RecordedRequest] = []
        self.on_catalog_fetch: Callable[[], None] | None = None
        # Queues of forced statuses, popped left-to-right; empty means "behave
        # normally". A test drives 429-then-200, or a 401 from the exchange,
        # by pre-loading these.
        self.sign_in_statuses: list[int] = []
        self.exchange_statuses: list[int] = []
        self.sign_out_statuses: list[int] = []
        # Everything this stub has minted and not superseded.
        self.sessions: set[str] = set()
        self.jwts: set[str] = set()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler_class())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def sign_ins(self) -> int:
        return sum(1 for r in self.requests if r.path == "/auth/sign-in/email")

    @property
    def sign_outs(self) -> int:
        return sum(1 for r in self.requests if r.path == "/auth/sign-out")

    @property
    def exchanges(self) -> int:
        return sum(1 for r in self.requests if r.path == "/auth/token")

    @property
    def export_requests(self) -> list[_RecordedRequest]:
        return [r for r in self.requests if not r.path.startswith("/auth/")]

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

            def _record(self) -> None:
                stub.requests.append(
                    _RecordedRequest(
                        self.path,
                        self.headers.get("Authorization"),
                        self.headers.get("Origin"),
                    )
                )

            def _send_json(self, status: int, payload: dict[str, Any], **headers: str) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                for name, value in headers.items():
                    self.send_header(name.replace("_", "-"), value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                self._record()
                if self.path == "/auth/sign-out":
                    forced = stub.sign_out_statuses.pop(0) if stub.sign_out_statuses else None
                    if forced == 302:
                        # A proxy that redirects only this path -- the shape
                        # that reaches the redirect handler, not the socket
                        # layer, and so raises SourceError rather than OSError.
                        self._send_json(
                            302, {"message": "moved"}, Location="https://elsewhere.example.org/x"
                        )
                        return
                    if forced is not None:
                        self._send_json(forced, {"message": "forced"})
                        return
                    bearer = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                    stub.sessions.discard(bearer)
                    self._send_json(200, {"success": True})
                    return
                if self.path != "/auth/sign-in/email":
                    self.send_error(404)
                    return
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                forced = stub.sign_in_statuses.pop(0) if stub.sign_in_statuses else None
                if forced == 429:
                    # better-auth's own limiter emits X-Retry-After, NOT the
                    # standard header the express limiter sends -- a producer
                    # that reads only one of the two waits the wrong amount.
                    # None stands in for a limiter that sends neither (a proxy
                    # or WAF in front of both).
                    hint = (
                        {"X_Retry_After": stub.sign_in_retry_after}
                        if stub.sign_in_retry_after is not None
                        else {}
                    )
                    self._send_json(429, {"message": "Too many requests"}, **hint)
                    return
                if forced is not None:
                    self._send_json(forced, {"message": "forced"})
                    return
                if (
                    stub.credentials is not None
                    and (
                        body.get("email"),
                        body.get("password"),
                    )
                    != stub.credentials
                ):
                    self._send_json(401, {"message": "Invalid email or password"})
                    return
                session = f"session-{len(stub.sessions) + 1}"
                stub.sessions.add(session)
                self._send_json(200, {"token": session, "user": {"id": "svc-user"}})

            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                if self.path == "/auth/token":
                    self._record()
                    forced = stub.exchange_statuses.pop(0) if stub.exchange_statuses else None
                    if forced is not None:
                        self._send_json(forced, {"message": "forced"})
                        return
                    bearer = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                    if bearer not in stub.sessions:
                        self._send_json(401, {"message": "Unauthorized"})
                        return
                    jwt = _fake_jwt(stub.jwt_ttl_seconds, unreadable_exp=stub.unreadable_jwt_exp)
                    stub.jwts.add(jwt)
                    self._send_json(200, {"token": jwt})
                    return

                self._record()
                if stub.credentials is not None:
                    bearer = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                    if bearer not in stub.jwts:
                        self._send_json(401, {"error": "Unauthorized: Invalid or expired token."})
                        return
                by_id_match = re.match(r"^/library/(\d+)/compilation-tracks$", self.path)
                if by_id_match is not None:
                    backend_id = int(by_id_match.group(1))
                    if backend_id not in stub.compilation_tracks_by_id:
                        self.send_error(404)
                        return
                    self._send_json(
                        200,
                        {
                            "library_id": backend_id,
                            "tracks": stub.compilation_tracks_by_id[backend_id],
                        },
                    )
                    return
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

            def _redirect(self) -> None:
                stub.requests.append(
                    _RecordedRequest(
                        self.path,
                        self.headers.get("Authorization"),
                        self.headers.get("Origin"),
                    )
                )
                self.send_response(stub.status)
                self.send_header("Location", stub.target_base + self.path)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                self._redirect()

            # A proxy that redirects GETs redirects POSTs too, and the POST is
            # the dangerous one: it carries the sign-in password.
            def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                length = int(self.headers.get("Content-Length") or 0)
                self.rfile.read(length)
                self._redirect()

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


def _row(**overrides: Any) -> dict[str, Any]:
    """One raw library.db row for the field-classification tests below.

    Merges onto ``_DEFAULT_ROW`` (id 1) the same way ``_make_library_db``
    does, so a classifier test only has to name the field(s) it cares about.
    """
    merged: dict[str, Any] = {"id": 1, **_DEFAULT_ROW}
    merged.update(overrides)
    return merged


class TestClassifyField:
    """``classify_field(col, mysql_row, backend_row)`` -- Part 1's per-column
    model: agree / normalized / mismatch, derived by replaying Backend's own
    ETL transforms (``lib/backend_catalog_norm.py``) against the raw
    mysql-sourced value.
    """

    # --- artist ---------------------------------------------------------

    def test_artist_agrees_when_backend_equals_mysql_verbatim(self) -> None:
        mod = _load_module()
        mysql_row = _row(artist="Stereolab")
        backend_row = _row(artist="Stereolab")
        assert mod.classify_field("artist", mysql_row, backend_row) == ("agree", None)

    def test_artist_various_artists_drop_is_normalized(self) -> None:
        mod = _load_module()
        mysql_row = _row(artist="Various Artists - latin")
        backend_row = _row(artist="Various Artists")
        tier, cls = mod.classify_field("artist", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "various_artists"

    def test_artist_tab_and_newline_substituted_before_normalizing(self) -> None:
        mod = _load_module()
        mysql_row = _row(artist="Jessica Pratt\n(reissue)")
        backend_row = _row(artist="Jessica Pratt (reissue)")
        tier, cls = mod.classify_field("artist", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "trimmed_or_substituted"

    def test_artist_fold_equal_stored_spelling_is_normalized(self) -> None:
        """``ensureArtist`` returns the STORED spelling on a fold match, e.g.
        an existing artist row already spelled in NFD (decomposed) form --
        not reproducible by replaying ``normalize_artist_name`` alone."""
        mod = _load_module()
        nfc = "Nilüfer Yanya"  # ü = U+00FC
        nfd = unicodedata.normalize("NFD", nfc)  # u + U+0308 (combining diaeresis)
        assert nfc != nfd, "the two composition forms must be byte-distinct for this test"
        mysql_row = _row(artist=nfc)
        backend_row = _row(artist=nfd)
        tier, cls = mod.classify_field("artist", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "fold_equal"

    def test_artist_genuine_difference_is_a_mismatch(self) -> None:
        mod = _load_module()
        mysql_row = _row(artist="Stereolab")
        backend_row = _row(artist="Cat Power")
        assert mod.classify_field("artist", mysql_row, backend_row)[0] == "mismatch"

    # --- call_letters -----------------------------------------------------

    def test_call_letters_agree_when_equal(self) -> None:
        mod = _load_module()
        mysql_row = _row(call_letters="ST")
        backend_row = _row(call_letters="ST")
        assert mod.classify_field("call_letters", mysql_row, backend_row) == ("agree", None)

    def test_call_letters_uppercased_is_normalized(self) -> None:
        mod = _load_module()
        mysql_row = _row(call_letters="st")
        backend_row = _row(call_letters="ST")
        tier, cls = mod.classify_field("call_letters", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "uppercased"

    def test_call_letters_va_override_from_artist_not_from_the_code_itself(self) -> None:
        """On a VA row ``normalize_code_letters`` is never called -- the
        ``isVarious`` branch short-circuits straight to "V/A" regardless of
        what the mysql-side code letters actually were."""
        mod = _load_module()
        mysql_row = _row(artist="Various Artists", call_letters="XY")
        backend_row = _row(artist="Various Artists", call_letters="V/A")
        tier, cls = mod.classify_field("call_letters", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "various"

    def test_call_letters_empty_falls_back_to_double_question_mark(self) -> None:
        mod = _load_module()
        mysql_row = _row(call_letters=None)
        backend_row = _row(call_letters="??")
        tier, cls = mod.classify_field("call_letters", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "fallback_unknown"

    def test_call_letters_whitespace_only_also_falls_back(self) -> None:
        """Whitespace-only mysql call letters is the same 'nothing to derive
        from' case as None/empty -- ``normalize_code_letters`` returns None
        for all three, so the fallback class must match regardless of which
        falsy-ish shape the raw value took."""
        mod = _load_module()
        mysql_row = _row(call_letters="   ")
        backend_row = _row(call_letters="??")
        tier, cls = mod.classify_field("call_letters", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "fallback_unknown"

    def test_call_letters_stored_casing_is_normalized_case_insensitively(self) -> None:
        """An artist row created outside this ETL keeps its own casing;
        ``ensureArtist`` resolves on ``lower(code_letters)`` and never
        rewrites it, so a case-only difference from the derived (uppercase)
        expectation is normalized, not a defect."""
        mod = _load_module()
        mysql_row = _row(call_letters="ST")
        backend_row = _row(call_letters="St")
        tier, cls = mod.classify_field("call_letters", mysql_row, backend_row)
        assert tier == "normalized"

    def test_call_letters_z_code_gets_its_own_class_not_uppercased(self) -> None:
        """The unanchored ``/Z-[A-Z]/`` -> "V/A" return is a THIRD population,
        distinct from both plain uppercasing and the ``isVarious`` branch.

        ``lib/backend_catalog_norm.normalize_code_letters``'s docstring turns on
        exactly this split (the plan's R11 ``Z--`` measurement), and these class
        names are the key space the residue ledger's ``baselines`` block will
        share -- so filing the Z-code cohort under "uppercased" would make the
        ledger unable to size the one distinction the port preserves.
        """
        mod = _load_module()
        mysql_row = _row(artist="Stereolab", call_letters="Z-S")
        backend_row = _row(artist="Stereolab", call_letters="V/A")
        tier, cls = mod.classify_field("call_letters", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "various_artists_code"

    def test_call_letters_z_dash_dash_is_not_a_z_code(self) -> None:
        """``Z--`` does not match ``/Z-[A-Z]/`` -- it keeps its value via the
        3-char branch, so it is ordinary uppercasing, not the Z-code class.
        3,104 rows on the 2026-07-19 prod snapshot ride on this distinction."""
        mod = _load_module()
        mysql_row = _row(artist="Stereolab", call_letters="z--")
        backend_row = _row(artist="Stereolab", call_letters="Z--")
        tier, cls = mod.classify_field("call_letters", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "uppercased"

    def test_call_letters_genuine_difference_is_a_mismatch(self) -> None:
        mod = _load_module()
        mysql_row = _row(call_letters="ST")
        backend_row = _row(call_letters="XY")
        assert mod.classify_field("call_letters", mysql_row, backend_row)[0] == "mismatch"

    # --- genre --------------------------------------------------------

    def test_genre_agrees_when_equal(self) -> None:
        mod = _load_module()
        mysql_row = _row(genre="Rock")
        backend_row = _row(genre="Rock")
        assert mod.classify_field("genre", mysql_row, backend_row) == ("agree", None)

    def test_genre_case_difference_is_normalized(self) -> None:
        mod = _load_module()
        mysql_row = _row(genre="rock")
        backend_row = _row(genre="Rock")
        tier, cls = mod.classify_field("genre", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "case_folded"

    def test_genre_genuine_difference_is_a_mismatch(self) -> None:
        mod = _load_module()
        mysql_row = _row(genre="Rock")
        backend_row = _row(genre="Electronic")
        assert mod.classify_field("genre", mysql_row, backend_row)[0] == "mismatch"

    # --- format ---------------------------------------------------------

    def test_format_agrees_when_equal(self) -> None:
        mod = _load_module()
        mysql_row = _row(format="cd")
        backend_row = _row(format="cd")
        assert mod.classify_field("format", mysql_row, backend_row) == ("agree", None)

    def test_format_separator_dropped_is_normalized(self) -> None:
        mod = _load_module()
        mysql_row = _row(format="Vinyl - LP")
        backend_row = _row(format='vinyl 12"')
        tier, cls = mod.classify_field("format", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "format_derived"

    def test_format_case_difference_is_normalized(self) -> None:
        mod = _load_module()
        mysql_row = _row(format="cd")
        backend_row = _row(format="CD")
        tier, cls = mod.classify_field("format", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "format_derived"

    def test_format_unparseable_mysql_value_is_a_mismatch(self) -> None:
        mod = _load_module()
        mysql_row = _row(format="cassette")
        backend_row = _row(format="cd")
        assert mod.classify_field("format", mysql_row, backend_row)[0] == "mismatch"

    def test_format_genuine_difference_is_a_mismatch(self) -> None:
        mod = _load_module()
        mysql_row = _row(format="cd")
        backend_row = _row(format="vinyl")
        assert mod.classify_field("format", mysql_row, backend_row)[0] == "mismatch"

    # --- artist_call_number ---------------------------------------------

    def test_artist_call_number_agrees_when_equal(self) -> None:
        mod = _load_module()
        mysql_row = _row(artist_call_number=100)
        backend_row = _row(artist_call_number=100)
        assert mod.classify_field("artist_call_number", mysql_row, backend_row) == ("agree", None)

    def test_artist_call_number_null_coalesces_to_zero(self) -> None:
        mod = _load_module()
        mysql_row = _row(artist="Stereolab", artist_call_number=None)
        backend_row = _row(artist="Stereolab", artist_call_number=0)
        tier, cls = mod.classify_field("artist_call_number", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "null_coalesced_zero"

    def test_artist_call_number_various_forces_zero_regardless_of_mysql_value(self) -> None:
        mod = _load_module()
        mysql_row = _row(artist="Various Artists", artist_call_number=100)
        backend_row = _row(artist="Various Artists", artist_call_number=0)
        tier, cls = mod.classify_field("artist_call_number", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "various"

    @pytest.mark.parametrize("raw", ["", "   ", "NULL"])
    def test_artist_call_number_coalesces_normalize_null_shapes(self, raw: str) -> None:
        """Backend reads this column through ``toNullableNumber``, which maps
        empty *and* non-numeric text to null before the ``?? 0``. So every shape
        ``_normalize`` already treats as NULL -- ``''``, whitespace, and the
        literal ``"NULL"`` the mysql export pipeline is known to emit -- derives
        ``0``, not just a true SQL NULL. Branching on ``mysql_raw is not None``
        instead files them as defects.
        """
        mod = _load_module()
        mysql_row = _row(artist="Stereolab", artist_call_number=raw)
        backend_row = _row(artist="Stereolab", artist_call_number=0)
        tier, cls = mod.classify_field("artist_call_number", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "null_coalesced_zero"

    def test_artist_call_number_genuine_difference_is_a_mismatch(self) -> None:
        mod = _load_module()
        mysql_row = _row(artist="Stereolab", artist_call_number=100)
        backend_row = _row(artist="Stereolab", artist_call_number=999)
        assert mod.classify_field("artist_call_number", mysql_row, backend_row)[0] == "mismatch"

    # --- release_call_number ----------------------------------------------

    def test_release_call_number_agrees_when_equal(self) -> None:
        mod = _load_module()
        mysql_row = _row(release_call_number=1)
        backend_row = _row(release_call_number=1)
        assert mod.classify_field("release_call_number", mysql_row, backend_row) == (
            "agree",
            None,
        )

    def test_release_call_number_null_coalesces_to_zero(self) -> None:
        mod = _load_module()
        mysql_row = _row(release_call_number=None)
        backend_row = _row(release_call_number=0)
        tier, cls = mod.classify_field("release_call_number", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "null_coalesced_zero"

    @pytest.mark.parametrize("raw", ["", "   ", "NULL"])
    def test_release_call_number_coalesces_normalize_null_shapes(self, raw: str) -> None:
        """Same ``toNullableNumber`` coercion as ``artist_call_number``."""
        mod = _load_module()
        mysql_row = _row(release_call_number=raw)
        backend_row = _row(release_call_number=0)
        tier, cls = mod.classify_field("release_call_number", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "null_coalesced_zero"

    def test_release_call_number_genuine_difference_is_a_mismatch(self) -> None:
        mod = _load_module()
        mysql_row = _row(release_call_number=1)
        backend_row = _row(release_call_number=2)
        assert mod.classify_field("release_call_number", mysql_row, backend_row)[0] == "mismatch"

    # --- title / alternate_artist_name / album_artist ----------------------

    @pytest.mark.parametrize("col", ["title", "alternate_artist_name", "album_artist"])
    def test_tab_newline_columns_substitute_before_byte_compare(self, col: str) -> None:
        mod = _load_module()
        mysql_row = _row(**{col: "Value\twith\ntab and newline"})
        backend_row = _row(**{col: "Value with tab and newline"})
        tier, cls = mod.classify_field(col, mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "tab_newline_substituted"

    @pytest.mark.parametrize("col", ["title", "alternate_artist_name", "album_artist"])
    def test_tab_newline_columns_require_byte_identity_beyond_that(self, col: str) -> None:
        mod = _load_module()
        mysql_row = _row(**{col: "Same Value"})
        backend_row = _row(**{col: "Different Value"})
        tier, _cls = mod.classify_field(col, mysql_row, backend_row)
        assert tier == "mismatch"

    # --- cross_reference_names -------------------------------------------

    def test_cross_reference_names_agrees_when_equal(self) -> None:
        mod = _load_module()
        mysql_row = _row(cross_reference_names="Csillagrablók | Hermanos Gutiérrez")
        backend_row = _row(cross_reference_names="Csillagrablók | Hermanos Gutiérrez")
        assert mod.classify_field("cross_reference_names", mysql_row, backend_row) == (
            "agree",
            None,
        )

    def test_cross_reference_names_reordering_is_normalized(self) -> None:
        """MySQL's GROUP_CONCAT has no ORDER BY; Backend's array is ordered
        differently -- order is never significant."""
        mod = _load_module()
        mysql_row = _row(cross_reference_names="Csillagrablók | Hermanos Gutiérrez")
        backend_row = _row(cross_reference_names="Hermanos Gutiérrez | Csillagrablók")
        tier, cls = mod.classify_field("cross_reference_names", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "fold_equal"

    def test_cross_reference_names_fold_equal_spelling_is_normalized(self) -> None:
        mod = _load_module()
        nfc = "Nilüfer Yanya"  # ü = U+00FC
        nfd = unicodedata.normalize("NFD", nfc)  # u + U+0308
        assert nfc != nfd, "the two composition forms must be byte-distinct for this test"
        mysql_row = _row(cross_reference_names=f"Csillagrablók | {nfc}")
        backend_row = _row(cross_reference_names=f"Csillagrablók | {nfd}")
        tier, cls = mod.classify_field("cross_reference_names", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "fold_equal"

    def test_cross_reference_names_cardinality_loss_is_normalized_not_mismatch(self) -> None:
        """A fold-collapse or a never-imported crossref both shrink Backend's
        set relative to MySQL's, and the row can't distinguish the two -- so
        this ships as a reported residual, not a gated defect."""
        mod = _load_module()
        mysql_row = _row(cross_reference_names="Csillagrablók | Hermanos Gutiérrez")
        backend_row = _row(cross_reference_names="Csillagrablók")
        tier, cls = mod.classify_field("cross_reference_names", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "cardinality_loss"

    def test_cross_reference_names_cardinality_gain_is_normalized_not_mismatch(self) -> None:
        """The symmetric counterpart of ``cardinality_loss`` above, and it
        exists for the same reason: the row cannot distinguish the causes.

        ``LIBRARY_CODE_CROSS_REFERENCE`` is joined per *code*, but this
        harness only ever sees rows that carry a *release*. A duplicate
        ``LIBRARY_CODE`` with zero releases is therefore invisible here while
        still being folded into Backend's single ``artists`` row by
        ``ensureArtist`` -- so Backend legitimately holds aliases that
        ``LIBRARY_SELECT_SQL``'s release-keyed subquery can never return. The
        2026-08-13 prod run measured 11 such rows and **zero** of any other
        shape (WXYC/discogs-etl#346 step 8).

        Treating a superset as a defect would therefore gate the release on a
        divergence that is an artifact of what the query can see, not of the
        data. Note this is deliberately *not* blanket acceptance -- a set that
        is neither subset nor superset is still a mismatch, which the next
        test pins.
        """
        mod = _load_module()
        mysql_row = _row(cross_reference_names="Csillagrablók")
        backend_row = _row(cross_reference_names="Csillagrablók | Hermanos Gutiérrez")
        tier, cls = mod.classify_field("cross_reference_names", mysql_row, backend_row)
        assert tier == "normalized"
        assert cls == "cardinality_gain"

    def test_cross_reference_names_crossing_sets_are_still_a_mismatch(self) -> None:
        """Backend dropping one alias *and* gaining another is neither a
        subset nor a superset, and no fold-collapse or invisible-duplicate
        story produces it. That stays gated -- it is what keeps this column
        able to catch genuine crossref corruption at all."""
        mod = _load_module()
        mysql_row = _row(cross_reference_names="Csillagrablók | Jessica Pratt")
        backend_row = _row(cross_reference_names="Csillagrablók | Hermanos Gutiérrez")
        assert mod.classify_field("cross_reference_names", mysql_row, backend_row)[0] == "mismatch"

    def test_cross_reference_names_disjoint_sets_are_still_a_mismatch(self) -> None:
        mod = _load_module()
        mysql_row = _row(cross_reference_names="Jessica Pratt")
        backend_row = _row(cross_reference_names="Hermanos Gutiérrez")
        assert mod.classify_field("cross_reference_names", mysql_row, backend_row)[0] == "mismatch"


class TestColumnModelsDriftGuard:
    """Mirrors ``test_select_statements_match_sync_library_sh``'s style: set
    equality, not containment, so an added OR removed diffed column fails."""

    def test_column_models_keys_match_diff_columns_exactly(self) -> None:
        mod = _load_module()
        assert set(mod.COLUMN_MODELS) == set(mod.DIFF_COLUMNS)


class TestClassifyMatchedRows:
    """``_classify_matched_rows`` -- the row-level engine behind
    ``diff_library_dbs``'s ``field_mismatches`` -- also computes the
    ``normalizations`` (column -> class -> count) shape a later step wires
    into ``ParityDiff``.
    """

    def test_normalized_tier_is_excluded_from_field_mismatches(self) -> None:
        mod = _load_module()
        mysql_rows = {1: _row(genre="rock")}
        backend_rows = {1: _row(genre="Rock")}
        field_mismatches, normalizations = mod._classify_matched_rows(mysql_rows, backend_rows, [1])
        assert field_mismatches["genre"] == 0
        assert normalizations["genre"]["case_folded"] == 1

    def test_field_mismatches_still_counts_genuine_mismatches(self) -> None:
        mod = _load_module()
        mysql_rows = {1: _row(genre="Rock")}
        backend_rows = {1: _row(genre="Electronic")}
        field_mismatches, normalizations = mod._classify_matched_rows(mysql_rows, backend_rows, [1])
        assert field_mismatches["genre"] == 1
        assert "genre" not in normalizations

    def test_normalizations_is_keyed_column_then_class(self) -> None:
        mod = _load_module()
        mysql_rows = {1: _row(call_letters="st"), 2: _row(id=2, call_letters="xy")}
        backend_rows = {1: _row(call_letters="ST"), 2: _row(id=2, call_letters="XY")}
        _, normalizations = mod._classify_matched_rows(mysql_rows, backend_rows, [1, 2])
        assert normalizations["call_letters"] == {"uppercased": 2}

    def test_field_mismatches_stays_fully_keyed_over_diff_columns(self) -> None:
        """Zeros included, even when nothing at all is a mismatch --
        ``_print_human`` subscripts every ``DIFF_COLUMNS`` entry unguarded."""
        mod = _load_module()
        mysql_rows = {1: _row()}
        backend_rows = {1: _row()}
        field_mismatches, _ = mod._classify_matched_rows(mysql_rows, backend_rows, [1])
        assert set(field_mismatches) == set(mod.DIFF_COLUMNS)
        assert all(v == 0 for v in field_mismatches.values())


class TestFoldCollapseResolution:
    """Fold-collapse: the cross-row widening measured on prod 2026-08-13
    (WXYC/discogs-etl#346, plan step 9).

    tubafrenzy's unit of artist identity is the ``LIBRARY_CODE`` row and
    nothing stops two of them sharing a ``PRESENTATION_NAME`` (295 names do,
    across 596 code rows). Backend's unit is the ``artists`` row, matched by
    ``fold_artist_name``, so those collapse into one row that can hold only
    ONE ``code_letters`` and one ``artist_genre_code`` per genre. Every
    release under the *other* code then reads as a field mismatch against a
    value that is perfectly legitimate -- just not that release's.

    Unlike every other tier this is not a property of a row *pair*: deciding
    it needs the whole mysql side, because the explanation lives in a
    sibling row. So it resolves in ``_classify_matched_rows`` (which already
    holds both full maps) rather than in ``classify_field``, and
    ``COLUMN_MODELS`` stays per-pair and pure.

    Deliberately NOT blanket acceptance -- the value Backend holds must be
    one the mysql side itself supplies for that folded artist. A value from
    nowhere is still a mismatch, and the negative tests below are the point
    of the whole design.
    """

    def test_call_letters_from_a_fold_sibling_is_normalized(self) -> None:
        mod = _load_module()
        mysql_rows = {
            1: _row(artist="Dosh", call_letters="DO"),
            2: _row(id=2, artist="dosh", call_letters="DS"),
        }
        backend_rows = {
            1: _row(artist="Dosh", call_letters="DO"),
            2: _row(id=2, artist="dosh", call_letters="DO"),
        }
        field_mismatches, normalizations = mod._classify_matched_rows(
            mysql_rows, backend_rows, [1, 2]
        )
        assert field_mismatches["call_letters"] == 0
        assert normalizations["call_letters"]["fold_collapsed"] == 1

    def test_call_letters_unknown_fallback_from_a_blank_sibling_is_normalized(self) -> None:
        """``ensureArtist`` derives ``'??'`` from a duplicate whose
        ``CALL_LETTERS`` is blank; the harness compares against the release's
        own (populated) code, so the base classifier cannot absorb it. Two
        such rows in the prod run -- Mudboy and Uniform."""
        mod = _load_module()
        mysql_rows = {
            1: _row(artist="Mudboy", call_letters=""),
            2: _row(id=2, artist="Mudboy", call_letters="MU"),
        }
        backend_rows = {
            1: _row(artist="Mudboy", call_letters="??"),
            2: _row(id=2, artist="Mudboy", call_letters="??"),
        }
        field_mismatches, normalizations = mod._classify_matched_rows(
            mysql_rows, backend_rows, [1, 2]
        )
        assert field_mismatches["call_letters"] == 0
        assert normalizations["call_letters"]["fold_collapsed"] == 1

    def test_call_letters_with_no_fold_sibling_is_still_a_mismatch(self) -> None:
        mod = _load_module()
        mysql_rows = {1: _row(artist="Juana Molina", call_letters="JU")}
        backend_rows = {1: _row(artist="Juana Molina", call_letters="ZZ")}
        field_mismatches, normalizations = mod._classify_matched_rows(mysql_rows, backend_rows, [1])
        assert field_mismatches["call_letters"] == 1
        assert "call_letters" not in normalizations

    def test_artist_call_number_from_a_same_genre_fold_sibling_is_normalized(self) -> None:
        mod = _load_module()
        mysql_rows = {
            1: _row(artist="Bluegrass Album Band", genre="OCS", artist_call_number=8),
            2: _row(id=2, artist="Bluegrass Album Band", genre="OCS", artist_call_number=18),
        }
        backend_rows = {
            1: _row(artist="Bluegrass Album Band", genre="OCS", artist_call_number=8),
            2: _row(id=2, artist="Bluegrass Album Band", genre="OCS", artist_call_number=8),
        }
        field_mismatches, normalizations = mod._classify_matched_rows(
            mysql_rows, backend_rows, [1, 2]
        )
        assert field_mismatches["artist_call_number"] == 0
        assert normalizations["artist_call_number"]["fold_collapsed"] == 1

    def test_artist_call_number_sibling_in_another_genre_does_not_explain_it(self) -> None:
        """``ensureGenreArtistCrossref`` is keyed ``(artist_id, genre_id)``,
        so the collapse only ever happens WITHIN a genre. A same-artist row
        filed under a different genre has its own crossref row and cannot
        supply the value -- widening past the genre would silently accept a
        real defect."""
        mod = _load_module()
        mysql_rows = {
            1: _row(artist="Yellow Swans", genre="Rock", artist_call_number=40),
            2: _row(id=2, artist="Yellow Swans", genre="Electronic", artist_call_number=45),
        }
        backend_rows = {
            1: _row(artist="Yellow Swans", genre="Rock", artist_call_number=45),
            2: _row(id=2, artist="Yellow Swans", genre="Electronic", artist_call_number=45),
        }
        field_mismatches, normalizations = mod._classify_matched_rows(
            mysql_rows, backend_rows, [1, 2]
        )
        assert field_mismatches["artist_call_number"] == 1
        assert "artist_call_number" not in normalizations

    def test_artist_call_number_null_sibling_coalesces_to_zero(self) -> None:
        """``job.ts:996``'s ``?? 0`` runs per row, so a sibling holding NULL
        supplies ``0`` to the group, not NULL."""
        mod = _load_module()
        mysql_rows = {
            1: _row(artist="Boys Life", genre="Rock", artist_call_number=12),
            2: _row(id=2, artist="Boys Life", genre="Rock", artist_call_number=None),
        }
        backend_rows = {
            1: _row(artist="Boys Life", genre="Rock", artist_call_number=0),
            2: _row(id=2, artist="Boys Life", genre="Rock", artist_call_number=0),
        }
        field_mismatches, normalizations = mod._classify_matched_rows(
            mysql_rows, backend_rows, [1, 2]
        )
        assert field_mismatches["artist_call_number"] == 0
        assert normalizations["artist_call_number"]["fold_collapsed"] == 1

    def test_a_sibling_backend_never_imported_cannot_supply_the_value(self) -> None:
        """``job.ts:959-990`` skips a ``db_only`` genre (and an unparseable
        format, and an empty artist/title) *before* reaching ``ensureArtist``
        / ``ensureGenreArtistCrossref``, so such a row wrote no
        ``code_letters`` and no ``artist_genre_code``. Letting it into the
        group would soften the gate from "a value the mysql side supplies" to
        "a value some never-imported row happens to carry" -- and the row
        whose value it laundered would be a genuine defect."""
        mod = _load_module()
        mysql_rows = {
            1: _row(artist="Cat Power", call_letters="CA"),
            2: _row(id=2, artist="Cat Power", call_letters="ZZ", genre="db_only"),
        }
        backend_rows = {1: _row(artist="Cat Power", call_letters="ZZ")}
        field_mismatches, normalizations = mod._classify_matched_rows(mysql_rows, backend_rows, [1])
        assert field_mismatches["call_letters"] == 1
        assert "call_letters" not in normalizations

    def test_artist_call_number_sibling_genre_match_is_case_insensitive(self) -> None:
        """Backend resolves a genre through ``genreMap.get(name.toLowerCase())``
        (``job.ts:951,965``), so two ``GENRE.REFERENCE_NAME``s differing only
        in case share one ``genre_id`` and therefore one crossref row --
        matching ``_classify_genre``'s own case-insensitive comparison."""
        mod = _load_module()
        mysql_rows = {
            1: _row(artist="Cuong Vu", genre="Jazz", artist_call_number=7),
            2: _row(id=2, artist="Cuong Vu", genre="jazz", artist_call_number=19),
        }
        backend_rows = {
            1: _row(artist="Cuong Vu", genre="Jazz", artist_call_number=19),
            2: _row(id=2, artist="Cuong Vu", genre="jazz", artist_call_number=19),
        }
        field_mismatches, normalizations = mod._classify_matched_rows(
            mysql_rows, backend_rows, [1, 2]
        )
        assert field_mismatches["artist_call_number"] == 0
        assert normalizations["artist_call_number"]["fold_collapsed"] == 1

    def test_fold_collapse_does_not_apply_to_unmodelled_columns(self) -> None:
        """``title`` has no fold-identity story: a sibling holding the value
        must not launder a genuine content divergence."""
        mod = _load_module()
        mysql_rows = {
            1: _row(artist="Dosh", title="Tommy"),
            2: _row(id=2, artist="Dosh", title="Wolves and Wishes"),
        }
        backend_rows = {
            1: _row(artist="Dosh", title="Wolves and Wishes"),
            2: _row(id=2, artist="Dosh", title="Wolves and Wishes"),
        }
        field_mismatches, _ = mod._classify_matched_rows(mysql_rows, backend_rows, [1, 2])
        assert field_mismatches["title"] == 1

    def test_fold_collapse_columns_are_a_subset_of_diff_columns(self) -> None:
        mod = _load_module()
        assert set(mod.FOLD_COLLAPSE_COLUMNS) <= set(mod.DIFF_COLUMNS)


class TestFieldMismatchesTieringEndToEnd:
    """The headline change: ``field_mismatches`` stops counting deliberate
    Backend normalizations (Part 1's baseline classes) and only counts
    genuine defects."""

    def test_a_row_with_only_deliberate_normalizations_has_zero_mismatches(
        self, tmp_path: Path
    ) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(
            mysql_db,
            [
                {
                    "id": 1,
                    "artist": "Various Artists - latin",
                    "call_letters": "xy",
                    "artist_call_number": None,
                    "release_call_number": None,
                    "genre": "rock",
                    "format": "vinyl - lp",
                }
            ],
        )
        _make_library_db(
            backend_db,
            [
                {
                    "id": 1,
                    "artist": "Various Artists",
                    "call_letters": "V/A",
                    "artist_call_number": 0,
                    "release_call_number": 0,
                    "genre": "Rock",
                    "format": 'vinyl 12"',
                }
            ],
        )

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.matched == 1
        assert set(result.field_mismatches) == set(mod.DIFF_COLUMNS)
        assert all(v == 0 for v in result.field_mismatches.values())

    def test_field_mismatches_still_flags_a_genuine_defect_among_normalizations(
        self, tmp_path: Path
    ) -> None:
        """Deliberate normalizations elsewhere in the row don't mask a real
        defect in one column."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(
            mysql_db,
            [{"id": 1, "call_letters": "xy", "genre": "rock"}],
        )
        _make_library_db(
            backend_db,
            # call_letters/genre are deliberate normalizations; artist is a
            # genuine, unrelated defect.
            [{"id": 1, "call_letters": "XY", "genre": "Rock", "artist": "Cat Power"}],
        )

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.field_mismatches["call_letters"] == 0
        assert result.field_mismatches["genre"] == 0
        assert result.field_mismatches["artist"] == 1

    def test_normalization_counts_reach_a_real_invocation(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """``normalizations`` is not in ``ParityDiff`` yet, so the log line is
        its ONLY surface until the ledger PR wires it in.

        ``init_logger`` pins the root level at INFO and this CLI has no
        verbosity flag, so a ``logger.debug`` here is unreachable in every
        invocation the harness actually has -- "logged rather than discarded"
        would be discarded. Pinned at INFO, one line per run, on stderr so the
        ``--json`` stdout contract is untouched.
        """
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "rock"}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Rock"}])

        with caplog.at_level(logging.INFO, logger="catalog_parity_diff"):
            mod.run_diff(str(mysql_db), str(backend_db))

        records = [r for r in caplog.records if "normalizations" in r.__dict__]
        assert records, "the normalization counts never reached the log"
        assert records[0].normalizations == {"genre": {"case_folded": 1}}


class TestCtaModelling:
    """The CTA multiset gets the same TAB/NL substitution as the main library
    columns, plus the empty-artist row-drop rule Backend's importer applies
    (``parseLegacyCompilationTrackRows``, ``job.ts:710-711``)."""

    def test_cta_tab_newline_substituted_before_comparing(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(
            mysql_db,
            [{"id": 1}],
            cta_rows=[(1, "Duke Ellington\tJohn Coltrane", "In a Sentimental\nMood")],
        )
        _make_library_db(
            backend_db,
            [{"id": 1}],
            cta_rows=[(1, "Duke Ellington John Coltrane", "In a Sentimental Mood")],
        )

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.cta_missing == 0
        assert result.cta_extra == 0

    @pytest.mark.parametrize("artist_name", ["  ", "\t", "NULL"])
    def test_cta_identical_row_never_manufactures_drift(
        self, tmp_path: Path, artist_name: str
    ) -> None:
        """Agreement must never come out of the harness as drift.

        The legacy model has to be applied to BOTH multisets, not just the
        mysql one: a one-sided drop deletes a row from one counter while the
        other keeps it, and the difference reports as ``cta_extra`` on data
        that is byte-identical on both sides. In a tool whose job is to certify
        seven consecutive clean parity days, that is the worst failure
        direction there is.
        """
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        for path in (mysql_db, backend_db):
            _make_library_db(path, [{"id": 1}], cta_rows=[(1, artist_name, "la paradoja")])

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.cta_missing == 0
        assert result.cta_extra == 0

    def test_cta_literal_null_artist_is_a_name_not_an_empty_row(self, tmp_path: Path) -> None:
        """``parseLegacyCompilationTrackRows`` drops on ``trim().length === 0``
        (``job.ts:710-711``) -- so the 4-character name "NULL" is kept, and
        Backend not holding it is genuine drift. Screening the drop through
        ``_normalize`` (which also swallows the literal ``"NULL"``) would file
        that missing row as expected instead."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}], cta_rows=[(1, "NULL", "la paradoja")])
        _make_library_db(backend_db, [{"id": 1}], cta_rows=[])

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.cta_missing == 1
        assert result.cta_extra == 0

    def test_cta_empty_artist_row_is_expected_missing_not_drift(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(
            mysql_db,
            [{"id": 1}],
            cta_rows=[
                (1, "  ", "Untitled Track"),
                (1, "Juana Molina", "la paradoja"),
            ],
        )
        _make_library_db(
            backend_db,
            [{"id": 1}],
            cta_rows=[(1, "Juana Molina", "la paradoja")],
        )

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.cta_missing == 0
        assert result.cta_extra == 0

    def test_cta_genuinely_missing_row_still_counts(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(
            mysql_db,
            [{"id": 1}],
            cta_rows=[(1, "Juana Molina", "la paradoja")],
        )
        _make_library_db(backend_db, [{"id": 1}], cta_rows=[])

        result = mod.run_diff(str(mysql_db), str(backend_db))
        assert result.cta_missing == 1
        assert result.cta_extra == 0


def _ledger(**overrides: Any) -> Any:
    """A ResidueLedger with no enumerated ids and no baseline -- the "rules
    only" starting point for tests that must prove a row is expected WITHOUT
    an enumerated ledger entry."""
    mod = _load_module()
    defaults: dict[str, Any] = {
        "collapsed_ids": frozenset(),
        "normalizations_baseline": {},
        "cta_missing_baseline": None,
        "cta_extra_baseline": None,
        "measured_date": None,
    }
    defaults.update(overrides)
    return mod.ResidueLedger(**defaults)


class TestResidueLedgerLoader:
    """``load_residue_ledger`` reads the vendored ``ledger.json`` shape."""

    def _write_ledger_json(self, path: Path, **overrides: Any) -> None:
        payload = {
            "measured_date": "2026-08-11",
            "collapsed_mysql_ids": {"8": 7, "151": 150},
            "baselines": {
                "measured_date": None,
                "normalizations": {},
                "cta_missing": None,
                "cta_extra": None,
            },
        }
        payload.update(overrides)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_loads_collapsed_ids_and_baselines(self, tmp_path: Path) -> None:
        mod = _load_module()
        path = tmp_path / "ledger.json"
        self._write_ledger_json(
            path,
            baselines={
                "measured_date": "2026-08-13",
                "normalizations": {"genre": {"case_folded": 42}},
                "cta_missing": 2771,
                "cta_extra": 6932,
            },
        )

        ledger = mod.load_residue_ledger(path)

        assert ledger.collapsed_ids == frozenset({8, 151})
        assert ledger.normalizations_baseline == {"genre": {"case_folded": 42}}
        assert ledger.cta_missing_baseline == 2771
        assert ledger.cta_extra_baseline == 6932
        assert ledger.measured_date == "2026-08-11"

    def test_missing_file_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        with pytest.raises(mod.SourceError):
            mod.load_residue_ledger(tmp_path / "does-not-exist.json")

    def test_malformed_json_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        path = tmp_path / "ledger.json"
        path.write_text("not json{{{", encoding="utf-8")
        with pytest.raises(mod.SourceError):
            mod.load_residue_ledger(path)

    def test_missing_collapsed_key_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        path = tmp_path / "ledger.json"
        path.write_text(
            json.dumps({"measured_date": "2026-08-11", "baselines": {}}), encoding="utf-8"
        )
        with pytest.raises(mod.SourceError):
            mod.load_residue_ledger(path)

    def test_real_vendored_ledger_loads(self) -> None:
        """Sanity check against the actual vendored file this repo ships."""
        mod = _load_module()
        ledger = mod.load_residue_ledger(REPO_ROOT / "vendor" / "parity-residue" / "ledger.json")
        assert len(ledger.collapsed_ids) == 599

    @pytest.mark.parametrize(
        "baselines",
        [
            pytest.param("oops", id="string"),
            pytest.param(["oops"], id="list"),
            pytest.param(3, id="int"),
        ],
    )
    def test_truthy_non_dict_baselines_raises_source_error(
        self, tmp_path: Path, baselines: object
    ) -> None:
        """A malformed ``baselines`` must exit 3 like any other bad source.

        ``data.get("baselines") or {}`` only substitutes for a FALSY value, so
        a truthy non-dict reached ``.get`` and raised ``AttributeError`` --
        which is not in the caught tuple, so it escaped ``main``'s
        ``except SourceError`` and surfaced as exit 1 with a traceback. Exit 1
        is deliberately reserved for an uncaught crash so a CI runner can tell
        it apart from a drift verdict (exit 4), which made this a contract
        violation, not just an ugly error.
        """
        mod = _load_module()
        path = tmp_path / "ledger.json"
        path.write_text(
            json.dumps({"collapsed_mysql_ids": {"8": 7}, "baselines": baselines}),
            encoding="utf-8",
        )
        with pytest.raises(mod.SourceError):
            mod.load_residue_ledger(path)

    @pytest.mark.parametrize("key", ["cta_missing", "cta_extra"])
    def test_non_integer_cta_baseline_raises_source_error(self, tmp_path: Path, key: str) -> None:
        """A non-integer CTA baseline must fail at LOAD time, not at compare time.

        Left unvalidated it survives the loader and defers the failure to
        ``cta_missing <= "0"`` inside ``diff_library_dbs``, which raises
        ``TypeError`` from the blanket handler as exit 3 -- right code, but
        from the wrong place and with a message that points at the diff
        rather than at the ledger the operator has to fix.
        """
        mod = _load_module()
        path = tmp_path / "ledger.json"
        baselines = {"normalizations": {}, "cta_missing": 0, "cta_extra": 0}
        baselines[key] = "0"
        path.write_text(
            json.dumps({"collapsed_mysql_ids": {"8": 7}, "baselines": baselines}),
            encoding="utf-8",
        )
        with pytest.raises(mod.SourceError):
            mod.load_residue_ledger(path)

    def test_non_dict_normalizations_baseline_raises_source_error(self, tmp_path: Path) -> None:
        mod = _load_module()
        path = tmp_path / "ledger.json"
        path.write_text(
            json.dumps(
                {
                    "collapsed_mysql_ids": {"8": 7},
                    "baselines": {"normalizations": "oops"},
                }
            ),
            encoding="utf-8",
        )
        with pytest.raises(mod.SourceError):
            mod.load_residue_ledger(path)


class TestRuleBMissingReason:
    """``_rule_b_missing_reason`` -- skip paths 1/3/5/6, purely from a row's
    own data. Unit-level (no sqlite, no ledger, no diff_library_dbs): that
    wiring is discogs-etl#370 step 5's concern, not step 4's."""

    def test_db_only_genre_is_a_reason(self) -> None:
        mod = _load_module()
        assert mod._rule_b_missing_reason(_row(genre="  Db_Only  ")) == "db_only_genre"

    def test_unparseable_format_is_a_reason(self) -> None:
        mod = _load_module()
        assert mod._rule_b_missing_reason(_row(format="cassette")) == "unparseable_format"

    def test_null_artist_name_is_a_reason(self) -> None:
        """A Python None must classify the same as '', so the predicate is
        spelled `value is None or value.strip() == ""` rather than
        `value.strip() == ""` alone. `parse_library_tsv` maps the `\\N`
        sentinel to None (lib/library_db.py::_parse_nullable_field), and a
        row mapping reaching this predicate can carry None from any source.

        This says nothing about how a SQL NULL in *these two* columns would
        arrive -- `mysql -B -N` on this server prints one as the literal text
        "NULL" instead, which after #375 is reported as unexplained drift
        rather than forgiven; see the sibling test below and
        `_rule_b_missing_reason`'s own docstring."""
        mod = _load_module()
        assert mod._rule_b_missing_reason(_row(artist=None)) == "empty_artist_name"

    def test_whitespace_only_artist_name_is_the_same_reason_as_null(self) -> None:
        mod = _load_module()
        assert mod._rule_b_missing_reason(_row(artist="   ")) == "empty_artist_name"

    @pytest.mark.parametrize("column", ["artist", "title"])
    def test_literal_null_string_is_reported_as_unexplained_drift(self, column: str) -> None:
        """Measured 2026-08-14 against prod tubafrenzy MySQL (#375): TITLE IS
        NULL and PRESENTATION_NAME IS NULL both count 0, and neither column
        holds the uppercase string 'NULL' either (BINARY TITLE = 'NULL' and
        BINARY PRESENTATION_NAME = 'NULL' both count 0). There is no SQL NULL
        arriving as the string "NULL" for these two columns to catch, so the
        broad `_normalize(value) is None` predicate was over-matching: an
        album or artist genuinely titled "NULL" is a legal row Backend
        imports rather than skips, and folding it into empty_artist_name /
        empty_album_title would silently forgive real drift instead of
        reporting it.

        This is the behaviour change from #375: before the narrowing this
        asserted "empty_artist_name" / "empty_album_title"; now it asserts
        None (unexplained -- no row-derivable reason), because the predicate
        is spelled as an explicit empty check instead of reusing
        `_normalize`'s NULL-string collapse. It is inert on today's data --
        no row is titled uppercase "NULL" -- and only starts mattering if one
        ever appears, at which point it is correctly reported as drift.
        """
        mod = _load_module()
        assert mod._rule_b_missing_reason(_row(**{column: "NULL"})) is None

    def test_mixed_case_null_string_title_is_also_unexplained_drift(self) -> None:
        """Guards the case-sensitivity boundary against the two rows that
        actually exist: LIBRARY_RELEASE ids 18930 and 55924 hold the title
        "Null" (mixed case, not "NULL"). `_normalize` compares
        case-sensitively (see TestNormalize.test_case_is_not_folded), so
        neither the pre-#375 predicate nor the narrowed one ever matched
        these two ids -- this pins that "Null" reads as unexplained drift
        both before and after the narrowing, i.e. #375 does not change their
        classification.
        """
        mod = _load_module()
        assert mod._rule_b_missing_reason(_row(title="Null")) is None

    def test_ledger_empty_title_ids_still_resolve_to_empty_album_title(
        self, tmp_path: Path
    ) -> None:
        """The six ledger ids (`residue-ledger.md` Set 2) are byte-exact empty
        strings in prod, not SQL NULLs and not whitespace-only (measured
        2026-08-14: `LENGTH(TITLE) = 0` for all six) -- so the narrowed
        `value is None or value.strip() == ""` check still catches every one
        of them, same as the broad `_normalize` check did.

        Built as real library.db rows and read back through
        `_load_library_rows` rather than asserted against six hand-made dicts:
        `_rule_b_missing_reason` never reads `id`, so a per-id parametrize
        would be the same assertion six times over and would pin nothing about
        the ids themselves. Going through sqlite makes the id load-bearing (it
        keys the loaded mapping, and the row set is asserted whole) and pins
        that a stored '' survives the round-trip as '' rather than arriving as
        None -- the two representations this ticket had to tell apart.
        """
        mod = _load_module()
        ledger_ids = [21107, 39290, 51871, 52374, 65301, 66329]
        db = tmp_path / "ledger-empty-titles.db"
        _make_library_db(db, [{"id": ledger_id, "title": ""} for ledger_id in ledger_ids])
        conn = sqlite3.connect(db)
        rows = mod._load_library_rows(conn, "mysql")
        conn.close()

        assert sorted(rows) == sorted(ledger_ids)
        assert all(row["title"] == "" for row in rows.values())
        assert {ledger_id: mod._rule_b_missing_reason(rows[ledger_id]) for ledger_id in rows} == {
            ledger_id: "empty_album_title" for ledger_id in ledger_ids
        }

    def test_whitespace_only_title_is_the_same_reason_as_empty(self) -> None:
        mod = _load_module()
        assert mod._rule_b_missing_reason(_row(title="   ")) == "empty_album_title"

    def test_cta_drop_rule_keeps_a_literal_null_named_artist(self, tmp_path: Path) -> None:
        """The CTA drop rule and `_rule_b_missing_reason` now apply the same
        rule, for the same reason on both sides.

        COMPILATION_TRACK_ARTIST.ARTIST_NAME is documented NOT NULL, and
        TITLE / PRESENTATION_NAME are measured to hold zero SQL NULLs (#375),
        so in every one of these columns a "NULL" can only be a genuine
        name -- never a SQL NULL in disguise. Both predicates are therefore
        `str.strip() == ""`, and neither must drift toward `_normalize`,
        which would file a genuinely-missing "NULL"-named row as expected
        residue. (Before #375 this asymmetry ran the other way and the
        docstring here recorded it as deliberate; the measurement removed it.)
        """
        mod = _load_module()
        db = tmp_path / "cta.db"
        _make_library_db(db, [{"id": 1}], cta_rows=[(1, "NULL", "la paradoja")])
        conn = sqlite3.connect(db)
        counts = mod._load_cta_counts(conn)
        conn.close()

        # Kept, not dropped: sum of 1 means the "NULL"-named artist survived.
        assert sum(counts.values()) == 1

    def test_empty_album_title_is_a_reason(self) -> None:
        mod = _load_module()
        assert mod._rule_b_missing_reason(_row(title="")) == "empty_album_title"

    def test_ordinary_row_has_no_reason(self) -> None:
        """Stand-in for skip paths 2/4/7/8 (not row-derivable): a row with no
        db_only genre, a parseable format, and non-empty artist/title has no
        rule to explain it at all."""
        mod = _load_module()
        assert mod._rule_b_missing_reason(_row(genre="Rock", format="CD")) is None


class TestIsMintedId:
    """Rule A: ``id >= 1_000_000`` is expected residue by construction
    (BS#1963 mints there) -- no ledger entry required."""

    def test_minted_id_is_true(self) -> None:
        mod = _load_module()
        assert mod._is_minted_id(1_000_042) is True

    def test_floor_itself_is_minted(self) -> None:
        mod = _load_module()
        assert mod._is_minted_id(1_000_000) is True

    def test_below_floor_is_not_minted(self) -> None:
        mod = _load_module()
        assert mod._is_minted_id(999_999) is False


class TestClassifyRowExpectations:
    """``_classify_row_expectations`` -- the pure function combining Rule
    A, Rule B, and the frozen 599-id enumeration. No sqlite, no ParityDiff:
    just the (missing_ids, extra_ids) -> counts arithmetic."""

    def test_rule_b_match_is_expected_without_a_ledger_entry(self) -> None:
        # Return shape: (missing_expected, missing_unexplained, extra_expected, extra_unexplained)
        mod = _load_module()
        mysql_rows = {1: _row(id=1, genre="db_only")}
        result = mod._classify_row_expectations(mysql_rows, [1], [], _ledger())
        assert result == (1, 0, 0, 0)

    def test_no_rule_match_and_no_ledger_entry_is_unexplained(self) -> None:
        mod = _load_module()
        mysql_rows = {1: _row(id=1, genre="Rock", format="CD")}
        result = mod._classify_row_expectations(mysql_rows, [1], [], _ledger())
        assert result == (0, 1, 0, 0)

    def test_minted_extra_id_is_expected_without_a_ledger_entry(self) -> None:
        mod = _load_module()
        result = mod._classify_row_expectations({}, [], [1_000_042], _ledger())
        assert result == (0, 0, 1, 0)

    def test_non_minted_extra_id_is_unexplained(self) -> None:
        mod = _load_module()
        result = mod._classify_row_expectations({}, [], [42], _ledger())
        assert result == (0, 0, 0, 1)

    def test_enumerated_id_is_expected_missing_even_with_ordinary_row_data(self) -> None:
        """The 599 collapse ids are the one thing that genuinely needs a
        ledger ENTRY (not just a rule): an ordinary row with no rule-matching
        field is only expected-missing because it is enumerated."""
        mod = _load_module()
        mysql_rows = {11406: _row(id=11406, genre="Rock", format="CD")}
        ledger = _ledger(collapsed_ids=frozenset({11406}))
        result = mod._classify_row_expectations(mysql_rows, [11406], [], ledger)
        assert result == (1, 0, 0, 0)

    def test_ledger_id_absent_from_missing_ids_does_not_raise(self) -> None:
        """A stale ledger entry (an id the ledger names but today's
        missing_ids does not contain at all) must not crash -- it simply
        never gets looked up, since this function only ever looks UP an
        actual missing id in the ledger, never the other way around."""
        mod = _load_module()
        mysql_rows = {1: _row(id=1)}
        ledger = _ledger(collapsed_ids=frozenset({999_999}))
        result = mod._classify_row_expectations(mysql_rows, [], [], ledger)
        assert result == (0, 0, 0, 0)


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


class TestClassificationRequiresLedger:
    """Field tiering is unconditional; row-level expected/unexplained
    classification and ``clean`` are not computed at all without a ledger."""

    def test_without_ledger_everything_is_unexplained_and_clean_is_none(
        self, tmp_path: Path
    ) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "db_only"}, {"id": 2}])
        _make_library_db(backend_db, [{"id": 2}, {"id": 1_000_042}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn, ledger=None)
        mysql_conn.close()
        backend_conn.close()

        assert result.clean is None
        assert result.missing_expected == 0
        assert result.extra_expected == 0
        assert result.missing_unexplained == result.missing_in_backend
        assert result.extra_unexplained == result.extra_in_backend

    def test_ledger_defaults_to_none_for_existing_callers(self, tmp_path: Path) -> None:
        """The pre-existing diff_library_dbs/run_diff call sites pass no
        ledger at all -- confirm that call shape still works and yields the
        no-ledger (clean=None) behaviour rather than erroring."""
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

        assert result.clean is None


class TestParityDiffFieldOrder:
    """Field order is constrained by the dataclass (Part 3): the new fields
    append after ``cta_extra`` in declaration order, with defaulted fields
    (``normalizations`` and the two id lists) last."""

    def test_declaration_order_matches_the_json_contract(self) -> None:
        mod = _load_module()
        names = [f.name for f in fields(mod.ParityDiff)]
        assert names == [
            "matched",
            "missing_in_backend",
            "extra_in_backend",
            "field_mismatches",
            "cta_missing",
            "cta_extra",
            "clean",
            "missing_unexplained",
            "missing_expected",
            "extra_unexplained",
            "extra_expected",
            "normalizations",
            "missing_in_backend_ids",
            "extra_in_backend_ids",
        ]

    def test_module_imports_without_typeerror(self) -> None:
        """A non-default field trailing a defaulted one makes @dataclass raise
        TypeError at class-definition time -- i.e. at import. Re-importing
        under a fresh module name (rather than relying on _load_module()'s
        cache) proves THIS load succeeds, not a memoized earlier one."""
        spec = importlib.util.spec_from_file_location(
            "catalog_parity_diff_reimport_check", SCRIPT_PATH
        )
        assert spec is not None and spec.loader is not None
        fresh = importlib.util.module_from_spec(spec)
        # Needed for `from __future__ import annotations` dataclasses to
        # resolve their string-annotated field types at decoration time --
        # mirrors _load_module()'s own sys.modules registration above.
        sys.modules["catalog_parity_diff_reimport_check"] = fresh
        spec.loader.exec_module(fresh)  # raises TypeError if field order is wrong
        assert fresh.ParityDiff is not None


class TestCleanVerdictEndToEnd:
    """``clean`` is true only on zero unexplained missing/extra, zero field
    mismatches, and CTA within its documented baseline."""

    def test_clean_true_when_everything_matches_and_cta_within_baseline(
        self, tmp_path: Path
    ) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        rows = [{"id": 1, "title": "DOGA", "artist": "Juana Molina"}]
        _make_library_db(mysql_db, rows)
        _make_library_db(backend_db, rows)

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        ledger = _ledger(cta_missing_baseline=0, cta_extra_baseline=0)
        result = mod.diff_library_dbs(mysql_conn, backend_conn, ledger=ledger)
        mysql_conn.close()
        backend_conn.close()

        assert result.clean is True

    def test_clean_false_on_an_unexplained_missing_row(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}, {"id": 2, "genre": "Rock", "format": "CD"}])
        _make_library_db(backend_db, [{"id": 1}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        ledger = _ledger(cta_missing_baseline=0, cta_extra_baseline=0)
        result = mod.diff_library_dbs(mysql_conn, backend_conn, ledger=ledger)
        mysql_conn.close()
        backend_conn.close()

        assert result.clean is False
        assert result.missing_unexplained == 1

    def test_clean_false_on_a_genuine_field_mismatch(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "Rock"}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Electronic"}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        ledger = _ledger(cta_missing_baseline=0, cta_extra_baseline=0)
        result = mod.diff_library_dbs(mysql_conn, backend_conn, ledger=ledger)
        mysql_conn.close()
        backend_conn.close()

        assert result.clean is False

    def test_normalization_alone_does_not_break_clean(self, tmp_path: Path) -> None:
        """A deliberate normalization (case-different genre) is reported,
        never gating."""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1, "genre": "rock"}])
        _make_library_db(backend_db, [{"id": 1, "genre": "Rock"}])

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        ledger = _ledger(cta_missing_baseline=0, cta_extra_baseline=0)
        result = mod.diff_library_dbs(mysql_conn, backend_conn, ledger=ledger)
        mysql_conn.close()
        backend_conn.close()

        assert result.clean is True
        assert result.normalizations["genre"]["case_folded"] == 1

    def test_cta_above_baseline_is_not_clean(self, tmp_path: Path) -> None:
        """CTA drift beyond the recorded baseline fails the verdict."""
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
        ledger = _ledger(cta_missing_baseline=0, cta_extra_baseline=0)
        result = mod.diff_library_dbs(mysql_conn, backend_conn, ledger=ledger)
        mysql_conn.close()
        backend_conn.close()

        assert result.cta_missing == 1
        assert result.clean is False

    def test_cta_below_baseline_is_still_clean(self, tmp_path: Path) -> None:
        """The baseline is a CEILING, not an equality target.

        The plan fixes the two baseline values but never states the
        comparison operator, so this pins the interpretation: a CTA delta
        that has *shrunk* below its recorded baseline is the migration
        working, and must not fail a soak day. Without this case every
        baseline assertion in the suite uses 0-vs-0, where ``<=`` and ``==``
        are indistinguishable -- a later edit to either operator would stay
        green.
        """
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
        ledger = _ledger(cta_missing_baseline=5, cta_extra_baseline=5)
        result = mod.diff_library_dbs(mysql_conn, backend_conn, ledger=ledger)
        mysql_conn.close()
        backend_conn.close()

        assert result.cta_missing == 1
        assert result.cta_extra == 0
        assert result.clean is True

    def test_clean_false_without_a_populated_cta_baseline(self, tmp_path: Path) -> None:
        """A ledger with no baseline yet (None/None, the shipped default)
        cannot certify CTA is within bounds -- clean stays False even when
        everything else matches, matching 'until step 6, clean cannot be
        true.'"""
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        rows = [{"id": 1}]
        _make_library_db(mysql_db, rows)
        _make_library_db(backend_db, rows)

        mysql_conn = sqlite3.connect(mysql_db)
        backend_conn = sqlite3.connect(backend_db)
        result = mod.diff_library_dbs(mysql_conn, backend_conn, ledger=_ledger())
        mysql_conn.close()
        backend_conn.close()

        assert result.clean is False


class TestResidueLedgerCli:
    """``--residue-ledger`` / ``--fail-on-drift`` CLI wiring."""

    def _write_ledger(self, path: Path, *, cta_missing_baseline=0, cta_extra_baseline=0) -> None:
        path.write_text(
            json.dumps(
                {
                    "measured_date": "2026-08-11",
                    "collapsed_mysql_ids": {},
                    "baselines": {
                        "measured_date": "2026-08-11",
                        "normalizations": {},
                        "cta_missing": cta_missing_baseline,
                        "cta_extra": cta_extra_baseline,
                    },
                }
            ),
            encoding="utf-8",
        )

    def test_fail_on_drift_with_residue_ledger_none_exits_2(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        exit_code = mod.main(
            [
                "--mysql-db",
                str(mysql_db),
                "--backend-db",
                str(backend_db),
                "--residue-ledger",
                "none",
                "--fail-on-drift",
            ]
        )
        assert exit_code == 2

    def test_residue_ledger_none_alone_reports_clean_null(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        exit_code = mod.main(
            [
                "--mysql-db",
                str(mysql_db),
                "--backend-db",
                str(backend_db),
                "--residue-ledger",
                "none",
                "--json",
            ]
        )
        assert exit_code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["clean"] is None

    def test_residue_ledger_missing_path_exits_3(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])

        exit_code = mod.main(
            [
                "--mysql-db",
                str(mysql_db),
                "--backend-db",
                str(backend_db),
                "--residue-ledger",
                str(tmp_path / "does-not-exist.json"),
            ]
        )
        assert exit_code == 3

    def test_residue_ledger_malformed_json_exits_3(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}])
        _make_library_db(backend_db, [{"id": 1}])
        bad_ledger = tmp_path / "ledger.json"
        bad_ledger.write_text("not json{{{", encoding="utf-8")

        exit_code = mod.main(
            [
                "--mysql-db",
                str(mysql_db),
                "--backend-db",
                str(backend_db),
                "--residue-ledger",
                str(bad_ledger),
            ]
        )
        assert exit_code == 3

    def test_fail_on_drift_exits_4_when_not_clean(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        _make_library_db(mysql_db, [{"id": 1}, {"id": 2, "genre": "Rock", "format": "CD"}])
        _make_library_db(backend_db, [{"id": 1}])
        ledger_path = tmp_path / "ledger.json"
        self._write_ledger(ledger_path)

        exit_code = mod.main(
            [
                "--mysql-db",
                str(mysql_db),
                "--backend-db",
                str(backend_db),
                "--residue-ledger",
                str(ledger_path),
                "--fail-on-drift",
            ]
        )
        assert exit_code == 4

    def test_fail_on_drift_exits_0_when_clean(self, tmp_path: Path) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        rows = [{"id": 1}]
        _make_library_db(mysql_db, rows)
        _make_library_db(backend_db, rows)
        ledger_path = tmp_path / "ledger.json"
        self._write_ledger(ledger_path)

        exit_code = mod.main(
            [
                "--mysql-db",
                str(mysql_db),
                "--backend-db",
                str(backend_db),
                "--residue-ledger",
                str(ledger_path),
                "--fail-on-drift",
            ]
        )
        assert exit_code == 0

    def test_default_residue_ledger_resolves_from_an_arbitrary_cwd(self, tmp_path: Path) -> None:
        """The default --residue-ledger path must resolve relative to the
        module, never the cwd -- an operator's mktemp -d workflow launches
        this from anywhere. Runs as a real subprocess with cwd=tmp_path (a
        directory with no vendor/ nearby) to prove it, not just call the
        function in-process where cwd never mattered to begin with."""
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
            cwd=str(tmp_path),
        )

        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        # clean is not None only when a ledger actually loaded -- proving the
        # module-relative default found the real vendored ledger.json despite
        # running from a cwd with nothing under it.
        assert payload["clean"] is not None

    def test_print_human_and_log_carry_the_verdict(
        self, tmp_path: Path, capsys: pytest.CaptureFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        mod = _load_module()
        mysql_db = tmp_path / "mysql.db"
        backend_db = tmp_path / "backend.db"
        rows = [{"id": 1}]
        _make_library_db(mysql_db, rows)
        _make_library_db(backend_db, rows)
        ledger_path = tmp_path / "ledger.json"
        self._write_ledger(ledger_path)

        with caplog.at_level(logging.INFO):
            exit_code = mod.main(
                [
                    "--mysql-db",
                    str(mysql_db),
                    "--backend-db",
                    str(backend_db),
                    "--residue-ledger",
                    str(ledger_path),
                ]
            )
        assert exit_code == 0

        human_out = capsys.readouterr().out
        assert "clean" in human_out

        final_record = next(
            r for r in caplog.records if r.message == "catalog parity diff complete"
        )
        assert final_record.clean is True
        assert final_record.missing_unexplained == 0
        assert final_record.extra_unexplained == 0


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


class TestCatalogRowToIdMapEntry:
    """``_catalog_row_to_id_map_entry`` -- the id-map sibling of
    ``_catalog_row_to_library_row``, keeping the Backend serial id the
    general producer discards (WXYC/Backend-Service#2152)."""

    def test_maps_legacy_release_id_to_backend_serial_id(self) -> None:
        mod = _load_module()
        row = _catalog_row(legacy_release_id=72_101)
        assert row["id"] != row["legacy_release_id"]  # the fixture's own invariant

        assert mod._catalog_row_to_id_map_entry(row) == (72_101, row["id"])

    def test_missing_id_field_raises(self) -> None:
        mod = _load_module()
        row = _catalog_row(legacy_release_id=72_101)
        del row["id"]
        with pytest.raises(mod.SourceError, match="no 'id' field"):
            mod._catalog_row_to_id_map_entry(row)

    def test_non_integer_id_field_raises(self) -> None:
        mod = _load_module()
        row = _catalog_row(legacy_release_id=72_101, id="not-a-number")
        with pytest.raises(mod.SourceError, match="non-integer 'id'"):
            mod._catalog_row_to_id_map_entry(row)


class TestFetchCompilationTracksById:
    """``_fetch_compilation_tracks``: GET /library/{id}/compilation-tracks
    (BS#1964), the only source of ``track_position`` for the FFFD capture
    mode (the bulk CTA export omits it -- see
    ``_catalog_row_to_id_map_entry``'s docstring)."""

    def test_happy_path_returns_the_tracks_list(self) -> None:
        mod = _load_module()
        with _BackendStub(
            catalog_rows=[],
            cta_rows=[],
            compilation_tracks_by_id={
                5_001: [
                    {
                        "id": 1,
                        "artist_name": "Csillagrablók",
                        "track_title": "Reménytelen Tánc",
                        "track_position": "3",
                    }
                ]
            },
        ) as stub:
            token_source = mod._TokenSource.__new__(mod._TokenSource)
            token_source._static = "svc-token"
            tracks = mod._fetch_compilation_tracks(stub.base_url, 5_001, token_source)

        assert tracks == [
            {
                "id": 1,
                "artist_name": "Csillagrablók",
                "track_title": "Reménytelen Tánc",
                "track_position": "3",
            }
        ]

    def test_unknown_id_returns_empty_list_not_an_error(self) -> None:
        """A 404 -- the release vanished between the catalog snapshot and
        this follow-up call -- degrades gracefully; it is not this mode's
        job to fail the whole capture over one stale id."""
        mod = _load_module()
        with _BackendStub(catalog_rows=[], cta_rows=[], compilation_tracks_by_id={}) as stub:
            token_source = mod._TokenSource.__new__(mod._TokenSource)
            token_source._static = "svc-token"
            assert mod._fetch_compilation_tracks(stub.base_url, 5_001, token_source) == []

    def test_refreshes_a_rejected_token_and_retries_once(self, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_EMAIL_ENV, "parity@wxyc.invalid")
        monkeypatch.setenv(mod.BACKEND_PASSWORD_ENV, "sekrit")
        with _BackendStub(
            catalog_rows=[],
            cta_rows=[],
            credentials=("parity@wxyc.invalid", "sekrit"),
            compilation_tracks_by_id={
                5_001: [
                    {
                        "artist_name": "µ-Ziq",
                        "track_title": "Hasty Boom Alert",
                        "track_position": "1",
                    }
                ]
            },
        ) as stub:
            token_source = mod._TokenSource(stub.base_url)
            # A stale/garbage bearer forces the 401-then-refresh path.
            token_source._jwt = "stale-jwt"
            token_source._jwt_expires_at = time.time() + 900
            tracks = mod._fetch_compilation_tracks(stub.base_url, 5_001, token_source)

        assert tracks[0]["artist_name"] == "µ-Ziq"


class TestCaptureFffdCtaPairs:
    """``capture_fffd_cta_pairs``: the WXYC/Backend-Service#2152 end-to-end
    orchestration -- MySQL truth from an already-built library.db,
    Backend's corrupt rows + track_position over HTTP, paired via
    ``lib.fffd_pair_capture.find_fffd_pairs``."""

    def test_resolves_a_corrupt_row_and_renders_sql_values(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        mysql_db = tmp_path / "mysql.db"
        _make_library_db(
            mysql_db,
            rows=[],
            cta_rows=[(50_340, "Csillagrablók", "Reménytelen Tánc")],
        )
        with _BackendStub(
            catalog_rows=[_catalog_row(id=5_001, legacy_release_id=50_340)],
            cta_rows=[
                {
                    "legacy_release_id": 50_340,
                    "artist_name": "Csillagrablók",
                    "track_title": "Rem�nytelen T�nc",
                }
            ],
            compilation_tracks_by_id={
                5_001: [
                    {
                        "artist_name": "Csillagrablók",
                        "track_title": "Rem�nytelen T�nc",
                        "track_position": "3",
                    }
                ]
            },
        ) as stub:
            report = mod.capture_fffd_cta_pairs(str(mysql_db), stub.base_url)

        assert report["unresolved"] == []
        assert len(report["resolved"]) == 1
        row = report["resolved"][0]
        assert row["legacy_release_id"] == 50_340
        assert row["track_position"] == "3"
        assert row["true_artist_name"] is None
        assert row["true_track_title"] == "Reménytelen Tánc"
        assert [c["codepoint"] for c in row["true_track_title_codepoints"]] == [
            "U+00E9",
            "U+00E1",
        ]
        assert "'Reménytelen Tánc'" in report["sql_values"]
        assert "50340" in report["sql_values"]

    def test_zero_candidates_reports_unresolved_not_a_guess(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        mysql_db = tmp_path / "mysql.db"
        _make_library_db(
            mysql_db,
            rows=[],
            cta_rows=[(1, "Nilüfer Yanya", "Midnight Sky")],  # different track_title
        )
        with _BackendStub(
            catalog_rows=[_catalog_row(id=5_002, legacy_release_id=1)],
            cta_rows=[
                {
                    "legacy_release_id": 1,
                    "artist_name": "Nil�fer Yanya",
                    "track_title": "Midnight Sun",
                }
            ],
            compilation_tracks_by_id={5_002: []},
        ) as stub:
            report = mod.capture_fffd_cta_pairs(str(mysql_db), stub.base_url)

        assert report["resolved"] == []
        assert len(report["unresolved"]) == 1
        assert report["unresolved"][0]["reason"] == "zero_candidates"
        assert report["sql_values"] == ""

    def test_clean_rows_never_appear_in_either_list(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        mysql_db = tmp_path / "mysql.db"
        _make_library_db(mysql_db, rows=[], cta_rows=[(1, "Stereolab", "Miss Modular")])
        with _BackendStub(
            catalog_rows=[_catalog_row(id=5_003, legacy_release_id=1)],
            cta_rows=[
                {"legacy_release_id": 1, "artist_name": "Stereolab", "track_title": "Miss Modular"}
            ],
            compilation_tracks_by_id={5_003: []},
        ) as stub:
            report = mod.capture_fffd_cta_pairs(str(mysql_db), stub.base_url)

        assert report == {"resolved": [], "unresolved": [], "sql_values": ""}


class TestCaptureFffdCtaPairsCli:
    """``main``'s ``--capture-fffd-cta-pairs`` branch: flag validation +
    end-to-end wiring through the real CLI entry point."""

    def test_requires_backend_source(self) -> None:
        mod = _load_module()
        assert mod.main(["--capture-fffd-cta-pairs", "-", "--mysql-db", "x"]) == 2

    def test_requires_a_mysql_source_or_prebuilt_db(self) -> None:
        mod = _load_module()
        assert (
            mod.main(["--capture-fffd-cta-pairs", "-", "--backend-source", "https://api.wxyc.org"])
            == 2
        )

    def test_mysql_source_without_mysql_db_is_a_usage_error(self) -> None:
        mod = _load_module()
        assert (
            mod.main(
                [
                    "--capture-fffd-cta-pairs",
                    "-",
                    "--backend-source",
                    "https://api.wxyc.org",
                    "--mysql-source",
                    "mysql://u:p@h/db",
                ]
            )
            == 2
        )

    def test_end_to_end_via_mysql_source_and_backend_source(
        self, tmp_path: Path, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        monkeypatch.setattr(
            mod,
            "_mysql_runner",
            _FakeMysqlRunner(
                library_tsv="",
                cta_tsv="50340\tCsillagrablók\tReménytelen Tánc\n",
            ),
        )
        with _BackendStub(
            catalog_rows=[_catalog_row(id=5_001, legacy_release_id=50_340)],
            cta_rows=[
                {
                    "legacy_release_id": 50_340,
                    "artist_name": "Csillagrablók",
                    "track_title": "Rem�nytelen T�nc",
                }
            ],
            compilation_tracks_by_id={
                5_001: [
                    {
                        "artist_name": "Csillagrablók",
                        "track_title": "Rem�nytelen T�nc",
                        "track_position": "3",
                    }
                ]
            },
        ) as stub:
            exit_code = mod.main(
                [
                    "--capture-fffd-cta-pairs",
                    "-",
                    "--mysql-source",
                    "mysql://wxyc:sekrit@127.0.0.1/wxycmusic",
                    "--mysql-db",
                    str(tmp_path / "mysql.db"),
                    "--backend-source",
                    stub.base_url,
                ]
            )

        assert exit_code == 0
        report = json.loads(capsys.readouterr().out)
        assert len(report["resolved"]) == 1
        assert report["resolved"][0]["true_track_title"] == "Reménytelen Tánc"

    def test_written_to_a_file_path_when_not_dash(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        monkeypatch.setattr(
            mod,
            "_mysql_runner",
            _FakeMysqlRunner(library_tsv="", cta_tsv=""),
        )
        out = tmp_path / "fffd-pairs.json"
        with _BackendStub(catalog_rows=[], cta_rows=[]) as stub:
            exit_code = mod.main(
                [
                    "--capture-fffd-cta-pairs",
                    str(out),
                    "--mysql-source",
                    "mysql://wxyc:sekrit@127.0.0.1/wxycmusic",
                    "--mysql-db",
                    str(tmp_path / "mysql.db"),
                    "--backend-source",
                    stub.base_url,
                ]
            )

        assert exit_code == 0
        assert json.loads(out.read_text()) == {
            "resolved": [],
            "unresolved": [],
            "sql_values": "",
        }

    def test_unresolved_rows_exit_4_with_the_report_still_written(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """ "Ran fine, captured nothing usable" must not read as success.

        This mode exists to hand WXYC/Backend-Service#2152 a specific set of
        pairs. A run where every corrupt row came back unresolved is exactly
        as actionable as a crash and exactly as invisible at exit 0 -- the
        same argument ``--fail-on-drift``'s exit 4 already makes for the diff,
        so it reuses that code rather than inventing one. The report is still
        written first: the unresolved rows and their candidates are the whole
        diagnostic.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        monkeypatch.setattr(
            mod,
            "_mysql_runner",
            _FakeMysqlRunner(library_tsv="", cta_tsv="50340\tCsillagrablók\tEgy Másik Dal\n"),
        )
        out = tmp_path / "fffd-pairs.json"
        with _BackendStub(
            catalog_rows=[_catalog_row(id=5_001, legacy_release_id=50_340)],
            cta_rows=[
                {
                    "legacy_release_id": 50_340,
                    "artist_name": "Csillagrablók",
                    "track_title": "Rem�nytelen T�nc",
                }
            ],
            compilation_tracks_by_id={5_001: []},
        ) as stub:
            exit_code = mod.main(
                [
                    "--capture-fffd-cta-pairs",
                    str(out),
                    "--mysql-source",
                    "mysql://wxyc:sekrit@127.0.0.1/wxycmusic",
                    "--mysql-db",
                    str(tmp_path / "mysql.db"),
                    "--backend-source",
                    stub.base_url,
                ]
            )

        assert exit_code == 4
        report = json.loads(out.read_text())
        assert report["resolved"] == []
        assert report["unresolved"][0]["reason"] == "zero_candidates"

    def test_unwritable_output_path_is_rejected_before_any_source_work(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A bad ``PATH`` must cost nothing.

        This mode is only runnable in CI (repo-secret credentials plus a
        working ``mariadb-client``), so discovering an unwritable path *after*
        the MySQL export and every Backend fetch have completed throws away a
        report that cannot casually be re-taken -- and the retry then trips
        ``_require_absent`` on the half-built ``--mysql-db``.
        """
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        runner = _FakeMysqlRunner(library_tsv="", cta_tsv="")
        monkeypatch.setattr(mod, "_mysql_runner", runner)

        exit_code = mod.main(
            [
                "--capture-fffd-cta-pairs",
                str(tmp_path / "no-such-dir" / "fffd-pairs.json"),
                "--mysql-source",
                "mysql://wxyc:sekrit@127.0.0.1/wxycmusic",
                "--mysql-db",
                str(tmp_path / "mysql.db"),
                "--backend-source",
                "https://api.wxyc.org",
            ]
        )

        assert exit_code == 2
        assert runner.calls == []
        assert not (tmp_path / "mysql.db").exists()

    def test_a_late_write_failure_falls_back_to_stdout_rather_than_losing_the_capture(
        self, tmp_path: Path, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        """The pre-flight check cannot cover a disk that fills mid-write."""
        mod = _load_module()
        monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
        monkeypatch.setattr(
            mod,
            "_mysql_runner",
            _FakeMysqlRunner(library_tsv="", cta_tsv="50340\tCsillagrablók\tReménytelen Tánc\n"),
        )

        out = tmp_path / "fffd-pairs.json"
        real_write_text = Path.write_text

        def _explode(self, *args, **kwargs):
            # Scoped to the report itself -- the TSV the fake mysql runner
            # writes goes through this same method and must still succeed.
            if self == out:
                raise OSError(28, "No space left on device")
            return real_write_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", _explode)
        with _BackendStub(
            catalog_rows=[_catalog_row(id=5_001, legacy_release_id=50_340)],
            cta_rows=[
                {
                    "legacy_release_id": 50_340,
                    "artist_name": "Csillagrablók",
                    "track_title": "Rem�nytelen T�nc",
                }
            ],
            compilation_tracks_by_id={
                5_001: [
                    {
                        "artist_name": "Csillagrablók",
                        "track_title": "Rem�nytelen T�nc",
                        "track_position": "3",
                    }
                ]
            },
        ) as stub:
            exit_code = mod.main(
                [
                    "--capture-fffd-cta-pairs",
                    str(out),
                    "--mysql-source",
                    "mysql://wxyc:sekrit@127.0.0.1/wxycmusic",
                    "--mysql-db",
                    str(tmp_path / "mysql.db"),
                    "--backend-source",
                    stub.base_url,
                ]
            )

        assert exit_code == 3
        captured = capsys.readouterr()
        report = json.loads(captured.out)
        assert report["resolved"][0]["true_track_title"] == "Reménytelen Tánc"


class TestServiceAccountMint:
    """Minting and refreshing the Backend service-account JWT (#365).

    The JWT that ``requirePermissions`` accepts lives 15 minutes (better-auth's
    default, which Backend-Service does not override), so it cannot be a static
    CI secret for a soak that runs 7+ consecutive days: what CI stores is the
    service account's *password*, and the producer mints per run.

    The load-bearing constraint on how it refreshes is the auth service's rate
    limiter. ``/auth/sign-in`` is capped at 10 per 15 minutes by the express
    limiter and 3 per 10 seconds by better-auth's own; ``/auth/token`` is
    exempt from the former and generous under the latter. So the *ordinary*
    refresh re-exchanges against a cached session, and only an exchange that
    itself 401s costs a second sign-in -- capped at one per process, or a
    broken credential becomes a sign-in storm against a limiter shared with
    every real DJ logging in.
    """

    @staticmethod
    def _use_credentials(mod, monkeypatch, stub) -> None:
        monkeypatch.delenv(mod.BACKEND_TOKEN_ENV, raising=False)
        monkeypatch.setenv(mod.BACKEND_EMAIL_ENV, "catalog-parity@wxyc.invalid")
        monkeypatch.setenv(mod.BACKEND_PASSWORD_ENV, "hunter2-but-48-bytes")
        monkeypatch.setenv(mod.BACKEND_AUTH_URL_ENV, f"{stub.base_url}/auth")

    def test_mints_a_token_from_the_credential_pair(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_ins == 1
            assert stub.exchanges == 1
            # Every export carried a bearer this stub actually issued.
            for request in stub.export_requests:
                assert (request.authorization or "").removeprefix("Bearer ") in stub.jwts
            # better-auth's CSRF guard rejects an origin-less sign-in.
            sign_in = next(r for r in stub.requests if r.path == "/auth/sign-in/email")
            assert sign_in.origin == mod._DEFAULT_AUTH_ORIGIN
        assert out.exists()

    def test_refreshes_by_re_exchanging_not_re_signing_in(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A 401 mid-run costs an exchange, never a sign-in.

        This is the assertion the rate limiter makes load-bearing: a run that
        outlives its 15-minute token (three snapshot attempts x two exports,
        each with a 300s budget) must not spend a sign-in per refresh.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)

            # Expire the minted JWT the instant the first export is served, so
            # the CTA export gets a 401 the way a real 15-minute expiry would
            # deliver one: mid-run, on a token that was valid when it left.
            def expire_everything() -> None:
                stub.jwts.clear()

            stub.on_catalog_fetch = expire_everything
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_ins == 1, "a refresh must not re-sign-in"
            assert stub.exchanges == 2
        assert out.exists()

    def test_re_signs_in_once_when_the_exchange_itself_401s(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A dead session is the one thing that legitimately costs a sign-in."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.exchange_statuses = [401]
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_ins == 2
            assert stub.exchanges == 2
        assert out.exists()

    def test_a_retried_sign_in_does_not_cost_the_refresh_path(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """The 429 retry is part of one sign-in, not a second one.

        Counting HTTP attempts rather than sign-ins conflates the two budgets:
        a single rate-limited-then-successful sign-in would exhaust the
        allowance, and every later refresh -- against a session that is
        perfectly good -- would abort without issuing an exchange at all.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.sign_in_statuses = [429]
            monkeypatch.setattr(mod.time, "sleep", lambda _seconds: None)
            stub.on_catalog_fetch = stub.jwts.clear
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.exchanges == 2, "the refresh must survive a retried sign-in"
            assert stub.sign_ins == 2, "one 429 plus its retry -- and no more"
        assert out.exists()

    def test_recovering_a_dead_session_does_not_end_the_refresh_path(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Spending the sign-in allowance must not disable re-exchanging.

        The allowance exists to bound sign-ins against a limiter shared with
        every DJ logging in. Gating the *exchange* loop on it as well means a
        run that legitimately recovers one dead session can never refresh
        again -- which is the whole reason the token source exists.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.exchange_statuses = [401]  # spends the second sign-in
            stub.on_catalog_fetch = stub.jwts.clear  # then forces a refresh
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_ins == 2
            assert stub.exchanges == 3, "the post-recovery refresh must re-exchange"
        assert out.exists()

    def test_gives_up_rather_than_looping_on_a_dead_credential(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row()],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.exchange_statuses = [401, 401, 401, 401]
            with pytest.raises(mod.SourceError, match="401"):
                mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_ins <= 2, "a broken credential must not storm the limiter"
        assert not out.exists()

    def test_retries_a_rate_limited_sign_in_once(self, tmp_path: Path, monkeypatch) -> None:
        """429 carries X-Retry-After from better-auth, Retry-After from express."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.sign_in_statuses = [429]
            monkeypatch.setattr(mod.time, "sleep", lambda _seconds: None)
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_ins == 2
        assert out.exists()

    def test_a_second_429_fails_with_the_status(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row()],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.sign_in_statuses = [429, 429]
            monkeypatch.setattr(mod.time, "sleep", lambda _seconds: None)
            with pytest.raises(mod.SourceError, match="429"):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert not out.exists()

    def test_a_retry_hint_longer_than_the_cap_fails_fast(self, tmp_path: Path, monkeypatch) -> None:
        """Truncating the hint buys a certain second 429 instead of an answer.

        The express limiter's window is 15 minutes, so its draft-7 hint can
        legitimately be ~900s. Waiting the 10s cap and retrying anyway is a
        guaranteed refusal; the operator wants the real number.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        slept: list[float] = []
        with _BackendStub(
            catalog_rows=[_catalog_row()],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.sign_in_statuses = [429]
            stub.sign_in_retry_after = "900"
            monkeypatch.setattr(mod.time, "sleep", slept.append)
            with pytest.raises(mod.SourceError, match="900"):
                mod._build_library_db_from_backend(stub.base_url, str(out))

            assert slept == [], "no point sleeping out a window this run cannot clear"
            assert stub.sign_ins == 1
        assert not out.exists()

    def test_the_refresh_margin_covers_a_whole_fetch(self, tmp_path: Path, monkeypatch) -> None:
        """A token must not start a fetch that can outlive it.

        A fetch runs for up to ``_HTTP_TIMEOUT_SECONDS``, so a token with less
        life than that is not usable even though it has not expired yet. The
        401 retry would paper over it -- at the cost of re-fetching a whole
        export.
        """
        mod = _load_module()
        assert mod._JWT_REFRESH_MARGIN_SECONDS >= mod._HTTP_TIMEOUT_SECONDS

        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
            jwt_ttl_seconds=mod._HTTP_TIMEOUT_SECONDS - 100,
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.exchanges == 2, "a token too short for a fetch is not reused"
        assert out.exists()

    def test_an_unreadable_exp_costs_one_exchange_not_one_per_call(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """An unparseable ``exp`` must not turn every call into an exchange.

        Treating it as "expired" re-exchanges on every ``token()`` -- 6+ per
        run across three snapshot attempts x two exports -- against
        better-auth's 3-per-10s window. Assuming the documented 15-minute life
        bounds it, and the 401 retry remains the authority.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
            unreadable_jwt_exp=True,
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.exchanges == 1
        assert out.exists()

    def test_signs_out_the_session_it_minted(self, tmp_path: Path, monkeypatch) -> None:
        """A run must not leave a year-long credential behind it.

        Backend-Service pins ``session.expiresIn`` to 365 days, so every
        un-revoked sign-in is a standalone catalog:read credential that
        survives a password rotation (``admin/set-user-password`` revokes
        nothing). A daily soak would accumulate one per run.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_outs == 1
            assert stub.sessions == set(), "the minted session must not outlive the run"
        assert out.exists()

    def test_signs_out_even_when_the_export_fails(self, tmp_path: Path, monkeypatch) -> None:
        """The failure path is the one that would otherwise leak most."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[],  # an empty catalog is a producer failure
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            with pytest.raises(mod.SourceError):
                mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_outs == 1
            assert stub.sessions == set()
        assert not out.exists()

    def test_a_failed_sign_out_does_not_fail_the_run(self, tmp_path: Path, monkeypatch) -> None:
        """Best-effort: the export already succeeded, and the JWT expires anyway."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.sign_out_statuses = [500]
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_outs == 1
        assert out.exists()

    def test_a_redirected_sign_out_does_not_fail_a_finished_run(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Best-effort has to mean every failure, not just the socket ones.

        The redirect guard raises ``SourceError`` -- a ``RuntimeError``, not an
        ``OSError`` -- so a proxy that redirects only ``/sign-out`` would throw
        out of the cleanup and fail a build that had already succeeded.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.sign_out_statuses = [302]
            mod._build_library_db_from_backend(stub.base_url, str(out))
        assert out.exists()

    def test_a_failed_sign_out_never_masks_the_real_failure(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """An exception from `finally` replaces the one being raised.

        The operator needs the diagnosis they can act on -- the empty export --
        not a redirect on a cleanup call they never asked for.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[],  # the real failure: a producer that read nothing
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.sign_out_statuses = [302]
            with pytest.raises(mod.SourceError, match="returned no rows"):
                mod._build_library_db_from_backend(stub.base_url, str(out))
        assert not out.exists()

    def test_a_429_without_a_usable_hint_fails_fast(self, tmp_path: Path, monkeypatch) -> None:
        """No hint means no way to tell a 10s window from a 15-minute one.

        Both limiters we know about send one, so a 429 without it came from
        something else in front of them -- and retrying into the express
        limiter's 15-minute window would burn a second slot from a budget
        shared with every DJ signing in, to learn nothing.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        slept: list[float] = []
        with _BackendStub(
            catalog_rows=[_catalog_row()],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.sign_in_statuses = [429]
            stub.sign_in_retry_after = None
            monkeypatch.setattr(mod.time, "sleep", slept.append)
            with pytest.raises(mod.SourceError, match="429"):
                mod._build_library_db_from_backend(stub.base_url, str(out))

            assert slept == []
            assert stub.sign_ins == 1
        assert not out.exists()

    def test_a_zero_retry_hint_still_waits(self, tmp_path: Path, monkeypatch) -> None:
        """`Retry-After: 0` is not an invitation to retry inside the same tick."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        slept: list[float] = []
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            stub.sign_in_statuses = [429]
            stub.sign_in_retry_after = "0"
            monkeypatch.setattr(mod.time, "sleep", slept.append)
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert slept and all(delay >= 1 for delay in slept)
        assert out.exists()

    def test_nothing_to_sign_out_of_in_static_mode(self, tmp_path: Path, monkeypatch) -> None:
        """A token the operator supplied is not this run's to revoke."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(catalog_rows=[_catalog_row()], cta_rows=[]) as stub:
            monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
            monkeypatch.delenv(mod.BACKEND_EMAIL_ENV, raising=False)
            monkeypatch.delenv(mod.BACKEND_PASSWORD_ENV, raising=False)
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_outs == 0
        assert out.exists()

    def test_a_stale_static_token_falls_back_to_the_credentials(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A leftover $BACKEND_CATALOG_TOKEN must not strand a run that can mint.

        #365's original acceptance criterion asked for that secret, so an
        unattended run may well inherit one alongside the credential pair.
        Failing on a 15-minute-old JWT while holding everything needed to mint
        a fresh one -- and advising the operator to set variables that are
        already set -- is the wrong end state.
        """
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "stale-token")
            monkeypatch.setenv(mod.BACKEND_EMAIL_ENV, "catalog-parity@wxyc.invalid")
            monkeypatch.setenv(mod.BACKEND_PASSWORD_ENV, "hunter2-but-48-bytes")
            monkeypatch.setenv(mod.BACKEND_AUTH_URL_ENV, f"{stub.base_url}/auth")
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_ins == 1, "the fallback mints rather than giving up"
            assert stub.exchanges == 1
        assert out.exists()

    def test_a_pre_minted_token_still_wins(self, tmp_path: Path, monkeypatch) -> None:
        """Static mode is how an operator runs a one-off without the password."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(catalog_rows=[_catalog_row()], cta_rows=[]) as stub:
            monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "svc-token")
            monkeypatch.setenv(mod.BACKEND_EMAIL_ENV, "catalog-parity@wxyc.invalid")
            monkeypatch.setenv(mod.BACKEND_PASSWORD_ENV, "hunter2-but-48-bytes")
            mod._build_library_db_from_backend(stub.base_url, str(out))

            assert stub.sign_ins == 0
            assert stub.exchanges == 0
            assert all(r.authorization == "Bearer svc-token" for r in stub.export_requests)

    def test_an_expired_static_token_says_what_to_do_about_it(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A 401 on a static token is terminal -- there is nothing to refresh from."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row()],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            monkeypatch.setenv(mod.BACKEND_TOKEN_ENV, "stale-token")
            monkeypatch.delenv(mod.BACKEND_EMAIL_ENV, raising=False)
            monkeypatch.delenv(mod.BACKEND_PASSWORD_ENV, raising=False)
            with pytest.raises(mod.SourceError, match=mod.BACKEND_PASSWORD_ENV):
                mod._build_library_db_from_backend(stub.base_url, str(out))
            assert stub.sign_ins == 0
        assert not out.exists()

    def test_re_exchanges_a_token_that_is_about_to_expire(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Don't start a 300-second fetch with 30 seconds of token left."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
            jwt_ttl_seconds=30,
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            mod._build_library_db_from_backend(stub.base_url, str(out))

            # One exchange per export: each one saw a token inside the margin.
            assert stub.exchanges == 2
            assert stub.sign_ins == 1
        assert out.exists()

    def test_requires_a_token_or_the_credential_pair(self, tmp_path: Path, monkeypatch) -> None:
        mod = _load_module()
        for var in (mod.BACKEND_TOKEN_ENV, mod.BACKEND_EMAIL_ENV, mod.BACKEND_PASSWORD_ENV):
            monkeypatch.delenv(var, raising=False)
        with pytest.raises(mod.SourceError) as excinfo:
            mod._build_library_db_from_backend("https://api.wxyc.org", str(tmp_path / "b.db"))
        # Both routes named: an operator reading this shouldn't have to guess
        # which of the two credential shapes the harness wanted.
        assert mod.BACKEND_TOKEN_ENV in str(excinfo.value)
        assert mod.BACKEND_EMAIL_ENV in str(excinfo.value)
        assert mod.BACKEND_PASSWORD_ENV in str(excinfo.value)

    def test_refuses_a_plaintext_auth_url(self, tmp_path: Path, monkeypatch) -> None:
        """The password is on that wire -- the message must say so."""
        mod = _load_module()
        monkeypatch.delenv(mod.BACKEND_TOKEN_ENV, raising=False)
        monkeypatch.setenv(mod.BACKEND_EMAIL_ENV, "catalog-parity@wxyc.invalid")
        monkeypatch.setenv(mod.BACKEND_PASSWORD_ENV, "hunter2-but-48-bytes")
        monkeypatch.setenv(mod.BACKEND_AUTH_URL_ENV, "http://auth.example.org/auth")
        with pytest.raises(mod.SourceError) as excinfo:
            mod._build_library_db_from_backend("https://api.wxyc.org", str(tmp_path / "b.db"))
        message = str(excinfo.value)
        assert "https" in message
        assert mod.BACKEND_AUTH_URL_ENV in message
        assert mod.BACKEND_PASSWORD_ENV in message
        # Must not misname the secret at risk: no bearer token is involved here.
        assert mod.BACKEND_TOKEN_ENV not in message

    def test_refuses_a_cross_origin_redirect_on_sign_in(self, tmp_path: Path, monkeypatch) -> None:
        """A 302 out of sign-in would replay the password to a host nobody named."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row()],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as elsewhere:
            with _RedirectingStub(elsewhere.base_url) as redirector:
                self._use_credentials(mod, monkeypatch, elsewhere)
                monkeypatch.setenv(mod.BACKEND_AUTH_URL_ENV, f"{redirector.base_url}/auth")
                with pytest.raises(mod.SourceError, match="redirect"):
                    mod._build_library_db_from_backend(elsewhere.base_url, str(out))
            assert elsewhere.sign_ins == 0
        assert not out.exists()

    def test_derives_the_auth_url_from_the_backend_source(self, monkeypatch) -> None:
        """One flag, not two: --backend-source https://api.wxyc.org implies /auth."""
        mod = _load_module()
        monkeypatch.delenv(mod.BACKEND_AUTH_URL_ENV, raising=False)
        assert mod._default_auth_url("https://api.wxyc.org") == "https://api.wxyc.org/auth"
        assert mod._default_auth_url("https://api.wxyc.org/") == "https://api.wxyc.org/auth"

    def test_never_prints_the_session_token_or_password(
        self, tmp_path: Path, monkeypatch, capsys
    ) -> None:
        """A sign-in success body carries a session token; it must not surface."""
        mod = _load_module()
        out = tmp_path / "backend.db"
        with _BackendStub(
            catalog_rows=[_catalog_row(legacy_release_id=72_101)],
            cta_rows=[],
            credentials=("catalog-parity@wxyc.invalid", "hunter2-but-48-bytes"),
        ) as stub:
            self._use_credentials(mod, monkeypatch, stub)
            mod._build_library_db_from_backend(stub.base_url, str(out))
            printed = capsys.readouterr()
            haystack = printed.out + printed.err
            assert "hunter2-but-48-bytes" not in haystack
            for session in stub.sessions:
                assert session not in haystack
            for jwt in stub.jwts:
                assert jwt not in haystack


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
