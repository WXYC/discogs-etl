"""A truncated catalog export must fail the build, not shorten it (#420).

``lib/backend_library_source.py`` consumes the NDJSON export line by line,
which means the body reaches it through ``http.client``'s ``readinto`` /
``readline`` path -- and that path deliberately does **not** raise
``IncompleteRead`` when the socket ends early. The stdlib says so in a comment
in ``HTTPResponse.readinto``: "Ideally, we would raise IncompleteRead if the
content-length wasn't satisfied, but it might break compatibility." So a body
cut at a line boundary used to yield 101 of 200 rows with no exception at all.

Two things masked it. A cut *gzip* stream raises ``EOFError`` from the
decompressor, and a cut mid-*line* fails the JSON parse. What was left
uncovered was the exact combination of identity encoding and a cut that lands
on a newline -- which is reachable the moment anything in front of
Backend-Service decompresses on the way through (an nginx ``gunzip`` block, a
CDN), since none of that topology is asserted anywhere.

These tests drive ``_fetch_ndjson_once`` against a raw-socket server rather
than ``_BackendStub``, because what is under test *is* the framing: a
``Content-Length`` that over-declares, a chunked body that stops early, and a
chunked body that stops early behind a perfectly well-formed terminating
0-chunk. ``http.server`` will not emit a response that lies about its own
length, so the bytes are written by hand.
"""

from __future__ import annotations

import gzip
import json
import socket
import socketserver
import threading
from typing import Any, Callable

import pytest

from lib.backend_library_source import _fetch_ndjson_once
from lib.catalog_source_common import SourceError

_LAST_MODIFIED = "Wed, 17 Sep 2026 00:00:00 GMT"

# The probe in #420 served 200 rows and cut after 101. Same shape here: the
# half that arrives is well-formed NDJSON, so nothing downstream of the
# transport has any way to notice the other half is missing.
_TOTAL_ROWS = 200
_DELIVERED_ROWS = 101

_ARTISTS = (
    "Juana Molina",
    "Jessica Pratt",
    "Chuquimamani-Condori",
    "Stereolab",
    "Nilüfer Yanya",
)


def _rows(count: int) -> list[dict[str, Any]]:
    return [
        {"legacy_release_id": 72_000 + i, "artist_name": _ARTISTS[i % len(_ARTISTS)]}
        for i in range(count)
    ]


def _ndjson(rows: list[dict[str, Any]]) -> bytes:
    return "".join(json.dumps(row) + "\n" for row in rows).encode("utf-8")


def _mapper(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (payload["legacy_release_id"], payload["artist_name"])


def _headers(*extra: str) -> bytes:
    lines = [
        "HTTP/1.1 200 OK",
        "Content-Type: application/x-ndjson",
        f"Last-Modified: {_LAST_MODIFIED}",
        "Connection: close",
        *extra,
    ]
    return ("\r\n".join(lines) + "\r\n\r\n").encode("ascii")


def _with_content_length(body: bytes, *, declared: int | None = None, **headers: str) -> bytes:
    """A response whose ``Content-Length`` may over-declare what follows it."""
    declared = len(body) if declared is None else declared
    extra = [f"{name.replace('_', '-')}: {value}" for name, value in headers.items()]
    return _headers(f"Content-Length: {declared}", *extra) + body


def _chunked(body: bytes, *, terminate: bool, **headers: str) -> bytes:
    """A chunked response, optionally with the well-formed terminating 0-chunk."""
    extra = [f"{name.replace('_', '-')}: {value}" for name, value in headers.items()]
    framed = f"{len(body):x}\r\n".encode("ascii") + body + b"\r\n"
    if terminate:
        framed += b"0\r\n\r\n"
    return _headers("Transfer-Encoding: chunked", *extra) + framed


class _CannedBackend:
    """Answers every connection with one hand-written response, then hangs up.

    Deliberately not an ``http.server`` subclass: ``BaseHTTPRequestHandler``
    frames the body for you, and the whole point here is to serve bodies whose
    framing disagrees with their contents.
    """

    def __init__(self, response: bytes) -> None:
        self.response = response
        self._server = socketserver.ThreadingTCPServer(
            ("127.0.0.1", 0), self._handler_class(), bind_and_activate=False
        )
        self._server.allow_reuse_address = True
        self._server.daemon_threads = True
        self._server.server_bind()
        self._server.server_activate()
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/library/catalog"

    def __enter__(self) -> _CannedBackend:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def _handler_class(self) -> type[socketserver.BaseRequestHandler]:
        backend = self

        class _Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                while True:  # drain the request head; a GET carries no body
                    line = self.rfile.readline()
                    if line in (b"\r\n", b"\n", b""):
                        break
                self.wfile.write(backend.response)
                self.wfile.flush()
                # A clean FIN rather than a close-with-RST, so the truncation
                # under test is "the body stopped", not "the socket broke".
                self.connection.shutdown(socket.SHUT_WR)

        return _Handler


def _fetch(response: bytes) -> tuple[list[tuple[Any, ...]], str]:
    with _CannedBackend(response) as backend:
        return _fetch_ndjson_once(backend.url, "svc-token", _mapper)


def _expect_refusal(response: bytes, match: str) -> None:
    with pytest.raises(SourceError, match=match):
        _fetch(response)


class TestTruncatedIdentityBody:
    """The hole #420 opened: identity encoding, cut on a line boundary."""

    def test_a_short_content_length_body_raises_rather_than_returning_half(self) -> None:
        full = _ndjson(_rows(_TOTAL_ROWS))
        sent = _ndjson(_rows(_DELIVERED_ROWS))
        _expect_refusal(
            _with_content_length(sent, declared=len(full)),
            r"declared \d+ bytes",
        )

    def test_a_chunked_body_that_stops_early_raises(self) -> None:
        _expect_refusal(
            _chunked(_ndjson(_rows(_DELIVERED_ROWS)), terminate=False),
            "Content-Length",
        )

    def test_a_chunked_body_that_stops_early_behind_a_0_chunk_raises(self) -> None:
        """The nastiest case: the framing is impeccable and the body is short."""
        _expect_refusal(
            _chunked(_ndjson(_rows(_DELIVERED_ROWS)), terminate=True),
            "Content-Length",
        )

    def test_a_body_with_no_length_and_no_chunking_raises(self) -> None:
        """Connection-close framing carries no integrity signal whatsoever."""
        _expect_refusal(_headers() + _ndjson(_rows(_DELIVERED_ROWS)), "Content-Length")


class TestTruncatedGzipBody:
    """No regression on the path that already failed loudly."""

    def test_a_cut_gzip_stream_still_raises(self) -> None:
        compressed = gzip.compress(_ndjson(_rows(_TOTAL_ROWS)))
        cut = compressed[: len(compressed) // 2]
        _expect_refusal(
            _with_content_length(cut, declared=len(compressed), Content_Encoding="gzip"),
            "did not decompress",
        )

    def test_a_cut_chunked_gzip_stream_still_raises(self) -> None:
        compressed = gzip.compress(_ndjson(_rows(_TOTAL_ROWS)))
        cut = compressed[: len(compressed) // 2]
        _expect_refusal(
            _chunked(cut, terminate=True, Content_Encoding="gzip"),
            "did not decompress",
        )

    def test_a_gzip_stream_cut_at_a_member_boundary_raises(self) -> None:
        """Decompression alone cannot see this one; the length check can.

        A multi-member gzip stream truncated between members decompresses
        cleanly and yields half the rows -- the same silent shortfall as the
        identity case, wearing the encoding that was supposed to be safe.
        """
        first = gzip.compress(_ndjson(_rows(_DELIVERED_ROWS)))
        second = gzip.compress(_ndjson(_rows(_TOTAL_ROWS)[_DELIVERED_ROWS:]))
        _expect_refusal(
            _with_content_length(first, declared=len(first + second), Content_Encoding="gzip"),
            r"declared \d+ bytes",
        )


class TestCompleteBodiesAreUnaffected:
    """Every framing the producer is allowed to accept still round-trips."""

    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(lambda body: _with_content_length(body), id="identity-content-length"),
            pytest.param(
                lambda body: _with_content_length(gzip.compress(body), Content_Encoding="gzip"),
                id="gzip-content-length",
            ),
            pytest.param(
                lambda body: _chunked(gzip.compress(body), terminate=True, Content_Encoding="gzip"),
                id="gzip-chunked",
            ),
        ],
    )
    def test_all_rows_and_the_watermark_come_back(self, build: Callable[[bytes], bytes]) -> None:
        expected = _rows(_TOTAL_ROWS)
        rows, watermark = _fetch(build(_ndjson(expected)))
        assert rows == [_mapper(row) for row in expected]
        assert watermark == _LAST_MODIFIED

    def test_an_empty_identity_body_is_not_mistaken_for_a_short_read(self) -> None:
        """Zero rows is the *caller's* error to raise, and it already does."""
        assert _fetch(_with_content_length(b"")) == ([], _LAST_MODIFIED)
