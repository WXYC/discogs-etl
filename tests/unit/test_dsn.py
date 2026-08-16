"""Unit tests for lib/dsn.py, the single canonical DSN redactor.

Three scripts had grown their own redactor and two of them failed open. The
naive form -- ``db_url.split("@")[-1]`` -- assumes a URL DSN. libpq also
accepts keyword/value conninfo (``host=... password=...``), which psycopg
connects with happily and which contains no ``@`` at all, so the "redaction"
returned the whole string, password included. These tests pin the parsed
behaviour for every DSN shape the pipeline can be handed.
"""

from __future__ import annotations

import pytest

from lib.dsn import redact_dsn


class TestUrlForm:
    def test_url_form_drops_userinfo(self) -> None:
        assert (
            redact_dsn("postgresql://etluser:hunter2@db.example.com:5433/discogs")
            == "db.example.com:5433/discogs"
        )

    def test_url_form_without_credentials_still_normalizes(self) -> None:
        """The scheme prefix is not useful to an operator and is dropped, so
        credentialed and uncredentialed DSNs log in one identical shape."""
        assert (
            redact_dsn("postgresql://db.example.com:5433/discogs") == "db.example.com:5433/discogs"
        )

    def test_url_form_without_port(self) -> None:
        assert redact_dsn("postgresql://etl:pw@db.example.com/discogs") == "db.example.com/discogs"

    def test_percent_encoded_password_does_not_survive_decoding(self) -> None:
        """A password with a reserved character arrives percent-encoded; the
        conninfo parser decodes it, so the redactor must be reading named
        fields rather than trimming a prefix off the raw string."""
        described = redact_dsn("postgresql://etl:p%40ss@db.example.com:5433/discogs")
        assert "p@ss" not in described
        assert "p%40ss" not in described
        assert described == "db.example.com:5433/discogs"


class TestKeywordValueForm:
    """The regression this module exists to prevent (#361 follow-up)."""

    CONNINFO = "host=cache-host port=5432 dbname=discogs user=svc password=hunter2"

    def test_keyword_value_form_never_leaks_password(self) -> None:
        described = redact_dsn(self.CONNINFO)
        assert "hunter2" not in described
        assert "password" not in described
        assert "svc" not in described
        assert described == "cache-host:5432/discogs"

    def test_naive_split_would_have_leaked(self) -> None:
        """Pins *why* the parsed implementation is required: the string the
        old redactor returned for this input was the input itself."""
        assert "hunter2" in self.CONNINFO.split("@")[-1]

    def test_quoted_password_with_spaces_never_leaks(self) -> None:
        described = redact_dsn("host=cache-host dbname=discogs password='hunter 2'")
        assert "hunter" not in described
        assert described == "cache-host/discogs"


class TestDegradedInputs:
    def test_non_numeric_port_neither_crashes_nor_leaks(self) -> None:
        """urlparse's ``.port`` raises on a non-numeric port before psycopg
        can produce its clearer connect error; conninfo parsing passes it
        through, and the log line must simply never crash or leak."""
        described = redact_dsn("postgresql://etluser:hunter2@host:notaport/db")
        assert "hunter2" not in described
        assert "etluser" not in described

    def test_unparseable_input_degrades_to_placeholder(self) -> None:
        """Never echo a string we failed to parse -- it may be a credentialed
        DSN we simply did not understand."""
        assert redact_dsn("%%not-a-dsn%%") == "<unparseable target>"

    @pytest.mark.parametrize("raw", ["", "   "])
    def test_empty_dsn_describes_the_libpq_defaults(self, raw: str) -> None:
        assert redact_dsn(raw) == "<local socket>/<default db>"

    def test_socket_dsn_without_host(self) -> None:
        assert redact_dsn("postgresql:///discogs") == "<local socket>/discogs"

    def test_host_without_dbname(self) -> None:
        assert redact_dsn("host=cache-host port=5433") == "cache-host:5433/<default db>"
