"""Single canonical redactor for PostgreSQL connection strings.

Every script in this repo that logs which database it is about to talk to
routes through :func:`redact_dsn`. The pipeline log is teed to the S3 rebuild
log bucket by ``rebuild-cache-bootstrap.sh``, so a connection string echoed on
the happy path publishes the cache credential (#361, #227).

Redaction must *parse*, not pattern-match. Three call sites had independently
grown ``db_url.split("@")[-1]``, which silently assumes the URL DSN form.
libpq also accepts keyword/value conninfo --
``host=cache-host dbname=discogs user=svc password=hunter2`` -- which
``psycopg.connect()`` accepts and which contains no ``@`` at all, so that
expression returned the entire string, password included. It failed open on
exactly the input a shell-set ``DATABASE_URL_DISCOGS`` is most likely to hold
when someone hand-writes one.
"""

from __future__ import annotations

from psycopg.conninfo import conninfo_to_dict

#: Returned when the DSN cannot be parsed. Deliberately not the input string:
#: a DSN we failed to understand may still be a credentialed one.
UNPARSEABLE = "<unparseable target>"


def redact_dsn(db_url: str) -> str:
    """Return a loggable ``host[:port]/dbname`` for *db_url*, never credentials.

    Uses psycopg's conninfo parser so both DSN forms libpq accepts are
    handled: URL (``postgresql://user:pw@host:5433/discogs``) and keyword/value
    (``host=host port=5433 dbname=discogs user=user password=pw``). Only the
    host, port, and database name are ever read out of the parsed mapping, so
    a credential can never reach the returned string regardless of which
    conninfo key it arrived under.

    Missing components degrade to descriptive placeholders (``<local socket>``,
    ``<default db>``) rather than being omitted, so the shape of the log line
    stays stable. A parse failure degrades to :data:`UNPARSEABLE` rather than
    risking echoing the raw string or raising -- the caller is on its way to
    ``connect()``, which will produce a far clearer error a moment later.

    Args:
        db_url: A libpq connection string in either supported form.

    Returns:
        ``host[:port]/dbname`` with placeholders substituted for absent
        components, or :data:`UNPARSEABLE` if *db_url* could not be parsed.
    """
    try:
        info = conninfo_to_dict(db_url)
    except Exception:
        return UNPARSEABLE
    host = info.get("host") or "<local socket>"
    port = f":{info['port']}" if info.get("port") else ""
    dbname = info.get("dbname") or "<default db>"
    return f"{host}{port}/{dbname}"
