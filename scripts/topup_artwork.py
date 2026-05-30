"""LML#221: one-shot top-up drain for releases lacking ``artwork_url``.

``release.artwork_url`` is populated by ``scripts/import_csv.py:import_artwork``
from the monthly Discogs XML dump's ``release_image.csv``. Releases with no
``release_image.csv`` entry (including those that had images uploaded to
Discogs *after* the dump cut-off) end up NULL.

Migration 0008 added ``release.artwork_checked_at`` plus the partial index
``release_artwork_null_idx ON release(id) WHERE artwork_url IS NULL AND
artwork_checked_at IS NULL`` so the never-asked tail can be scanned without
a full sequential read. This script walks that tail, asks Discogs
``GET /releases/{id}``, and writes back ``(artwork_url, artwork_checked_at)``
— stamping ``artwork_checked_at`` even when no image exists collapses the
"never asked vs asked-but-imageless" ambiguity at the schema level.

Auth
----

Accepts either Discogs auth shape (matches LML's ``DiscogsService``):

* Personal access token — ``DISCOGS_TOKEN`` (or legacy ``DISCOGS_API_TOKEN``).
* OAuth consumer pair — ``DISCOGS_API_KEY`` + ``DISCOGS_API_SECRET``.

If both are exported the token wins. The WXYC shared secrets file ships the
OAuth-pair shape; LML's runtime env on Railway carries the personal token.
Using a different shape from LML's runtime gives the drain its own Discogs
identity (and therefore its own rate-limit quota), which is the design point
behind the conservative default rate below.

Pacing
------

LML's runtime caller pulls against Discogs's ~60/min ceiling per token. The
default ``--rate 10`` rate keeps headroom under LML's running 50/min when
this drain shares an identity, and is over-conservative when it doesn't.
Off-hours operators can raise it.

Idempotence
-----------

The candidate query uses the partial index predicate. Re-running picks up
only still-NULL-both rows. Failed Discogs calls (network, 429, 5xx) leave
the row untouched so the next invocation retries it.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

import psycopg

from lib.observability import init_logger

logger = logging.getLogger(__name__)

DEFAULT_RATE_PER_MINUTE = 10
DEFAULT_BATCH_SIZE = 100
DEFAULT_LIMIT = 1000
DEFAULT_USER_AGENT = "wxyc-discogs-etl-topup/0.1 (+https://github.com/WXYC/discogs-etl)"
DISCOGS_API_BASE = "https://api.discogs.com"
HTTP_TIMEOUT_S = 20.0


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def extract_artwork_uri(release_json: dict[str, Any]) -> str | None:
    """Return ``images[0].uri`` if present, else ``None``.

    Mirrors the extraction shape at
    ``library-metadata-lookup/discogs/service.py`` so a release that LML
    would consider imageless is also imageless here. ``None`` is a valid
    drain outcome: the row gets ``artwork_checked_at`` stamped without
    ``artwork_url`` being filled.
    """
    images = release_json.get("images") or []
    if not images:
        return None
    return images[0].get("uri") or None


class TokenBucket:
    """Sleep-based pacing: one emission per ``60 / rate_per_minute`` seconds.

    ``now_fn`` + ``sleep_fn`` are injectable so unit tests can exercise the
    pacing without sleeping wall-clock time.
    """

    def __init__(self, rate_per_minute: int) -> None:
        if rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be > 0")
        self.interval_s = 60.0 / rate_per_minute
        self.last_emit: float | None = None

    def acquire(
        self,
        now_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        now = now_fn()
        if self.last_emit is None:
            self.last_emit = now
            return
        wait = (self.last_emit + self.interval_s) - now
        if wait > 0:
            sleep_fn(wait)
            self.last_emit = now_fn()
        else:
            self.last_emit = now


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class TopupSummary:
    candidates: int = 0  # rows the SELECT returned
    fetched: int = 0  # Discogs calls that returned 200
    with_artwork: int = 0  # fetched and had at least one image
    without_artwork: int = 0  # fetched and the release legitimately has no images
    deleted: int = 0  # 404 from Discogs (release was removed)
    failed: int = 0  # network / 5xx / persistent 429 — row left untouched
    updated: int = 0  # rows we wrote back to PG


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def fetch_pending_ids(conn: psycopg.Connection, limit: int) -> list[int]:
    """Read release IDs needing the artwork check.

    The ORDER BY id + LIMIT pattern lets the partial index serve the
    drain as a bounded sequential walk; stable order also makes resumed
    runs predictable.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
              FROM release
             WHERE artwork_url IS NULL
               AND artwork_checked_at IS NULL
             ORDER BY id
             LIMIT %s
            """,
            (limit,),
        )
        return [row[0] for row in cur.fetchall()]


def write_artwork_result(
    conn: psycopg.Connection, release_id: int, artwork_url: str | None
) -> None:
    """Stamp ``artwork_checked_at`` and (optionally) set ``artwork_url``.

    A NULL ``artwork_url`` is meaningful: it records "asked Discogs, no
    image" so LML can treat the row as a full cache hit per migration 0008.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE release
               SET artwork_url = %s,
                   artwork_checked_at = now()
             WHERE id = %s
            """,
            (artwork_url, release_id),
        )


# ---------------------------------------------------------------------------
# Discogs client
# ---------------------------------------------------------------------------


def _build_auth_header(
    token: str | None,
    api_key: str | None,
    api_secret: str | None,
) -> str:
    """Return the ``Authorization`` value for the Discogs API.

    Mirrors LML's two-mode selector at
    ``library-metadata-lookup/discogs/service.py:254``. ``token`` takes
    precedence over the OAuth pair when both are supplied — matches LML
    and lets an operator who exports both shapes pick the simpler one
    without surprises.
    """
    if token:
        return f"Discogs token={token}"
    if api_key and api_secret:
        return f"Discogs key={api_key}, secret={api_secret}"
    if api_key or api_secret:
        raise ValueError("OAuth-pair auth requires both api_key and api_secret; got only one")
    raise ValueError("Provide either token or api_key + api_secret")


def make_discogs_client(
    token: str | None = None,
    *,
    api_key: str | None = None,
    api_secret: str | None = None,
    user_agent: str = DEFAULT_USER_AGENT,
    timeout_s: float = HTTP_TIMEOUT_S,
) -> Callable[[int], dict[str, Any] | None]:
    """Return ``fetch(release_id) -> dict | None``.

    Auth modes (mutually exclusive, ``token`` wins when both supplied):
        * Personal access token: ``make_discogs_client(token="abc")``
        * OAuth consumer pair: ``make_discogs_client(api_key="k", api_secret="s")``

    Return value contract:
        * ``dict`` — Discogs returned 200 with JSON.
        * ``None`` — Discogs returned 404 (release withdrawn). Caller
          stamps ``artwork_url=NULL`` + ``artwork_checked_at=now()``.
        * Raises on transient errors so the orchestrator can record a
          ``failed`` count without writing back.
    """
    auth_header = _build_auth_header(token, api_key, api_secret)

    def fetch(release_id: int) -> dict[str, Any] | None:
        url = f"{DISCOGS_API_BASE}/releases/{release_id}"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": auth_header,
                "User-Agent": user_agent,
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    return fetch


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_topup(
    db_url: str,
    *,
    limit: int,
    rate_per_minute: int,
    batch_size: int,
    dry_run: bool,
    discogs_client: Callable[[int], dict[str, Any] | None],
    bucket: TokenBucket | None = None,
) -> TopupSummary:
    """Drive the drain end-to-end and return per-bucket counts.

    Caller owns the Discogs client (so tests inject a fake) and the rate
    bucket (so off-hours runs can swap in a faster one). When the bucket
    is omitted, a fresh one is built from ``rate_per_minute``.
    """
    summary = TopupSummary()
    bucket = bucket or TokenBucket(rate_per_minute)

    with psycopg.connect(db_url) as conn:
        candidates = fetch_pending_ids(conn, limit)
        summary.candidates = len(candidates)
        logger.info(
            "drain candidates selected",
            extra={"step": "topup", "candidates": summary.candidates, "limit": limit},
        )

        pending_writes: list[tuple[int, str | None]] = []

        for release_id in candidates:
            bucket.acquire()
            try:
                payload = discogs_client(release_id)
            except Exception as exc:
                summary.failed += 1
                logger.warning(
                    "discogs fetch failed",
                    extra={"step": "topup", "release_id": release_id, "error": str(exc)},
                )
                continue

            if payload is None:
                summary.deleted += 1
                pending_writes.append((release_id, None))
                continue

            summary.fetched += 1
            uri = extract_artwork_uri(payload)
            if uri:
                summary.with_artwork += 1
            else:
                summary.without_artwork += 1
            pending_writes.append((release_id, uri))

            if len(pending_writes) >= batch_size:
                _flush(conn, pending_writes, summary, dry_run=dry_run)
                pending_writes.clear()

        if pending_writes:
            _flush(conn, pending_writes, summary, dry_run=dry_run)

    return summary


def _flush(
    conn: psycopg.Connection,
    pending: list[tuple[int, str | None]],
    summary: TopupSummary,
    *,
    dry_run: bool,
) -> None:
    if dry_run:
        logger.info("dry-run skipping writes", extra={"step": "topup", "deferred": len(pending)})
        return
    for release_id, uri in pending:
        write_artwork_result(conn, release_id, uri)
        summary.updated += 1
    conn.commit()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL"),
        help="Cache DB URL. Defaults to $DATABASE_URL_DISCOGS, then $DATABASE_URL.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max releases to process in this invocation (default {DEFAULT_LIMIT}).",
    )
    p.add_argument(
        "--rate",
        type=int,
        default=DEFAULT_RATE_PER_MINUTE,
        help=(
            f"Discogs requests per minute (default {DEFAULT_RATE_PER_MINUTE}). "
            "Stay well under 60 to leave headroom for LML's runtime traffic."
        ),
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per UPDATE commit (default {DEFAULT_BATCH_SIZE}).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and log counts without writing back to the cache.",
    )
    return p


def _credentials_from_env() -> tuple[str | None, str | None, str | None]:
    """Return ``(token, api_key, api_secret)`` from environment.

    Accepts either auth shape supported by ``make_discogs_client``:
      * ``DISCOGS_TOKEN`` (or legacy alias ``DISCOGS_API_TOKEN``) — personal token.
      * ``DISCOGS_API_KEY`` + ``DISCOGS_API_SECRET`` — OAuth consumer pair
        (matches the WXYC secrets-file shape).

    Precedence is concentrated in ``_build_auth_header`` so this helper
    stays a thin env reader: it returns whatever is present and lets the
    factory decide which mode to use.
    """
    token = os.environ.get("DISCOGS_TOKEN") or os.environ.get("DISCOGS_API_TOKEN")
    api_key = os.environ.get("DISCOGS_API_KEY")
    api_secret = os.environ.get("DISCOGS_API_SECRET")
    return token, api_key, api_secret


def main(argv: list[str] | None = None) -> int:
    init_logger(repo="discogs-etl", tool="discogs-etl topup_artwork")
    args = _build_parser().parse_args(argv)

    if not args.database_url:
        logger.error("missing --database-url / $DATABASE_URL_DISCOGS / $DATABASE_URL")
        return 2

    token, api_key, api_secret = _credentials_from_env()
    try:
        client = make_discogs_client(token=token, api_key=api_key, api_secret=api_secret)
    except ValueError as exc:
        logger.error(
            "Discogs credentials missing: %s. "
            "Export $DISCOGS_TOKEN, or $DISCOGS_API_KEY + $DISCOGS_API_SECRET.",
            exc,
        )
        return 2

    summary = run_topup(
        args.database_url,
        limit=args.limit,
        rate_per_minute=args.rate,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        discogs_client=client,
    )
    logger.info(
        "drain complete",
        extra={"step": "topup", **dataclasses.asdict(summary)},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
