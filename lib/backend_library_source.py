"""Build a daily-sync-shaped ``library.db`` from Backend-Service over HTTP.

Extracted verbatim from ``scripts/catalog_parity_diff.py``, where this producer
was reachable only as one half of a parity diff. WXYC/discogs-etl#346 retires
the tubafrenzy MySQL catalog source, after which this is *the* daily
``library.db`` build rather than a comparison baseline, so it needs an entry
point that does not require a second database to diff against.

The public entry point is :func:`build_library_db_from_backend`. The private
spellings are preserved and re-exported by ``catalog_parity_diff.py`` so that
harness's tests continue to exercise this code unchanged.

Credentials come from ``$BACKEND_CATALOG_TOKEN``, or the
``$BACKEND_CATALOG_EMAIL`` / ``$BACKEND_CATALOG_PASSWORD`` pair.
"""

from __future__ import annotations

import base64
import gzip
import io
import json
import logging
import os
import time
import zlib
from collections.abc import Callable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from lib.catalog_source_common import (
    SourceError,
    _build_into,
    _report,
    _require_absent,
    _Row,
)
from lib.library_db import CROSS_REFERENCE_SEPARATOR

logger = logging.getLogger(__name__)


# Env var holding a pre-minted Backend-Service service-account JWT. Decision
# D3 (Option B) deliberately picked an HTTP/contract-governed producer over a
# direct-Postgres one precisely so that no prod-DB credential has to exist in
# GitHub Actions.
#
# This is the *manual* route: a JWT from better-auth lives 15 minutes (the
# plugin default, which Backend-Service does not override), so it suits an
# operator running a one-off with a token already in hand and cannot be a
# stored CI secret for a soak measured in days. Unattended runs use the
# credential pair below and mint per run.
BACKEND_TOKEN_ENV = "BACKEND_CATALOG_TOKEN"


# The service account's own credentials (#365) -- what CI actually stores.
# `catalog-parity@wxyc.invalid` holds the `member` org role, the least-privileged
# role carrying `catalog:read`.
BACKEND_EMAIL_ENV = "BACKEND_CATALOG_EMAIL"


BACKEND_PASSWORD_ENV = "BACKEND_CATALOG_PASSWORD"


# Optional overrides. The auth base URL defaults to `<--backend-source>/auth`,
# which is where prod serves better-auth (https://api.wxyc.org/auth), so a
# normal invocation names one URL rather than two.
BACKEND_AUTH_URL_ENV = "BACKEND_AUTH_URL"


BACKEND_AUTH_ORIGIN_ENV = "BACKEND_AUTH_ORIGIN"


# better-auth's CSRF guard rejects a sign-in with no Origin header
# (MISSING_OR_NULL_ORIGIN) -- which is every non-browser caller, this one
# included. The value has to be one of the auth server's
# BETTER_AUTH_TRUSTED_ORIGINS; dj-site's is the one every headless WXYC
# client sends (wxyc-canary does the same).
_DEFAULT_AUTH_ORIGIN = "https://dj.wxyc.org"


_SIGN_IN_PATH = "/sign-in/email"


_EXCHANGE_PATH = "/token"


_SIGN_OUT_PATH = "/sign-out"


# Cleanup budget. Deliberately not _HTTP_TIMEOUT_SECONDS: the export is
# already done by the time this runs, and a hung revocation must not hold a
# finished run open for five minutes.
_SIGN_OUT_TIMEOUT_SECONDS = 30


# better-auth's default JWT life, which Backend-Service does not override.
# Only a fallback: it schedules the next refresh when a token's own `exp` is
# unreadable, so a claim-format change upstream costs one assumption rather
# than an exchange per call.
_ASSUMED_JWT_TTL_SECONDS = 900


# Sign-ins per process. Two: the first, plus one recovery for a session that
# turns out to be dead. The refresh path re-exchanges instead (see
# `_TokenSource`), so a long run does not spend these -- and a credential that
# is simply wrong fails after two rather than hammering a rate limiter shared
# with every DJ logging in. Counted per sign-in, not per HTTP attempt: the
# 429 retry below belongs to the sign-in that provoked it.
_MAX_SIGN_INS = 2


# Attempts per exchange: one against the cached session, and -- if that session
# turns out to be dead -- one against a fresh one. Deliberately its own budget
# rather than a share of _MAX_SIGN_INS, because an exhausted sign-in allowance
# must not disable refreshing against a session that still works. That
# conflation is what made a long soak run unable to refresh at all.
_MAX_EXCHANGE_ATTEMPTS = 2


# One retry on a rate-limited sign-in, waiting at most this long. Two limiters
# sit in front of that path: the express one (10 per 15 min, draft-7
# `Retry-After`) and better-auth's own (3 per 10s, `X-Retry-After`). The cap
# has to be able to clear the shorter window, so it is 10s and not
# wxyc-canary's 5s. A hint longer than this -- or missing entirely -- is not
# waited out; see `_sign_in`.
_SIGN_IN_RETRY_CAP_SECONDS = 10


_SIGN_IN_RETRY_FLOOR_SECONDS = 1


_CATALOG_PATH = "/library/catalog"


_COMPILATION_TRACKS_PATH = "/library/catalog/compilation-tracks"


# The full catalog is ~2.6 MB gzipped and the CTA export ~3.2 MB (measured
# against the 2026-07-19 prod snapshot, api.yaml BS#1965 notes), both served
# from a per-watermark in-memory cache -- but a cold cache has to build the
# whole body first, so the budget is generous.
_HTTP_TIMEOUT_SECONDS = 300


# Refresh a JWT this far before its `exp` rather than after. A fetch can run
# for the whole timeout above, so a token with less life than that left cannot
# safely start one: it is "expired" for our purposes while the clock still
# says otherwise. The 401 retry would recover, but at the price of re-fetching
# an entire export.
_JWT_REFRESH_MARGIN_SECONDS = _HTTP_TIMEOUT_SECONDS


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
                "it crosses origins, and urllib would replay the parity service-account "
                f"credentials to the new host -- the bearer token on a catalog fetch, the "
                f"${BACKEND_PASSWORD_ENV} sign-in password on a mint. Point --backend-source "
                f"(or ${BACKEND_AUTH_URL_ENV}) at the origin that actually serves it."
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


# Module-level opener so every catalog fetch goes through the redirect guard
# above; the bare `urllib.request.urlopen` would use the permissive default.
_opener = build_opener(_SameOriginRedirectHandler())


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


def _resolve_https_base_url(source: str, *, source_label: str, secret_description: str) -> str:
    """Validate and normalize a base URL that a secret is about to travel to.

    Two URLs now carry a secret -- the catalog exports (a bearer token) and
    the auth service (the sign-in password) -- and they are named by different
    things and protect different secrets, so the caller supplies both labels.
    A message that says "bearer token" when the password is what's at risk
    sends the operator to the wrong env var.
    """
    parts = urlsplit(source)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise SourceError(
            f"{source_label} must be an http(s) base URL "
            f"(e.g. https://api.wxyc.org), got {source!r}"
        )
    if parts.scheme == "http" and (parts.hostname or "") not in _PLAINTEXT_OK_HOSTS:
        raise SourceError(
            f"refusing to send {secret_description} over plaintext http "
            f"to {parts.hostname!r} (from {source_label}) -- use https (plain http is "
            "allowed only for a loopback address, for local testing)"
        )
    return f"{parts.scheme}://{parts.netloc}{parts.path.rstrip('/')}"


def _resolve_backend_base_url(source: str) -> str:
    """Validate and normalize the Backend base URL."""
    return _resolve_https_base_url(
        source,
        source_label="--backend-source",
        secret_description=(
            f"the service-account bearer token (${BACKEND_TOKEN_ENV}, or one minted "
            f"from ${BACKEND_EMAIL_ENV})"
        ),
    )


def _default_auth_url(base_url: str) -> str:
    """Where better-auth lives relative to the Backend base URL.

    Production serves it at https://api.wxyc.org/auth, i.e. the same origin as
    the catalog exports -- so the common case needs no second flag, and
    ``$BACKEND_AUTH_URL`` exists for the environments where that isn't true.
    """
    return f"{base_url.rstrip('/')}/auth"


class _AuthStatusError(Exception):
    """An auth-service call that came back with a status, not a token.

    Internal to the mint path: `_TokenSource` turns each of these into either
    a retry (429 on sign-in, 401 on an exchange) or a ``SourceError``. It
    carries the response headers so the 429 branch can honor a retry hint.
    """

    def __init__(self, code: int, detail: str, headers: Any) -> None:
        super().__init__(f"HTTP {code}: {detail}")
        self.code = code
        self.detail = detail
        self.headers = headers

    def retry_hint_seconds(self) -> float | None:
        """The server's own wait hint, or None when it did not give a usable one.

        The express limiter sends draft-7 ``Retry-After``; better-auth's own
        limiter sends ``X-Retry-After`` and nothing else. Reading only one of
        the two means ignoring the hint from half the limiters that can
        produce this response.

        Returned unclamped, because the caller has to be able to tell a hint
        it can wait out from one it cannot: silently truncating a 15-minute
        window to a 10-second sleep buys a certain second refusal.
        """
        for header in ("Retry-After", "X-Retry-After"):
            raw = self.headers.get(header) if self.headers is not None else None
            if raw is None:
                continue
            try:
                return max(float(str(raw).strip()), 0.0)
            except ValueError:
                continue  # HTTP-date form: treat as "no usable hint"
        return None


def _jwt_expiry_epoch(token: str) -> float:
    """The ``exp`` claim, or the assumed life when it cannot be read.

    A local, signature-unverified decode: this only schedules the *next*
    refresh, and the authority on whether a token is good remains the 401 from
    Backend-Service. An unreadable claim therefore falls back to better-auth's
    documented 15 minutes rather than to "expired" -- the latter would make
    every single ``token()`` call an exchange, six-plus per run, against a
    limiter that allows three every ten seconds.
    """
    try:
        segment = token.split(".")[1]
        padded = segment + "=" * (-len(segment) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
        return float(claims["exp"])
    except Exception:  # noqa: BLE001 - any malformed shape falls back
        return time.time() + _ASSUMED_JWT_TTL_SECONDS


class _TokenSource:
    """Supplies a currently-valid bearer for the catalog exports.

    Two modes:

    - **Static** (``$BACKEND_CATALOG_TOKEN``): hand back what the operator
      supplied. A 401 is terminal -- there is nothing here to refresh from,
      and saying so beats retrying a token that cannot improve.
    - **Credential** (``$BACKEND_CATALOG_EMAIL`` + ``$BACKEND_CATALOG_PASSWORD``):
      sign in once, then exchange that session for a 15-minute JWT as often
      as needed.

    The asymmetry between those two cached values is the whole design, and it
    is dictated by where the rate limiters sit. ``/auth/sign-in`` is capped at
    10 per 15 minutes (express, keyed on the caller's IP) and 3 per 10 seconds
    (better-auth's own); ``/auth/token`` is exempt from the first and
    generously bounded by the second. A parity run can outlive 15 minutes --
    three snapshot attempts over two exports, each with a 300-second budget --
    so refreshes must be exchanges, not sign-ins. The session behind them is
    good for a year server-side, so one sign-in covers the whole run, and only
    an exchange that 401s spends another.
    """

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url
        self._static = os.environ.get(BACKEND_TOKEN_ENV) or None
        self._email = os.environ.get(BACKEND_EMAIL_ENV) or None
        self._password = os.environ.get(BACKEND_PASSWORD_ENV) or None
        self._session: str | None = None
        self._jwt: str | None = None
        self._jwt_expires_at = 0.0
        self._sign_ins = 0
        self._auth_url = ""
        self._origin = ""

        if self._static:
            # Credentials, when they are also set, are this token's fallback
            # rather than dead weight -- but resolving the auth URL now would
            # refuse a plaintext one the run may never touch.
            return
        self._enter_credential_mode()

    def _enter_credential_mode(self) -> None:
        """Validate the credential pair and pin the URL the password goes to."""
        if not (self._email and self._password):
            raise SourceError(
                "no Backend service-account credentials: set "
                f"${BACKEND_EMAIL_ENV} + ${BACKEND_PASSWORD_ENV} (the unattended route -- "
                "the harness signs in and mints a fresh token per run), or "
                f"${BACKEND_TOKEN_ENV} with a JWT you already hold (a one-off; better-auth "
                "JWTs expire after 15 minutes). Either way the principal needs catalog:read."
            )
        self._auth_url = _resolve_https_base_url(
            os.environ.get(BACKEND_AUTH_URL_ENV) or _default_auth_url(self._base_url),
            source_label=f"${BACKEND_AUTH_URL_ENV}",
            secret_description=f"the ${BACKEND_PASSWORD_ENV} sign-in password",
        )
        self._origin = os.environ.get(BACKEND_AUTH_ORIGIN_ENV) or _DEFAULT_AUTH_ORIGIN
        self._static = None

    def token(self) -> str:
        """A bearer valid now -- minting or refreshing if it isn't."""
        if self._static:
            return self._static
        if self._jwt and time.time() < self._jwt_expires_at - _JWT_REFRESH_MARGIN_SECONDS:
            return self._jwt
        self._jwt = self._exchange()
        self._jwt_expires_at = _jwt_expiry_epoch(self._jwt)
        return self._jwt

    def invalidate(self) -> None:
        """Discard the cached JWT after Backend-Service rejected it.

        Drops the *JWT* only. The session it came from is almost certainly
        still good -- a 15-minute token against a year-long session -- and
        re-signing-in here is what would put a long run on a collision course
        with the sign-in limiter.
        """
        if self._static:
            if not (self._email and self._password):
                raise SourceError(
                    f"the ${BACKEND_TOKEN_ENV} token was rejected (401) and cannot be "
                    "refreshed: better-auth JWTs expire after 15 minutes. Mint a fresh one, "
                    f"or set ${BACKEND_EMAIL_ENV} + ${BACKEND_PASSWORD_ENV} so the harness "
                    "can mint per run -- which is what an unattended soak needs."
                )
            # Both are set, which is what an unattended run inherits if the
            # secret #365 originally specified is left in place. Stranding it
            # on a 15-minute-old JWT -- while holding everything needed to
            # mint a fresh one -- would be a self-inflicted outage.
            logger.warning(
                "the pre-minted token was rejected; falling back to the credential pair",
                extra={"step": "backend_producer"},
            )
            _report(
                f"WARNING: ${BACKEND_TOKEN_ENV} was rejected (401); minting from "
                f"${BACKEND_EMAIL_ENV} instead"
            )
            self._enter_credential_mode()
            return
        self._jwt = None
        self._jwt_expires_at = 0.0

    def close(self) -> None:
        """Revoke the session this run minted, if it minted one.

        Backend-Service pins ``session.expiresIn`` to a year, so a session
        left behind is a standalone catalog:read credential with a year to
        run -- and ``admin/set-user-password`` revokes nothing, so a password
        rotation would not clear it. Unattended runs would accumulate one per
        run.

        Best-effort by design: the export has already happened, and the worst
        case of a failed revocation is the state every run had before this
        existed. A token supplied through ``$BACKEND_CATALOG_TOKEN`` is not
        this run's to revoke and is left alone.
        """
        session, self._session = self._session, None
        self._jwt = None
        self._jwt_expires_at = 0.0
        if not session:
            return
        url = self._auth_url + _SIGN_OUT_PATH
        request = Request(
            url,
            data=b"{}",
            headers={
                "Authorization": f"Bearer {session}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Origin": self._origin,
            },
        )
        try:
            with _opener.open(request, timeout=_SIGN_OUT_TIMEOUT_SECONDS):
                pass
        except Exception as exc:  # noqa: BLE001 - see below: nothing may escape here
            # Deliberately every exception, not just OSError. This runs from a
            # `finally`, where anything raised *replaces* the exception in
            # flight -- so a redirect on the cleanup call (SourceError, a
            # RuntimeError) would overwrite the torn-snapshot or empty-export
            # diagnosis the operator actually needs, and on the success path
            # would fail a build that had already finished.
            #
            # Named loudly: the leftover session outlives this process by a
            # year, so an operator who sees this may want to revoke it by hand
            # (POST /auth/admin/revoke-user-sessions).
            logger.warning(
                "could not sign out the parity session; it stays valid server-side",
                extra={"step": "backend_producer", "error": str(exc)},
            )
            _report(f"WARNING: sign-out at {url} failed ({exc}); the session was not revoked")

    def _exchange(self) -> str:
        """Trade the cached session for a JWT, re-signing-in at most once.

        Bounded by its own attempt budget. Gating this loop on the sign-in
        allowance instead would mean that once a run had spent that allowance
        -- on a rate-limited first sign-in, or on one legitimate dead-session
        recovery -- every later refresh would raise without issuing a single
        exchange, against a session that was still perfectly good.
        """
        url = self._auth_url + _EXCHANGE_PATH
        last: _AuthStatusError | None = None
        for _attempt in range(_MAX_EXCHANGE_ATTEMPTS):
            session = self._session or self._sign_in()
            request = Request(
                url,
                headers={
                    "Authorization": f"Bearer {session}",
                    "Origin": self._origin,
                    "Accept": "application/json",
                },
            )
            try:
                return _auth_token_from(request, "the token exchange")
            except _AuthStatusError as exc:
                if exc.code != 401:
                    raise SourceError(
                        f"the token exchange at {url} failed with HTTP {exc.code}: {exc.detail}"
                    ) from exc
                # The session is dead (rotated, revoked, expired). That is the
                # one condition worth another sign-in.
                last = exc
                self._session = None
        raise SourceError(
            f"the token exchange at {url} returned HTTP 401 on every one of "
            f"{_MAX_EXCHANGE_ATTEMPTS} attempts as ${BACKEND_EMAIL_ENV}: "
            f"{last.detail if last else 'no detail'}. Check that the service account "
            "exists, is not banned, and holds catalog:read."
        )

    def _sign_in(self) -> str:
        """Exchange the password for a session token. One retry on a 429.

        Costs one unit of the sign-in allowance however many HTTP attempts it
        takes -- the 429 retry is part of this sign-in, not a second one.
        """
        url = self._auth_url + _SIGN_IN_PATH
        if self._sign_ins >= _MAX_SIGN_INS:
            raise SourceError(
                f"signing in as ${BACKEND_EMAIL_ENV} at {url} already ran "
                f"{self._sign_ins} times this run and the session it returns keeps coming "
                "back dead. Check that the service account exists, is not banned, and that "
                f"${BACKEND_PASSWORD_ENV} is current."
            )
        self._sign_ins += 1
        body = json.dumps({"email": self._email, "password": self._password}).encode("utf-8")
        for attempt in (1, 2):
            request = Request(
                url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    # Absent, better-auth's CSRF guard 400s with
                    # MISSING_OR_NULL_ORIGIN before ever checking the password.
                    "Origin": self._origin,
                },
            )
            try:
                session = _auth_token_from(request, "sign-in")
            except _AuthStatusError as exc:
                if exc.code == 429 and attempt == 1:
                    hint = exc.retry_hint_seconds()
                    if hint is None:
                        # Both limiters in front of this path send a hint, so
                        # a 429 without one came from something else (a proxy,
                        # a WAF) whose window we cannot guess. Retrying blind
                        # into what may be the express limiter's 15 minutes
                        # spends a second slot from a budget shared with every
                        # DJ signing in, and learns nothing.
                        raise SourceError(
                            f"sign-in as ${BACKEND_EMAIL_ENV} at {url} was rate limited "
                            "(HTTP 429) with no usable Retry-After/X-Retry-After hint, so there "
                            "is no way to tell a "
                            "10-second window from a 15-minute one. Re-run later, and check "
                            "whether something else is signing in as this service account."
                        ) from exc
                    if hint > _SIGN_IN_RETRY_CAP_SECONDS:
                        # The express limiter's window is 15 minutes. Sleeping
                        # the cap and retrying into a window this long is a
                        # guaranteed second refusal; the operator wants the
                        # real number, not a truncated one.
                        raise SourceError(
                            f"sign-in as ${BACKEND_EMAIL_ENV} at {url} was rate limited "
                            f"(HTTP 429) and asks for {hint:g}s, longer than the "
                            f"{_SIGN_IN_RETRY_CAP_SECONDS}s "
                            "this run will wait. Re-run after that window, and check whether "
                            "something else is signing in as this service account."
                        ) from exc
                    # A floor, because `Retry-After: 0` is not an invitation to
                    # retry inside the same tick of whatever window refused us.
                    delay = max(hint, _SIGN_IN_RETRY_FLOOR_SECONDS)
                    logger.warning(
                        "sign-in was rate limited; retrying once",
                        extra={"step": "backend_producer", "delay_seconds": delay},
                    )
                    time.sleep(delay)
                    continue
                raise SourceError(
                    f"sign-in as ${BACKEND_EMAIL_ENV} at {url} failed with "
                    f"HTTP {exc.code}: {exc.detail}"
                ) from exc
            self._session = session
            return session
        raise AssertionError("unreachable: the retry loop either returns or raises")


def _auth_token_from(request: Request, what: str) -> str:
    """Run one auth-service call and return its ``token`` field.

    Both auth responses are ``{"token": ...}`` -- a session from sign-in, a JWT
    from the exchange. Non-2xx bodies are surfaced (truncated) because they
    carry a diagnosis and never a credential; 2xx bodies are *not*, because
    they carry exactly the credential this whole path exists to protect.
    """
    try:
        with _opener.open(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
            body = response.read()
    except SourceError:
        raise  # the cross-origin redirect guard
    except HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", "replace")[:200].replace("\n", " ").strip()
        except Exception:  # noqa: BLE001 - a body we cannot read is not the failure
            pass
        raise _AuthStatusError(exc.code, detail, exc.headers) from exc
    except (URLError, OSError) as exc:
        raise SourceError(f"{what} at {request.full_url} failed: {exc}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SourceError(f"{what} at {request.full_url} returned a non-JSON body") from exc
    token = payload.get("token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token:
        raise SourceError(
            f"{what} at {request.full_url} returned 200 with no token field; "
            "the auth response shape has changed"
        )
    return token


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


class _CountedBody(io.RawIOBase):
    """A response body that remembers how many bytes it actually delivered.

    ``http.client`` will not tell us. ``HTTPResponse.readinto`` -- the path a
    line-by-line read takes -- closes the connection on a short body and
    returns, with a standing comment in the stdlib saying that raising
    ``IncompleteRead`` there "might break compatibility". Only ``read()`` of
    the whole body raises. So the byte count has to be kept here and compared
    against ``Content-Length`` by the caller.
    """

    def __init__(self, response: Any) -> None:
        self._response = response
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        count = self._response.readinto(buffer) or 0
        self.bytes_read += count
        return count


def _declared_length(response: Any) -> int | None:
    """``Content-Length`` as an int, or None if absent or unparseable."""
    raw = response.headers.get("Content-Length")
    if raw is None:
        return None
    try:
        return int(raw.strip())
    except (AttributeError, ValueError):
        return None


def _fetch_ndjson(
    url: str, token_source: _TokenSource, mapper: Callable[[dict[str, Any]], _Row]
) -> tuple[list[_Row], str]:
    """GET a gzipped-NDJSON export; return its mapped rows and the watermark.

    Retries exactly once on a 401, after asking ``token_source`` for a fresh
    bearer: a 15-minute JWT can expire between the two exports of a snapshot
    pair, or partway through a re-fetch, and re-reading the catalog is far
    cheaper than failing a parity day over a token turnover. Once, not in a
    loop -- a second 401 is an authorization problem, not an expiry, and
    should say so instead of retrying until the limiter notices.
    """
    for attempt in (1, 2):
        try:
            return _fetch_ndjson_once(url, token_source.token(), mapper)
        except _AuthStatusError as exc:
            if exc.code == 401 and attempt == 1:
                logger.info(
                    "catalog export returned 401; refreshing the service-account token",
                    extra={"step": "backend_producer", "url": url},
                )
                token_source.invalidate()
                continue
            raise SourceError(f"failed to fetch {url}: HTTP {exc.code}: {exc.detail}") from exc
    raise AssertionError("unreachable: the retry loop either returns or raises")


def _fetch_ndjson_once(
    url: str, token: str, mapper: Callable[[dict[str, Any]], _Row]
) -> tuple[list[_Row], str]:
    """One export GET. Raises ``_AuthStatusError`` so the caller can refresh."""
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
            declared = _declared_length(response)
            if encoding != "gzip" and declared is None:
                # Neither integrity signal is present, so a body cut on a line
                # boundary is indistinguishable from a complete one -- chunked
                # framing included, and a well-formed terminating 0-chunk
                # after a short body most of all. Refuse rather than build a
                # library.db out of however much arrived.
                raise SourceError(
                    f"{url} answered with neither Content-Encoding: gzip nor a Content-Length, "
                    "so a truncated export could not be told apart from a complete one; "
                    "something in front of Backend-Service is decompressing or re-framing "
                    "the response"
                )
            body = io.BufferedReader(_CountedBody(response))
            stream = gzip.GzipFile(fileobj=body) if encoding == "gzip" else body
            rows = _map_ndjson_lines(url, stream, mapper)
            if declared is not None and body.raw.bytes_read < declared:
                # Catches the identity short read, and also the one truncation
                # gzip cannot see: a multi-member stream cut between members
                # decompresses cleanly and simply ends early.
                raise SourceError(
                    f"{url} delivered {body.raw.bytes_read} of the declared {declared} bytes; "
                    "the export was truncated in transit"
                )
    except SourceError:
        raise
    except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
        raise SourceError(
            f"{url} declared Content-Encoding: gzip but did not decompress: {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise SourceError(f"{url} returned a body that is not valid UTF-8: {exc}") from exc
    except HTTPError as exc:
        # Must precede the URLError clause below -- HTTPError subclasses it, so
        # the generic branch would swallow the status and with it any chance of
        # telling "the token just expired" apart from "the export is broken".
        detail = ""
        try:
            detail = exc.read().decode("utf-8", "replace")[:200].replace("\n", " ").strip()
        except Exception:  # noqa: BLE001 - a body we cannot read is not the failure
            pass
        raise _AuthStatusError(exc.code, detail, exc.headers) from exc
    except (URLError, OSError) as exc:
        raise SourceError(f"failed to fetch {url}: {exc}") from exc
    return rows, watermark


def _fetch_consistent_snapshot(
    base_url: str,
    token_source: _TokenSource,
    *,
    catalog_mapper: Callable[[dict[str, Any]], _Row] = _catalog_row_to_library_row,
    compilation_tracks_required: bool = True,
) -> tuple[list[_Row], list[_Row]]:
    """Fetch both exports, retrying until they describe the same catalog snapshot.

    Returns rows mapped by ``catalog_mapper`` (defaulting to the ``library``
    row shape the general Backend producer builds a library.db from) and by
    the fixed ``_catalog_cta_row_to_library_row`` -- the CTA side stays fixed
    because the dangling-id cross-check below (and every caller) needs
    ``row[0]`` to be ``legacy_release_id`` on a plain subscriptable sequence;
    a caller wanting a different CTA row shape (``--capture-fffd-cta-pairs``,
    WXYC/Backend-Service#2152, wants a ``CtaRow`` dataclass) maps the
    returned tuples afterward rather than this function taking on a second
    varying mapper. ``--capture-fffd-cta-pairs`` passes
    ``catalog_mapper=_catalog_row_to_id_map_entry`` to keep the Backend
    serial id the default mapper discards -- the torn-snapshot retry logic
    and the dangling-id cross-check are unaffected either way, since element
    0 of both catalog-row shapes is ``legacy_release_id``.

    ``compilation_tracks_required=False`` makes a *failed* CTA fetch degrade
    to an empty CTA side instead of propagating. The daily build passes it;
    every other caller leaves it True. The asymmetry is deliberate and it is
    the one the retired MySQL read path had: the catalog export IS the build,
    while compilation tracks are supplementary, so a broken supplementary
    export must not cost production a day of catalog freshness. Note this
    tolerates the *fetch* failing, not a fetched row being malformed --
    ``_catalog_cta_row_to_library_row``'s contract violations still raise,
    because those mean Backend is serving something the schema forbids rather
    than not serving at all.

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
            base_url + _CATALOG_PATH, token_source, catalog_mapper
        )
        try:
            cta_rows, cta_watermark = _fetch_ndjson(
                base_url + _COMPILATION_TRACKS_PATH, token_source, _catalog_cta_row_to_library_row
            )
        except SourceError as exc:
            if compilation_tracks_required:
                raise
            # Return rather than continue: the retry loop above exists to
            # resolve a *torn* pair, and an export that will not answer is not
            # a tear -- there is no second side left to disagree with the
            # catalog. Looping would cost Backend two more full catalog
            # exports to arrive at exactly this answer.
            logger.warning(
                "the compilation-track export could not be fetched; "
                "building without the compilation_track_artist table",
                extra={"step": "backend_producer", "source": base_url, "error": str(exc)},
            )
            _report(
                f"WARNING: {base_url}{_COMPILATION_TRACKS_PATH} could not be fetched "
                f"({exc}); building without a compilation_track_artist table"
            )
            return catalog_rows, []

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
            cross-origin redirect, missing credentials (neither
            ``$BACKEND_CATALOG_TOKEN`` nor the ``$BACKEND_CATALOG_EMAIL`` /
            ``$BACKEND_CATALOG_PASSWORD`` pair), a sign-in or token-exchange
            failure, a catalog fetch or decode failure, a torn snapshot, an
            empty catalog, or a row that violates the api.yaml contract. A
            failed *compilation-track* fetch is the one exception: it degrades
            to a build without that table, because the daily sync must not
            lose a day of catalog freshness to a broken supplementary export.
    """
    _require_absent(output_path, "backend")
    base_url = _resolve_backend_base_url(source)
    token_source = _TokenSource(base_url)
    try:
        _build_from_backend_snapshot(base_url, output_path, token_source)
    finally:
        # The failure path is the one that would leak most: a run that dies
        # mid-export has still minted a year-long session.
        token_source.close()


def _build_from_backend_snapshot(
    base_url: str, output_path: str, token_source: _TokenSource
) -> None:
    """Fetch a consistent snapshot and write it, with the token source live."""
    library_rows, compilation_rows = _fetch_consistent_snapshot(
        base_url, token_source, compilation_tracks_required=False
    )
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
        #
        # Reached by two different routes now: an export that answered with no
        # rows, and one that could not be fetched at all (which logs its own
        # HTTP detail above, since only that site knows the difference). The
        # wording below covers both rather than asserting which one happened.
        logger.warning(
            "no compilation-track rows; building without the table",
            extra={"step": "backend_producer", "source": base_url},
        )
        _report(
            f"WARNING: no rows from {base_url}{_COMPILATION_TRACKS_PATH}; "
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


# Public entry point. The private spelling above is what the parity harness
# and its tests already call; this alias is the name new callers use.
build_library_db_from_backend = _build_library_db_from_backend
