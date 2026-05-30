"""LML#221 top-up drain — unit tests.

Pure-function coverage for the artwork-extract shape and the TokenBucket
pacing primitive. The DB-touching paths live in
``tests/integration/test_topup_artwork.py``.
"""

from __future__ import annotations

import pytest

from scripts.topup_artwork import (
    TokenBucket,
    _build_auth_header,
    _credentials_from_env,
    extract_artwork_uri,
    make_discogs_client,
)


class TestExtractArtworkUri:
    """``extract_artwork_uri(release_json)``.

    Mirrors the extraction shape at
    ``library-metadata-lookup/discogs/service.py`` (the ``GET /releases/{id}``
    handler): ``artwork_url = images[0].uri if images else None``. The
    drain stamps ``artwork_checked_at`` either way, so the ``None`` path is
    the "asked, genuinely no image" signal.
    """

    def test_returns_primary_image_uri_when_present(self) -> None:
        payload = {
            "id": 5001,
            "images": [
                {"type": "primary", "uri": "https://img.discogs.com/doga-front.jpg"},
                {"type": "secondary", "uri": "https://img.discogs.com/doga-back.jpg"},
            ],
        }
        assert extract_artwork_uri(payload) == "https://img.discogs.com/doga-front.jpg"

    def test_returns_none_when_images_empty(self) -> None:
        assert extract_artwork_uri({"id": 5002, "images": []}) is None

    def test_returns_none_when_images_missing(self) -> None:
        assert extract_artwork_uri({"id": 5003}) is None

    def test_returns_none_when_first_image_lacks_uri(self) -> None:
        payload = {"id": 5004, "images": [{"type": "primary"}]}
        assert extract_artwork_uri(payload) is None


class TestTokenBucket:
    """``TokenBucket(rate_per_minute)``.

    Sleep-based pacing: each ``acquire()`` blocks until the next slot,
    derived from ``60 / rate_per_minute`` seconds between emissions.
    Injectable ``now_fn`` + ``sleep_fn`` keep the unit test wall-clock-free.
    """

    def _fake_clock(self, ticks: list[float]):
        """Yields successive values from ``ticks`` per call."""
        it = iter(ticks)
        return lambda: next(it)

    def test_first_acquire_does_not_sleep(self) -> None:
        slept: list[float] = []
        bucket = TokenBucket(rate_per_minute=60)  # 1 token/s

        bucket.acquire(now_fn=self._fake_clock([0.0, 0.0]), sleep_fn=slept.append)

        assert slept == []

    def test_acquire_sleeps_until_next_interval(self) -> None:
        slept: list[float] = []
        bucket = TokenBucket(rate_per_minute=60)  # 1s interval

        bucket.acquire(now_fn=self._fake_clock([0.0, 0.0]), sleep_fn=slept.append)
        # Second acquire 0.25s later -> must sleep 0.75s
        bucket.acquire(
            now_fn=self._fake_clock([0.25, 1.0]),
            sleep_fn=slept.append,
        )

        assert slept == [pytest.approx(0.75, rel=1e-6)]

    def test_acquire_does_not_sleep_when_interval_already_elapsed(self) -> None:
        slept: list[float] = []
        bucket = TokenBucket(rate_per_minute=60)

        bucket.acquire(now_fn=self._fake_clock([0.0, 0.0]), sleep_fn=slept.append)
        # Second acquire 2.0s later (well past 1.0s interval) -> no sleep
        bucket.acquire(now_fn=self._fake_clock([2.0, 2.0]), sleep_fn=slept.append)

        assert slept == []

    def test_rate_per_minute_zero_rejected(self) -> None:
        with pytest.raises(ValueError):
            TokenBucket(rate_per_minute=0)


class TestBuildAuthHeader:
    """``_build_auth_header(token, api_key, api_secret)``.

    Mirrors LML's two-mode auth selector at
    ``library-metadata-lookup/discogs/service.py:254`` so this script can
    drink from the same secrets store (the WXYC secrets file ships the
    OAuth-pair shape; personal access tokens are only in some operators'
    individual setups).
    """

    def test_token_only_builds_token_header(self) -> None:
        assert _build_auth_header(token="abc", api_key=None, api_secret=None) == (
            "Discogs token=abc"
        )

    def test_key_and_secret_build_oauth_header(self) -> None:
        assert _build_auth_header(token=None, api_key="k", api_secret="s") == (
            "Discogs key=k, secret=s"
        )

    def test_token_takes_precedence_over_oauth_pair(self) -> None:
        """When both are supplied, token wins — same precedence rule as LML.

        Matters when an operator exports both env shapes by accident; we
        deterministically pick one rather than failing closed.
        """
        assert _build_auth_header(token="abc", api_key="k", api_secret="s") == ("Discogs token=abc")

    def test_no_credentials_rejected(self) -> None:
        with pytest.raises(ValueError, match="token.*api_key"):
            _build_auth_header(token=None, api_key=None, api_secret=None)

    def test_key_without_secret_rejected(self) -> None:
        """OAuth pair is all-or-nothing — partial config indicates a deploy mistake."""
        with pytest.raises(ValueError, match="api_key.*api_secret"):
            _build_auth_header(token=None, api_key="k", api_secret=None)

    def test_secret_without_key_rejected(self) -> None:
        with pytest.raises(ValueError, match="api_key.*api_secret"):
            _build_auth_header(token=None, api_key=None, api_secret="s")


class TestMakeDiscogsClient:
    """``make_discogs_client`` wires the auth header into the fetch closure.

    The HTTP layer itself is exercised end-to-end by integration tests via
    a fake client; these tests assert only the header-construction wiring,
    which is the part that can silently regress when the function signature
    changes.
    """

    def test_token_kwarg_accepted(self) -> None:
        fetch = make_discogs_client(token="abc")
        assert callable(fetch)

    def test_oauth_pair_kwargs_accepted(self) -> None:
        fetch = make_discogs_client(api_key="k", api_secret="s")
        assert callable(fetch)

    def test_no_credentials_rejected_at_factory(self) -> None:
        """Surface the auth error at factory time, not at first fetch call."""
        with pytest.raises(ValueError):
            make_discogs_client()


class TestCredentialsFromEnv:
    """``_credentials_from_env()`` reads both auth shapes off the environment.

    The WXYC secrets file (``$WXYC_SECRETS_FILE``) ships the OAuth-pair
    shape — ``DISCOGS_API_KEY`` + ``DISCOGS_API_SECRET`` — so operators
    sourcing from there don't have a personal-access ``DISCOGS_TOKEN`` to
    set. This helper picks whichever shape is present, returning the
    triple ``(token, api_key, api_secret)`` for direct splat into
    ``make_discogs_client``.
    """

    def _clear_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for name in (
            "DISCOGS_TOKEN",
            "DISCOGS_API_TOKEN",
            "DISCOGS_API_KEY",
            "DISCOGS_API_SECRET",
        ):
            monkeypatch.delenv(name, raising=False)

    def test_token_only_env_returns_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_env(monkeypatch)
        monkeypatch.setenv("DISCOGS_TOKEN", "abc")
        assert _credentials_from_env() == ("abc", None, None)

    def test_legacy_api_token_alias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``DISCOGS_API_TOKEN`` is the legacy env name for the personal token."""
        self._clear_env(monkeypatch)
        monkeypatch.setenv("DISCOGS_API_TOKEN", "abc")
        assert _credentials_from_env() == ("abc", None, None)

    def test_oauth_pair_env_returns_pair(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_env(monkeypatch)
        monkeypatch.setenv("DISCOGS_API_KEY", "k")
        monkeypatch.setenv("DISCOGS_API_SECRET", "s")
        assert _credentials_from_env() == (None, "k", "s")

    def test_both_shapes_returns_both_for_factory_to_resolve(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Don't pre-resolve in the env helper; let ``_build_auth_header`` arbitrate.

        Keeps the helper a thin env reader and concentrates the precedence
        rule in one place.
        """
        self._clear_env(monkeypatch)
        monkeypatch.setenv("DISCOGS_TOKEN", "abc")
        monkeypatch.setenv("DISCOGS_API_KEY", "k")
        monkeypatch.setenv("DISCOGS_API_SECRET", "s")
        assert _credentials_from_env() == ("abc", "k", "s")

    def test_empty_env_returns_all_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_env(monkeypatch)
        assert _credentials_from_env() == (None, None, None)
