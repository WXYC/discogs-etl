"""LML#221 top-up drain — unit tests.

Pure-function coverage for the artwork-extract shape and the TokenBucket
pacing primitive. The DB-touching paths live in
``tests/integration/test_topup_artwork.py``.
"""

from __future__ import annotations

import pytest

from scripts.topup_artwork import TokenBucket, extract_artwork_uri


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
