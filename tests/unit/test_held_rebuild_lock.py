"""``held_rebuild_lock``: acquire-or-refuse with the actual holder named."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lib import rebuild_lock
from lib.rebuild_lock import REBUILD_LOCK_KEY, RebuildLockRefused, held_rebuild_lock


def _refuse(monkeypatch: pytest.MonkeyPatch, describe) -> None:
    monkeypatch.setattr(rebuild_lock, "try_acquire_rebuild_lock", lambda url, *, holder: None)
    monkeypatch.setattr(rebuild_lock, "describe_rebuild_lock_holder", describe)


@pytest.mark.parametrize(
    ("describe", "expected"),
    [
        (lambda url: "discogs-etl-rebuild", "discogs-etl-rebuild"),
        (lambda url: "", "set no application_name"),
        (lambda url: None, "released"),
    ],
)
def test_refusal_names_the_holder(monkeypatch, describe, expected) -> None:
    _refuse(monkeypatch, describe)
    with pytest.raises(RebuildLockRefused) as exc:
        with held_rebuild_lock("postgresql://x", holder="h"):
            pytest.fail("body must not run")
    assert expected in str(exc.value)
    assert str(REBUILD_LOCK_KEY) in str(exc.value)


def test_failed_holder_lookup_still_refuses(monkeypatch) -> None:
    def boom(url: str) -> str:
        raise RuntimeError("connection refused")

    _refuse(monkeypatch, boom)
    with pytest.raises(RebuildLockRefused, match="could not be looked up"):
        with held_rebuild_lock("postgresql://x", holder="h"):
            pytest.fail("body must not run")


def test_releases_on_exit_even_when_body_raises(monkeypatch) -> None:
    conn = MagicMock()
    released = []
    monkeypatch.setattr(rebuild_lock, "try_acquire_rebuild_lock", lambda url, *, holder: conn)
    monkeypatch.setattr(rebuild_lock, "release_rebuild_lock", released.append)
    with pytest.raises(ValueError):
        with held_rebuild_lock("postgresql://x", holder="h") as got:
            assert got is conn
            raise ValueError
    assert released == [conn]
