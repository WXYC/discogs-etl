"""Unit tests for ``scripts/check_cache_drift.py``.

The drift watchdog compares the count of distinct artists in the WXYC
library catalog (sqlite ``library.db``) to the count of distinct artists
in the discogs-cache (Postgres ``release_artist``). When the ratio of
covered artists drops below a configurable threshold, the script logs a
warning, optionally posts a Slack notification, and exits non-zero so
the calling CI workflow fires its failure alert.

These tests cover the pure logic only: counting distinct artists from a
sqlite library.db, computing the ratio, and the decision function. The
Postgres path is exercised in the ``pg``-marked integration tests and
end-to-end inside the ``rebuild-cache.yml`` workflow.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_cache_drift.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("check_cache_drift", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["check_cache_drift"] = mod
    spec.loader.exec_module(mod)
    return mod


def _make_library_db(path: Path, artists: list[str]) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE library (id INTEGER PRIMARY KEY, title TEXT, artist TEXT, format TEXT)"
    )
    for i, artist in enumerate(artists, start=1):
        cur.execute(
            "INSERT INTO library (id, title, artist, format) VALUES (?, ?, ?, ?)",
            (i, f"title-{i}", artist, "LP"),
        )
    conn.commit()
    conn.close()


class TestCountLibraryArtists:
    def test_returns_distinct_artist_count(self, tmp_path: Path) -> None:
        db = tmp_path / "library.db"
        _make_library_db(
            db,
            [
                "Juana Molina",
                "Stereolab",
                "Cat Power",
                "Stereolab",  # duplicate
                "Jessica Pratt",
            ],
        )
        mod = _load_module()
        assert mod.count_library_artists(str(db)) == 4

    def test_empty_library_returns_zero(self, tmp_path: Path) -> None:
        db = tmp_path / "library.db"
        _make_library_db(db, [])
        mod = _load_module()
        assert mod.count_library_artists(str(db)) == 0


class TestEvaluateDrift:
    """Pure decision function: given the two counts and a threshold, classify."""

    def test_zero_library_count_is_an_error(self) -> None:
        mod = _load_module()
        result = mod.evaluate_drift(library_count=0, cache_count=100, min_ratio=0.7)
        assert result.ok is False
        assert result.ratio is None
        assert "library count is 0" in result.reason.lower()

    def test_ratio_at_or_above_threshold_is_ok(self) -> None:
        mod = _load_module()
        result = mod.evaluate_drift(library_count=1000, cache_count=800, min_ratio=0.7)
        assert result.ok is True
        assert result.ratio == pytest.approx(0.8)

    def test_ratio_below_threshold_is_drift(self) -> None:
        mod = _load_module()
        result = mod.evaluate_drift(library_count=1000, cache_count=500, min_ratio=0.7)
        assert result.ok is False
        assert result.ratio == pytest.approx(0.5)
        assert "below threshold" in result.reason.lower()

    def test_threshold_boundary_is_inclusive(self) -> None:
        """ratio == min_ratio passes (>=, not >)."""
        mod = _load_module()
        result = mod.evaluate_drift(library_count=1000, cache_count=700, min_ratio=0.7)
        assert result.ok is True

    def test_cache_larger_than_library_is_ok(self) -> None:
        """Cache covers more artists than library is fine -- ratio caps semantically."""
        mod = _load_module()
        result = mod.evaluate_drift(library_count=100, cache_count=500, min_ratio=0.7)
        assert result.ok is True
        assert result.ratio == pytest.approx(5.0)


class TestPostSlackAlert:
    """Slack webhook notification is best-effort and never raises."""

    def test_no_webhook_is_a_noop(self) -> None:
        mod = _load_module()
        # Should return False (no post attempted) without raising.
        assert mod.post_slack_alert(webhook_url=None, message="hi") is False
        assert mod.post_slack_alert(webhook_url="", message="hi") is False

    def test_webhook_post_serializes_text_field(self) -> None:
        mod = _load_module()
        captured: dict = {}

        def fake_urlopen(req, timeout=None):  # noqa: ARG001
            captured["url"] = req.full_url
            captured["data"] = req.data
            captured["headers"] = dict(req.headers)

            class _Resp:
                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

                def read(self):
                    return b""

            return _Resp()

        with patch.object(mod, "urlopen", fake_urlopen):
            ok = mod.post_slack_alert(
                webhook_url="https://hooks.slack.com/services/XXX",
                message="drift detected: ratio=0.42 < 0.7",
            )

        assert ok is True
        assert captured["url"] == "https://hooks.slack.com/services/XXX"
        payload = json.loads(captured["data"].decode("utf-8"))
        assert "drift detected" in payload["text"]

    def test_webhook_post_failure_is_swallowed(self) -> None:
        mod = _load_module()

        def fake_urlopen(req, timeout=None):  # noqa: ARG001
            raise OSError("network down")

        with patch.object(mod, "urlopen", fake_urlopen):
            ok = mod.post_slack_alert(
                webhook_url="https://hooks.slack.com/services/XXX",
                message="hi",
            )
        assert ok is False


class TestRunWatchdogCli:
    """CLI integration: build args, invoke run, check exit code."""

    def test_run_returns_zero_when_ratio_meets_threshold(self, tmp_path: Path) -> None:
        mod = _load_module()
        db = tmp_path / "library.db"
        _make_library_db(db, ["A", "B", "C", "D"])  # 4 distinct

        # Stub the PG count so we don't need a live database.
        with patch.object(mod, "count_cache_artists", return_value=4):
            exit_code = mod.run(
                library_db=str(db),
                database_url="postgresql://stub",
                min_ratio=0.5,
                slack_webhook=None,
            )
        assert exit_code == 0

    def test_run_returns_nonzero_when_drift_exceeds_threshold(self, tmp_path: Path) -> None:
        mod = _load_module()
        db = tmp_path / "library.db"
        _make_library_db(db, ["A", "B", "C", "D"])  # 4 distinct

        with patch.object(mod, "count_cache_artists", return_value=1):
            exit_code = mod.run(
                library_db=str(db),
                database_url="postgresql://stub",
                min_ratio=0.5,
                slack_webhook=None,
            )
        assert exit_code != 0

    def test_run_posts_slack_when_drifting_and_webhook_set(self, tmp_path: Path) -> None:
        mod = _load_module()
        db = tmp_path / "library.db"
        _make_library_db(db, ["A", "B", "C", "D"])

        posted: list[str] = []

        def fake_post(webhook_url, message):
            posted.append(message)
            return True

        with (
            patch.object(mod, "count_cache_artists", return_value=1),
            patch.object(mod, "post_slack_alert", side_effect=fake_post),
        ):
            exit_code = mod.run(
                library_db=str(db),
                database_url="postgresql://stub",
                min_ratio=0.5,
                slack_webhook="https://hooks.slack.com/services/XXX",
            )

        assert exit_code != 0
        assert len(posted) == 1
        assert "drift" in posted[0].lower() or "below" in posted[0].lower()

    def test_run_does_not_post_slack_when_healthy(self, tmp_path: Path) -> None:
        mod = _load_module()
        db = tmp_path / "library.db"
        _make_library_db(db, ["A", "B"])

        called = {"n": 0}

        def fake_post(webhook_url, message):
            called["n"] += 1
            return True

        with (
            patch.object(mod, "count_cache_artists", return_value=10),
            patch.object(mod, "post_slack_alert", side_effect=fake_post),
        ):
            exit_code = mod.run(
                library_db=str(db),
                database_url="postgresql://stub",
                min_ratio=0.5,
                slack_webhook="https://hooks.slack.com/services/XXX",
            )

        assert exit_code == 0
        assert called["n"] == 0
