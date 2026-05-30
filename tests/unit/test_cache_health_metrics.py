"""Unit tests for ``scripts/cache_health_metrics.py``.

The cache-health watchdog publishes three counts from the ``release`` table
to CloudWatch every time it runs:

* ``release_count`` — total rows.
* ``artwork_never_asked_count`` — ``artwork_url IS NULL AND artwork_checked_at IS NULL``.
* ``artwork_imageless_count`` — ``artwork_url IS NULL AND artwork_checked_at IS NOT NULL``.

The two NULL-share components decompose the headline "% NULL artwork_url"
metric from #241 into the drainable share (LML#221's job) and the
genuinely-imageless share (unfixable). Only the never-asked share should
drive an alarm.

These tests cover the pure logic only: counting the three states from a
prepared cursor, building the CloudWatch payload from those counts, and
the orchestration that calls both. The Postgres path is exercised in
``tests/integration/test_cache_health_metrics.py`` against the docker DB.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "cache_health_metrics.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("cache_health_metrics", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["cache_health_metrics"] = mod
    spec.loader.exec_module(mod)
    return mod


class TestCountArtworkStates:
    """Single SQL round-trip returns the three counts as an ``ArtworkStates`` triple."""

    def test_returns_triple_from_single_query(self) -> None:
        # Mock psycopg.connect to return a context-managed connection whose
        # cursor returns one row of three integers.
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (84405, 39585, 240)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        with patch.dict(sys.modules, {"psycopg": MagicMock()}) as patched:
            patched["psycopg"].connect.return_value.__enter__.return_value = mock_conn
            mod = _load_module()
            states = mod.count_artwork_states("postgresql://fake")
        assert states.total == 84405
        assert states.never_asked == 39585
        assert states.imageless == 240
        # One round trip — important because the query is wall-clock-sensitive
        # against the prod cache from a non-Railway location.
        assert mock_cursor.execute.call_count == 1

    def test_treats_null_counts_as_zero(self) -> None:
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None, None, None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        with patch.dict(sys.modules, {"psycopg": MagicMock()}) as patched:
            patched["psycopg"].connect.return_value.__enter__.return_value = mock_conn
            mod = _load_module()
            states = mod.count_artwork_states("postgresql://fake")
        assert states == mod.ArtworkStates(total=0, never_asked=0, imageless=0)


class TestBuildMetricData:
    """Pure: ``ArtworkStates`` -> CloudWatch ``MetricData`` payload."""

    def test_emits_three_metrics(self) -> None:
        mod = _load_module()
        data = mod.build_metric_data(
            mod.ArtworkStates(total=80000, never_asked=39000, imageless=200)
        )
        names = sorted(d["MetricName"] for d in data)
        assert names == [
            "artwork_imageless_count",
            "artwork_never_asked_count",
            "release_count",
        ]

    def test_metric_values_match_counts(self) -> None:
        mod = _load_module()
        data = mod.build_metric_data(
            mod.ArtworkStates(total=80000, never_asked=39000, imageless=200)
        )
        by_name = {d["MetricName"]: d for d in data}
        assert by_name["release_count"]["Value"] == 80000
        assert by_name["artwork_never_asked_count"]["Value"] == 39000
        assert by_name["artwork_imageless_count"]["Value"] == 200

    def test_metric_unit_is_count(self) -> None:
        mod = _load_module()
        data = mod.build_metric_data(mod.ArtworkStates(total=1, never_asked=0, imageless=0))
        for d in data:
            assert d["Unit"] == "Count"

    def test_empty_state_emits_zero_values(self) -> None:
        mod = _load_module()
        data = mod.build_metric_data(mod.ArtworkStates(total=0, never_asked=0, imageless=0))
        assert {d["MetricName"]: d["Value"] for d in data} == {
            "release_count": 0,
            "artwork_never_asked_count": 0,
            "artwork_imageless_count": 0,
        }


class TestPublishMetrics:
    """Sends one ``put_metric_data`` call carrying all three metrics together."""

    def test_publishes_to_configured_namespace(self) -> None:
        mod = _load_module()
        client = MagicMock()
        states = mod.ArtworkStates(total=80000, never_asked=39000, imageless=200)
        mod.publish_metrics(client=client, namespace="WXYC/DiscogsCache", states=states)
        client.put_metric_data.assert_called_once()
        kwargs = client.put_metric_data.call_args.kwargs
        assert kwargs["Namespace"] == "WXYC/DiscogsCache"
        assert len(kwargs["MetricData"]) == 3

    def test_skips_publish_when_dry_run(self) -> None:
        mod = _load_module()
        client = MagicMock()
        mod.publish_metrics(
            client=client,
            namespace="WXYC/DiscogsCache",
            states=mod.ArtworkStates(total=1, never_asked=0, imageless=0),
            dry_run=True,
        )
        client.put_metric_data.assert_not_called()


class TestRun:
    """Orchestrator: count + publish + log; never raises on transport errors."""

    def test_exits_zero_on_happy_path(self) -> None:
        mod = _load_module()
        client = MagicMock()
        with patch.object(
            mod, "count_artwork_states", return_value=mod.ArtworkStates(80000, 39000, 200)
        ):
            rc = mod.run(
                database_url="postgresql://fake",
                cloudwatch_client=client,
                namespace="WXYC/DiscogsCache",
                dry_run=False,
            )
        assert rc == 0
        client.put_metric_data.assert_called_once()

    def test_dry_run_skips_publish_but_still_returns_zero(self) -> None:
        mod = _load_module()
        client = MagicMock()
        with patch.object(
            mod, "count_artwork_states", return_value=mod.ArtworkStates(80000, 39000, 200)
        ):
            rc = mod.run(
                database_url="postgresql://fake",
                cloudwatch_client=client,
                namespace="WXYC/DiscogsCache",
                dry_run=True,
            )
        assert rc == 0
        client.put_metric_data.assert_not_called()


class TestMain:
    """CLI: flag parsing and env-var fallback."""

    def test_requires_database_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for env in ("DATABASE_URL_DISCOGS", "DATABASE_URL"):
            monkeypatch.delenv(env, raising=False)
        mod = _load_module()
        rc = mod.main(["--namespace", "WXYC/DiscogsCache"])
        assert rc == 2

    def test_database_url_falls_back_to_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASE_URL_DISCOGS", "postgresql://from-env")
        monkeypatch.delenv("DATABASE_URL", raising=False)
        mod = _load_module()
        with (
            patch.object(mod, "run", return_value=0) as run,
            patch.object(mod, "_build_cloudwatch_client") as build_client,
        ):
            build_client.return_value = MagicMock()
            rc = mod.main(["--namespace", "WXYC/DiscogsCache"])
        assert rc == 0
        assert run.call_args.kwargs["database_url"] == "postgresql://from-env"

    def test_dry_run_skips_cloudwatch_client_construction(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``--dry-run`` is documented as "skip the CloudWatch publish".

        It should also skip *building* the CloudWatch client, so a developer
        without boto3 installed (boto3 is a ``[dev]``-only dep) can still run
        ``--dry-run`` to validate counts.
        """
        monkeypatch.setenv("DATABASE_URL_DISCOGS", "postgresql://from-env")
        monkeypatch.delenv("DATABASE_URL", raising=False)
        mod = _load_module()
        with (
            patch.object(mod, "run", return_value=0),
            patch.object(mod, "_build_cloudwatch_client") as build_client,
        ):
            rc = mod.main(["--namespace", "WXYC/DiscogsCache", "--dry-run"])
        assert rc == 0
        build_client.assert_not_called()
