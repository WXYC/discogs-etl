"""An ERROR from a pipeline step reaches PostHog when POSTHOG_ERRORS_API_KEY is set (discogs-etl#516).

``wxyc_etl.logger.init_logger`` (wxyc-etl >= 0.10.3, ``posthog`` extra) installs the
PostHog errors handler when the key is present. These tests stub the ``posthog`` module
so no network client is built, and skip when the installed wxyc-etl predates the handler.
"""

from __future__ import annotations

import logging
import sys
import threading
import types
from pathlib import Path

import pytest


class _StubPosthog:
    instances: list[_StubPosthog] = []

    def __init__(self, api_key, host=None, **_kwargs):
        self.api_key = api_key
        self.captured: list[tuple] = []
        _StubPosthog.instances.append(self)

    def capture(self, event, **kwargs):
        self.captured.append((event, kwargs))

    def capture_exception(self, exc_info, **kwargs):
        self.captured.append(("$exception", kwargs))

    def flush(self):
        pass


@pytest.fixture
def posthog_logger(monkeypatch):
    logger_mod = pytest.importorskip("wxyc_etl.logger")
    if not hasattr(logger_mod, "_build_posthog_handler"):
        pytest.skip("wxyc_etl.logger predates the PostHog errors handler")

    stub = types.ModuleType("posthog")
    stub.Posthog = _StubPosthog
    monkeypatch.setitem(sys.modules, "posthog", stub)
    _StubPosthog.instances.clear()

    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    saved_hooks = (sys.excepthook, threading.excepthook)
    for handler in saved_handlers:
        root.removeHandler(handler)
    monkeypatch.setattr(logger_mod, "_INITIALIZED", False)
    monkeypatch.setattr(logger_mod, "_POSTHOG_CLIENT", None)
    monkeypatch.setattr(logger_mod, "_POSTHOG_BUDGET", None)
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    yield
    for handler in list(root.handlers):
        root.removeHandler(handler)
    for handler in saved_handlers:
        root.addHandler(handler)
    root.setLevel(saved_level)
    sys.excepthook, threading.excepthook = saved_hooks


def test_error_reaches_posthog_when_key_set(posthog_logger, monkeypatch):
    monkeypatch.setenv("POSTHOG_ERRORS_API_KEY", "phc_test")

    from lib.observability import init_logger

    init_logger(repo="discogs-etl", tool="discogs-etl test")
    logging.getLogger("discogs-etl.step").error("import failed", extra={"step": "import"})

    (client,) = _StubPosthog.instances
    assert client.api_key == "phc_test"
    ((event, kwargs),) = client.captured
    assert event == "$exception"
    assert kwargs["properties"]["repo"] == "discogs-etl"
    assert kwargs["properties"]["step"] == "import"


def test_error_not_sent_when_key_unset(posthog_logger, monkeypatch):
    monkeypatch.delenv("POSTHOG_ERRORS_API_KEY", raising=False)

    from lib.observability import init_logger

    init_logger(repo="discogs-etl", tool="discogs-etl test")
    logging.getLogger("discogs-etl.step").error("import failed")

    assert _StubPosthog.instances == []


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_pyproject_pins_wxyc_etl_with_posthog_extra() -> None:
    text = (REPO_ROOT / "pyproject.toml").read_text()
    assert '"wxyc-etl[posthog]>=0.10.3,<0.11.0"' in text


@pytest.mark.parametrize(
    "workflow,expected",
    [("rebuild-cache.yml", 3), ("sync-library.yml", 2)],
)
def test_workflows_pass_both_error_keys(workflow: str, expected: int) -> None:
    text = (REPO_ROOT / ".github" / "workflows" / workflow).read_text()
    assert text.count("SENTRY_DSN: ${{ secrets.SENTRY_DSN }}") == expected
    assert text.count("POSTHOG_ERRORS_API_KEY: ${{ secrets.POSTHOG_ERRORS_API_KEY }}") == expected


def test_bootstrap_reads_and_exports_the_posthog_key() -> None:
    text = (REPO_ROOT / "scripts" / "rebuild-cache-bootstrap.sh").read_text()
    assert 'POSTHOG_ERRORS_API_KEY="$(ssm_param "${SSM_PREFIX}/POSTHOG_ERRORS_API_KEY")"' in text
    assert "SENTRY_DSN POSTHOG_ERRORS_API_KEY\n" in text


def test_provision_secrets_writes_the_posthog_key_as_optional() -> None:
    text = (REPO_ROOT / "infra" / "ephemeral-rebuild" / "provision-secrets.sh").read_text()
    assert "put_secret POSTHOG_ERRORS_API_KEY" in text
    assert "SENTRY_DSN (or blank to skip)" in text
