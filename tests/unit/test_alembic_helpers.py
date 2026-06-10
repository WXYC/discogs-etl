"""Unit tests for lib.alembic_helpers.

The migration-level integration tests exercise the helpers transitively (a
broken helper would cause every side-channel migration to fail), but they
all need a live Docker PG and a stamped DB. Direct unit tests run without
those preconditions and pin three contracts:

* ``resolve_db_url`` honours the canonical env-var precedence
  (``DATABASE_URL_DISCOGS`` over ``DATABASE_URL``) and includes the
  revision label in its error message.
* ``refuse_offline`` imports ``alembic.context`` lazily, so the module
  can be imported (and ``resolve_db_url`` exercised) without alembic
  being installed.
* The helper module itself does not import alembic at module load time.
"""

from __future__ import annotations

import importlib
import sys

import pytest

from lib import alembic_helpers


def test_resolve_db_url_prefers_database_url_discogs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL_DISCOGS", "postgresql://canonical/discogs")
    monkeypatch.setenv("DATABASE_URL", "postgresql://deprecated/fallback")
    assert alembic_helpers.resolve_db_url("0012_test") == "postgresql://canonical/discogs"


def test_resolve_db_url_falls_back_to_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL_DISCOGS", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://deprecated/fallback")
    assert alembic_helpers.resolve_db_url("0012_test") == "postgresql://deprecated/fallback"


def test_resolve_db_url_treats_empty_string_as_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty string should not be treated as a configured URL.

    Falsy fallback is the documented behaviour — psycopg.connect('') would
    raise an opaque parser error rather than the actionable RuntimeError.
    """
    monkeypatch.setenv("DATABASE_URL_DISCOGS", "")
    monkeypatch.setenv("DATABASE_URL", "postgresql://deprecated/fallback")
    assert alembic_helpers.resolve_db_url("0012_test") == "postgresql://deprecated/fallback"


def test_resolve_db_url_raises_with_revision_label(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL_DISCOGS", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match=r"0012_specific_label"):
        alembic_helpers.resolve_db_url("0012_specific_label")


def test_module_does_not_import_alembic_at_load() -> None:
    """``resolve_db_url`` must be usable without alembic — the lazy-import
    contract on ``refuse_offline`` is the whole reason the helper exists.

    Re-imports the module against a fresh module cache and inspects
    ``sys.modules`` to confirm alembic is not pulled in by the top-level
    import. ``refuse_offline``'s import is deferred to call time.
    """
    # Drop both the helper and alembic from the module cache so the re-import
    # is observed cleanly. The actual alembic package, if installed, may
    # already be cached from earlier in the test session; what we're testing
    # is that *importing alembic_helpers* doesn't trigger it.
    for name in list(sys.modules):
        if name == "lib.alembic_helpers" or name == "alembic" or name.startswith("alembic."):
            sys.modules.pop(name, None)

    importlib.import_module("lib.alembic_helpers")

    assert "alembic" not in sys.modules, (
        "lib.alembic_helpers must not import alembic at module load — "
        "the lazy import inside refuse_offline is the whole point."
    )
