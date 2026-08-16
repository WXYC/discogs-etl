"""Every pipeline script logs its target database before connecting; none may
log the credential (#361, closing the DSN half of #227).

``rebuild-cache-bootstrap.sh`` tees the whole pipeline log to the S3 rebuild
log bucket, and these four scripts receive the cache DSN on argv, so the
startup line published the cache credential on the *happy* path -- no failure
required. Each is exercised for both DSN forms libpq accepts, because the
keyword/value form is what defeated the naive ``split("@")[-1]`` redactor:
it contains no ``@``, so the whole string (password included) was "redacted"
to itself.

The scripts are driven only as far as their connect call, which is stubbed to
raise a sentinel -- the log line under test is emitted immediately before it.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"

URL_DSN = "postgresql://svc:hunter2@cache-host:5433/discogs"
CONNINFO_DSN = "host=cache-host port=5433 dbname=discogs user=svc password=hunter2"
BOTH_DSN_FORMS = pytest.mark.parametrize("dsn", [URL_DSN, CONNINFO_DSN], ids=["url", "conninfo"])


def _load(name: str):
    """Load a script as a module, reusing an already-loaded copy.

    verify_cache in particular must not be loaded twice in one interpreter:
    a second module object shadows the first and breaks ProcessPool pickling
    for workers holding references to the original's symbols (see #109).
    """
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _StopBeforeConnect(Exception):
    """Sentinel raised by the stubbed connect, after the log line is emitted."""


def _assert_redacted(caplog, module) -> str:
    """Assert nothing credential-shaped reached the log; return the messages."""
    logged = " ".join(record.getMessage() for record in caplog.records)
    assert "hunter2" not in logged
    assert "password" not in logged
    assert "svc" not in logged
    # Redaction must not cost the operator the one fact the line exists for.
    assert "cache-host:5433/discogs" in logged
    return logged


class TestDedupReleases:
    @BOTH_DSN_FORMS
    def test_startup_line_redacts_dsn(self, caplog, dsn: str) -> None:
        module = _load("dedup_releases")
        ns = argparse.Namespace(
            database_url=dsn,
            library_labels=None,
            label_hierarchy=None,
            keep_release_ids=None,
        )
        with caplog.at_level(logging.INFO, logger=module.logger.name):
            with (
                patch.object(module, "parse_args", return_value=ns),
                patch.object(module, "init_logger"),
                patch.object(module.psycopg, "connect", side_effect=_StopBeforeConnect),
            ):
                with pytest.raises(_StopBeforeConnect):
                    module.main()
        _assert_redacted(caplog, module)


class TestImportCsv:
    @BOTH_DSN_FORMS
    def test_startup_line_redacts_dsn(self, caplog, tmp_path: Path, dsn: str) -> None:
        module = _load("import_csv")
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        with caplog.at_level(logging.INFO, logger=module.logger.name):
            with (
                patch.object(sys, "argv", ["import_csv.py", str(csv_dir), dsn]),
                patch.object(module, "init_logger"),
                patch.object(module.psycopg, "connect", side_effect=_StopBeforeConnect),
            ):
                with pytest.raises(_StopBeforeConnect):
                    module.main()
        _assert_redacted(caplog, module)


class TestVerifyCache:
    @BOTH_DSN_FORMS
    def test_startup_line_redacts_dsn(self, caplog, tmp_path: Path, dsn: str) -> None:
        module = _load("verify_cache")
        library_db = tmp_path / "library.db"
        library_db.touch()
        ns = argparse.Namespace(
            database_url=dsn,
            library_db=library_db,
            mappings_file=None,
        )
        with caplog.at_level(logging.INFO, logger=module.logger.name):
            with (
                patch.object(module, "parse_args", return_value=ns),
                patch.object(
                    module, "load_artist_mappings", return_value={"keep": [], "prune": []}
                ),
                patch.object(module.LibraryIndex, "from_sqlite", return_value=MagicMock()),
                patch.object(module.asyncpg, "connect", side_effect=_StopBeforeConnect),
            ):
                with pytest.raises(_StopBeforeConnect):
                    asyncio.run(module.async_main())
        _assert_redacted(caplog, module)


class TestResolveCollisions:
    @BOTH_DSN_FORMS
    def test_startup_line_redacts_dsn(self, caplog, tmp_path: Path, dsn: str) -> None:
        module = _load("resolve_collisions")
        input_csv = tmp_path / "genre-analysis-results.csv"
        input_csv.touch()
        library_db = tmp_path / "library.db"
        library_db.touch()
        argv = [
            "--input",
            str(input_csv),
            "--library-db",
            str(library_db),
            "--output",
            str(tmp_path / "out.csv"),
            "--database-url",
            dsn,
        ]
        with caplog.at_level(logging.INFO, logger=module.logger.name):
            with (
                patch.object(module, "init_logger"),
                patch.object(module, "load_wrong_person_entries", return_value={}),
                patch.object(module, "load_wxyc_titles"),
                patch.object(module.psycopg, "connect", side_effect=_StopBeforeConnect),
            ):
                with pytest.raises(_StopBeforeConnect):
                    module.main(argv)
        _assert_redacted(caplog, module)
