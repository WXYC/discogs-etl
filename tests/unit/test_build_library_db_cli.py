"""Tests for ``scripts/build_library_db.py`` (WXYC/discogs-etl#346).

The CLI is a thin adapter over ``lib.backend_library_source``: it parses two
arguments, calls the producer, and maps failures onto the exit-code taxonomy
the rest of this repo already uses. The producer itself is covered by the 243
tests in ``test_catalog_parity_diff.py``, which exercise it through the parity
harness against a live fake Backend -- so these tests deliberately stub the
producer and assert only the wiring. Testing the producer twice would pin the
same behaviour in two places and make the seam harder to move, not safer.

Exit codes match ``catalog_parity_diff.py`` so an operator reading a failed
daily-sync run sees the same numbers the parity soak taught them:

    0  built
    2  usage error (a missing or unusable argument)
    3  the library.db could not be built
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_library_db.py"


def _load():
    spec = importlib.util.spec_from_file_location("build_library_db", SCRIPT_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["build_library_db"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def mod():
    return _load()


class TestWiring:
    def test_passes_source_and_output_through_to_the_producer(self, mod, tmp_path):
        seen = {}

        def fake(source: str, output_path: str) -> None:
            seen["source"] = source
            seen["output"] = output_path

        mod.build_library_db_from_backend = fake
        out = tmp_path / "library.db"

        assert mod.main(["--source", "https://api.wxyc.org", "--output", str(out)]) == 0
        assert seen == {"source": "https://api.wxyc.org", "output": str(out)}

    def test_reports_the_built_path_on_stdout(self, mod, tmp_path, capsys):
        mod.build_library_db_from_backend = lambda source, output_path: None
        out = tmp_path / "library.db"

        mod.main(["--source", "https://api.wxyc.org", "--output", str(out)])

        assert str(out) in capsys.readouterr().out


class TestObservability:
    def test_initializes_the_logger_before_building(self, mod, tmp_path):
        """Pinned, because nothing else in this repo pins it.

        ``init_logger`` is a convention every entrypoint follows and no test
        enforces -- ``test_logger_init.py`` only checks that the scripts
        *compile*. Without it no handler is installed, so the producer's
        ``logger.info`` row-count line (the only record of how large a
        production catalog build came out) is dropped at the root logger, and
        its empty-CTA warning escapes through ``logging.lastResort`` with none
        of the ``repo`` / ``tool`` / ``run_id`` tags and no Sentry.

        It must run *before* the producer, not after: a build that raises is
        exactly the run whose logs are worth having.
        """
        calls = []
        order = []
        mod.init_logger = lambda **kwargs: (calls.append(kwargs), order.append("logger"))
        mod.build_library_db_from_backend = lambda source, output_path: order.append("build")

        mod.main(["--source", "https://api.wxyc.org", "--output", str(tmp_path / "library.db")])

        assert calls, "the CLI never initialized the logger"
        assert calls[0]["repo"] == "discogs-etl"
        assert order == ["logger", "build"]


class TestExitCodes:
    def test_a_build_failure_exits_three(self, mod, tmp_path, capsys):
        def boom(source: str, output_path: str) -> None:
            raise mod.SourceError("catalog export returned no rows")

        mod.build_library_db_from_backend = boom

        code = mod.main(
            ["--source", "https://api.wxyc.org", "--output", str(tmp_path / "library.db")]
        )

        assert code == 3
        assert "catalog export returned no rows" in capsys.readouterr().err

    def test_an_unexpected_producer_failure_also_exits_three(self, mod, tmp_path, capsys):
        # The daily sync's wrapper only distinguishes "built" from "did not
        # build". A stray TypeError out of the producer is still a failure to
        # build, and letting it escape as a traceback with exit 1 would make
        # the shell caller treat it as a different class of problem than the
        # SourceError immediately above it.
        def boom(source: str, output_path: str) -> None:
            raise TypeError("unexpected")

        mod.build_library_db_from_backend = boom

        code = mod.main(
            ["--source", "https://api.wxyc.org", "--output", str(tmp_path / "library.db")]
        )

        assert code == 3
        assert "unexpected" in capsys.readouterr().err

    @pytest.mark.parametrize(
        "argv",
        [
            pytest.param(["--source", "https://api.wxyc.org"], id="no-output"),
            pytest.param(["--output", "/tmp/library.db"], id="no-source"),
        ],
    )
    def test_a_missing_required_argument_is_a_usage_error(self, mod, argv):
        # argparse exits 2 by raising SystemExit rather than returning, which
        # is the same code this module returns for its own usage errors --
        # pinned so a later hand-rolled check cannot silently disagree.
        with pytest.raises(SystemExit) as excinfo:
            mod.main(argv)
        assert excinfo.value.code == 2

    # Deliberately no "a failed build leaves no stub" test here. Any stub the
    # CLI could leave is written by the producer, so a test that stubs the
    # producer out asserts nothing -- the file is absent because nothing ever
    # created it, not because the publish is atomic. That property belongs to
    # `_atomic_output` and is covered where it lives.
