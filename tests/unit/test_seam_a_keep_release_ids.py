"""Seam A: the converter's ``--keep-release-ids`` allowlist wiring (#424).

``discogs-xml-converter#81`` shipped ``--keep-release-ids`` on 2026-07-29 for
exactly one purpose: emit a WXYC library pinned release (and its full child row
set) even when the ``(artist, title)`` filter rejects it, so the pins reach the
CSVs at all and the #327/#328 dedup and prune exemptions have something to
protect. ``run_pipeline.py`` never passed it. The 2026-09-04 rebuild therefore
filtered 27,286 pins out of the converter's output, and the import-stage prune
deleted every one of them.

``rebuild-cache.sh`` downloads the converter's *latest* GitHub release with no
tag pin, so the flag's availability has to be feature-probed rather than
assumed, and an ambiguous probe must omit the flag — passing an unknown
argument would abort the whole rebuild.
"""

from __future__ import annotations

import importlib.util
import logging
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

_spec = importlib.util.spec_from_file_location(
    "run_pipeline_seam_a",
    Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py",
)
assert _spec is not None and _spec.loader is not None
run_pipeline = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_pipeline)


@pytest.fixture(autouse=True)
def _stub_rebuild_lock():
    """Same rationale as tests/unit/test_run_pipeline.py: main() takes a real
    PG advisory lock (#354) before anything else, and these are no-infrastructure
    unit tests."""
    with (
        patch.object(run_pipeline, "try_acquire_rebuild_lock", return_value=MagicMock()),
        patch.object(run_pipeline, "release_rebuild_lock"),
    ):
        yield


HELP_WITH_FLAG = """Usage: discogs-xml-converter build [OPTIONS] <PATH>

Options:
      --data-dir <DIR>
      --library-db <FILE>
      --keep-release-ids <FILE>  Allowlist of release ids to emit
"""

HELP_WITHOUT_FLAG = """Usage: discogs-xml-converter build [OPTIONS] <PATH>

Options:
      --data-dir <DIR>
      --library-db <FILE>
"""


class TestConverterCapabilityProbe:
    def _probe(self, **run_kwargs):
        with patch.object(run_pipeline.subprocess, "run", **run_kwargs) as mock_run:
            supported = run_pipeline.converter_supports_keep_release_ids(
                "discogs-xml-converter", "build"
            )
        return supported, mock_run

    def test_help_advertising_the_flag_is_supported(self) -> None:
        supported, mock_run = self._probe(
            return_value=subprocess.CompletedProcess([], 0, HELP_WITH_FLAG, "")
        )
        assert supported is True
        assert mock_run.call_args[0][0] == ["discogs-xml-converter", "build", "--help"], (
            "probe the same subcommand we are about to invoke — the flag is declared "
            "per-subcommand on the converter side"
        )

    def test_help_without_the_flag_is_unsupported(self) -> None:
        supported, _ = self._probe(
            return_value=subprocess.CompletedProcess([], 0, HELP_WITHOUT_FLAG, "")
        )
        assert supported is False

    def test_help_on_stderr_still_counts(self) -> None:
        """Some CLIs print help to stderr and exit non-zero; scan both streams."""
        supported, _ = self._probe(
            return_value=subprocess.CompletedProcess([], 2, "", HELP_WITH_FLAG)
        )
        assert supported is True

    @pytest.mark.parametrize(
        "exc",
        [
            FileNotFoundError("no such binary"),
            subprocess.TimeoutExpired(cmd="discogs-xml-converter", timeout=30),
        ],
    )
    def test_probe_failure_is_unsupported_not_an_abort(self, exc, caplog) -> None:
        with caplog.at_level(logging.WARNING):
            supported, _ = self._probe(side_effect=exc)
        assert supported is False, (
            "an ambiguous probe must resolve to 'unsupported': passing an unknown "
            "argument to the converter would abort the rebuild outright"
        )
        assert "keep-release-ids" in caplog.text


class TestConvertAndFilterPassesTheAllowlist:
    def _invoke(self, *, keep_release_ids, supported, **kwargs):
        with (
            patch.object(run_pipeline, "run_step") as mock_run,
            patch.object(
                run_pipeline, "converter_supports_keep_release_ids", return_value=supported
            ) as mock_probe,
        ):
            run_pipeline.convert_and_filter(
                Path("/data/releases.xml.gz"),
                Path("/tmp/csv"),
                "discogs-xml-converter",
                keep_release_ids=keep_release_ids,
                **kwargs,
            )
        return mock_run.call_args[0][1], mock_probe

    def test_flag_appended_in_csv_mode(self) -> None:
        cmd, probe = self._invoke(keep_release_ids=Path("/tmp/keep.txt"), supported=True)
        assert "--keep-release-ids" in cmd
        assert "/tmp/keep.txt" in cmd
        assert probe.call_args[0][1] == "build"

    def test_flag_appended_in_direct_pg_mode(self) -> None:
        cmd, probe = self._invoke(
            keep_release_ids=Path("/tmp/keep.txt"),
            supported=True,
            database_url="postgresql:///discogs",
        )
        assert "--keep-release-ids" in cmd
        assert probe.call_args[0][1] == "import", "the import subcommand must be probed, not build"

    def test_flag_omitted_and_warned_when_unsupported(self, caplog) -> None:
        with caplog.at_level(logging.WARNING):
            cmd, _ = self._invoke(keep_release_ids=Path("/tmp/keep.txt"), supported=False)
        assert "--keep-release-ids" not in cmd
        assert "#424" in caplog.text, "the degraded scope must be visible in the run log"

    def test_no_probe_at_all_without_an_allowlist(self) -> None:
        cmd, probe = self._invoke(keep_release_ids=None, supported=True)
        assert "--keep-release-ids" not in cmd
        probe.assert_not_called()


class TestPrepareConverterKeepReleaseIds:
    def test_returns_a_written_allowlist_path(self, tmp_path) -> None:
        def fake_write(db_url, path):
            path.write_text("101\n202\n")
            return 2

        with patch.object(run_pipeline, "write_keep_release_ids", side_effect=fake_write):
            path = run_pipeline.prepare_converter_keep_release_ids("postgresql:///test")

        assert path is not None
        assert path.read_text() == "101\n202\n"

    def test_degrades_to_none_on_a_database_error(self, caplog) -> None:
        """This read only *widens* the converter's output. The protective read
        is the one _run_database_build already performs for the dedup / prune /
        import seams, which propagates every error (#327). So a failure here
        must not abort the rebuild before a single byte of the dump is read —
        it degrades to the converter's default scope, where the import seam's
        pin exemption and shortfall guard take over.
        """
        with (
            patch.object(
                run_pipeline,
                "write_keep_release_ids",
                side_effect=psycopg.OperationalError("connection refused"),
            ),
            caplog.at_level(logging.WARNING),
        ):
            path = run_pipeline.prepare_converter_keep_release_ids("postgresql:///test")

        assert path is None
        assert "#424" in caplog.text


class TestXmlPipelineWiring:
    """End-to-end argument flow through main() in --xml mode."""

    def _run_main(self, tmp_path, extra_args=()):
        xml_file = tmp_path / "releases.xml.gz"
        xml_file.touch()
        library_db = tmp_path / "library.db"
        library_db.touch()
        args = run_pipeline.parse_args(
            ["--xml", str(xml_file), "--library-db", str(library_db), *extra_args]
        )
        keep_path = tmp_path / "keep_release_ids.txt"
        keep_path.write_text("101\n")
        order: list[str] = []
        convert_kwargs: list[dict] = []

        def fake_prepare(db_url):
            order.append("prepare_keep_ids")
            return keep_path

        def fake_convert(xml, output_dir, converter, **kwargs):
            order.append("convert_and_filter")
            convert_kwargs.append(kwargs)

        with (
            patch.object(
                run_pipeline, "prepare_converter_keep_release_ids", side_effect=fake_prepare
            ),
            patch.object(run_pipeline, "convert_and_filter", side_effect=fake_convert),
            patch.object(run_pipeline, "_run_database_build"),
            patch.object(run_pipeline, "_run_database_build_post_import"),
            patch.object(run_pipeline, "wait_for_postgres"),
            patch.object(run_pipeline, "run_sql_file"),
            patch.object(run_pipeline, "set_tables_unlogged"),
            patch.object(psycopg, "connect", return_value=MagicMock()),
            patch.object(run_pipeline, "parse_args", return_value=args),
        ):
            run_pipeline.main()
        return order, convert_kwargs, keep_path

    def test_allowlist_is_built_before_conversion_and_forwarded(self, tmp_path) -> None:
        order, convert_kwargs, keep_path = self._run_main(tmp_path)
        assert order == ["prepare_keep_ids", "convert_and_filter"], (
            "the allowlist must exist before the converter starts scanning — after "
            "the scan there is nothing left to include"
        )
        assert convert_kwargs[0].get("keep_release_ids") == keep_path

    def test_direct_pg_mode_forwards_it_too(self, tmp_path) -> None:
        order, convert_kwargs, keep_path = self._run_main(tmp_path, ["--direct-pg"])
        assert order.index("prepare_keep_ids") < order.index("convert_and_filter")
        assert convert_kwargs[0].get("keep_release_ids") == keep_path


class TestImportStepReceivesTheAllowlist:
    """``_run_database_build`` already writes the authoritative allowlist for the
    dedup and prune seams; the import step is the third consumer (#424)."""

    def _captured_cmds(self) -> list[list[str]]:
        captured: list[list[str]] = []

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [True]
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(
                run_pipeline, "run_step", side_effect=lambda n, cmd, *a, **k: captured.append(cmd)
            ),
            patch.object(run_pipeline, "wait_for_postgres"),
            patch.object(run_pipeline, "run_sql_file"),
            patch.object(run_pipeline, "run_sql_statements_parallel"),
            patch.object(run_pipeline, "set_tables_unlogged"),
            patch.object(run_pipeline, "set_tables_logged"),
            patch.object(run_pipeline, "run_vacuum"),
            patch.object(run_pipeline, "check_reload_invariant"),
            patch.object(run_pipeline, "report_sizes"),
            patch.object(psycopg, "connect", return_value=mock_conn),
        ):
            run_pipeline._run_database_build(
                "postgresql:///test", Path("/tmp/csv"), None, sys.executable
            )
        return captured

    def test_base_step_carries_keep_release_ids(self) -> None:
        cmds = self._captured_cmds()
        base = [c for c in cmds if "--base-only" in c]
        assert len(base) == 1
        assert "--keep-release-ids" in base[0], (
            "the import prune is the third seam #327 left unguarded; it needs the "
            "same allowlist dedup and verify_cache already get"
        )
        flag_idx = base[0].index("--keep-release-ids")
        assert base[0][flag_idx + 1].endswith("keep_release_ids.txt")
        assert base[0].index("--base-only") < flag_idx < base[0].index("/tmp/csv"), (
            "flags must precede the positional csv_dir / db_url arguments"
        )

    def test_tracks_step_does_not_carry_it(self) -> None:
        """--tracks-only never prunes releases, so the allowlist is meaningless
        there (and every extra arg is one more thing to get wrong)."""
        cmds = self._captured_cmds()
        tracks = [c for c in cmds if "--tracks-only" in c]
        assert len(tracks) == 1
        assert "--keep-release-ids" not in tracks[0]
