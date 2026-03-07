"""Unit tests for scripts/run_pipeline.py — streaming run_step() and arg parsing."""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Load run_pipeline as a module (it's a script, not a package).
_spec = importlib.util.spec_from_file_location(
    "run_pipeline",
    Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py",
)
run_pipeline = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_pipeline)

run_sql_statements_parallel = run_pipeline.run_sql_statements_parallel


class TestRunStepStreaming:
    """run_step() streams subprocess output line-by-line."""

    def test_output_lines_appear_in_logger(self, caplog) -> None:
        """Each line of subprocess output is logged individually."""
        with caplog.at_level(logging.INFO, logger=run_pipeline.logger.name):
            run_pipeline.run_step(
                "echo test",
                [sys.executable, "-c", "print('line1'); print('line2')"],
            )
        logged = [r.message for r in caplog.records]
        assert any("line1" in msg for msg in logged)
        assert any("line2" in msg for msg in logged)

    def test_nonzero_exit_triggers_sys_exit(self) -> None:
        """Non-zero exit code triggers sys.exit(1)."""
        with pytest.raises(SystemExit, match="1"):
            run_pipeline.run_step(
                "fail test",
                [sys.executable, "-c", "import sys; sys.exit(42)"],
            )

    def test_elapsed_time_logged(self, caplog) -> None:
        """Elapsed time is logged on completion."""
        with caplog.at_level(logging.INFO, logger=run_pipeline.logger.name):
            run_pipeline.run_step(
                "elapsed test",
                [sys.executable, "-c", "pass"],
            )
        logged = [r.message for r in caplog.records]
        assert any("completed in" in msg for msg in logged)

    def test_stderr_is_captured(self, caplog) -> None:
        """Stderr output is also logged (merged with stdout)."""
        with caplog.at_level(logging.INFO, logger=run_pipeline.logger.name):
            run_pipeline.run_step(
                "stderr test",
                [sys.executable, "-c", "import sys; sys.stderr.write('err_msg\\n')"],
            )
        logged = [r.message for r in caplog.records]
        assert any("err_msg" in msg for msg in logged)


class TestArgParsing:
    """Argument parsing for --resume and --state-file flags."""

    def test_resume_flag_parsed(self) -> None:
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--resume",
            ]
        )
        assert args.resume is True

    def test_resume_default_false(self) -> None:
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.resume is False

    def test_state_file_flag_parsed(self) -> None:
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--state-file",
                "/tmp/state.json",
            ]
        )
        assert args.state_file == Path("/tmp/state.json")

    def test_state_file_default(self) -> None:
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.state_file == Path(".pipeline_state.json")

    def test_converter_default(self) -> None:
        args = run_pipeline.parse_args(["--xml", "/tmp/releases.xml.gz"])
        assert args.converter == "discogs-xml-converter"

    def test_converter_custom(self) -> None:
        args = run_pipeline.parse_args(
            ["--xml", "/tmp/releases.xml.gz", "--converter", "/usr/local/bin/my-converter"]
        )
        assert args.converter == "/usr/local/bin/my-converter"

    def test_resume_invalid_with_xml(self) -> None:
        """--resume is only valid with --csv-dir, not --xml."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(
                [
                    "--xml",
                    "/tmp/releases.xml.gz",
                    "--resume",
                ]
            )

    def test_target_db_url_parsed(self) -> None:
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--library-db",
                "/tmp/library.db",
                "--target-db-url",
                "postgresql://localhost/target",
            ]
        )
        assert args.target_db_url == "postgresql://localhost/target"

    def test_target_db_url_default_none(self) -> None:
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.target_db_url is None

    def test_target_db_url_requires_library_db(self) -> None:
        """--target-db-url without --library-db should error."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(
                [
                    "--csv-dir",
                    "/tmp/csv",
                    "--target-db-url",
                    "postgresql://localhost/target",
                ]
            )

    def test_library_labels_parsed(self) -> None:
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--library-labels",
                "/tmp/library_labels.csv",
            ]
        )
        assert args.library_labels == Path("/tmp/library_labels.csv")

    def test_library_labels_default_none(self) -> None:
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.library_labels is None

    def test_wxyc_db_url_requires_library_db(self) -> None:
        """--wxyc-db-url without --library-db should error."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(
                [
                    "--csv-dir",
                    "/tmp/csv",
                    "--wxyc-db-url",
                    "mysql://user:pass@host/db",
                ]
            )

    def test_label_hierarchy_parsed(self) -> None:
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--label-hierarchy",
                "/tmp/label_hierarchy.csv",
            ]
        )
        assert args.label_hierarchy == Path("/tmp/label_hierarchy.csv")

    def test_label_hierarchy_default_none(self) -> None:
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.label_hierarchy is None

    def test_xml_accepts_directory_path(self) -> None:
        """--xml can accept a directory path (for multi-XML input)."""
        args = run_pipeline.parse_args(["--xml", "/tmp/xml_dumps/"])
        assert args.xml == Path("/tmp/xml_dumps/")

    def test_keep_csv_flag_parsed(self) -> None:
        """--keep-csv is parsed as a Path."""
        args = run_pipeline.parse_args(
            ["--xml", "/tmp/releases.xml.gz", "--keep-csv", "/tmp/kept_csvs"]
        )
        assert args.keep_csv == Path("/tmp/kept_csvs")

    def test_keep_csv_default_none(self) -> None:
        """--keep-csv defaults to None."""
        args = run_pipeline.parse_args(["--xml", "/tmp/releases.xml.gz"])
        assert args.keep_csv is None

    def test_keep_csv_only_valid_with_xml(self) -> None:
        """--keep-csv is only meaningful with --xml, not --csv-dir."""
        # Should still parse (no error), but it's ignored in csv-dir mode
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv", "--keep-csv", "/tmp/kept"])
        assert args.keep_csv == Path("/tmp/kept")


class TestRunSqlStatementsParallel:
    """Test parallel SQL statement execution."""

    def test_all_statements_executed(self) -> None:
        """All statements are executed exactly once."""
        from unittest.mock import MagicMock, patch

        executed: list[str] = []
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        def track_execute(stmt):
            executed.append(stmt)

        mock_cursor.execute.side_effect = track_execute

        stmts = [
            "CREATE INDEX idx_a ON t(a)",
            "CREATE INDEX idx_b ON t(b)",
            "CREATE INDEX idx_c ON t(c)",
        ]

        with patch.object(run_pipeline.psycopg, "connect", return_value=mock_conn):
            run_sql_statements_parallel("postgresql:///test", stmts)

        assert set(executed) == set(stmts)
        assert len(executed) == 3

    def test_empty_statements_is_noop(self) -> None:
        """Empty list of statements doesn't crash."""
        from unittest.mock import MagicMock, patch

        with patch.object(run_pipeline.psycopg, "connect", return_value=MagicMock()):
            run_sql_statements_parallel("postgresql:///test", [])

    def test_description_logged(self, caplog) -> None:
        """Description is logged when provided."""
        from unittest.mock import MagicMock, patch

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(run_pipeline.psycopg, "connect", return_value=mock_conn),
            caplog.at_level(logging.INFO, logger=run_pipeline.logger.name),
        ):
            run_sql_statements_parallel(
                "postgresql:///test",
                ["CREATE INDEX idx_x ON t(x)"],
                description="test indexes",
            )

        assert any("test indexes" in r.message for r in caplog.records)


class TestXmlModeEnrichment:
    """In --xml mode, library_artists.txt is generated from library.db when not provided."""

    def test_enrich_called_with_library_db_only(self, tmp_path) -> None:
        """When --library-db is provided without --library-artists, the pipeline
        generates library_artists.txt from library.db via enrich_library_artists."""
        xml_file = tmp_path / "releases.xml.gz"
        xml_file.touch()
        library_db = tmp_path / "library.db"
        library_db.touch()

        args = run_pipeline.parse_args(
            [
                "--xml",
                str(xml_file),
                "--library-db",
                str(library_db),
            ]
        )

        enrich_calls = []
        convert_calls = []

        def fake_enrich(lib_db, output, wxyc_url=None):
            enrich_calls.append((lib_db, output, wxyc_url))

        def fake_convert(xml, output_dir, converter, library_artists=None):
            convert_calls.append((xml, output_dir, converter, library_artists))

        with (
            patch.object(run_pipeline, "enrich_library_artists", side_effect=fake_enrich),
            patch.object(run_pipeline, "convert_and_filter", side_effect=fake_convert),
            patch.object(run_pipeline, "_run_database_build"),
            patch.object(run_pipeline, "parse_args", return_value=args),
        ):
            run_pipeline.main()

        assert len(enrich_calls) == 1, "enrich_library_artists should be called"
        assert len(convert_calls) == 1, "convert_and_filter should be called"
        # The generated artist list path should be passed to the converter
        assert convert_calls[0][3] is not None, (
            "library_artists path should be passed to convert_and_filter"
        )
