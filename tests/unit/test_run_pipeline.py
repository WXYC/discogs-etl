"""Unit tests for scripts/run_pipeline.py — streaming run_step() and arg parsing."""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

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


class TestRunVacuum:
    """run_vacuum() delegates to run_sql_statements_parallel for parallel execution."""

    def test_vacuum_uses_parallel_execution(self) -> None:
        """run_vacuum should call run_sql_statements_parallel with VACUUM FULL statements."""
        from unittest.mock import patch

        with patch.object(run_pipeline, "run_sql_statements_parallel") as mock_parallel:
            run_pipeline.run_vacuum("postgresql:///test")

        mock_parallel.assert_called_once()
        args, kwargs = mock_parallel.call_args
        db_url, statements = args[0], args[1]
        assert db_url == "postgresql:///test"
        assert len(statements) == 6
        assert all(s.startswith("VACUUM FULL ") for s in statements)
        assert "VACUUM FULL release" in statements
        assert "VACUUM FULL cache_metadata" in statements
        assert kwargs.get("description") or args[2] if len(args) > 2 else True


class TestPipelineTables:
    """PIPELINE_TABLES constant is shared between run_vacuum and set_tables_*."""

    def test_pipeline_tables_matches_vacuum_tables(self) -> None:
        """PIPELINE_TABLES should contain the same tables used by run_vacuum."""
        expected = {
            "release",
            "release_artist",
            "release_label",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        }
        assert set(run_pipeline.PIPELINE_TABLES) == expected

    def test_run_vacuum_uses_pipeline_tables(self) -> None:
        """run_vacuum should generate VACUUM FULL from PIPELINE_TABLES."""
        from unittest.mock import patch

        with patch.object(run_pipeline, "run_sql_statements_parallel") as mock_parallel:
            run_pipeline.run_vacuum("postgresql:///test")

        statements = mock_parallel.call_args[0][1]
        vacuum_tables = {s.replace("VACUUM FULL ", "") for s in statements}
        assert vacuum_tables == set(run_pipeline.PIPELINE_TABLES)


class TestSetTablesUnlogged:
    """set_tables_unlogged() generates ALTER TABLE SET UNLOGGED in FK order."""

    def test_children_first_then_parent(self) -> None:
        """Children are set UNLOGGED before the parent (release) for FK ordering."""
        from unittest.mock import patch

        with patch.object(run_pipeline, "run_sql_statements_parallel") as mock_parallel:
            run_pipeline.set_tables_unlogged("postgresql:///test")

        assert mock_parallel.call_count == 2
        # First call: child tables
        child_stmts = mock_parallel.call_args_list[0][0][1]
        assert all("SET UNLOGGED" in s for s in child_stmts)
        assert not any(
            "release" == s.split()[-2] for s in child_stmts if s.endswith("SET UNLOGGED")
        )
        # Second call: parent table
        parent_stmts = mock_parallel.call_args_list[1][0][1]
        assert parent_stmts == ["ALTER TABLE release SET UNLOGGED"]

    def test_all_tables_covered(self) -> None:
        """All PIPELINE_TABLES are included across both phases."""
        from unittest.mock import patch

        with patch.object(run_pipeline, "run_sql_statements_parallel") as mock_parallel:
            run_pipeline.set_tables_unlogged("postgresql:///test")

        all_stmts = []
        for c in mock_parallel.call_args_list:
            all_stmts.extend(c[0][1])
        tables = {s.split()[2] for s in all_stmts}
        assert tables == set(run_pipeline.PIPELINE_TABLES)

    def test_descriptions_contain_unlogged(self) -> None:
        from unittest.mock import patch

        with patch.object(run_pipeline, "run_sql_statements_parallel") as mock_parallel:
            run_pipeline.set_tables_unlogged("postgresql:///test")

        for c in mock_parallel.call_args_list:
            desc = c[1].get("description", c[0][2] if len(c[0]) > 2 else "")
            assert "UNLOGGED" in desc


class TestSetTablesLogged:
    """set_tables_logged() generates ALTER TABLE SET LOGGED in FK order."""

    def test_parent_first_then_children(self) -> None:
        """Parent (release) is set LOGGED before children for FK ordering."""
        from unittest.mock import patch

        with patch.object(run_pipeline, "run_sql_statements_parallel") as mock_parallel:
            run_pipeline.set_tables_logged("postgresql:///test")

        assert mock_parallel.call_count == 2
        # First call: parent table
        parent_stmts = mock_parallel.call_args_list[0][0][1]
        assert parent_stmts == ["ALTER TABLE release SET LOGGED"]
        # Second call: child tables
        child_stmts = mock_parallel.call_args_list[1][0][1]
        assert all("SET LOGGED" in s for s in child_stmts)
        assert len(child_stmts) == len(run_pipeline.PIPELINE_TABLES) - 1

    def test_all_tables_covered(self) -> None:
        """All PIPELINE_TABLES are included across both phases."""
        from unittest.mock import patch

        with patch.object(run_pipeline, "run_sql_statements_parallel") as mock_parallel:
            run_pipeline.set_tables_logged("postgresql:///test")

        all_stmts = []
        for c in mock_parallel.call_args_list:
            all_stmts.extend(c[0][1])
        tables = {s.split()[2] for s in all_stmts}
        assert tables == set(run_pipeline.PIPELINE_TABLES)

    def test_descriptions_contain_logged(self) -> None:
        from unittest.mock import patch

        with patch.object(run_pipeline, "run_sql_statements_parallel") as mock_parallel:
            run_pipeline.set_tables_logged("postgresql:///test")

        for c in mock_parallel.call_args_list:
            desc = c[1].get("description", c[0][2] if len(c[0]) > 2 else "")
            assert "LOGGED" in desc


class TestDirectPgUnloggedBeforeConverter:
    """In --direct-pg mode, set_tables_unlogged is called before the converter."""

    def test_unlogged_before_convert_and_filter(self, tmp_path) -> None:
        """set_tables_unlogged must be called before convert_and_filter in direct-PG mode."""
        xml_file = tmp_path / "releases.xml.gz"
        xml_file.touch()

        args = run_pipeline.parse_args(["--xml", str(xml_file), "--direct-pg"])

        call_order = []

        def track_set_unlogged(db_url):
            call_order.append("set_tables_unlogged")

        def track_convert(xml, output_dir, converter, library_artists=None, database_url=None):
            call_order.append("convert_and_filter")

        with (
            patch.object(run_pipeline, "parse_args", return_value=args),
            patch.object(run_pipeline, "wait_for_postgres"),
            patch.object(run_pipeline, "run_sql_file"),
            patch.object(run_pipeline.psycopg, "connect") as mock_conn,
            patch.object(run_pipeline, "set_tables_unlogged", side_effect=track_set_unlogged),
            patch.object(run_pipeline, "convert_and_filter", side_effect=track_convert),
            patch.object(run_pipeline, "_run_database_build_post_import"),
        ):
            mock_cursor = mock_conn.return_value.__enter__.return_value.cursor.return_value
            mock_cursor.__enter__ = lambda self: self
            mock_cursor.__exit__ = lambda self, *a: None
            run_pipeline.main()

        assert "set_tables_unlogged" in call_order, "set_tables_unlogged should be called"
        assert "convert_and_filter" in call_order, "convert_and_filter should be called"
        unlogged_idx = call_order.index("set_tables_unlogged")
        convert_idx = call_order.index("convert_and_filter")
        assert unlogged_idx < convert_idx, (
            f"set_tables_unlogged (index {unlogged_idx}) must come before "
            f"convert_and_filter (index {convert_idx})"
        )


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


# ---------------------------------------------------------------------------
# wait_for_postgres
# ---------------------------------------------------------------------------


class TestWaitForPostgres:
    """wait_for_postgres() polls until Postgres is ready or times out."""

    def test_success_on_first_try(self) -> None:
        """Successful connection on the first attempt returns immediately."""
        mock_conn = MagicMock()
        with patch.object(run_pipeline.psycopg, "connect", return_value=mock_conn):
            run_pipeline.wait_for_postgres("postgresql:///test")
        mock_conn.close.assert_called_once()

    def test_retry_then_success(self) -> None:
        """First call raises OperationalError, second succeeds."""
        mock_conn = MagicMock()
        with (
            patch.object(
                run_pipeline.psycopg,
                "connect",
                side_effect=[run_pipeline.psycopg.OperationalError("refused"), mock_conn],
            ),
            patch.object(run_pipeline.time, "sleep"),
        ):
            run_pipeline.wait_for_postgres("postgresql:///test")
        mock_conn.close.assert_called_once()

    def test_timeout_exits(self) -> None:
        """All connection attempts fail and timeout is exceeded -> sys.exit(1)."""
        # monotonic returns: first call sets deadline, subsequent calls exceed it
        with (
            patch.object(
                run_pipeline.psycopg,
                "connect",
                side_effect=run_pipeline.psycopg.OperationalError("refused"),
            ),
            patch.object(run_pipeline.time, "monotonic", side_effect=[0.0, 100.0]),
            patch.object(run_pipeline.time, "sleep"),
            pytest.raises(SystemExit, match="1"),
        ):
            run_pipeline.wait_for_postgres("postgresql:///test")


# ---------------------------------------------------------------------------
# run_sql_file
# ---------------------------------------------------------------------------


class TestRunSqlFile:
    """run_sql_file() executes SQL from a file against the database."""

    def test_happy_path(self, tmp_path) -> None:
        """SQL file contents are executed via cursor."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE t (id int)")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(run_pipeline.psycopg, "connect", return_value=mock_conn):
            run_pipeline.run_sql_file("postgresql:///test", sql_file)

        mock_cursor.execute.assert_called_once_with("CREATE TABLE t (id int)")
        mock_conn.close.assert_called_once()

    def test_sql_error_exits(self, tmp_path) -> None:
        """psycopg.Error during execution triggers sys.exit(1)."""
        sql_file = tmp_path / "bad.sql"
        sql_file.write_text("INVALID SQL")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = run_pipeline.psycopg.Error("syntax error")
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(run_pipeline.psycopg, "connect", return_value=mock_conn),
            pytest.raises(SystemExit, match="1"),
        ):
            run_pipeline.run_sql_file("postgresql:///test", sql_file)
        mock_conn.close.assert_called()

    def test_strip_concurrently_removes_keyword(self, tmp_path) -> None:
        """strip_concurrently=True removes CONCURRENTLY from SQL."""
        sql_file = tmp_path / "indexes.sql"
        sql_file.write_text("CREATE INDEX CONCURRENTLY idx_a ON t(a)")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(run_pipeline.psycopg, "connect", return_value=mock_conn):
            run_pipeline.run_sql_file("postgresql:///test", sql_file, strip_concurrently=True)

        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "CONCURRENTLY" not in executed_sql
        assert "CREATE INDEX idx_a ON t(a)" == executed_sql


# ---------------------------------------------------------------------------
# run_sql_statements_parallel — error propagation
# ---------------------------------------------------------------------------


class TestRunSqlStatementsParallelError:
    """Test that psycopg.Error from a parallel statement is re-raised."""

    def test_psycopg_error_is_reraised(self) -> None:
        """A psycopg.Error in a parallel statement propagates to the caller."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = run_pipeline.psycopg.Error("disk full")
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(run_pipeline.psycopg, "connect", return_value=mock_conn),
            pytest.raises(run_pipeline.psycopg.Error, match="disk full"),
        ):
            run_sql_statements_parallel("postgresql:///test", ["CREATE INDEX idx_x ON t(x)"])


# ---------------------------------------------------------------------------
# report_sizes
# ---------------------------------------------------------------------------


class TestReportSizes:
    """report_sizes() queries pg_stat_user_tables and logs results."""

    def test_logs_table_sizes(self, caplog) -> None:
        """Fetched rows are logged with table names and row counts."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("release", 50000, "120 MB"),
            ("release_artist", 80000, "45 MB"),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(run_pipeline.psycopg, "connect", return_value=mock_conn),
            caplog.at_level(logging.INFO, logger=run_pipeline.logger.name),
        ):
            run_pipeline.report_sizes("postgresql:///test")

        mock_cursor.execute.assert_called_once()
        logged = [r.message for r in caplog.records]
        assert any("release" in msg and "50,000" in msg for msg in logged)
        assert any("release_artist" in msg and "80,000" in msg for msg in logged)
        mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# convert_and_filter
# ---------------------------------------------------------------------------


class TestConvertAndFilter:
    """convert_and_filter() constructs the converter command and delegates to run_step."""

    def test_command_with_library_artists(self) -> None:
        """Command includes --library-artists when provided."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.convert_and_filter(
                Path("/data/releases.xml.gz"),
                Path("/tmp/csv"),
                "discogs-xml-converter",
                library_artists=Path("/data/library_artists.txt"),
            )

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][1]
        assert cmd[0] == "discogs-xml-converter"
        assert "/data/releases.xml.gz" in cmd
        assert "--output-dir" in cmd
        assert "--library-artists" in cmd
        assert "/data/library_artists.txt" in cmd

    def test_command_with_database_url(self) -> None:
        """Command includes --database-url for direct-PG mode."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.convert_and_filter(
                Path("/data/releases.xml.gz"),
                Path("/tmp/csv"),
                "discogs-xml-converter",
                database_url="postgresql:///discogs",
            )

        cmd = mock_run.call_args[0][1]
        assert "--database-url" in cmd
        assert "postgresql:///discogs" in cmd
        # Description mentions PostgreSQL
        description = mock_run.call_args[0][0]
        assert "PostgreSQL" in description

    def test_command_without_optional_args(self) -> None:
        """Command omits --library-artists and --database-url when not provided."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.convert_and_filter(
                Path("/data/releases.xml.gz"),
                Path("/tmp/csv"),
                "discogs-xml-converter",
            )

        cmd = mock_run.call_args[0][1]
        assert "--library-artists" not in cmd
        assert "--database-url" not in cmd
        description = mock_run.call_args[0][0]
        assert "CSV" in description


# ---------------------------------------------------------------------------
# enrich_library_artists (orchestrator wrapper)
# ---------------------------------------------------------------------------


class TestEnrichLibraryArtists:
    """enrich_library_artists() constructs the enrichment command."""

    def test_command_with_wxyc_db_url(self) -> None:
        """Command includes --wxyc-db-url when provided."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.enrich_library_artists(
                Path("/data/library.db"),
                Path("/tmp/library_artists.txt"),
                wxyc_db_url="mysql://user:pass@host/db",
            )

        cmd = mock_run.call_args[0][1]
        assert "--library-db" in cmd
        assert "/data/library.db" in cmd
        assert "--output" in cmd
        assert "/tmp/library_artists.txt" in cmd
        assert "--wxyc-db-url" in cmd
        assert "mysql://user:pass@host/db" in cmd

    def test_command_without_wxyc_db_url(self) -> None:
        """Command omits --wxyc-db-url when not provided."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.enrich_library_artists(
                Path("/data/library.db"),
                Path("/tmp/library_artists.txt"),
            )

        cmd = mock_run.call_args[0][1]
        assert "--library-db" in cmd
        assert "--output" in cmd
        assert "--wxyc-db-url" not in cmd


# ---------------------------------------------------------------------------
# _load_or_create_state
# ---------------------------------------------------------------------------


class TestLoadOrCreateState:
    """_load_or_create_state() handles resume modes."""

    def test_resume_with_existing_state_file(self, tmp_path) -> None:
        """When --resume and state file exists, load it."""
        state_file = tmp_path / "state.json"
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        # Write a valid state file
        state_data = {
            "version": 3,
            "database_url": "postgresql:///test",
            "csv_dir": str(csv_dir.resolve()),
            "steps": {s: {"status": "pending"} for s in run_pipeline.STEP_NAMES},
        }
        state_data["steps"]["create_schema"] = {"status": "completed"}
        state_file.write_text(json.dumps(state_data))

        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                str(csv_dir),
                "--resume",
                "--state-file",
                str(state_file),
                "--database-url",
                "postgresql:///test",
            ]
        )

        state = run_pipeline._load_or_create_state(args)
        assert state.is_completed("create_schema")
        assert not state.is_completed("import_csv")

    def test_resume_without_state_file_uses_db_introspect(self, tmp_path) -> None:
        """When --resume but no state file, infer from database."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        state_file = tmp_path / "nonexistent_state.json"

        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                str(csv_dir),
                "--resume",
                "--state-file",
                str(state_file),
                "--database-url",
                "postgresql:///test",
            ]
        )

        mock_state = run_pipeline.PipelineState(db_url="postgresql:///test", csv_dir="")
        mock_state.mark_completed("create_schema")

        with patch("lib.db_introspect.infer_pipeline_state", return_value=mock_state):
            state = run_pipeline._load_or_create_state(args)

        assert state.is_completed("create_schema")
        assert state.csv_dir == str(csv_dir.resolve())

    def test_fresh_state_no_resume(self, tmp_path) -> None:
        """Without --resume, create a fresh PipelineState."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                str(csv_dir),
                "--database-url",
                "postgresql:///test",
            ]
        )

        state = run_pipeline._load_or_create_state(args)
        assert not any(state.is_completed(s) for s in run_pipeline.STEP_NAMES)
        assert state.db_url == "postgresql:///test"


# ---------------------------------------------------------------------------
# main() — input validation
# ---------------------------------------------------------------------------


class TestMainValidation:
    """main() validates file paths before running the pipeline."""

    def test_missing_xml_file_exits(self, tmp_path) -> None:
        """Non-existent XML file triggers sys.exit(1)."""
        args = run_pipeline.parse_args(["--xml", str(tmp_path / "missing.xml.gz")])
        with (
            patch.object(run_pipeline, "parse_args", return_value=args),
            pytest.raises(SystemExit, match="1"),
        ):
            run_pipeline.main()

    def test_missing_library_artists_file_exits(self, tmp_path) -> None:
        """Non-existent library_artists.txt triggers sys.exit(1)."""
        xml_file = tmp_path / "releases.xml.gz"
        xml_file.touch()
        args = run_pipeline.parse_args(
            [
                "--xml",
                str(xml_file),
                "--library-artists",
                str(tmp_path / "missing_artists.txt"),
            ]
        )
        with (
            patch.object(run_pipeline, "parse_args", return_value=args),
            pytest.raises(SystemExit, match="1"),
        ):
            run_pipeline.main()

    def test_missing_csv_dir_exits(self, tmp_path) -> None:
        """Non-existent CSV directory triggers sys.exit(1)."""
        args = run_pipeline.parse_args(["--csv-dir", str(tmp_path / "missing_csv")])
        with (
            patch.object(run_pipeline, "parse_args", return_value=args),
            pytest.raises(SystemExit, match="1"),
        ):
            run_pipeline.main()

    def test_missing_library_db_exits(self, tmp_path) -> None:
        """Non-existent library.db triggers sys.exit(1)."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                str(csv_dir),
                "--library-db",
                str(tmp_path / "missing_library.db"),
            ]
        )
        with (
            patch.object(run_pipeline, "parse_args", return_value=args),
            pytest.raises(SystemExit, match="1"),
        ):
            run_pipeline.main()

    def test_missing_library_labels_exits(self, tmp_path) -> None:
        """Non-existent library_labels.csv triggers sys.exit(1)."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                str(csv_dir),
                "--library-labels",
                str(tmp_path / "missing_labels.csv"),
            ]
        )
        with (
            patch.object(run_pipeline, "parse_args", return_value=args),
            pytest.raises(SystemExit, match="1"),
        ):
            run_pipeline.main()


# ---------------------------------------------------------------------------
# parse_args — additional validation
# ---------------------------------------------------------------------------


class TestParseArgsValidation:
    """Additional argument validation in parse_args."""

    def test_direct_pg_without_xml_exits(self) -> None:
        """--direct-pg without --xml triggers parser.error (sys.exit(2))."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(["--csv-dir", "/tmp/csv", "--direct-pg"])

    def test_generate_library_db_with_library_db_exits(self) -> None:
        """--generate-library-db and --library-db are mutually exclusive."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(
                [
                    "--csv-dir",
                    "/tmp/csv",
                    "--generate-library-db",
                    "--library-db",
                    "/tmp/library.db",
                ]
            )
