"""Unit tests for scripts/run_pipeline.py — streaming run_step() and arg parsing."""

from __future__ import annotations

import importlib.util
import json
import logging
import subprocess
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

    def test_nonzero_exit_raises_called_process_error(self) -> None:
        """Non-zero exit code raises CalledProcessError (#180).

        Earlier behavior was bare ``sys.exit(1)`` inside run_step, which is
        catchable by ``except SystemExit`` and was observed live to *not*
        terminate the process under Sentry-enabled logging on the 2026-05-10
        ephemeral-rebuild run #3 (instance ``i-0af07e0f56910ab9a``). Raising
        an exception propagates through ``main()``'s default handler, so a
        misbehaving logger plugin can capture-and-rethrow but not silently
        swallow the failure.
        """
        with pytest.raises(subprocess.CalledProcessError) as excinfo:
            run_pipeline.run_step(
                "fail test",
                [sys.executable, "-c", "import sys; sys.exit(42)"],
            )
        assert excinfo.value.returncode == 42

    def test_run_step_failure_propagates_to_process_exit_code(self, tmp_path) -> None:
        """End-to-end pin: a failing child step yields a non-zero process exit.

        Spawns a fresh Python interpreter, imports ``run_pipeline``, and
        invokes ``run_step`` against a child that exits 42. The outer
        subprocess must exit non-zero — this is the property that broke
        on the 2026-05-10 run #3 incident (#180), where the ERROR log
        fired but the process exited 0 anyway, and the ephemeral-rebuild
        bootstrap reported success.
        """
        script_path = Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py"
        driver = tmp_path / "driver.py"
        driver.write_text(
            "import importlib.util, sys\n"
            f"spec = importlib.util.spec_from_file_location('rp', r'{script_path}')\n"
            "mod = importlib.util.module_from_spec(spec)\n"
            "spec.loader.exec_module(mod)\n"
            "mod.run_step('boom', [sys.executable, '-c', 'import sys; sys.exit(42)'])\n"
        )
        result = subprocess.run(
            [sys.executable, str(driver)],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0, (
            f"run_step's child failure was swallowed; parent exit was "
            f"{result.returncode}. stderr:\n{result.stderr}"
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

    def test_truncate_existing_flag_parsed(self) -> None:
        """--truncate-existing flows through to import_csv subprocess to
        wipe stale rows before COPY. Use when re-running against a DB with
        partial state from a prior failed rebuild."""
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv", "--truncate-existing"])
        assert args.truncate_existing is True

    def test_truncate_existing_default_false(self) -> None:
        """Default is False — a fresh-DB rebuild doesn't need it and the
        wasted TRUNCATE DDL would slow the empty case."""
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.truncate_existing is False

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

    def test_target_db_url_emits_deprecation_warning(self, capsys) -> None:
        """--target-db-url is accepted but warns it is deprecated."""
        run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--library-db",
                "/tmp/library.db",
                "--target-db-url",
                "postgresql://localhost/target",
            ]
        )
        err = capsys.readouterr().err
        assert "--target-db-url" in err
        assert "deprecated" in err.lower()

    def test_database_url_flag_overrides_env(self, monkeypatch) -> None:
        """Explicit --database-url takes precedence over env vars."""
        monkeypatch.setenv("DATABASE_URL_DISCOGS", "postgresql://from-discogs-env/db")
        monkeypatch.setenv("DATABASE_URL", "postgresql://from-generic-env/db")
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--database-url",
                "postgresql://from-flag/db",
            ]
        )
        assert args.database_url == "postgresql://from-flag/db"

    def test_database_url_discogs_env_var_used(self, monkeypatch) -> None:
        """DATABASE_URL_DISCOGS is the preferred env fallback."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setenv("DATABASE_URL_DISCOGS", "postgresql://from-discogs-env/db")
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.database_url == "postgresql://from-discogs-env/db"

    def test_database_url_discogs_preferred_over_generic(self, monkeypatch) -> None:
        """DATABASE_URL_DISCOGS wins over DATABASE_URL when both are set."""
        monkeypatch.setenv("DATABASE_URL_DISCOGS", "postgresql://from-discogs-env/db")
        monkeypatch.setenv("DATABASE_URL", "postgresql://from-generic-env/db")
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.database_url == "postgresql://from-discogs-env/db"

    def test_database_url_generic_env_var_with_warning(self, monkeypatch, capsys) -> None:
        """Falling back to DATABASE_URL still works but emits a deprecation warning."""
        monkeypatch.delenv("DATABASE_URL_DISCOGS", raising=False)
        monkeypatch.setenv("DATABASE_URL", "postgresql://from-generic-env/db")
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.database_url == "postgresql://from-generic-env/db"
        err = capsys.readouterr().err
        assert "DATABASE_URL" in err
        assert "DATABASE_URL_DISCOGS" in err
        assert "deprecated" in err.lower()

    def test_database_url_default_when_no_env(self, monkeypatch) -> None:
        """Falls back to the built-in default when no env vars are set."""
        monkeypatch.delenv("DATABASE_URL_DISCOGS", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.database_url == "postgresql://localhost:5432/discogs"

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
        # One VACUUM FULL per pipeline table (derived from the constant so this
        # test stays in sync as PIPELINE_TABLES grows; see #105 for the
        # ``release_video`` addition).
        assert len(statements) == len(run_pipeline.PIPELINE_TABLES)
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
            "release_genre",
            "release_style",
            "release_track",
            "release_track_artist",
            "release_video",
            "cache_metadata",
        }
        assert set(run_pipeline.PIPELINE_TABLES) == expected

    def test_release_video_included(self) -> None:
        """Regression for #105: ``release_video`` must be in PIPELINE_TABLES.

        ``release_video`` has a FK to ``release``. PostgreSQL prohibits an
        UNLOGGED table from being referenced by a LOGGED table (and vice
        versa), so ``ALTER TABLE release SET UNLOGGED`` fails with
        ``could not change table "release" to unlogged because it
        references logged table "release_video"`` unless ``release_video``
        is toggled in lockstep with the other pipeline tables.
        """
        assert "release_video" in run_pipeline.PIPELINE_TABLES, (
            "release_video must be in PIPELINE_TABLES so set_tables_unlogged "
            "and set_tables_logged toggle it together with release; otherwise "
            "the FK constraint blocks the ALTER TABLE (see #105)."
        )

    def test_pipeline_tables_covers_all_release_fk_referrers(self) -> None:
        """Every table with a FK to ``release`` in create_database.sql must
        be in PIPELINE_TABLES, so the SET UNLOGGED / SET LOGGED toggles
        cannot leave the schema in a mixed-persistence state that PG
        rejects.

        This static check catches the next ``release_video``-style
        omission at unit-test time, before it manifests as an integration
        failure.
        """
        import re

        schema_sql = (
            Path(__file__).parent.parent.parent / "schema" / "create_database.sql"
        ).read_text()
        # Find every "CREATE TABLE <name> (" header followed by a body
        # containing "REFERENCES release(id)". This is intentionally
        # forgiving of whitespace.
        table_pattern = re.compile(
            r"CREATE\s+TABLE\s+(\w+)\s*\((.*?)\n\);",
            re.DOTALL | re.IGNORECASE,
        )
        referrers: set[str] = set()
        for match in table_pattern.finditer(schema_sql):
            name, body = match.group(1), match.group(2)
            if re.search(r"REFERENCES\s+release\s*\(\s*id\s*\)", body, re.IGNORECASE):
                referrers.add(name)
        assert referrers, "expected to find FK references to release(id)"
        # release itself is the parent and is always in PIPELINE_TABLES.
        missing = referrers - set(run_pipeline.PIPELINE_TABLES)
        assert not missing, (
            f"Tables with FK to release(id) missing from PIPELINE_TABLES: "
            f"{sorted(missing)}. They will block ALTER TABLE release "
            "SET UNLOGGED/LOGGED (see #105)."
        )

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

        def track_convert(
            xml, output_dir, converter, library_artists=None, database_url=None, **kwargs
        ):
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


class TestXmlModeLibraryDbForwarding:
    """In --xml mode, --library-db is forwarded straight to the converter so the
    converter applies its own pair-wise (artist, title) filter. This replaced
    the older auto-enrichment path (--library-db → library_artists.txt →
    converter --library-artists) once the converter learned --library-db."""

    def test_library_db_forwarded_to_converter(self, tmp_path) -> None:
        """When --library-db is provided, args.library_db lands in the
        convert_and_filter call so the converter does the pair-filter itself."""
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

        convert_calls = []

        def fake_convert(xml, output_dir, converter, **kwargs):
            convert_calls.append(kwargs)

        with (
            patch.object(run_pipeline, "convert_and_filter", side_effect=fake_convert),
            patch.object(run_pipeline, "_run_database_build"),
            patch.object(run_pipeline, "parse_args", return_value=args),
        ):
            run_pipeline.main()

        assert len(convert_calls) == 1, "convert_and_filter should be called"
        assert convert_calls[0].get("library_db") == library_db, (
            "library_db must be forwarded to convert_and_filter so the converter "
            "applies its --library-db pair filter"
        )

    def test_library_artists_forwarded_when_no_library_db(self, tmp_path) -> None:
        """When --library-artists is provided alone (no --library-db), the
        operator-supplied artist list flows straight to the converter as before."""
        xml_file = tmp_path / "releases.xml.gz"
        xml_file.touch()
        prebuilt_artists = tmp_path / "library_artists.txt"
        prebuilt_artists.write_text("Juana Molina\nStereolab\n")

        args = run_pipeline.parse_args(
            [
                "--xml",
                str(xml_file),
                "--library-artists",
                str(prebuilt_artists),
            ]
        )

        convert_calls = []

        def fake_convert(xml, output_dir, converter, **kwargs):
            convert_calls.append(kwargs)

        with (
            patch.object(run_pipeline, "convert_and_filter", side_effect=fake_convert),
            patch.object(run_pipeline, "_run_database_build"),
            patch.object(run_pipeline, "parse_args", return_value=args),
        ):
            run_pipeline.main()

        assert len(convert_calls) == 1
        assert convert_calls[0].get("library_artists") == prebuilt_artists
        assert convert_calls[0].get("library_db") is None


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

    def test_build_subcommand_when_no_database_url(self) -> None:
        """CSV mode dispatches to the converter's `build` subcommand."""
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
        # Subcommand is the first positional arg after the binary, before any flags.
        assert cmd[1] == "build"
        assert "/data/releases.xml.gz" in cmd
        # New CLI uses --data-dir; --output-dir is the deprecated alias.
        assert "--data-dir" in cmd
        assert "--output-dir" not in cmd
        assert "--library-artists" in cmd
        assert "/data/library_artists.txt" in cmd

    def test_import_subcommand_when_database_url_set(self) -> None:
        """Direct-PG mode dispatches to the converter's `import` subcommand
        and includes --database-url."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.convert_and_filter(
                Path("/data/releases.xml.gz"),
                Path("/tmp/csv"),
                "discogs-xml-converter",
                database_url="postgresql:///discogs",
            )

        cmd = mock_run.call_args[0][1]
        assert cmd[1] == "import"
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
        assert cmd[1] == "build"
        assert "--library-artists" not in cmd
        assert "--database-url" not in cmd
        description = mock_run.call_args[0][0]
        assert "CSV" in description

    def test_xml_type_forwarded_when_set(self) -> None:
        """When xml_type is provided, --xml-type is forwarded to the converter
        so it can skip per-file root-element auto-detection. Required for FIFO
        inputs where the auto-detect open/close kills the upstream writer."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.convert_and_filter(
                Path("/data/releases.xml.gz"),
                Path("/tmp/csv"),
                "discogs-xml-converter",
                xml_type="releases",
            )

        cmd = mock_run.call_args[0][1]
        assert "--xml-type" in cmd
        assert "releases" in cmd

    def test_xml_type_omitted_when_not_set(self) -> None:
        """When xml_type is None, --xml-type is not forwarded; the converter
        falls back to its default auto-detection."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.convert_and_filter(
                Path("/data/releases.xml.gz"),
                Path("/tmp/csv"),
                "discogs-xml-converter",
            )

        cmd = mock_run.call_args[0][1]
        assert "--xml-type" not in cmd

    def test_library_db_forwarded_when_set(self) -> None:
        """library_db is forwarded as --library-db to the converter so it can
        run its built-in pair-wise (artist, title) filter, narrowing release
        output from ~4M to ~50K. Mirrors the --xml-type forwarding pattern.
        Replaces the old run_pipeline-side pair_filter_csvs() post-pass."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.convert_and_filter(
                Path("/data/releases.xml.gz"),
                Path("/tmp/csv"),
                "discogs-xml-converter",
                library_db=Path("/data/library.db"),
            )

        cmd = mock_run.call_args[0][1]
        assert "--library-db" in cmd
        assert "/data/library.db" in cmd
        assert "--library-artists" not in cmd

    def test_library_db_omitted_when_not_set(self) -> None:
        """library_db is None → no --library-db on the converter command."""
        with patch.object(run_pipeline, "run_step") as mock_run:
            run_pipeline.convert_and_filter(
                Path("/data/releases.xml.gz"),
                Path("/tmp/csv"),
                "discogs-xml-converter",
            )

        cmd = mock_run.call_args[0][1]
        assert "--library-db" not in cmd


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

    def test_resume_without_state_file_infers_from_database(self, tmp_path) -> None:
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

        mock_state = run_pipeline.PipelineState(
            db_url="postgresql:///test",
            csv_dir=str(csv_dir.resolve()),
            steps=run_pipeline.STEP_NAMES,
        )
        mock_state.mark_completed("create_schema")

        with patch.object(run_pipeline, "_infer_pipeline_state", return_value=mock_state):
            state = run_pipeline._load_or_create_state(args)

        assert state.is_completed("create_schema")

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

    def test_generate_library_db_without_catalog_source_exits(self) -> None:
        """--generate-library-db requires --catalog-source (or --wxyc-db-url)."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(
                [
                    "--csv-dir",
                    "/tmp/csv",
                    "--generate-library-db",
                ]
            )

    def test_library_artists_and_library_db_mutually_exclusive(self) -> None:
        """--library-artists and --library-db pick different filter strategies on
        the converter side (artist-only ~4M vs pair ~50K). The converter rejects
        both at once, and so does run_pipeline so the operator gets the error
        before the multi-GB dump download starts."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(
                [
                    "--xml",
                    "/tmp/dump.xml.gz",
                    "--library-artists",
                    "/tmp/library_artists.txt",
                    "--library-db",
                    "/tmp/library.db",
                ]
            )

    def test_pair_filter_flag_is_gone(self) -> None:
        """--pair-filter was removed when the converter learned --library-db.
        The pair-wise narrowing now happens inside the converter's streaming
        scanner; no separate post-CSV pass exists in run_pipeline."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(
                [
                    "--xml",
                    "/tmp/dump.xml.gz",
                    "--library-db",
                    "/tmp/library.db",
                    "--pair-filter",
                ]
            )
