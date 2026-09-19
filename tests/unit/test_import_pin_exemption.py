"""Unit tests for the import-seam pinned-release exemption (WXYC/discogs-etl#424).

``scripts/import_csv.py::import_release_via_upsert`` used to delete every
release absent from the incoming dump's staging table, with no exemption for
WXYC library pinned overrides (``lml_cache.library_release_override``). The
2026-09-04 monthly rebuild ran the converter without the Seam-A allowlist and
that prune deleted 27,286 of 58,012 pinned releases, mechanically undoing the
#329 backfill. #327's protection only ever covered the dedup and verify-prune
seams, so the import prune was an unguarded third seam.

These tests cover the pure logic and SQL shape; the live-Postgres behaviour
(a pinned id surviving, a control id not, and the guard firing before the
child TRUNCATE) is in ``tests/integration/test_import_pin_exemption.py``.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv_pin_exemption", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)


class TestPrunePinExemptionSql:
    """The pin-exempting prune must stay an indexed anti-join.

    ``PRUNE_STALE_RELEASES_SQL``'s ``NOT EXISTS`` form exists because
    PostgreSQL cannot plan ``NOT IN`` as an anti-join, and the resulting
    O(n*m) SubPlan stalled a rebuild for 2h20m (#298 / #302). The pin
    exemption adds a *second* anti-join, so the same constraint applies to
    both legs.
    """

    def test_exempting_prune_uses_two_not_exists_legs(self) -> None:
        sql = " ".join(_ic.PRUNE_STALE_RELEASES_EXEMPT_PINS_SQL.split()).lower()
        assert "not in (select" not in sql, "prune must not regress to a NOT IN subquery"
        assert sql.count("not exists") == 2, (
            "the exempting prune needs both anti-joins: absent-from-dump AND not-pinned; "
            f"got: {sql}"
        )
        assert "release_staging" in sql and "release_keep_ids" in sql

    def test_exempting_prune_only_deletes_from_release(self) -> None:
        sql = " ".join(_ic.PRUNE_STALE_RELEASES_EXEMPT_PINS_SQL.split()).lower()
        assert sql.startswith("delete from release r ")

    def test_staleness_marker_targets_retained_pins_only(self) -> None:
        """Retained pins lose their children (the child tables are TRUNCATEd)
        and must not be advertised to LML as a fresh hit.

        LML's ``is_pg_hit`` for a release is
        ``not_found or artwork_checked_at is not None or tracklist``, so a
        retained-but-childless row with a non-NULL ``artwork_checked_at``
        would be served with an empty tracklist and never re-fetched.
        NULLing the column restores the miss → API → re-hydrate path.
        """
        sql = " ".join(_ic.MARK_RETAINED_PINS_STALE_SQL.split()).lower()
        assert "update release" in sql
        assert "artwork_checked_at = null" in sql
        assert "release_keep_ids" in sql, "only pinned rows may be marked"
        assert "not exists" in sql and "release_staging" in sql, (
            "only pins the dump did NOT refresh may be marked stale"
        )
        assert "artwork_url" not in sql, "the artwork URL itself must survive"


class TestEvaluatePinShortfall:
    """The guard fires on the *share* of pins the dump is missing, not a count.

    An absolute floor would either be a hair-trigger (upstream churn deletes
    releases every month) or useless at scale. 2026-09-04 was 27,286/58,012 =
    47%; ordinary churn is single-digit percent.
    """

    @pytest.mark.parametrize(
        "present,missing,ratio,expect_reason",
        [
            (0, 0, 0.25, False),  # empty/fresh cache: nothing to lose
            (100, 0, 0.25, False),  # every pin refreshed by the dump
            (100, 25, 0.25, False),  # exactly at the threshold is allowed
            (100, 26, 0.25, True),  # over the threshold aborts
            (58012, 27286, 0.25, True),  # the 2026-09-04 rebuild
            (58012, 27286, 1.0, False),  # operator override disables the guard
        ],
    )
    def test_threshold(self, present, missing, ratio, expect_reason) -> None:
        reason = _ic.evaluate_pin_shortfall(
            pinned_present=present, pinned_missing=missing, max_ratio=ratio
        )
        assert (reason is not None) is expect_reason

    def test_reason_carries_both_counts_and_the_threshold(self) -> None:
        reason = _ic.evaluate_pin_shortfall(
            pinned_present=58012, pinned_missing=27286, max_ratio=0.25
        )
        assert reason is not None
        assert "27,286" in reason and "58,012" in reason
        assert "47" in reason  # the measured share


class TestResolveMaxPinShortfallRatio:
    def test_default_when_env_unset(self) -> None:
        assert _ic.resolve_max_pin_shortfall_ratio({}) == _ic.DEFAULT_MAX_PIN_SHORTFALL_RATIO

    def test_env_override(self) -> None:
        assert _ic.resolve_max_pin_shortfall_ratio({"MAX_PIN_SHORTFALL_RATIO": "0.6"}) == 0.6

    @pytest.mark.parametrize("raw", ["", "abc", "-0.1", "1.5"])
    def test_unusable_value_falls_back_to_default(self, raw, caplog) -> None:
        with caplog.at_level(logging.WARNING):
            ratio = _ic.resolve_max_pin_shortfall_ratio({"MAX_PIN_SHORTFALL_RATIO": raw})
        assert ratio == _ic.DEFAULT_MAX_PIN_SHORTFALL_RATIO
        if raw:
            assert "MAX_PIN_SHORTFALL_RATIO" in caplog.text


def _mock_conn():
    cursor = MagicMock()
    cursor.rowcount = 0
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


def _executed(cursor) -> list[str]:
    return [c.args[0] for c in cursor.execute.call_args_list if c.args]


class TestImportReleaseViaUpsertGuardOrdering:
    """The shortfall guard must run before the first destructive statement.

    The 2026-09-04 run *did* hit a guard — #357's copy-swap shortfall — but at
    the next seam down, long after the import prune had committed its DELETE.
    A guard that fires after the damage is a post-mortem, not a guard, so this
    one has to land before the child TRUNCATE (which is what makes a mid-run
    abort leave the release-full/children-empty state of #298).
    """

    def test_guard_raises_before_any_truncate_or_delete(self, tmp_path) -> None:
        (tmp_path / "release.csv").write_text("id,title\n1,X\n")
        conn, cursor = _mock_conn()
        with (
            patch.object(_ic, "import_csv", return_value=100),
            patch.object(_ic, "count_pins_missing_from_dump", return_value=(100, 90)),
            pytest.raises(_ic.PinnedReleaseShortfallError) as excinfo,
        ):
            _ic.import_release_via_upsert(conn, tmp_path, keep_release_ids={7, 8})

        assert "90" in str(excinfo.value)
        sql = " ".join(_executed(cursor)).lower()
        assert "truncate" not in sql, (
            "the guard fired after the child TRUNCATE — an abort there leaves the "
            "release-full / children-empty state of #298"
        )
        assert "delete from release" not in sql

    def test_pruned_ids_are_exempted_and_retained_pins_marked(self, tmp_path) -> None:
        (tmp_path / "release.csv").write_text("id,title\n1,X\n")
        conn, cursor = _mock_conn()
        with (
            patch.object(_ic, "import_csv", return_value=100),
            patch.object(_ic, "count_pins_missing_from_dump", return_value=(100, 3)),
        ):
            _ic.import_release_via_upsert(conn, tmp_path, keep_release_ids={7, 8})

        statements = _executed(cursor)
        joined = " ".join(" ".join(s.split()) for s in statements)
        assert _ic.PRUNE_STALE_RELEASES_EXEMPT_PINS_SQL in joined, (
            "a keep set must select the pin-exempting prune"
        )
        assert _ic.PRUNE_STALE_RELEASES_SQL not in statements, (
            "the unguarded prune must not also run"
        )
        assert _ic.MARK_RETAINED_PINS_STALE_SQL in joined, (
            "pins the dump did not refresh must be marked stale for LML re-hydration"
        )
        truncate_idx = next(i for i, s in enumerate(statements) if "TRUNCATE" in s)
        prune_idx = next(i for i, s in enumerate(statements) if "DELETE FROM release" in s)
        assert truncate_idx < prune_idx

    def test_no_keep_set_leaves_the_original_prune_untouched(self, tmp_path) -> None:
        """Without a keep set the seam must be byte-identical to pre-#424 — no
        keep table, no extra UPDATE, and the plan-pinned SQL unchanged."""
        (tmp_path / "release.csv").write_text("id,title\n1,X\n")
        conn, cursor = _mock_conn()
        with patch.object(_ic, "import_csv", return_value=100):
            _ic.import_release_via_upsert(conn, tmp_path)

        statements = _executed(cursor)
        assert _ic.PRUNE_STALE_RELEASES_SQL in statements
        joined = " ".join(statements).lower()
        assert "release_keep_ids" not in joined
        assert "artwork_checked_at = null" not in joined


class TestMainForwardsKeepReleaseIds:
    def test_base_only_passes_the_parsed_allowlist_to_the_upsert(self, tmp_path) -> None:
        keep_file = tmp_path / "keep_release_ids.txt"
        keep_file.write_text("101\n202\n")
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()

        argv = [
            "import_csv.py",
            "--base-only",
            "--keep-release-ids",
            str(keep_file),
            str(csv_dir),
            "postgresql:///discogs",
        ]
        with (
            patch.object(_ic.sys, "argv", argv),
            patch.object(_ic.psycopg, "connect", return_value=MagicMock()),
            patch.object(_ic, "import_release_via_upsert", return_value=1) as mock_upsert,
            patch.object(_ic, "_import_tables_parallel", return_value=0),
            patch.object(_ic, "import_artwork", return_value=0),
            patch.object(_ic, "populate_release_year", return_value=0),
            patch.object(_ic, "populate_cache_metadata", return_value=0),
            patch.object(_ic, "create_track_count_table", return_value=0),
            patch.object(_ic, "import_artist_details", return_value=0),
            patch.object(_ic, "_import_masters_best_effort", return_value=0),
        ):
            _ic.main()

        assert mock_upsert.call_args.kwargs["keep_release_ids"] == {101, 202}

    def test_absent_flag_means_no_allowlist(self, tmp_path) -> None:
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        argv = ["import_csv.py", "--base-only", str(csv_dir), "postgresql:///discogs"]
        with (
            patch.object(_ic.sys, "argv", argv),
            patch.object(_ic.psycopg, "connect", return_value=MagicMock()),
            patch.object(_ic, "import_release_via_upsert", return_value=1) as mock_upsert,
            patch.object(_ic, "_import_tables_parallel", return_value=0),
            patch.object(_ic, "import_artwork", return_value=0),
            patch.object(_ic, "populate_release_year", return_value=0),
            patch.object(_ic, "populate_cache_metadata", return_value=0),
            patch.object(_ic, "create_track_count_table", return_value=0),
            patch.object(_ic, "import_artist_details", return_value=0),
            patch.object(_ic, "_import_masters_best_effort", return_value=0),
        ):
            _ic.main()

        assert mock_upsert.call_args.kwargs["keep_release_ids"] is None


class TestMissingAllowlistFileIsRefused:
    """An explicitly-passed allowlist file that does not exist must abort, not
    silently disable both protections (#424 review, HIGH).

    ``parse_keep_release_ids`` returns ``set()`` for a nonexistent path -- by
    design, so "no override file" and "empty override file" read alike. But
    ``main`` then collapsed that empty set to ``None`` via ``or None``, and a
    falsy ``keep_ids`` means ``import_release_via_upsert`` runs the UNGUARDED
    ``PRUNE_STALE_RELEASES_SQL`` *and* skips ``evaluate_pin_shortfall``
    entirely -- with no log line anywhere.

    Two realistic triggers, both ending in the 2026-09-04 outcome: an operator
    typos the path in a manual ``--base-only`` invocation, or the parent's
    ``tempfile.mkdtemp()`` directory is not visible to this child process
    (TMPDIR is redirected onto the EBS ``$WORK_DIR`` on the rebuild host).

    The sibling seam already refuses exactly this, at
    ``scripts/dedup_releases.py``'s "fail fast, before opening the scratch-build
    transaction" check. The seam that actually caused the incident must not be
    the lenient one.
    """

    def _argv(self, keep_path: Path, csv_dir: Path, *extra: str) -> list[str]:
        return [
            "import_csv.py",
            "--base-only",
            *extra,
            "--keep-release-ids",
            str(keep_path),
            str(csv_dir),
            "postgresql:///discogs",
        ]

    def test_nonexistent_allowlist_aborts_before_any_destructive_work(self, tmp_path) -> None:
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        missing = tmp_path / "typo.txt"
        with (
            patch.object(_ic.sys, "argv", self._argv(missing, csv_dir)),
            patch.object(_ic.psycopg, "connect") as mock_connect,
            patch.object(_ic, "import_release_via_upsert") as mock_upsert,
            patch.object(_ic, "_import_tables_parallel") as mock_parallel,
            pytest.raises(SystemExit) as exc,
        ):
            _ic.main()
        assert exc.value.code != 0, "a missing allowlist must not exit 0"
        mock_upsert.assert_not_called()
        mock_parallel.assert_not_called()
        mock_connect.assert_not_called()

    def test_error_names_the_path_that_was_not_found(self, tmp_path, caplog) -> None:
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        missing = tmp_path / "typo.txt"
        with (
            caplog.at_level(logging.ERROR),
            patch.object(_ic.sys, "argv", self._argv(missing, csv_dir)),
            patch.object(_ic.psycopg, "connect"),
            pytest.raises(SystemExit),
        ):
            _ic.main()
        assert str(missing) in caplog.text

    def test_an_existing_but_empty_allowlist_is_still_allowed(self, tmp_path) -> None:
        """Empty is a legitimate state (no pins yet), and distinguishable from
        missing only because the file is there. It must keep working."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        empty = tmp_path / "keep.txt"
        empty.write_text("")
        with (
            patch.object(_ic.sys, "argv", self._argv(empty, csv_dir)),
            patch.object(_ic.psycopg, "connect", return_value=MagicMock()),
            patch.object(_ic, "import_release_via_upsert", return_value=1) as mock_upsert,
            patch.object(_ic, "_import_tables_parallel", return_value=0),
            patch.object(_ic, "import_artwork", return_value=0),
            patch.object(_ic, "populate_release_year", return_value=0),
            patch.object(_ic, "populate_cache_metadata", return_value=0),
            patch.object(_ic, "create_track_count_table", return_value=0),
            patch.object(_ic, "import_artist_details", return_value=0),
            patch.object(_ic, "_import_masters_best_effort", return_value=0),
        ):
            _ic.main()
        mock_upsert.assert_called_once()


class TestTruncatePathDoesNotImplyPinProtection:
    """``--truncate-existing`` takes the ``_import_tables_parallel`` branch,
    which never parses the allowlist, never stages ``release_keep_ids`` and
    never runs the shortfall guard -- while ``_truncate_tables`` wipes
    ``release`` wholesale (#424 review, MEDIUM).

    Pins survive that path only if Seam A put them in the CSVs; nothing on this
    side of the pipe checks. Accepting the flag silently let a rebuild destroy
    all 58K pins with the protective flag apparently applied. It must say so.
    """

    def test_combination_warns_that_the_exemption_does_not_apply(self, tmp_path, caplog) -> None:
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        keep_file = tmp_path / "keep.txt"
        keep_file.write_text("101\n")
        argv = [
            "import_csv.py",
            "--base-only",
            "--truncate-existing",
            "--keep-release-ids",
            str(keep_file),
            str(csv_dir),
            "postgresql:///discogs",
        ]
        with (
            caplog.at_level(logging.WARNING),
            patch.object(_ic.sys, "argv", argv),
            patch.object(_ic.psycopg, "connect", return_value=MagicMock()),
            patch.object(_ic, "_truncate_tables", return_value=None),
            patch.object(_ic, "_import_tables_parallel", return_value=0),
            patch.object(_ic, "import_artwork", return_value=0),
            patch.object(_ic, "populate_release_year", return_value=0),
            patch.object(_ic, "populate_cache_metadata", return_value=0),
            patch.object(_ic, "create_track_count_table", return_value=0),
            patch.object(_ic, "import_artist_details", return_value=0),
            patch.object(_ic, "_import_masters_best_effort", return_value=0),
        ):
            _ic.main()
        text = caplog.text.lower()
        assert "truncate" in text and "keep-release-ids" in text, (
            "the truncate path must warn that the prune exemption and the "
            f"shortfall guard are both inactive. Got: {caplog.text!r}"
        )
