"""Integration tests for #493: count only the pins this rebuild lost.

``lib.pin_loss`` records, before the first destructive statement, which pinned
overrides (``lml_cache.library_release_override``) have a ``release`` row, in
the ``release_pin_start`` scratch table. After the api_fetch restore it
measures which of those still-pinned ids have lost their row. A pin that was
never cached is never in the start set, so a backlog of them cannot read as a
loss. The rebuild itself is simulated here by deleting and re-inserting
``release`` rows between the two calls.
"""

from __future__ import annotations

import logging

import psycopg
import pytest

from lib.pin_loss import START_TABLE, PinLoss, measure_pin_loss, record_pin_start_state

pytestmark = pytest.mark.pg

# KEPT survives the rebuild, LOST is deleted by it, NEVER_CACHED is pinned but
# never had a release row (the #446 Draft/deleted pins, or a backlog).
KEPT, LOST, NEVER_CACHED = 101, 202, 303


def _execute(db_url: str, *statements: str) -> None:
    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)


def _start_table_exists(db_url: str) -> bool:
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (START_TABLE,))
        return cur.fetchone()[0] is not None


@pytest.fixture()
def cache(fresh_db_url):
    """A cache holding KEPT and LOST, with all three ids pinned."""
    _execute(
        fresh_db_url,
        "CREATE TABLE release "
        "(id integer PRIMARY KEY, title text, not_found boolean NOT NULL DEFAULT FALSE)",
        f"INSERT INTO release VALUES ({KEPT}, 'Aluminum Tunes'), ({LOST}, 'DOGA')",
        "CREATE SCHEMA lml_cache",
        "CREATE TABLE lml_cache.library_release_override "
        "(library_id integer NOT NULL, discogs_release_id integer, source text NOT NULL)",
        "INSERT INTO lml_cache.library_release_override VALUES "
        f"(1, {KEPT}, 's'), (2, {LOST}, 's'), (3, {NEVER_CACHED}, 's'), (4, NULL, 's'), "
        f"(5, {KEPT}, 'duplicate pin')",
    )
    return fresh_db_url


class TestMeasurePinLoss:
    @pytest.mark.parametrize(
        ("during_run", "lost", "at_start"),
        [
            pytest.param([f"DELETE FROM release WHERE id = {LOST}"], [LOST], 2, id="lost-pin"),
            pytest.param([], [], 2, id="all-kept"),
            pytest.param(
                [
                    f"DELETE FROM release WHERE id = {LOST}",
                    f"INSERT INTO release (id, title) VALUES ({LOST}, 'DOGA')",
                ],
                [],
                2,
                id="restored-after-loss",
            ),
            pytest.param(
                [
                    f"DELETE FROM release WHERE id = {LOST}",
                    "DELETE FROM lml_cache.library_release_override "
                    f"WHERE discogs_release_id = {LOST}",
                ],
                [],
                1,
                id="unpinned-mid-run",
            ),
            pytest.param(
                [f"INSERT INTO release (id, title) VALUES ({NEVER_CACHED}, 'Moon Pix')"],
                [],
                2,
                id="cached-mid-run-not-in-start-set",
            ),
        ],
    )
    def test_counts_only_pins_present_at_start(self, cache, during_run, lost, at_start) -> None:
        assert record_pin_start_state(cache) is None
        _execute(cache, *during_run)

        assert measure_pin_loss(cache) == PinLoss(lost=lost, at_start=at_start)
        assert not _start_table_exists(cache), "the scratch table is dropped after the check"

    def test_never_cached_pin_is_never_counted(self, cache) -> None:
        """Every pin with no row at start stays out of both counts, however many."""
        _execute(
            cache,
            "INSERT INTO lml_cache.library_release_override "
            "SELECT g, g + 10000, 'backlog' FROM generate_series(1, 500) g",
        )
        record_pin_start_state(cache)
        assert measure_pin_loss(cache) == PinLoss(lost=[], at_start=2)

    def test_no_recorded_start_state_skips_the_check(self, cache) -> None:
        assert measure_pin_loss(cache) is None

    @pytest.mark.parametrize(
        "missing",
        ["DROP TABLE release", "DROP SCHEMA lml_cache CASCADE"],
        ids=["no-release-table", "no-lml-cache-schema"],
    )
    def test_missing_tables_record_an_empty_start_set(self, cache, missing) -> None:
        _execute(cache, missing)
        assert record_pin_start_state(cache) is None
        assert _start_table_exists(cache)
        assert measure_pin_loss(cache) == PinLoss(lost=[], at_start=0)

    def test_a_table_dropped_after_the_record_reads_as_no_loss(self, cache) -> None:
        record_pin_start_state(cache)
        _execute(cache, "DROP SCHEMA lml_cache CASCADE")
        assert measure_pin_loss(cache) == PinLoss(lost=[], at_start=0)

    def test_a_release_table_gone_since_the_record_loses_every_pin(self, cache) -> None:
        """A run that died between --fresh-rebuild's drop and create_schema lost them all."""
        record_pin_start_state(cache)
        _execute(cache, "DROP TABLE release")
        assert measure_pin_loss(cache) == PinLoss(lost=[KEPT, LOST], at_start=2)

    def test_a_tombstone_is_not_a_pin_present_at_start(self, cache) -> None:
        """LML's Discogs-404 tombstone (not_found) is never snapshotted, so no restore
        brings it back; losing it is not the #422 class."""
        _execute(cache, f"UPDATE release SET not_found = TRUE WHERE id = {LOST}")
        record_pin_start_state(cache)
        _execute(cache, f"DELETE FROM release WHERE id = {LOST}")
        assert measure_pin_loss(cache) == PinLoss(lost=[], at_start=1)

    def test_a_failed_check_keeps_the_table_for_the_next_run(self, cache) -> None:
        record_pin_start_state(cache)
        _execute(cache, f"ALTER TABLE {START_TABLE} RENAME release_id TO rid")
        assert measure_pin_loss(cache) is None
        assert _start_table_exists(cache)

    def test_a_drop_that_cannot_get_its_lock_still_returns_the_loss(
        self, cache, monkeypatch
    ) -> None:
        monkeypatch.setattr("lib.pin_loss.DROP_LOCK_TIMEOUT", "100ms")
        record_pin_start_state(cache)
        _execute(cache, f"DELETE FROM release WHERE id = {LOST}")
        with psycopg.connect(cache) as holder:
            holder.execute(f"LOCK TABLE {START_TABLE} IN ACCESS SHARE MODE")
            assert measure_pin_loss(cache) == PinLoss(lost=[LOST], at_start=2)
        assert _start_table_exists(cache), "kept, so the next run reports it again"

    def test_any_other_error_aborts_the_record(self, cache) -> None:
        """A column typo or missing grant must not read as 'no pins'."""
        _execute(
            cache,
            "ALTER TABLE lml_cache.library_release_override RENAME discogs_release_id TO rid",
        )
        with pytest.raises(psycopg.errors.UndefinedColumn):
            record_pin_start_state(cache)


class TestLeftoverStartState:
    def test_previous_run_left_its_start_state_behind(self, cache) -> None:
        """A crashed run's losses are returned for reporting, then re-recorded fresh."""
        record_pin_start_state(cache)
        _execute(cache, f"DELETE FROM release WHERE id = {LOST}")  # the run dies here

        assert record_pin_start_state(cache) == PinLoss(lost=[LOST], at_start=2)
        # The fresh start set is the cache as it is now: LOST is no longer in it.
        _execute(cache, f"DELETE FROM release WHERE id = {KEPT}")
        assert measure_pin_loss(cache) == PinLoss(lost=[KEPT], at_start=1)

    def test_the_loss_is_reported_before_the_table_is_replaced(self, cache) -> None:
        """If the re-record fails, the crashed run's table and its loss survive."""
        record_pin_start_state(cache)
        _execute(cache, f"DELETE FROM release WHERE id = {LOST}")  # the run dies here
        reported: list[PinLoss] = []

        def report_then_fail(loss: PinLoss) -> None:
            reported.append(loss)
            assert _start_table_exists(cache), "reported while the table is still there"
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            record_pin_start_state(cache, report_previous=report_then_fail)
        assert reported == [PinLoss(lost=[LOST], at_start=2)]
        assert measure_pin_loss(cache) == PinLoss(lost=[LOST], at_start=2)

    def test_a_leftover_that_cannot_be_measured_aborts_and_is_kept(self, cache) -> None:
        """Nothing is destroyed yet, so abort rather than re-record over the evidence."""
        record_pin_start_state(cache)
        _execute(cache, f"ALTER TABLE {START_TABLE} RENAME release_id TO rid")
        with pytest.raises(psycopg.errors.UndefinedColumn):
            record_pin_start_state(cache)
        with psycopg.connect(cache) as conn:
            columns = conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                (START_TABLE,),
            ).fetchall()
        assert columns == [("rid",)]

    def test_resume_reuses_the_original_start_state(self, cache) -> None:
        record_pin_start_state(cache)
        _execute(cache, f"DELETE FROM release WHERE id = {LOST}")  # the run dies here

        assert record_pin_start_state(cache, reuse_existing=True) is None
        assert measure_pin_loss(cache) == PinLoss(lost=[LOST], at_start=2)

    def test_resume_without_start_state_skips_the_check(self, cache, caplog) -> None:
        with caplog.at_level(logging.WARNING, logger="lib.pin_loss"):
            assert record_pin_start_state(cache, reuse_existing=True) is None
        assert "start state unknown" in caplog.text
        assert not _start_table_exists(cache)
        assert measure_pin_loss(cache) is None
