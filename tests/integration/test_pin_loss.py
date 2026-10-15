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
import re
import threading
from contextlib import contextmanager

import psycopg
import pytest

from lib.pin_loss import (
    START_TABLE,
    PinLoss,
    drop_pin_start_state,
    measure_pin_loss,
    record_pin_start_state,
)

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


def _record(db_url: str, unmeasured: list | None = None, **kwargs) -> list[PinLoss]:
    """Record the start state; return what it reported for a left-behind table.

    An unmeasured leftover is appended to *unmeasured*; without that list it
    fails the test.
    """
    reported: list[PinLoss] = []

    def report_unmeasured(renamed_to, reason) -> None:
        assert unmeasured is not None, f"unexpected unmeasured leftover: {reason}"
        unmeasured.append((renamed_to, reason))

    record_pin_start_state(
        db_url, report_previous=reported.append, report_unmeasured=report_unmeasured, **kwargs
    )
    return reported


def _columns(db_url: str, table: str) -> list[str]:
    with psycopg.connect(db_url) as conn:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table,),
        ).fetchall()
    return [row[0] for row in rows]


@contextmanager
def _start_table_locked(db_url: str):
    """Hold ACCESS SHARE on the start table; let go after 5 s, so an unbounded
    wait fails the test instead of hanging it."""
    with psycopg.connect(db_url) as holder:
        holder.execute(f"LOCK TABLE {START_TABLE} IN ACCESS SHARE MODE")
        release = threading.Timer(5, holder.rollback)
        release.start()
        try:
            yield
        finally:
            release.cancel()


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
        assert _record(cache) == []
        _execute(cache, *during_run)

        assert measure_pin_loss(cache) == PinLoss(lost=lost, at_start=at_start)
        assert _start_table_exists(cache), "kept until drop_pin_start_state, after set_logged"
        drop_pin_start_state(cache)
        assert not _start_table_exists(cache)

    def test_never_cached_pin_is_never_counted(self, cache) -> None:
        """Every pin with no row at start stays out of both counts, however many."""
        _execute(
            cache,
            "INSERT INTO lml_cache.library_release_override "
            "SELECT g, g + 10000, 'backlog' FROM generate_series(1, 500) g",
        )
        _record(cache)
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
        assert _record(cache) == []
        assert _start_table_exists(cache)
        assert measure_pin_loss(cache) == PinLoss(lost=[], at_start=0)

    def test_a_table_dropped_after_the_record_reads_as_no_loss(self, cache) -> None:
        _record(cache)
        _execute(cache, "DROP SCHEMA lml_cache CASCADE")
        assert measure_pin_loss(cache) == PinLoss(lost=[], at_start=0)

    def test_a_release_table_gone_since_the_record_loses_every_pin(self, cache) -> None:
        """A run that died between --fresh-rebuild's drop and create_schema lost them all."""
        _record(cache)
        _execute(cache, "DROP TABLE release")
        assert measure_pin_loss(cache) == PinLoss(lost=[KEPT, LOST], at_start=2)

    def test_a_tombstone_is_not_a_pin_present_at_start(self, cache) -> None:
        """Losing LML's Discogs-404 tombstone (not_found) costs one 404 re-fetch and
        serves no data, so it is not the #422 class."""
        _execute(cache, f"UPDATE release SET not_found = TRUE WHERE id = {LOST}")
        _record(cache)
        _execute(cache, f"DELETE FROM release WHERE id = {LOST}")
        assert measure_pin_loss(cache) == PinLoss(lost=[], at_start=1)

    def test_a_failed_check_keeps_the_table_for_the_next_run(self, cache) -> None:
        _record(cache)
        _execute(cache, f"ALTER TABLE {START_TABLE} RENAME release_id TO rid")
        assert measure_pin_loss(cache) is None
        assert _start_table_exists(cache)

    def test_a_drop_that_cannot_get_its_lock_keeps_the_table(self, cache, monkeypatch) -> None:
        monkeypatch.setattr("lib.pin_loss.LOCK_TIMEOUT", "100ms")
        _record(cache)
        with _start_table_locked(cache):
            drop_pin_start_state(cache)  # never raises
        assert _start_table_exists(cache), "kept, so the next run reports it again"

    def test_any_other_error_aborts_the_record(self, cache) -> None:
        """A column typo or missing grant must not read as 'no pins'."""
        _execute(
            cache,
            "ALTER TABLE lml_cache.library_release_override RENAME discogs_release_id TO rid",
        )
        with pytest.raises(psycopg.errors.UndefinedColumn):
            _record(cache)


class TestLeftoverStartState:
    def test_previous_run_left_its_start_state_behind(self, cache) -> None:
        """A crashed run's losses are reported once, then re-recorded fresh."""
        _record(cache)
        _execute(cache, f"DELETE FROM release WHERE id = {LOST}")  # the run dies here

        assert _record(cache) == [PinLoss(lost=[LOST], at_start=2)]
        # The fresh start set is the cache as it is now: LOST is no longer in it.
        _execute(cache, f"DELETE FROM release WHERE id = {KEPT}")
        assert measure_pin_loss(cache) == PinLoss(lost=[KEPT], at_start=1)

    def test_the_loss_is_reported_before_the_table_is_replaced(self, cache) -> None:
        """If the re-record fails, the crashed run's table and its loss survive."""
        _record(cache)
        _execute(cache, f"DELETE FROM release WHERE id = {LOST}")  # the run dies here
        reported: list[PinLoss] = []

        def report_then_fail(loss: PinLoss) -> None:
            reported.append(loss)
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            record_pin_start_state(
                cache, report_previous=report_then_fail, report_unmeasured=pytest.fail
            )
        assert reported == [PinLoss(lost=[LOST], at_start=2)]
        assert measure_pin_loss(cache) == PinLoss(lost=[LOST], at_start=2)

    def test_a_locked_leftover_aborts_instead_of_waiting(self, cache, monkeypatch) -> None:
        monkeypatch.setattr("lib.pin_loss.LOCK_TIMEOUT", "100ms")
        _record(cache)
        with _start_table_locked(cache), pytest.raises(psycopg.errors.LockNotAvailable):
            _record(cache)
        assert _start_table_exists(cache)

    def test_an_unmeasurable_leftover_is_renamed_aside_and_the_run_carries_on(self, cache):
        """It is never dropped: renamed aside, reported, and a fresh start state recorded."""
        _record(cache)
        _execute(cache, f"ALTER TABLE {START_TABLE} RENAME release_id TO rid")
        unmeasured: list = []
        assert _record(cache, unmeasured=unmeasured) == []

        [(aside, reason)] = unmeasured
        assert re.fullmatch(rf"{START_TABLE}_unmeasured_\d{{14}}", aside)
        assert "UndefinedColumn" in reason
        assert _columns(cache, aside) == ["rid"], "the leftover is kept as it was"
        assert measure_pin_loss(cache) == PinLoss(lost=[], at_start=2)

    def test_a_leftover_that_cannot_be_renamed_either_aborts(self, cache, monkeypatch) -> None:
        monkeypatch.setattr("lib.pin_loss.LOCK_TIMEOUT", "100ms")
        _record(cache)
        _execute(cache, f"ALTER TABLE {START_TABLE} RENAME release_id TO rid")
        unmeasured: list = []
        with _start_table_locked(cache), pytest.raises(psycopg.errors.LockNotAvailable):
            _record(cache, unmeasured=unmeasured)

        [(renamed_to, reason)] = unmeasured
        assert renamed_to is None
        assert "UndefinedColumn" in reason and "LockNotAvailable" in reason
        assert _columns(cache, START_TABLE) == ["rid"], "never dropped"

    def test_resume_reuses_the_original_start_state(self, cache) -> None:
        _record(cache)
        _execute(cache, f"DELETE FROM release WHERE id = {LOST}")  # the run dies here

        assert _record(cache, reuse_existing=True) == []
        assert measure_pin_loss(cache) == PinLoss(lost=[LOST], at_start=2)

    def test_resume_without_start_state_skips_the_check(self, cache, caplog) -> None:
        with caplog.at_level(logging.WARNING, logger="lib.pin_loss"):
            assert _record(cache, reuse_existing=True) == []
        assert "start state unknown" in caplog.text
        assert not _start_table_exists(cache)
        assert measure_pin_loss(cache) is None
