"""Live-Postgres tests for the import-seam pinned-release exemption (#424).

The 2026-09-04 monthly rebuild deleted 27,286 of 58,012 WXYC library pinned
releases here — ``import_release_via_upsert``'s prune had no pin exemption, so
every pin the converter's ``(artist, title)`` filter missed was stale by
definition. These tests exercise the real DELETE against real rows:

* a pinned release absent from the incoming dump survives, and keeps its
  LML-back-patched ``artwork_url``;
* an otherwise-identical *unpinned* release absent from the dump is still
  deleted (the exemption is targeted, not a blanket bypass — the #327 test
  shape);
* the shortfall guard aborts before the child TRUNCATE, so a refused import
  leaves the cache exactly as it was.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv_pin_exemption_pg", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

import_release_via_upsert = _ic.import_release_via_upsert
PinnedReleaseShortfallError = _ic.PinnedReleaseShortfallError
PRUNE_STALE_RELEASES_EXEMPT_PINS_SQL = _ic.PRUNE_STALE_RELEASES_EXEMPT_PINS_SQL

pytestmark = pytest.mark.pg

# Canonical WXYC example artists (docs/test-fixtures.md).
PINNED_ID = 9101  # Chuquimamani-Condori — "Edits", pinned, off the artist filter
CONTROL_ID = 9102  # Cat Power — unpinned, equally absent from the dump
REFRESHED_ID = 9103  # Juana Molina — "DOGA", pinned and carried by the dump
# Three more pinned-and-carried releases (Jessica Pratt, Stereolab, Duke
# Ellington & John Coltrane) so the happy-path fixture looks like a real
# rebuild: 1 of 4 pins stranded = 25%, at the guard's ceiling rather than over
# it. A fixture where *every* pin is stranded would only ever exercise the
# guard, never the exemption.
REFRESHED_PIN_IDS = [9104, 9105, 9106]
ALL_PINS = {PINNED_ID, REFRESHED_ID, *REFRESHED_PIN_IDS}


class TestImportSeamPinExemption:
    @pytest.fixture(autouse=True)
    def _fresh_schema(self, fresh_db_url):
        self.db_url = fresh_db_url
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

    def _seed(self) -> None:
        """Cached releases with artwork + child rows, as a live cache has."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO release (id, title, artwork_url, artwork_checked_at) VALUES "
                "(%s, 'Edits', 'lml-artwork-pinned', '2026-08-01 00:00:00+00'), "
                "(%s, 'Moon Pix', 'lml-artwork-control', '2026-08-01 00:00:00+00'), "
                "(%s, 'DOGA', 'lml-artwork-refreshed', '2026-08-01 00:00:00+00')",
                (PINNED_ID, CONTROL_ID, REFRESHED_ID),
            )
            for release_id in REFRESHED_PIN_IDS:
                cur.execute(
                    "INSERT INTO release (id, title, artwork_url, artwork_checked_at) "
                    "VALUES (%s, 'Pinned and carried', 'lml-artwork', '2026-08-01 00:00:00+00')",
                    (release_id,),
                )
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_id, artist_name) VALUES "
                "(%s, 1, 'Chuquimamani-Condori'), (%s, 2, 'Cat Power'), (%s, 3, 'Juana Molina')",
                (PINNED_ID, CONTROL_ID, REFRESHED_ID),
            )
        conn.commit()
        conn.close()

    def _write_dump_csv(self, tmp_path: Path, ids: list[int]) -> None:
        """Converter output carrying only *ids* — i.e. the pair-wise filter's result."""
        lines = ["id,title,country,released,format,master_id"]
        for release_id in ids:
            lines.append(f"{release_id},DOGA,AR,2022,LP,")
        (tmp_path / "release.csv").write_text("\n".join(lines) + "\n")

    def _release_ids(self) -> set[int]:
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM release ORDER BY id")
            ids = {row[0] for row in cur.fetchall()}
        conn.close()
        return ids

    def _artwork(self, release_id: int) -> tuple:
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT artwork_url, artwork_checked_at FROM release WHERE id = %s",
                (release_id,),
            )
            row = cur.fetchone()
        conn.close()
        return row

    def test_pinned_release_survives_a_prune_that_deletes_its_control(self, tmp_path) -> None:
        """The acceptance criterion: a pinned id in the keep table is never
        deleted by the prune, while an unpinned sibling in the same state is."""
        self._seed()
        self._write_dump_csv(tmp_path, [REFRESHED_ID, *REFRESHED_PIN_IDS])

        conn = psycopg.connect(self.db_url)
        import_release_via_upsert(conn, tmp_path, keep_release_ids=ALL_PINS)
        conn.close()

        surviving = self._release_ids()
        assert PINNED_ID in surviving, (
            "the import prune deleted a pinned release — this is the 2026-09-04 "
            "defect (27,286 of 58,012 pins), see discogs-etl#424"
        )
        assert CONTROL_ID not in surviving, (
            "the exemption must be targeted: an unpinned release absent from the "
            "new dump is still stale and must be pruned"
        )
        assert REFRESHED_ID in surviving

    def test_retained_pin_keeps_artwork_but_is_marked_for_rehydration(self, tmp_path) -> None:
        """A retained pin has lost its children to the TRUNCATE, so it must not
        look like a fresh hit to LML — ``artwork_checked_at`` goes NULL while
        ``artwork_url`` survives."""
        self._seed()
        self._write_dump_csv(tmp_path, [REFRESHED_ID, *REFRESHED_PIN_IDS])

        conn = psycopg.connect(self.db_url)
        import_release_via_upsert(conn, tmp_path, keep_release_ids=ALL_PINS)
        conn.close()

        url, checked_at = self._artwork(PINNED_ID)
        assert url == "lml-artwork-pinned", "the retained pin lost its back-patched artwork"
        assert checked_at is None, (
            "a retained pin has no child rows; leaving artwork_checked_at set makes "
            "LML serve it as a hit with an empty tracklist and never re-fetch it"
        )

        # A release the dump *did* carry is genuinely fresh: untouched.
        refreshed_url, refreshed_checked = self._artwork(REFRESHED_ID)
        assert refreshed_url == "lml-artwork-refreshed"
        assert refreshed_checked is not None

    def test_shortfall_guard_aborts_before_the_child_truncate(self, tmp_path) -> None:
        """A dump that strands most of the pins is refused with the cache intact.

        #357's guard fired one seam too late on 2026-09-04: the prune had
        already committed. This one has to leave both ``release`` and its
        children exactly as they were.
        """
        self._seed()
        self._write_dump_csv(tmp_path, [REFRESHED_ID])

        conn = psycopg.connect(self.db_url)
        with pytest.raises(PinnedReleaseShortfallError) as excinfo:
            import_release_via_upsert(
                conn, tmp_path, keep_release_ids={PINNED_ID, CONTROL_ID, REFRESHED_ID}
            )
        conn.close()
        assert "2 of 3" in str(excinfo.value)

        assert self._release_ids() == {CONTROL_ID, *ALL_PINS}
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_artist")
            child_rows = cur.fetchone()[0]
        conn.close()
        assert child_rows == 3, (
            "the guard fired after the child TRUNCATE — a refused import must not "
            "leave the release-full / children-empty state of #298"
        )

    def test_override_ratio_lets_a_deliberate_rebuild_through(self, tmp_path, monkeypatch) -> None:
        self._seed()
        self._write_dump_csv(tmp_path, [REFRESHED_ID])
        monkeypatch.setenv("MAX_PIN_SHORTFALL_RATIO", "1.0")

        conn = psycopg.connect(self.db_url)
        import_release_via_upsert(
            conn, tmp_path, keep_release_ids={PINNED_ID, CONTROL_ID, REFRESHED_ID}
        )
        conn.close()

        surviving = self._release_ids()
        assert {PINNED_ID, CONTROL_ID, REFRESHED_ID} <= surviving, (
            "with the guard overridden every pin is exempt from the prune"
        )
        assert not set(REFRESHED_PIN_IDS) & surviving, (
            "releases that are neither pinned nor in the dump are still pruned"
        )


class TestExemptingPrunePlan:
    """The exempting prune must plan as anti-joins, not a NOT IN SubPlan.

    Sibling of ``test_import.py::TestPruneStaleReleasesPlan``: the added keep-id
    leg is subject to the same #298 / #302 planner trap as the first one.
    """

    @pytest.fixture(autouse=True)
    def _set_up(self, db_url):
        self.db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

    def test_exempting_prune_plans_as_anti_join(self) -> None:
        conn = psycopg.connect(self.db_url)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO release (id, title) VALUES "
                    "(9201, 'a'), (9202, 'b'), (9203, 'c') ON CONFLICT (id) DO NOTHING"
                )
                cur.execute("CREATE TEMP TABLE release_staging (LIKE release INCLUDING DEFAULTS)")
                cur.execute("INSERT INTO release_staging (id, title) VALUES (9201, 'a')")
                cur.execute("CREATE TEMP TABLE release_keep_ids (release_id integer PRIMARY KEY)")
                cur.execute("INSERT INTO release_keep_ids (release_id) VALUES (9202)")
                cur.execute("EXPLAIN " + PRUNE_STALE_RELEASES_EXEMPT_PINS_SQL)
                plan = "\n".join(row[0] for row in cur.fetchall())
        finally:
            conn.rollback()
            conn.close()

        assert "Anti Join" in plan, f"the exempting prune must anti-join. Plan was:\n{plan}"
        assert "SubPlan" not in plan, (
            f"the exempting prune regressed to a NOT IN-shaped SubPlan. Plan was:\n{plan}"
        )
