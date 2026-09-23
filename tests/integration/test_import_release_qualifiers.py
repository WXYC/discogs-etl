"""Live-Postgres tests for the release qualifier columns (WXYC/discogs-etl#428).

``discogs-xml-converter``'s ``release.csv`` writer has always emitted
``id, status, title, country, released, notes, data_quality, master_id,
format``, but the ``release`` table config in ``scripts/import_csv.py`` named
only six of those nine columns, so ``status``, ``notes`` and ``data_quality``
were produced on every run and thrown away at the COPY seam.

Two properties are pinned here, and the second is the one with teeth:

1. A *modern* ``release.csv`` populates all three columns — including through
   ``import_release_via_upsert``, the default incremental path, whose
   ``INSERT ... SELECT`` names its columns by hand and would otherwise leave
   the values in the staging table and never move them into ``release``.
2. A *legacy* ``release.csv`` that lacks the three columns still imports, with
   the columns landing NULL. The three are registered as
   ``optional_csv_columns`` rather than added to ``csv_columns`` precisely so
   this holds: ``import_csv`` bails with "Missing columns" and writes **zero
   rows** when a required column is absent from the header — the #204 failure
   mode — and on the upsert path zero rows raises rather than silently
   emptying the cache.

The modern fixture deliberately uses the converter's real column *order*
(qualifiers interleaved, not appended) so the header-name lookup is exercised
rather than a positional coincidence.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv_release_qualifiers_pg", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

import_csv = _ic.import_csv
import_release_via_upsert = _ic.import_release_via_upsert
BASE_TABLES = _ic.BASE_TABLES

pytestmark = pytest.mark.pg

QUALIFIER_COLUMNS = ("status", "notes", "data_quality")

# Canonical WXYC example releases (docs/test-fixtures.md). Each row carries a
# distinct (status, data_quality) pair so a column-swap bug cannot pass by
# accident, and only one row carries a note — ``notes`` is empty on most real
# Discogs releases, so a fixture where every row had one would not prove the
# empty cell still becomes NULL.
MODERN_ROWS = [
    # (id, status, title, country, released, notes, data_quality, master_id, format)
    (
        9201,
        "Accepted",
        "DOGA",
        "AR",
        "2022",
        "Reissue of the 2013 Sonamos LP.",
        "Correct",
        600,
        "LP",
    ),
    (9202, "Draft", "On Your Own Love Again", "US", "2015", "", "Needs Vote", 601, "LP"),
    (9203, "Deleted", "Edits", "US", "2023", "", "Entirely Incorrect", 602, "Vinyl"),
]

MODERN_HEADER = "id,status,title,country,released,notes,data_quality,master_id,format"
LEGACY_HEADER = "id,title,country,released,format,master_id"


def _write_modern_csv(tmp_path: Path) -> None:
    """Write a release.csv in the converter's real nine-column shape."""
    lines = [MODERN_HEADER]
    for rid, status, title, country, released, notes, quality, master_id, fmt in MODERN_ROWS:
        lines.append(
            f"{rid},{status},{title},{country},{released},{notes},{quality},{master_id},{fmt}"
        )
    (tmp_path / "release.csv").write_text("\n".join(lines) + "\n")


def _write_legacy_csv(tmp_path: Path) -> None:
    """Write a release.csv from a converter predating the qualifier columns."""
    lines = [LEGACY_HEADER]
    for rid, _status, title, country, released, _notes, _quality, master_id, fmt in MODERN_ROWS:
        lines.append(f"{rid},{title},{country},{released},{fmt},{master_id}")
    (tmp_path / "release.csv").write_text("\n".join(lines) + "\n")


class _FreshCache:
    """Shared fresh-schema fixture + query helpers."""

    @pytest.fixture(autouse=True)
    def _fresh_schema(self, fresh_db_url):
        self.db_url = fresh_db_url
        conn = psycopg.connect(fresh_db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

    def _qualifiers(self) -> dict[int, tuple]:
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT id, status, notes, data_quality FROM release ORDER BY id")
            rows = {row[0]: row[1:] for row in cur.fetchall()}
        conn.close()
        return rows


class TestUpsertPathCarriesQualifiers(_FreshCache):
    """``import_release_via_upsert`` — the default incremental rebuild path."""

    def test_modern_dump_populates_all_three(self, tmp_path) -> None:
        _write_modern_csv(tmp_path)

        conn = psycopg.connect(self.db_url)
        import_release_via_upsert(conn, tmp_path)
        conn.close()

        assert self._qualifiers() == {
            9201: ("Accepted", "Reissue of the 2013 Sonamos LP.", "Correct"),
            9202: ("Draft", None, "Needs Vote"),
            9203: ("Deleted", None, "Entirely Incorrect"),
        }, (
            "release.status / notes / data_quality did not reach `release` from a "
            "modern release.csv. The COPY lands them in release_staging; "
            "import_release_via_upsert's INSERT ... SELECT must name them too."
        )

    def test_modern_dump_refreshes_stale_qualifiers(self, tmp_path) -> None:
        """A second dump is authoritative — ON CONFLICT must SET the three."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO release (id, title, status, notes, data_quality) "
                "VALUES (9201, 'DOGA', 'Draft', 'stale note', 'Needs Vote')"
            )
        conn.commit()
        conn.close()

        _write_modern_csv(tmp_path)
        conn = psycopg.connect(self.db_url)
        import_release_via_upsert(conn, tmp_path)
        conn.close()

        assert self._qualifiers()[9201] == (
            "Accepted",
            "Reissue of the 2013 Sonamos LP.",
            "Correct",
        ), (
            "An existing row kept its pre-rebuild qualifier values. The dump is "
            "authoritative for dump-derived columns, so all three belong in the "
            "ON CONFLICT DO UPDATE SET list alongside title / format / master_id."
        )

    def test_legacy_dump_still_imports_and_leaves_them_null(self, tmp_path) -> None:
        """A converter predating #428's columns must not bounce the import.

        Zero rows from ``import_csv`` is not a soft failure on this path: the
        empty-staging floor raises rather than DELETE every release, so a
        "Missing columns" bail here aborts the whole rebuild.
        """
        _write_legacy_csv(tmp_path)

        conn = psycopg.connect(self.db_url)
        rows = import_release_via_upsert(conn, tmp_path)
        conn.close()

        assert rows == len(MODERN_ROWS)
        assert self._qualifiers() == {rid: (None, None, None) for rid, *_ in MODERN_ROWS}


class TestImportCsvPathCarriesQualifiers(_FreshCache):
    """The direct ``import_csv`` COPY, driven by the ``release`` table config."""

    @staticmethod
    def _release_config() -> dict:
        return next(t for t in BASE_TABLES if t["table"] == "release")

    def _import(self, tmp_path: Path) -> int:
        config = self._release_config()
        conn = psycopg.connect(self.db_url)
        rows = import_csv(
            conn,
            csv_path=tmp_path / "release.csv",
            table="release",
            csv_columns=config["csv_columns"],
            db_columns=config["db_columns"],
            required_columns=config["required"],
            transforms=config["transforms"],
            unique_key=config.get("unique_key"),
            optional_csv_columns=config.get("optional_csv_columns"),
        )
        conn.close()
        return rows

    def test_config_registers_the_three_as_optional(self) -> None:
        """They must be optional, not required: a legacy header would otherwise
        make ``import_csv`` log "Missing columns" and write zero rows (#204)."""
        config = self._release_config()
        optional = set(config.get("optional_csv_columns") or ())
        assert set(QUALIFIER_COLUMNS) <= optional, (
            f"release config's optional_csv_columns is {sorted(optional)}; it must "
            f"carry {list(QUALIFIER_COLUMNS)} so an older release.csv degrades to "
            f"NULL instead of bailing out of the import entirely."
        )
        assert set(QUALIFIER_COLUMNS).isdisjoint(config["csv_columns"]), (
            "The qualifier columns are listed in csv_columns as well as "
            "optional_csv_columns; import_csv would then append duplicates to "
            "the COPY column list."
        )

    def test_modern_dump_populates_all_three(self, tmp_path) -> None:
        _write_modern_csv(tmp_path)
        assert self._import(tmp_path) == len(MODERN_ROWS)
        assert self._qualifiers()[9202] == ("Draft", None, "Needs Vote")

    def test_legacy_dump_still_imports_and_leaves_them_null(self, tmp_path) -> None:
        _write_legacy_csv(tmp_path)
        assert self._import(tmp_path) == len(MODERN_ROWS)
        assert self._qualifiers() == {rid: (None, None, None) for rid, *_ in MODERN_ROWS}
