"""End-to-end coverage for verify_cache --dump-classification (discogs-etl#217 Phase 0).

Runs the real ``async_main`` dry-run path against a seeded Postgres cache and a
hand-built SQLite library, then asserts on the artifact files the prune audit
consumes. Exercises the whole wire path: the primary classification + override
pass, the second no-ANV classification pass, and the on-disk dump.

The fixture is shaped to make every classification deterministic against a tiny
2-artist library:

- id=1 Luke Vibert / "Drum 'n' Bass for Papa"   -> KEEP in both passes (canonical)
- id=2 Plug / "Drum 'n' Bass for Papa"           -> KEEP with ANV, PRUNE without
      (the #305 alias rescue: "Plug" is Luke Vibert's alternate_artist_name)
- id=3 Juana Molina / "DOGA"                     -> KEEP in both passes (canonical)
- id=4 Zzyzx Qxqxqx / "Xyzzy Plugh"              -> junk, PINNED -> lands in KEEP
- id=5 Foobar Nonexist / "Nowhere Album"         -> junk, not pinned -> PRUNE

Release formats are the normalized categories the import stage writes ('CD',
'Vinyl'), not raw Discogs descriptors, so the exact-match format filter passes.

See the module-loading note in the sibling integration tests for the importlib
dance (#109).
"""

from __future__ import annotations

import asyncio
import importlib.util
import sqlite3
import sys
from pathlib import Path

import psycopg
import pytest

pytestmark = pytest.mark.pg

REPO_ROOT = Path(__file__).parent.parent.parent
SCHEMA_DIR = REPO_ROOT / "schema"

_VERIFY_CACHE_PATH = REPO_ROOT / "scripts" / "verify_cache.py"
if "verify_cache" in sys.modules:
    _vc = sys.modules["verify_cache"]
else:
    _spec = importlib.util.spec_from_file_location("verify_cache", _VERIFY_CACHE_PATH)
    assert _spec is not None and _spec.loader is not None
    _vc = importlib.util.module_from_spec(_spec)
    sys.modules["verify_cache"] = _vc
    _spec.loader.exec_module(_vc)


# The alias-rescue release: keeps via alternate_artist_name, prunes without it.
ALIAS_RELEASE_ID = 2
# The always-pruned, non-pinned junk release.
JUNK_PRUNE_ID = 5
# The pinned junk release (would prune, but the override keeps it).
PINNED_ID = 4
# A single release credited to TWO primary artists (extra=0), one in-library
# (KEEP) and one junk (PRUNE). classify_all_releases classifies it per artist
# group, so its id lands in BOTH keep_ids and prune_ids; the dump must resolve
# it to KEEP (production copy-swap retains keep ∪ review) and NOT list it as
# pruned.
MULTI_ARTIST_KEEP_ID = 6


def _seed_cache(db_url: str) -> None:
    """Apply the cache schema and insert the fixture releases."""
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.executemany(
                "INSERT INTO release (id, title, format) VALUES (%s, %s, %s)",
                [
                    (1, "Drum 'n' Bass for Papa", "CD"),
                    (2, "Drum 'n' Bass for Papa", "CD"),
                    (3, "DOGA", "CD"),
                    (4, "Xyzzy Plugh", "CD"),
                    (5, "Nowhere Album", "CD"),
                    (6, "DOGA", "CD"),
                ],
            )
            cur.executemany(
                "INSERT INTO release_artist (release_id, artist_name, extra) VALUES (%s, %s, 0)",
                [
                    (1, "Luke Vibert"),
                    (2, "Plug"),
                    (3, "Juana Molina"),
                    (4, "Zzyzx Qxqxqx"),
                    (5, "Foobar Nonexist"),
                    # id=6: two primary artists -> KEEP via Juana Molina, PRUNE
                    # via the junk credit. Exercises the keep/prune overlap.
                    (6, "Juana Molina"),
                    (6, "Foobar Nonexist"),
                ],
            )
    finally:
        conn.close()


def _build_library(path: Path) -> None:
    """Write a SQLite library.db: one artist with an alias, one without."""
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE library (id INTEGER PRIMARY KEY, artist TEXT, title TEXT, "
            "format TEXT, alternate_artist_name TEXT)"
        )
        cur.executemany(
            "INSERT INTO library (artist, title, format, alternate_artist_name) "
            "VALUES (?, ?, ?, ?)",
            [
                # Artist WITH an alternate_artist_name (the #305 path).
                ("Luke Vibert", "Drum 'n' Bass for Papa", "CD", "Plug"),
                # Artist WITHOUT an alternate.
                ("Juana Molina", "DOGA", "CD", None),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _read_ids(path: Path) -> set[int]:
    text = path.read_text().strip()
    return {int(line) for line in text.splitlines() if line}


@pytest.fixture()
def dump_dir(tmp_path, fresh_db_url, monkeypatch):
    """Seed cache + library, run async_main --dump-classification, return the dump dir."""
    _seed_cache(fresh_db_url)

    library_db = tmp_path / "library.db"
    _build_library(library_db)

    keep_file = tmp_path / "keep_release_ids.txt"
    keep_file.write_text(f"{PINNED_ID}\n")

    out_dir = tmp_path / "prune-audit"

    argv = [
        "verify_cache.py",
        str(library_db),
        fresh_db_url,
        "--dump-classification",
        str(out_dir),
        "--keep-release-ids",
        str(keep_file),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    # Dry run (no --prune / --copy-to): the dump fires before the branch, and
    # the cache is never mutated.
    asyncio.run(_vc.async_main())
    return out_dir


class TestDumpClassificationEndToEnd:
    def test_all_artifact_files_written(self, dump_dir):
        for name in (
            "keep_ids.txt",
            "prune_ids.txt",
            "review_ids.txt",
            "override_ids.txt",
            "prune_ids_no_anv.txt",
            "prune_releases.jsonl",
            "counts.json",
        ):
            assert (dump_dir / name).exists(), f"missing {name}"

    def test_id_files_partition_the_release_set(self, dump_dir):
        """(i) keep / prune / review partition all six releases, disjointly."""
        keep = _read_ids(dump_dir / "keep_ids.txt")
        prune = _read_ids(dump_dir / "prune_ids.txt")
        review = _read_ids(dump_dir / "review_ids.txt")

        assert keep | prune | review == {1, 2, 3, 4, 5, 6}
        assert keep & prune == set()
        assert keep & review == set()
        assert prune & review == set()

        # The concrete expected split (post-override).
        assert keep == {1, 2, 3, 4, MULTI_ARTIST_KEEP_ID}
        assert prune == {JUNK_PRUNE_ID}
        assert review == set()

    def test_multi_artist_release_resolves_to_keep_not_prune(self, dump_dir):
        """A release that is KEEP via one primary artist and PRUNE via another
        must land in keep_ids only — production copy-swap retains it, so the
        audit must not count it as a coverage gap."""
        keep = _read_ids(dump_dir / "keep_ids.txt")
        prune = _read_ids(dump_dir / "prune_ids.txt")
        assert MULTI_ARTIST_KEEP_ID in keep
        assert MULTI_ARTIST_KEEP_ID not in prune

    def test_override_ids_file_reflects_keep_release_ids(self, dump_dir):
        """(ii) override_ids.txt is exactly the passed --keep-release-ids set."""
        assert _read_ids(dump_dir / "override_ids.txt") == {PINNED_ID}

    def test_no_anv_superset_and_diff_is_alias_release(self, dump_dir):
        """(iii) prune_ids_no_anv superset of prune_ids; the difference is the alias rescue."""
        prune = _read_ids(dump_dir / "prune_ids.txt")
        prune_no_anv = _read_ids(dump_dir / "prune_ids_no_anv.txt")

        assert prune_no_anv >= prune
        # The only release that flips prune->keep under the ANV index is the
        # alias-credited one (#305 uplift).
        assert prune_no_anv - prune == {ALIAS_RELEASE_ID}

    def test_prune_releases_jsonl_carries_raw_fields(self, dump_dir):
        """prune_releases.jsonl has one object per pruned id with raw artist/title."""
        import json

        lines = (dump_dir / "prune_releases.jsonl").read_text().splitlines()
        objs = [json.loads(line) for line in lines]
        assert [o["id"] for o in objs] == [JUNK_PRUNE_ID]
        assert objs[0]["artist"] == "Foobar Nonexist"
        assert objs[0]["title"] == "Nowhere Album"

    def test_counts_json_matches_id_files(self, dump_dir):
        import json

        counts = json.loads((dump_dir / "counts.json").read_text())
        assert counts["keep"] == 5
        assert counts["prune"] == 1
        assert counts["review"] == 0
        assert counts["override_applied"] == 1
        assert counts["dedup_note"] is None
        # total_release_rows is the fan-out row count (id=6 has two credits), so
        # it exceeds the six distinct release ids.
        assert counts["total_release_rows"] == 7
