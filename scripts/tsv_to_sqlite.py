"""Convert a MySQL TSV dump to a SQLite database with FTS5 index.

Reads a tab-separated file (as produced by ``mysql -B -N``) with 11 columns
corresponding to the WXYC library catalog schema and creates a
``library.db``: a ``library`` table, its FTS5 companion, the search indexes,
and optionally a ``compilation_track_artist`` table.

This is the **MySQL-sourced** producer -- the one that builds production's
``library.db`` nightly out of ``scripts/sync-library.sh``. The schema itself
lives in :mod:`lib.library_db`, shared with the Backend-sourced producer
(``scripts/catalog_parity_diff.py --backend-source``, WXYC/discogs-etl#351)
so the two builds cannot drift apart in shape. See that module's docstring
for the column list and why ``label`` is always NULL.

``cross_reference_names`` holds the pipe-joined (``" | "``) PRESENTATION_NAMEs
of any WXYC ``LIBRARY_CODE``s cataloger-cross-referenced to this row's own
code (e.g. a release filed under a band name carries its member's personal
name), sourced from ``LIBRARY_CODE_CROSS_REFERENCE`` via the correlated
subquery in ``sync-library.sh``. See WXYC/discogs-etl#334.

MySQL ``\\N`` values are converted to SQL NULL. Rows that do not contain
exactly 11 tab-separated fields are skipped with a warning on stderr.

Optionally also imports a ``compilation_track_artist`` table from a second,
3-column TSV (``library_release_id``, ``artist_name``, ``track_title``)
sourced from tubafrenzy's ``COMPILATION_TRACK_ARTIST`` table. This restores
the export dropped in the #65 slim-down (see WXYC/discogs-etl#332); LML's
``library/db.py`` detects the table at connect time and UNIONs in
compilations featuring a searched track artist.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.library_db import (  # noqa: E402
    build_library_db,
    parse_compilation_track_tsv,
    parse_library_tsv,
)
from lib.observability import init_logger  # noqa: E402


def tsv_to_sqlite(tsv_path: str, db_path: str, cta_tsv_path: str | None = None) -> int:
    """Import a MySQL TSV dump into a new SQLite database.

    Args:
        tsv_path: Path to the tab-separated input file.
        db_path: Path where the SQLite database will be created.
        cta_tsv_path: Optional path to a compilation_track_artist TSV (3
            columns: library_release_id, artist_name, track_title). Omit to
            skip the compilation_track_artist table entirely.

    Returns:
        The number of library rows successfully imported (unaffected by
        compilation_track_artist import counts).
    """
    cta_rows = parse_compilation_track_tsv(cta_tsv_path) if cta_tsv_path else None
    return build_library_db(db_path, parse_library_tsv(tsv_path), cta_rows)


def _parse_args(argv: list[str]) -> tuple[str, str, str | None]:
    """Parse CLI args: <tsv_path> <db_path> [--cta-tsv PATH]."""
    cta_tsv_path: str | None = None
    if "--cta-tsv" in argv:
        flag_index = argv.index("--cta-tsv")
        try:
            cta_tsv_path = argv[flag_index + 1]
        except IndexError:
            print("Usage: --cta-tsv requires a path argument", file=sys.stderr)
            sys.exit(1)
        positional = argv[:flag_index] + argv[flag_index + 2 :]
    else:
        positional = list(argv)

    if len(positional) != 2:
        print(
            f"Usage: {sys.argv[0]} <tsv_path> <db_path> [--cta-tsv <cta_tsv_path>]",
            file=sys.stderr,
        )
        sys.exit(1)

    return positional[0], positional[1], cta_tsv_path


if __name__ == "__main__":
    init_logger(repo="discogs-etl", tool="discogs-etl tsv_to_sqlite")
    tsv_path, db_path, cta_tsv_path = _parse_args(sys.argv[1:])
    n = tsv_to_sqlite(tsv_path, db_path, cta_tsv_path)
    print(f"Exported {n} rows to {db_path}")
