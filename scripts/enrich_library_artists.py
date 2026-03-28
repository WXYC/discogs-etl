#!/usr/bin/env python3
"""Generate an enriched library_artists.txt from library.db and WXYC MySQL data.

Extracts base artist names from the SQLite library database, then optionally
enriches with alternate names, cross-references, and release cross-references
from the WXYC MySQL catalog database.

Usage:
    # From library.db only (no MySQL enrichment):
    python scripts/enrich_library_artists.py \\
        --library-db library.db \\
        --output library_artists.txt

    # With WXYC MySQL enrichment:
    python scripts/enrich_library_artists.py \\
        --library-db library.db \\
        --wxyc-db-url mysql://user:pass@host:port/dbname \\
        --output library_artists.txt
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Import shared utilities from lib/
_LIB_DIR = Path(__file__).parent.parent / "lib"
sys.path.insert(0, str(_LIB_DIR.parent))
from lib.artist_splitting import split_artist_name_contextual  # noqa: E402
from lib.catalog_source import create_catalog_source  # noqa: E402
from lib.matching import is_compilation_artist  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--library-db",
        type=Path,
        required=True,
        metavar="FILE",
        help="Path to library.db (SQLite database with artist/title pairs).",
    )
    parser.add_argument(
        "--wxyc-db-url",
        type=str,
        default=None,
        metavar="URL",
        help="MySQL connection URL for WXYC catalog database "
        "(e.g. mysql://user:pass@host:port/dbname). "
        "Alias for --catalog-source tubafrenzy --catalog-db-url <url>. "
        "If omitted, only base artists from library.db are extracted.",
    )
    parser.add_argument(
        "--catalog-source",
        type=str,
        choices=["tubafrenzy", "backend-service"],
        default=None,
        metavar="SOURCE",
        help="Catalog source type: 'tubafrenzy' (MySQL) or 'backend-service' (PostgreSQL).",
    )
    parser.add_argument(
        "--catalog-db-url",
        type=str,
        default=None,
        metavar="URL",
        help="Database connection URL for the catalog source.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        metavar="FILE",
        help="Output path for library_artists.txt.",
    )
    return parser.parse_args(argv)


def extract_base_artists(library_db_path: Path) -> set[str]:
    """Extract unique artist names from library.db, excluding compilations.

    Args:
        library_db_path: Path to the SQLite library database.

    Returns:
        Set of distinct artist names (original case, no compilations).
    """
    logger.info("Extracting base artists from %s", library_db_path)
    conn = sqlite3.connect(library_db_path)
    try:
        cur = conn.execute("SELECT DISTINCT artist FROM library")
        artists = set()
        for (name,) in cur:
            if name and name.strip() and not is_compilation_artist(name):
                artists.add(name.strip())
    finally:
        conn.close()
    logger.info("Extracted %d base artists", len(artists))
    return artists


def extract_alternate_names(conn) -> set[str]:
    """Extract alternate artist names from LIBRARY_RELEASE.

    These are releases filed under one artist but credited to a different name
    (e.g., "Body Count" filed under Ice-T).

    Args:
        conn: MySQL connection.

    Returns:
        Set of alternate artist name strings.
    """
    logger.info("Extracting alternate artist names from LIBRARY_RELEASE")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ALTERNATE_ARTIST_NAME
            FROM LIBRARY_RELEASE
            WHERE ALTERNATE_ARTIST_NAME IS NOT NULL
              AND ALTERNATE_ARTIST_NAME != ''
        """)
        names = {row[0].strip() for row in cur if row[0] and row[0].strip()}
    logger.info("Found %d alternate artist names", len(names))
    return names


def extract_cross_referenced_artists(conn) -> set[str]:
    """Extract artist names from both sides of LIBRARY_CODE_CROSS_REFERENCE.

    These link related artists (solo projects, band members, name variants).

    Args:
        conn: MySQL connection.

    Returns:
        Set of cross-referenced artist names.
    """
    logger.info("Extracting cross-referenced artists from LIBRARY_CODE_CROSS_REFERENCE")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT lc.PRESENTATION_NAME
            FROM LIBRARY_CODE_CROSS_REFERENCE cr
            JOIN LIBRARY_CODE lc ON lc.ID = cr.CROSS_REFERENCING_ARTIST_ID
            UNION
            SELECT DISTINCT lc.PRESENTATION_NAME
            FROM LIBRARY_CODE_CROSS_REFERENCE cr
            JOIN LIBRARY_CODE lc ON lc.ID = cr.CROSS_REFERENCED_LIBRARY_CODE_ID
        """)
        names = {row[0].strip() for row in cur if row[0] and row[0].strip()}
    logger.info("Found %d cross-referenced artist names", len(names))
    return names


def extract_release_cross_ref_artists(conn) -> set[str]:
    """Extract artist names linked via RELEASE_CROSS_REFERENCE.

    These are artists associated with specific releases filed under other artists
    (collaborations, featured artists, remixers).

    Args:
        conn: MySQL connection.

    Returns:
        Set of artist names from release cross-references.
    """
    logger.info("Extracting artists from RELEASE_CROSS_REFERENCE")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT lc.PRESENTATION_NAME
            FROM RELEASE_CROSS_REFERENCE rcr
            JOIN LIBRARY_CODE lc ON lc.ID = rcr.CROSS_REFERENCING_ARTIST_ID
        """)
        names = {row[0].strip() for row in cur if row[0] and row[0].strip()}
    logger.info("Found %d release cross-reference artist names", len(names))
    return names


def _expand_multi_artist_names(all_names: set[str]) -> set[str]:
    """Expand multi-artist entries into individual components.

    Uses context-free splitting for unambiguous delimiters (comma, slash, plus)
    and contextual splitting for ampersand (only when a component is already
    a known standalone artist).

    Returns the expanded set (original names + new components).
    """
    import unicodedata

    # Build the normalized known-artists set for contextual splitting
    def _normalize(name: str) -> str:
        nfkd = unicodedata.normalize("NFKD", name)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()

    known_artists = {_normalize(n) for n in all_names}

    expanded: set[str] = set()
    split_count = 0

    for name in all_names:
        components = split_artist_name_contextual(name, known_artists)
        if components:
            split_count += 1
            for c in components:
                expanded.add(c)
                # Also add new components to known set so subsequent splits can use them
                known_artists.add(_normalize(c))

    if split_count:
        logger.info(
            "Split %d multi-artist entries into %d new components",
            split_count,
            len(expanded - all_names),
        )

    return expanded


def merge_and_write(
    base: set[str],
    alternates: set[str],
    cross_refs: set[str],
    release_cross_refs: set[str],
    output: Path,
) -> None:
    """Merge all artist name sources and write to output file.

    Names are sorted alphabetically for stable diffs. Empty strings and
    compilation artist names are filtered out. Original case is preserved.
    Multi-artist entries are expanded into individual components.

    Args:
        base: Artist names from library.db.
        alternates: Alternate artist names from LIBRARY_RELEASE.
        cross_refs: Artist names from LIBRARY_CODE_CROSS_REFERENCE.
        release_cross_refs: Artist names from RELEASE_CROSS_REFERENCE.
        output: Path to write the output file.
    """
    all_names = base | alternates | cross_refs | release_cross_refs

    # Expand multi-artist entries before filtering
    split_components = _expand_multi_artist_names(all_names)
    all_names = all_names | split_components

    filtered = sorted(
        name for name in all_names if name and name.strip() and not is_compilation_artist(name)
    )

    new_from_alternates = len(alternates - base)
    new_from_cross_refs = len(cross_refs - base - alternates)
    new_from_release_xrefs = len(release_cross_refs - base - alternates - cross_refs)

    logger.info(
        "Merged: %d base + %d new from alternates + %d new from cross-refs "
        "+ %d new from release cross-refs = %d total",
        len(base),
        new_from_alternates,
        new_from_cross_refs,
        new_from_release_xrefs,
        len(filtered),
    )

    with open(output, "w", encoding="utf-8") as f:
        for name in filtered:
            f.write(name + "\n")

    logger.info("Wrote %d artist names to %s", len(filtered), output)


def _resolve_catalog_args(args: argparse.Namespace) -> tuple[str, str] | None:
    """Resolve --catalog-source/--catalog-db-url or --wxyc-db-url into (source_type, db_url).

    Returns None if no catalog source is configured (library.db-only mode).
    """
    if args.catalog_source and args.catalog_db_url:
        return (args.catalog_source, args.catalog_db_url)
    if args.wxyc_db_url:
        return ("tubafrenzy", args.wxyc_db_url)
    if args.catalog_source and not args.catalog_db_url:
        logger.error("--catalog-source requires --catalog-db-url")
        sys.exit(1)
    if args.catalog_db_url and not args.catalog_source:
        logger.error("--catalog-db-url requires --catalog-source")
        sys.exit(1)
    return None


def main() -> None:
    args = parse_args()

    if not args.library_db.exists():
        logger.error("library.db not found: %s", args.library_db)
        sys.exit(1)

    # Source 1: Base names from library.db
    base = extract_base_artists(args.library_db)

    # Source 2: Catalog enrichment (optional)
    alternates: set[str] = set()
    cross_refs: set[str] = set()
    release_cross_refs: set[str] = set()

    catalog_args = _resolve_catalog_args(args)
    if catalog_args:
        source = create_catalog_source(*catalog_args)
        try:
            alternates = source.fetch_alternate_names()
            cross_refs = source.fetch_cross_referenced_artists()
            release_cross_refs = source.fetch_release_cross_ref_artists()
        finally:
            source.close()

    # Merge and write output
    args.output.parent.mkdir(parents=True, exist_ok=True)
    merge_and_write(base, alternates, cross_refs, release_cross_refs, args.output)


if __name__ == "__main__":
    main()
