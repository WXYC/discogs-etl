#!/usr/bin/env python3
"""Extract WXYC label preferences from FLOWSHEET_ENTRY_PROD.

Queries WXYC MySQL for (artist_name, release_title, label_name) triples
from flowsheet plays linked to library releases, then writes them as a CSV
for use by dedup_releases.py --library-labels.

The JOIN to LIBRARY_RELEASE ensures the release still exists in the library
(deleted releases are excluded).

Usage:
    python scripts/extract_library_labels.py \\
        --wxyc-db-url mysql://user:pass@host:port/db \\
        --output library_labels.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent))
from lib.catalog_source import create_catalog_source  # noqa: E402

EXTRACTION_QUERY = """
    SELECT DISTINCT
        lc.PRESENTATION_NAME AS artist_name,
        lr.TITLE AS release_title,
        fe.LABEL_NAME AS label_name
    FROM FLOWSHEET_ENTRY_PROD fe
    JOIN LIBRARY_RELEASE lr ON fe.LIBRARY_RELEASE_ID = lr.ID
    JOIN LIBRARY_CODE lc ON lr.LIBRARY_CODE_ID = lc.ID
    WHERE fe.LABEL_NAME IS NOT NULL
      AND fe.LABEL_NAME != ''
      AND fe.LIBRARY_RELEASE_ID > 0
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--wxyc-db-url",
        type=str,
        default=None,
        metavar="URL",
        help="MySQL connection URL for WXYC catalog database "
        "(e.g. mysql://user:pass@host:port/dbname). "
        "Alias for --catalog-source tubafrenzy --catalog-db-url <url>.",
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
        help="Output path for library_labels.csv.",
    )
    return parser.parse_args(argv)


def extract_library_labels(conn) -> set[tuple[str, str, str]]:
    """Extract (artist_name, release_title, label_name) triples from WXYC MySQL.

    Args:
        conn: MySQL connection (pymysql).

    Returns:
        Set of (artist_name, release_title, label_name) tuples, stripped and
        deduplicated. Rows with empty/null fields are excluded.
    """
    logger.info("Extracting library labels from FLOWSHEET_ENTRY_PROD")
    with conn.cursor() as cur:
        cur.execute(EXTRACTION_QUERY)
        rows = cur.fetchall()

    triples: set[tuple[str, str, str]] = set()
    for artist, title, label in rows:
        if not artist or not title or not label:
            continue
        artist_s = artist.strip()
        title_s = title.strip()
        label_s = label.strip()
        if artist_s and title_s and label_s:
            triples.add((artist_s, title_s, label_s))

    logger.info("Extracted %d unique (artist, title, label) triples", len(triples))
    return triples


def write_library_labels_csv(triples: set[tuple[str, str, str]], output: Path) -> None:
    """Write label triples to a CSV file.

    Args:
        triples: Set of (artist_name, release_title, label_name) tuples.
        output: Path to the output CSV file.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    sorted_rows = sorted(triples)
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["artist_name", "release_title", "label_name"])
        writer.writerows(sorted_rows)
    logger.info("Wrote %d label preferences to %s", len(sorted_rows), output)


def main() -> None:
    args = parse_args()

    # Resolve catalog source
    if args.catalog_source and args.catalog_db_url:
        source_type, db_url = args.catalog_source, args.catalog_db_url
    elif args.wxyc_db_url:
        source_type, db_url = "tubafrenzy", args.wxyc_db_url
    elif args.catalog_source and not args.catalog_db_url:
        logger.error("--catalog-source requires --catalog-db-url")
        sys.exit(1)
    elif args.catalog_db_url and not args.catalog_source:
        logger.error("--catalog-db-url requires --catalog-source")
        sys.exit(1)
    else:
        logger.error("One of --wxyc-db-url or --catalog-source/--catalog-db-url is required")
        sys.exit(1)

    source = create_catalog_source(source_type, db_url)
    try:
        triples = source.fetch_library_labels()
    finally:
        source.close()
    write_library_labels_csv(triples, args.output)


if __name__ == "__main__":
    main()
