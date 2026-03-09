#!/usr/bin/env python3
"""Filter Discogs CSV exports to only include releases by library artists.

This script significantly reduces the data size by only keeping releases
that have at least one artist matching the library catalog.

Usage:
    python filter_discogs_csv.py /path/to/library_artists.txt /path/to/csv_output/ /path/to/filtered_output/
"""

from __future__ import annotations

import csv
import logging
import sys
import unicodedata
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# CSV files that need to be filtered by release_id.
# Only includes files needed by the optimized schema (see 04-create-database.sql).
# Dropped tables (release_genre, release_style) are excluded.
RELEASE_ID_FILES = [
    "release.csv",
    "release_artist.csv",
    "release_label.csv",
    "release_track.csv",
    "release_track_artist.csv",
    "release_image.csv",  # for artwork_url extraction during import
]


def normalize_artist(name: str) -> str:
    """Normalize artist name for matching.

    Strips diacritics so that Discogs "Björk" matches library "Bjork".
    """
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower().strip()


def load_library_artists(path: Path) -> set[str]:
    """Load library artists into a normalized set."""
    logger.info(f"Loading library artists from {path}")
    artists = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            name = line.strip()
            if name:
                artists.add(normalize_artist(name))
    logger.info(f"Loaded {len(artists):,} unique library artists")
    return artists


def find_matching_release_ids(release_artist_path: Path, library_artists: set[str]) -> set[int]:
    """Find all release IDs that have at least one matching library artist.

    Uses csv.reader with positional indexing instead of csv.DictReader
    to avoid dict creation overhead on 100M+ row files.
    """
    logger.info(f"Scanning {release_artist_path} for matching artists...")
    matching_ids = set()
    total_rows = 0
    matched_rows = 0

    normalize_cache: dict[str, str] = {}

    with open(release_artist_path, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        release_id_idx = header.index("release_id")
        artist_name_idx = header.index("artist_name")
        for row in reader:
            total_rows += 1
            try:
                raw_name = row[artist_name_idx]
            except IndexError:
                continue
            artist_name = normalize_cache.get(raw_name)
            if artist_name is None:
                artist_name = normalize_artist(raw_name)
                normalize_cache[raw_name] = artist_name
            if artist_name in library_artists:
                release_id = int(row[release_id_idx])
                matching_ids.add(release_id)
                matched_rows += 1

            if total_rows % 500000 == 0:
                logger.info(
                    f"  Processed {total_rows:,} rows, found {len(matching_ids):,} matching releases"
                )

    logger.info(
        f"Finished: {matched_rows:,} artist matches across {len(matching_ids):,} releases "
        f"(from {total_rows:,} total rows)"
    )
    return matching_ids


def get_release_id_column(filename: str) -> str:
    """Get the column name containing release_id for each file type."""
    if filename == "release.csv":
        return "id"
    return "release_id"


def filter_csv_file(
    input_path: Path, output_path: Path, matching_ids: set[int], id_column: str
) -> tuple[int, int]:
    """Filter a CSV file to only include rows with matching release IDs.

    Uses csv.reader with positional indexing instead of csv.DictReader
    to avoid dict creation overhead on large files.
    """
    input_count = 0
    output_count = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        try:
            id_idx = header.index(id_column)
        except ValueError:
            raise ValueError(
                f"Column '{id_column}' not found in {input_path}. Available columns: {header}"
            ) from None

        with open(output_path, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)

            for row in reader:
                input_count += 1
                try:
                    release_id = int(row[id_idx])
                    if release_id in matching_ids:
                        writer.writerow(row)
                        output_count += 1
                except (ValueError, IndexError):
                    # Skip rows with invalid release IDs or short rows
                    pass

                if input_count % 1000000 == 0:
                    logger.info(f"  Processed {input_count:,} rows, kept {output_count:,}")

    return input_count, output_count


def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)

    library_artists_path = Path(sys.argv[1])
    csv_input_dir = Path(sys.argv[2])
    csv_output_dir = Path(sys.argv[3])

    if not library_artists_path.exists():
        logger.error(f"Library artists file not found: {library_artists_path}")
        sys.exit(1)

    if not csv_input_dir.exists():
        logger.error(f"CSV input directory not found: {csv_input_dir}")
        sys.exit(1)

    csv_output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Load library artists
    library_artists = load_library_artists(library_artists_path)

    # Step 2: Find matching release IDs
    release_artist_path = csv_input_dir / "release_artist.csv"
    if not release_artist_path.exists():
        logger.error(f"release_artist.csv not found in {csv_input_dir}")
        sys.exit(1)

    matching_ids = find_matching_release_ids(release_artist_path, library_artists)

    if not matching_ids:
        logger.warning("No matching releases found! Check artist name normalization.")
        sys.exit(1)

    logger.info(f"Found {len(matching_ids):,} releases to keep")

    # Step 3: Filter each CSV file
    stats = {}
    for filename in RELEASE_ID_FILES:
        input_path = csv_input_dir / filename
        if not input_path.exists():
            logger.warning(f"Skipping {filename} (not found)")
            continue

        output_path = csv_output_dir / filename
        id_column = get_release_id_column(filename)

        logger.info(f"Filtering {filename}...")
        input_count, output_count = filter_csv_file(
            input_path, output_path, matching_ids, id_column
        )

        reduction_pct = (1 - output_count / input_count) * 100 if input_count > 0 else 0
        stats[filename] = (input_count, output_count, reduction_pct)
        logger.info(f"  {input_count:,} → {output_count:,} rows ({reduction_pct:.1f}% reduction)")

    # Summary
    logger.info("\n=== Summary ===")
    logger.info(f"Library artists: {len(library_artists):,}")
    logger.info(f"Matching releases: {len(matching_ids):,}")
    for filename, (inp, out, pct) in stats.items():
        logger.info(f"  {filename}: {inp:,} → {out:,} ({pct:.1f}% reduction)")


if __name__ == "__main__":
    main()
