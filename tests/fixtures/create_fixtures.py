#!/usr/bin/env python3
"""Generate test fixture data for the discogs-cache test suite.

Creates minimal CSV files, a SQLite library.db, and library_artists.txt
that exercise the full pipeline: import, dedup, prune, and filter.

Run from the repo root:
    python tests/fixtures/create_fixtures.py

The generated files are checked into the repo so tests can run without
regenerating them.  Re-run this script if you need to modify the fixture data.
"""

import csv
import sqlite3
from pathlib import Path

FIXTURE_DIR = Path(__file__).parent
CSV_DIR = FIXTURE_DIR / "csv"


def write_csv(filename: str, headers: list[str], rows: list[list]) -> None:
    """Write a CSV file to the fixtures/csv/ directory."""
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    path = CSV_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  {filename}: {len(rows)} rows")


def create_release_csv() -> None:
    """Create release.csv with various edge cases.

    Includes:
    - Duplicate master_id values (releases 1001, 1002, 1003 share master_id 500)
    - Various date formats
    - Null/empty fields
    - Releases that match and don't match the library
    """
    headers = [
        "id",
        "status",
        "title",
        "country",
        "released",
        "notes",
        "data_quality",
        "master_id",
        "format",
    ]
    rows = [
        # Group 1: duplicate master_id 500 (Radiohead - OK Computer variants)
        # Release 1001 has 5 tracks (UK), 1002 has 3 tracks (US), 1003 has 1 track (JP)
        # Dedup should keep 1002 (US country preference beats higher track count)
        [1001, "Accepted", "OK Computer", "UK", "1997-06-16", "", "Correct", 500, "CD"],
        [1002, "Accepted", "OK Computer", "US", "1997-07-01", "", "Correct", 500, "Vinyl"],
        [1003, "Accepted", "OK Computer", "JP", "1997", "", "Correct", 500, "Cassette"],
        # Group 2: duplicate master_id 600 (Joy Division - Unknown Pleasures)
        # Release 2001 has 2 tracks (UK), 2002 has 4 tracks (DE) — no US release
        # Dedup should keep 2002 (most tracks, fallback when no US release)
        [2001, "Accepted", "Unknown Pleasures", "UK", "1979-06-15", "", "Correct", 600, "LP"],
        [2002, "Accepted", "Unknown Pleasures", "DE", "1979", "", "Correct", 600, "CD"],
        # No duplicate - unique master_id
        [3001, "Accepted", "Kid A", "UK", "2000-10-02", "", "Correct", 700, "CD"],
        # No master_id (should survive dedup)
        [4001, "Accepted", "Amnesiac", "UK", "2001-06-05", "", "Correct", "", "CD"],
        # Release that won't match library (should be pruned)
        [5001, "Accepted", "Unknown Album", "US", "2020-01-01", "", "Correct", 800, "CD"],
        [5002, "Accepted", "Another Unknown", "US", "", "", "Correct", 900, "CD"],
        # Bad date format
        [6001, "Accepted", "Homogenic", "UK", "Unknown", "", "Correct", 1000, "CD"],
        # Missing title (should be skipped during import - required field)
        [7001, "Accepted", "", "US", "2023", "", "Correct", 1100, "CD"],
        # Compilation release
        [8001, "Accepted", "Sugar Hill", "US", "1979", "", "Correct", 1200, "LP"],
        # Various date format edge cases
        [9001, "Accepted", "Abbey Road", "UK", "1969-09-26", "", "Correct", 1300, "LP"],
        [9002, "Accepted", "Bridge Over Troubled Water", "US", "1970", "", "Correct", 1400, "LP"],
        # Artist not in library
        [10001, "Accepted", "Some Random Album", "US", "2023-05-01", "", "Correct", 1500, "CD"],
        [10002, "Accepted", "Obscure Release", "DE", "2022", "", "Correct", 1600, "CD"],
    ]
    write_csv("release.csv", headers, rows)


def create_release_artist_csv() -> None:
    """Create release_artist.csv linking releases to artists."""
    headers = ["release_id", "artist_id", "artist_name", "extra", "anv", "position", "join_field"]
    rows = [
        # Radiohead releases (match library)
        [1001, 1, "Radiohead", 0, "", 1, ""],
        [1002, 1, "Radiohead", 0, "", 1, ""],
        [1003, 1, "Radiohead", 0, "", 1, ""],
        [3001, 1, "Radiohead", 0, "", 1, ""],
        [4001, 1, "Radiohead", 0, "", 1, ""],
        # Joy Division releases (match library)
        [2001, 2, "Joy Division", 0, "", 1, ""],
        [2002, 2, "Joy Division", 0, "", 1, ""],
        # Unknown artists (won't match library)
        [5001, 3, "DJ Unknown", 0, "", 1, ""],
        [5002, 4, "Mystery Band", 0, "", 1, ""],
        # Bjork (match library, tests accent handling)
        [6001, 5, "Björk", 0, "", 1, ""],
        # Note: release 7001 has empty title and is skipped during import,
        # so no child table rows should reference it.
        # Compilation
        [8001, 7, "Various", 0, "", 1, ""],
        # Beatles and Simon & Garfunkel (match library)
        [9001, 8, "Beatles, The", 0, "", 1, ""],
        [9002, 9, "Simon & Garfunkel", 0, "", 1, ""],
        # Not in library
        [10001, 10, "Random Artist X", 0, "", 1, ""],
        [10002, 11, "Obscure Band Y", 0, "", 1, ""],
        # Extra artist credit (should not be primary)
        [1001, 12, "Some Producer", 1, "", 2, ""],
    ]
    write_csv("release_artist.csv", headers, rows)


def create_release_track_csv() -> None:
    """Create release_track.csv with tracks for each release.

    Track counts matter for dedup (release with most tracks wins).
    """
    headers = ["release_id", "sequence", "position", "title", "duration"]
    rows = [
        # Release 1001 (OK Computer UK CD) - 5 tracks (most tracks, but not US)
        [1001, 1, "1", "Airbag", "4:44"],
        [1001, 2, "2", "Paranoid Android", "6:23"],
        [1001, 3, "3", "Subterranean Homesick Alien", "4:27"],
        [1001, 4, "4", "Exit Music (For a Film)", "4:24"],
        [1001, 5, "5", "Let Down", "4:59"],
        # Release 1002 (OK Computer US Vinyl) - 3 tracks (US wins despite fewer tracks)
        [1002, 1, "A1", "Airbag", "4:44"],
        [1002, 2, "A2", "Paranoid Android", "6:23"],
        [1002, 3, "A3", "Subterranean Homesick Alien", "4:27"],
        # Release 1003 (OK Computer JP Cassette) - 1 track
        [1003, 1, "1", "Airbag", "4:44"],
        # Release 2001 (Unknown Pleasures UK LP) - 2 tracks
        [2001, 1, "A1", "Disorder", "3:29"],
        [2001, 2, "A2", "Day of the Lords", "4:48"],
        # Release 2002 (Unknown Pleasures DE CD) - 4 tracks (wins by track count, no US)
        [2002, 1, "1", "Disorder", "3:29"],
        [2002, 2, "2", "Day of the Lords", "4:48"],
        [2002, 3, "3", "Candidate", "3:05"],
        [2002, 4, "4", "Insight", "4:03"],
        # Release 3001 (Kid A) - 2 tracks
        [3001, 1, "1", "Everything In Its Right Place", "4:11"],
        [3001, 2, "2", "Kid A", "4:44"],
        # Release 4001 (Amnesiac, no master_id) - 2 tracks
        [4001, 1, "1", "Packt Like Sardines in a Crushd Tin Box", "4:00"],
        [4001, 2, "2", "Pyramid Song", "4:49"],
        # Release 5001 (Unknown Album) - 1 track
        [5001, 1, "1", "Unknown Track", "3:00"],
        # Release 5002 (Another Unknown) - 1 track
        [5002, 1, "1", "Mystery Track", "2:30"],
        # Release 6001 (Homogenic) - 2 tracks
        [6001, 1, "1", "Hunter", "4:15"],
        [6001, 2, "2", "Joga", "5:05"],
        # Release 8001 (Sugar Hill compilation) - 2 tracks
        [8001, 1, "A1", "Rapper's Delight", "14:35"],
        [8001, 2, "A2", "Apache", "5:35"],
        # Release 9001 (Abbey Road) - 2 tracks
        [9001, 1, "A1", "Come Together", "4:20"],
        [9001, 2, "A2", "Something", "3:03"],
        # Release 9002 (Bridge Over Troubled Water) - 1 track
        [9002, 1, "A1", "Bridge Over Troubled Water", "4:52"],
        # Releases not in library
        [10001, 1, "1", "Random Track", "3:00"],
        [10002, 1, "1", "Obscure Track", "4:00"],
    ]
    write_csv("release_track.csv", headers, rows)


def create_release_track_artist_csv() -> None:
    """Create release_track_artist.csv for compilation track artists."""
    headers = ["release_id", "track_sequence", "artist_name"]
    rows = [
        # Compilation tracks
        [8001, 1, "Sugarhill Gang"],
        [8001, 2, "Incredible Bongo Band"],
    ]
    write_csv("release_track_artist.csv", headers, rows)


def create_release_label_csv() -> None:
    """Create release_label.csv with label names for releases.

    Includes:
    - Multiple labels per release (release 1001 has Parlophone and Capitol Records)
    - Labels for releases in the same dedup group (1001, 1002, 1003)
    - Labels for releases that won't match the library (5001, 5002)
    """
    headers = ["release_id", "label", "catno"]
    rows = [
        # Radiohead - OK Computer (dedup group, master_id 500)
        [1001, "Parlophone", "7243 8 55229 2 8"],
        [1001, "Capitol Records", "CDP 7243 8 55229 2 8"],
        [1002, "Capitol Records", "C1-55229"],
        [1003, "EMI", "TOCP-50201"],
        # Joy Division - Unknown Pleasures (dedup group, master_id 600)
        [2001, "Factory Records", "FACT 10"],
        [2002, "Qwest Records", "1-25840"],
        # Unique releases
        [3001, "Parlophone", "7243 5 27753 2 3"],
        [4001, "Parlophone", "7243 5 32764 2 8"],
        # Won't match library
        [5001, "Unknown Label", "UNK-001"],
        [5002, "Mystery Records", "MYS-002"],
        # Bjork
        [6001, "One Little Indian", "TPLP 71 CD"],
        # Compilation
        [8001, "Sugar Hill Records", "SH-542"],
        # Beatles, Simon & Garfunkel
        [9001, "Apple Records", "PCS 7088"],
        [9002, "Columbia", "KCS 9914"],
        # Not in library
        [10001, "Random Label", "RL-001"],
        [10002, "Obscure Label", "OL-002"],
    ]
    write_csv("release_label.csv", headers, rows)


def create_release_image_csv() -> None:
    """Create release_image.csv for artwork URL testing."""
    headers = ["release_id", "type", "width", "height", "uri"]
    rows = [
        # Primary image
        [1001, "primary", 600, 600, "https://img.discogs.com/abc123/release-1001.jpg"],
        [1001, "secondary", 300, 300, "https://img.discogs.com/abc123/release-1001-back.jpg"],
        # Only secondary (should be used as fallback)
        [2001, "secondary", 600, 600, "https://img.discogs.com/def456/release-2001.jpg"],
        # Primary for other releases
        [3001, "primary", 600, 600, "https://img.discogs.com/ghi789/release-3001.jpg"],
        [9001, "primary", 600, 600, "https://img.discogs.com/jkl012/release-9001.jpg"],
        # No image for some releases (5001, 5002) - tests artwork_url being NULL
    ]
    write_csv("release_image.csv", headers, rows)


def create_library_labels_csv() -> None:
    """Create library_labels.csv with WXYC label preferences.

    These represent labels WXYC actually owns for specific albums,
    used to influence dedup ranking via --library-labels.

    The existing release_label.csv has Discogs label data per release:
      - 1001: Parlophone, Capitol Records; 1002: Capitol Records; 1003: EMI
      - 2001: Factory Records; 2002: Qwest Records

    This fixture says WXYC owns the Parlophone pressing of OK Computer
    and the Factory Records pressing of Unknown Pleasures, which causes
    label-aware dedup to prefer 1001 over 1002 and 2001 over 2002
    (overriding the default track-count ranking).
    """
    headers = ["artist_name", "release_title", "label_name"]
    rows = [
        ["Joy Division", "Unknown Pleasures", "Factory Records"],
        ["Radiohead", "OK Computer", "Parlophone"],
    ]
    write_csv("library_labels.csv", headers, rows)


def create_label_hierarchy_csv() -> None:
    """Create label_hierarchy.csv with parent-child label relationships.

    Mirrors the output of discogs-xml-converter's label parser.
    Used to test sublabel resolution during label-aware dedup.

    The existing release_label.csv has:
      - 1001: Parlophone, Capitol Records
      - 1002: Capitol Records
      - 1003: EMI
    The library_labels.csv says WXYC owns "Parlophone" pressing.

    With this hierarchy, "EMI" (parent) matches releases labeled "Parlophone"
    or "Capitol Records" (sublabels), and vice versa.
    """
    headers = ["label_id", "label_name", "parent_label_id", "parent_label_name"]
    rows = [
        [2, "Parlophone", 1, "EMI"],
        [3, "Capitol Records", 1, "EMI"],
    ]
    write_csv("label_hierarchy.csv", headers, rows)


def create_library_db() -> None:
    """Create a SQLite library.db with (artist, title) pairs.

    These pairs determine KEEP/PRUNE outcomes in verify_cache.py.
    """
    db_path = FIXTURE_DIR / "library.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS library (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artist TEXT NOT NULL,
            title TEXT NOT NULL
        )
    """)

    # Library entries that should produce KEEP decisions
    entries = [
        ("Radiohead", "OK Computer"),
        ("Radiohead", "Kid A"),
        ("Radiohead", "Amnesiac"),
        ("Joy Division", "Unknown Pleasures"),
        ("Joy Division", "Closer"),
        ("Aphex Twin", "Selected Ambient Works 85-92"),
        ("Beatles, The", "Abbey Road"),
        ("Simon & Garfunkel", "Bridge Over Troubled Water"),
        ("Björk", "Homogenic"),
        # Compilation entry (Various prefix triggers compilation handling)
        ("Various Artists - Compilations", "Sugar Hill"),
        # Extra entries to make the index realistic
        ("Talking Heads", "Remain in Light"),
        ("Sonic Youth", "Daydream Nation"),
        ("Pixies", "Doolittle"),
        ("My Bloody Valentine", "Loveless"),
        ("Neutral Milk Hotel", "In the Aeroplane Over the Sea"),
        ("Pavement", "Slanted and Enchanted"),
        ("Guided By Voices", "Bee Thousand"),
        ("Built to Spill", "Perfect From Now On"),
        ("Modest Mouse", "The Lonesome Crowded West"),
        ("Sleater-Kinney", "Dig Me Out"),
    ]

    cur.executemany("INSERT INTO library (artist, title) VALUES (?, ?)", entries)
    conn.commit()
    conn.close()
    print(f"  library.db: {len(entries)} entries")


def create_library_artists_txt() -> None:
    """Create library_artists.txt for filter_csv.py testing.

    One artist name per line, matching the library.db entries.
    """
    artists = [
        "Radiohead",
        "Joy Division",
        "Aphex Twin",
        "The Beatles",
        "Simon & Garfunkel",
        "Björk",
        "Various Artists",
        "Talking Heads",
        "Sonic Youth",
        "Pixies",
        "My Bloody Valentine",
        "Neutral Milk Hotel",
        "Pavement",
        "Guided By Voices",
        "Built to Spill",
        "Modest Mouse",
        "Sleater-Kinney",
    ]

    path = FIXTURE_DIR / "library_artists.txt"
    with open(path, "w", encoding="utf-8") as f:
        for artist in artists:
            f.write(artist + "\n")
    print(f"  library_artists.txt: {len(artists)} artists")


def main() -> None:
    print("Generating test fixtures...")
    print()
    print("CSV files:")
    create_release_csv()
    create_release_artist_csv()
    create_release_track_csv()
    create_release_track_artist_csv()
    create_release_label_csv()
    create_release_image_csv()
    create_library_labels_csv()
    create_label_hierarchy_csv()
    print()
    print("Library data:")
    create_library_db()
    create_library_artists_txt()
    print()
    print("Done.")


if __name__ == "__main__":
    main()
