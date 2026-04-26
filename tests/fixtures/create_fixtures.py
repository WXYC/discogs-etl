#!/usr/bin/env python3
"""Generate test fixture data for the discogs-etl test suite.

Creates minimal CSV files, a SQLite library.db, and library_artists.txt
that exercise the full pipeline: import, dedup, prune, and filter.

Run from the repo root:
    python tests/fixtures/create_fixtures.py

The generated files are checked into the repo so tests can run without
regenerating them.  Re-run this script if you need to modify the fixture data.

Artist data uses the canonical WXYC artist pool from
wxyc-shared/src/test-utils/wxyc-example-data.json (canonicalArtistNames).
WXYC is a freeform college radio station -- fixtures should reflect what
the station actually plays, not mainstream rock defaults.
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
        # Group 1: duplicate master_id 500 (Autechre - Confield variants)
        # Release 1001 has 5 tracks (UK), 1002 has 3 tracks (US), 1003 has 1 track (JP)
        # Dedup should keep 1002 (US country preference beats higher track count)
        [1001, "Accepted", "Confield", "UK", "2001-04-23", "", "Correct", 500, "CD"],
        [1002, "Accepted", "Confield", "US", "2001-05-29", "", "Correct", 500, "Vinyl"],
        [1003, "Accepted", "Confield", "JP", "2001", "", "Correct", 500, "Cassette"],
        # Group 2: duplicate master_id 600 (Stereolab - Aluminum Tunes)
        # Release 2001 has 2 tracks (UK), 2002 has 4 tracks (DE) -- no US release
        # Dedup should keep 2002 (most tracks, fallback when no US release)
        [2001, "Accepted", "Aluminum Tunes", "UK", "1998-09-01", "", "Correct", 600, "LP"],
        [2002, "Accepted", "Aluminum Tunes", "DE", "1998", "", "Correct", 600, "CD"],
        # No duplicate - unique master_id
        [3001, "Accepted", "Amber", "UK", "1994-11-07", "", "Correct", 700, "CD"],
        # No master_id (should survive dedup)
        [4001, "Accepted", "Tri Repetae", "UK", "1995-11-13", "", "Correct", "", "CD"],
        # Release that won't match library (should be pruned)
        [5001, "Accepted", "Unknown Album", "US", "2020-01-01", "", "Correct", 800, "CD"],
        [5002, "Accepted", "Another Unknown", "US", "", "", "Correct", 900, "CD"],
        # Bad date format
        [6001, "Accepted", "PAINLESS", "UK", "Unknown", "", "Correct", 1000, "CD"],
        # Missing title (should be skipped during import - required field)
        [7001, "Accepted", "", "US", "2023", "", "Correct", 1100, "CD"],
        # Compilation release
        [
            8001,
            "Accepted",
            "Nordic Roots: A Northside Collection",
            "US",
            "1998",
            "",
            "Correct",
            1200,
            "CD",
        ],
        # Various date format edge cases
        [
            9001,
            "Accepted",
            "From Here We Go Sublime",
            "UK",
            "2007-03-26",
            "",
            "Correct",
            1300,
            "LP",
        ],
        [
            9002,
            "Accepted",
            "Duke Ellington & John Coltrane",
            "US",
            "1963",
            "",
            "Correct",
            1400,
            "LP",
        ],
        # Artist not in library
        [10001, "Accepted", "Some Random Album", "US", "2023-05-01", "", "Correct", 1500, "CD"],
        [10002, "Accepted", "Obscure Release", "DE", "2022", "", "Correct", 1600, "CD"],
    ]
    write_csv("release.csv", headers, rows)


def create_release_artist_csv() -> None:
    """Create release_artist.csv linking releases to artists.

    Artist ID convention (preserves the IDs used in the previous fixture):
      1  Autechre               (was Radiohead)
      2  Stereolab              (was Joy Division)
      3  DJ Unknown             (filler)
      4  Mystery Band           (filler)
      5  Nilufer Yanya          (was Bjork; tests diacritic handling)
      7  Various                (compilation marker)
      8  Field, The             (was "Beatles, The"; tests "X, The" inversion)
      9  Duke Ellington         (was Simon & Garfunkel; tests `&` multi-artist)
     10  Random Artist X        (filler)
     11  Obscure Band Y         (filler)
     12  Some Producer          (extra credit)
     13  John Coltrane          (second artist on release 9002, joined via `&`)
    """
    headers = [
        "release_id",
        "artist_id",
        "artist_name",
        "extra",
        "anv",
        "position",
        "join_field",
        "role",
    ]
    rows = [
        # Autechre releases (match library)
        [1001, 1, "Autechre", 0, "", 1, "", ""],
        [1002, 1, "Autechre", 0, "", 1, "", ""],
        [1003, 1, "Autechre", 0, "", 1, "", ""],
        [3001, 1, "Autechre", 0, "", 1, "", ""],
        [4001, 1, "Autechre", 0, "", 1, "", ""],
        # Stereolab releases (match library)
        [2001, 2, "Stereolab", 0, "", 1, "", ""],
        [2002, 2, "Stereolab", 0, "", 1, "", ""],
        # Unknown artists (won't match library)
        [5001, 3, "DJ Unknown", 0, "", 1, "", ""],
        [5002, 4, "Mystery Band", 0, "", 1, "", ""],
        # Nilufer Yanya (match library, tests diacritic handling: "Nilüfer Yanya" with U+00FC)
        [6001, 5, "Nilüfer Yanya", 0, "", 1, "", ""],
        # Note: release 7001 has empty title and is skipped during import,
        # so no child table rows should reference it.
        # Compilation -- bare V/A form (Discogs's canonical compilation artist)
        [8001, 7, "Various", 0, "", 1, "", ""],
        # Field, The (tests "X, The" inversion) and Duke Ellington & John Coltrane (multi-artist)
        [9001, 8, "Field, The", 0, "", 1, "", ""],
        [9002, 9, "Duke Ellington", 0, "", 1, "", "&"],
        [9002, 13, "John Coltrane", 0, "", 2, "", ""],
        # Not in library
        [10001, 10, "Random Artist X", 0, "", 1, "", ""],
        [10002, 11, "Obscure Band Y", 0, "", 1, "", ""],
        # Extra artist credit (should not be primary)
        [1001, 12, "Some Producer", 1, "", 2, "", ""],
    ]
    write_csv("release_artist.csv", headers, rows)


def create_release_track_csv() -> None:
    """Create release_track.csv with tracks for each release.

    Track counts matter for dedup (release with most tracks wins).
    """
    headers = ["release_id", "sequence", "position", "title", "duration"]
    rows = [
        # Release 1001 (Confield UK CD) - 5 tracks (most tracks, but not US)
        [1001, 1, "1", "VI Scose Poise", "4:51"],
        [1001, 2, "2", "Cfern", "5:41"],
        [1001, 3, "3", "Pen Expers", "5:54"],
        [1001, 4, "4", "Sim Gishel", "6:46"],
        [1001, 5, "5", "Parhelic Triangle", "8:18"],
        # Release 1002 (Confield US Vinyl) - 3 tracks (US wins despite fewer tracks)
        [1002, 1, "A1", "VI Scose Poise", "4:51"],
        [1002, 2, "A2", "Cfern", "5:41"],
        [1002, 3, "A3", "Pen Expers", "5:54"],
        # Release 1003 (Confield JP Cassette) - 1 track
        [1003, 1, "1", "VI Scose Poise", "4:51"],
        # Release 2001 (Aluminum Tunes UK LP) - 2 tracks
        [2001, 1, "A1", "Pop Quiz", "3:01"],
        [2001, 2, "A2", "Fuses", "7:29"],
        # Release 2002 (Aluminum Tunes DE CD) - 4 tracks (wins by track count, no US)
        [2002, 1, "1", "Pop Quiz", "3:01"],
        [2002, 2, "2", "Fuses", "7:29"],
        [2002, 3, "3", "Iron Man", "4:34"],
        [2002, 4, "4", "Le Coeur Et La Force", "6:11"],
        # Release 3001 (Amber) - 2 tracks
        [3001, 1, "1", "Foil", "4:11"],
        [3001, 2, "2", "Montreal", "4:44"],
        # Release 4001 (Tri Repetae, no master_id) - 2 tracks
        [4001, 1, "1", "Dael", "8:39"],
        [4001, 2, "2", "Clipper", "10:11"],
        # Release 5001 (Unknown Album) - 1 track
        [5001, 1, "1", "Unknown Track", "3:00"],
        # Release 5002 (Another Unknown) - 1 track
        [5002, 1, "1", "Mystery Track", "2:30"],
        # Release 6001 (PAINLESS) - 2 tracks
        [6001, 1, "1", "the dealer", "4:15"],
        [6001, 2, "2", "stabilise", "3:39"],
        # Release 8001 (Nordic Roots compilation) - 2 tracks
        [8001, 1, "1", "Slottet i Österrike", "4:18"],
        [8001, 2, "2", "Bortglömda Toner", "5:35"],
        # Release 9001 (From Here We Go Sublime) - 2 tracks
        [9001, 1, "A1", "Over the Ice", "5:20"],
        [9001, 2, "A2", "A Paw In My Face", "4:48"],
        # Release 9002 (Duke Ellington & John Coltrane) - 1 track
        [9002, 1, "A1", "In a Sentimental Mood", "4:19"],
        # Releases not in library
        [10001, 1, "1", "Random Track", "3:00"],
        [10002, 1, "1", "Obscure Track", "4:00"],
    ]
    write_csv("release_track.csv", headers, rows)


def create_release_track_artist_csv() -> None:
    """Create release_track_artist.csv for compilation track artists."""
    headers = ["release_id", "track_sequence", "artist_name"]
    rows = [
        # Compilation tracks (release 8001 = Nordic Roots compilation)
        [8001, 1, "Garmarna"],
        [8001, 2, "Hedningarna"],
    ]
    write_csv("release_track_artist.csv", headers, rows)


def create_release_label_csv() -> None:
    """Create release_label.csv with label names for releases.

    Includes:
    - Multiple labels per release (release 1001 has Warp and Arcola)
    - Labels for releases in the same dedup group (1001, 1002, 1003)
    - Labels for releases that won't match the library (5001, 5002)
    """
    headers = ["release_id", "label", "catno"]
    rows = [
        # Autechre - Confield (dedup group, master_id 500)
        [1001, "Warp Records", "WARPCD96"],
        [1001, "Arcola", "ARC-WARPCD96"],
        [1002, "Arcola", "ARC-CD96"],
        [1003, "Beat Records", "BRC-50"],
        # Stereolab - Aluminum Tunes (dedup group, master_id 600)
        [2001, "Duophonic UHF Disks", "DUHF-08"],
        [2002, "Drag City", "DC153"],
        # Unique releases
        [3001, "Warp Records", "WARP30"],
        [4001, "Warp Records", "WARP38"],
        # Won't match library
        [5001, "Unknown Label", "UNK-001"],
        [5002, "Mystery Records", "MYS-002"],
        # Nilufer Yanya
        [6001, "ATO Records", "ATO0589"],
        # Compilation
        [8001, "NorthSide", "NSD6029"],
        # Field, The; Duke Ellington & John Coltrane
        [9001, "Kompakt", "KOMPAKT 144"],
        [9002, "Impulse Records", "A-30"],
        # Not in library
        [10001, "Random Label", "RL-001"],
        [10002, "Obscure Label", "OL-002"],
    ]
    write_csv("release_label.csv", headers, rows)


def create_release_video_csv() -> None:
    """Create release_video.csv with video entries for various releases.

    Includes:
    - Multiple videos for one release (1001: 2 videos)
    - embed=false (2001: one video without embed)
    - Missing duration (5001: empty duration → NULL)
    - A release that will be pruned (5001: Unknown Album, not in library)
    """
    headers = ["release_id", "sequence", "src", "title", "duration", "embed"]
    rows = [
        # Confield UK CD (1001) — survives pipeline prune
        [1001, 1, "https://www.youtube.com/watch?v=abcdef01", "VI Scose Poise", 291, "true"],
        [1001, 2, "https://www.youtube.com/watch?v=abcdef02", "Cfern", 341, "true"],
        # Aluminum Tunes UK LP (2001) — survives pipeline prune; embed=false
        [2001, 1, "https://www.youtube.com/watch?v=uvwxyz01", "Pop Quiz", 181, "false"],
        # Amber (3001) — survives pipeline prune
        [
            3001,
            1,
            "https://www.youtube.com/watch?v=ghijkl01",
            "Foil",
            251,
            "true",
        ],
        # Unknown Album (5001) — pruned; empty duration
        [5001, 1, "https://www.youtube.com/watch?v=mnopqr01", "Unknown Track", "", "true"],
    ]
    write_csv("release_video.csv", headers, rows)


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
      - 1001: Warp Records, Arcola; 1002: Arcola; 1003: Beat Records
      - 2001: Duophonic UHF Disks; 2002: Drag City

    This fixture says WXYC owns the Warp Records pressing of Confield
    and the Duophonic UHF Disks pressing of Aluminum Tunes, which causes
    label-aware dedup to prefer 1001 over 1002 and 2001 over 2002
    (overriding the default track-count ranking).
    """
    headers = ["artist_name", "release_title", "label_name"]
    rows = [
        ["Stereolab", "Aluminum Tunes", "Duophonic UHF Disks"],
        ["Autechre", "Confield", "Warp Records"],
    ]
    write_csv("library_labels.csv", headers, rows)


def create_label_hierarchy_csv() -> None:
    """Create label_hierarchy.csv with parent-child label relationships.

    Mirrors the output of discogs-xml-converter's label parser.
    Used to test sublabel resolution during label-aware dedup.

    The existing release_label.csv has:
      - 1001: Warp Records, Arcola
      - 1002: Arcola
      - 1003: Beat Records
    The library_labels.csv says WXYC owns "Warp Records" pressing.

    With this hierarchy, "Warp Records" (parent) matches releases labeled
    "Arcola" (sublabel), and vice versa.
    """
    headers = ["label_id", "label_name", "parent_label_id", "parent_label_name"]
    rows = [
        [2, "Arcola", 1, "Warp Records"],
        [3, "Duophonic UHF Disks", 4, "Duophonic"],
    ]
    write_csv("label_hierarchy.csv", headers, rows)


def create_library_db() -> None:
    """Create a SQLite library.db with (artist, title, format) tuples.

    These entries determine KEEP/PRUNE outcomes in verify_cache.py.
    Some albums have multiple entries with different formats to test
    format-aware verify/prune.
    """
    db_path = FIXTURE_DIR / "library.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS library")
    cur.execute("""
        CREATE TABLE library (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artist TEXT NOT NULL,
            title TEXT NOT NULL,
            format TEXT
        )
    """)

    # Library entries that should produce KEEP decisions.
    # Artist names use the canonical WXYC pool (canonicalArtistNames in
    # wxyc-shared/src/test-utils/wxyc-example-data.json) plus controlled
    # variants for specific test cases:
    #   - "Field, The"            -- "X, The" inversion of canonical "The Field"
    #   - "Various Artists ..."   -- compilation markers (4 V/A shapes observed
    #                                in the production catalog)
    #   - "Nilüfer Yanya"         -- diacritic-bearing canonical artist
    entries = [
        # Multi-format same album (CD + LP for one Stereolab album)
        ("Stereolab", "Aluminum Tunes", "CD"),
        ("Stereolab", "Aluminum Tunes", "LP"),  # library owns both formats
        # Multi-album per artist + null format (Stereolab Dots and Loops)
        ("Stereolab", "Dots and Loops", None),  # some entries have no format
        # Multi-album per artist (Autechre catalog)
        ("Autechre", "Confield", "CD"),
        ("Autechre", "Amber", "CD"),
        ("Autechre", "Tri Repetae", "CD"),
        # Already-canonical artists from the original fixture (kept)
        ("Aphex Twin", "Selected Ambient Works 85-92", "CD"),
        ("Pixies", "Doolittle", "CD"),
        # "X, The" inversion test (canonical form is "The Field")
        ("Field, The", "From Here We Go Sublime", "LP"),
        # Multi-artist with `&` separator (canonical individuals: Duke Ellington
        # and John Coltrane -- the joint form is the WXYC catalog string)
        ("Duke Ellington & John Coltrane", "Duke Ellington & John Coltrane", "LP"),
        # Diacritic-bearing canonical artist (U+00FC)
        ("Nilüfer Yanya", "PAINLESS", "CD"),
        # Compilation entries -- the four V/A shapes observed in the
        # production WXYC catalog (tubafrenzy + staging Postgres dump):
        #   bare, genre + alphabetical sub-bucket, single-segment genre,
        #   bracketed group form. All trigger is_compilation_artist() via
        #   the substring match on "various".
        ("Various Artists", "Nordic Roots: A Northside Collection", "CD"),
        ("Various Artists - Rock - A", "All Tomorrow's Parties 5.0", "LP"),
        ("Various Artists - Hiphop", "Stones Throw Ten Years", "CD"),
        ("Various Artists [group]", "Sublime Frequencies: Radio Pyongyang", "LP"),
        # Filler entries from canonical pool (one each, mixed genres)
        ("Cat Power", "Moon Pix", "LP"),
        ("Jessica Pratt", "On Your Own Love Again", "LP"),
        ("Father John Misty", "I Love You, Honeybear", "LP"),
        ("Buck Meek", "Gasoline", "LP"),
        ("Sessa", "Pequena Vertigem de Amor", "LP"),
        ("Rochelle Jordan", "Through the Wall", "CD"),
    ]

    cur.executemany("INSERT INTO library (artist, title, format) VALUES (?, ?, ?)", entries)
    conn.commit()
    conn.close()
    print(f"  library.db: {len(entries)} entries")


def create_library_artists_txt() -> None:
    """Create library_artists.txt for filter_csv.py testing.

    One artist name per line. Mirrors what production tooling
    (`wxyc-enrich-library-artists` from wxyc-catalog) actually emits when
    derived from library.db:

    - Compilation artists (Various Artists, etc.) are excluded via
      `is_compilation_artist()`.
    - Multi-artist names joined by `&`/`,`/`/`/` + ` are split via
      `split_artist_name_contextual()`. So "Duke Ellington & John Coltrane"
      becomes two distinct lines.
    """
    artists = [
        "Stereolab",
        "Autechre",
        "Aphex Twin",
        "Pixies",
        "Field, The",  # "X, The" inversion test variant (canonical: "The Field")
        "Duke Ellington",
        "John Coltrane",
        "Nilüfer Yanya",
        "Cat Power",
        "Jessica Pratt",
        "Father John Misty",
        "Buck Meek",
        "Sessa",
        "Rochelle Jordan",
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
    create_release_video_csv()
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
