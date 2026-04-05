#!/usr/bin/env python3
"""Resolve name collisions between WXYC library artists and Discogs artists.

Reads WRONG_PERSON entries from a genre analysis CSV, looks up WXYC release
titles from library.db, and searches the Discogs PostgreSQL cache using title
trigram matching to find the correct Discogs artist for each mismatched entry.

Usage:
    python scripts/resolve_collisions.py \
        --input genre-analysis-results.csv \
        --library-db data/library.db \
        --output collision-resolutions.csv \
        --database-url postgresql://discogs:discogs@localhost:5433/discogs
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class WxycArtist:
    """A WXYC library artist identified as matching the wrong Discogs artist."""

    library_code_id: int
    artist_name: str
    wxyc_genre: str
    call_letters: str
    call_numbers: int
    titles: list[str] = field(default_factory=list)


@dataclass
class SearchCandidate:
    """A candidate Discogs release matching a title search."""

    release_id: int
    title: str
    artist_name: str
    artist_id: int | None
    title_sim: float
    artist_sim: float

    @property
    def combined_score(self) -> float:
        return self.title_sim * 0.6 + self.artist_sim * 0.4


@dataclass
class Resolution:
    """The resolution result for a single WXYC artist."""

    artist: WxycArtist
    wrong_discogs_artist_id: int | None
    status: str  # RESOLVED, AMBIGUOUS, UNRESOLVED
    best_candidate: SearchCandidate | None = None
    genres: list[str] = field(default_factory=list)
    styles: list[str] = field(default_factory=list)
    matched_title_count: int = 0


# ---------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------


def load_wrong_person_entries(csv_path: Path) -> dict[int, WxycArtist]:
    """Parse the genre analysis CSV, filter to WRONG_PERSON, deduplicate on library_code_id."""
    artists: dict[int, WxycArtist] = {}
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["diagnosis"] != "WRONG_PERSON":
                continue
            lcid = int(row["library_code_id"])
            if lcid not in artists:
                artists[lcid] = WxycArtist(
                    library_code_id=lcid,
                    artist_name=row["artist_name"],
                    wxyc_genre=row["wxyc_genre"],
                    call_letters=row["call_letters"],
                    call_numbers=int(row["call_numbers"]),
                )
    return artists


def load_wxyc_titles(library_db: Path, artists: dict[int, WxycArtist]) -> None:
    """Look up WXYC release titles for each artist from library.db.

    library.db doesn't have library_code_id directly — it has artist+genre.
    We match on artist name + genre to find release titles.
    """
    conn = sqlite3.connect(str(library_db))
    for artist in artists.values():
        rows = conn.execute(
            "SELECT title FROM library WHERE artist = ? AND genre = ?",
            (artist.artist_name, artist.wxyc_genre),
        ).fetchall()
        artist.titles = [row[0] for row in rows if row[0]]
    conn.close()

    # Report stats
    with_titles = sum(1 for a in artists.values() if a.titles)
    total_titles = sum(len(a.titles) for a in artists.values())
    logger.info(
        "Loaded titles: %d/%d artists have titles (%d total titles)",
        with_titles,
        len(artists),
        total_titles,
    )


# ---------------------------------------------------------------------------
# Discogs search
# ---------------------------------------------------------------------------


# Strategy 1: artist name B-tree filter + title trigram (fast, precise)
ARTIST_TITLE_SEARCH_SQL = """
SELECT r.id, r.title, ra.artist_name, ra.artist_id,
       similarity(lower(f_unaccent(r.title)), lower(f_unaccent(%s))) AS title_sim,
       1.0 AS artist_sim
FROM release r
JOIN release_artist ra ON ra.release_id = r.id AND ra.extra = 0
WHERE lower(left(ra.artist_name, 200)) = lower(%s)
  AND similarity(lower(f_unaccent(r.title)), lower(f_unaccent(%s))) > 0.3
ORDER BY similarity(lower(f_unaccent(r.title)), lower(f_unaccent(%s))) DESC
LIMIT 10
"""

# Strategy 2: title trigram only (slower, for when exact artist match fails)
# Uses artist_name trigram index to also match approximate names
TITLE_SEARCH_SQL = """
SELECT r.id, r.title, ra.artist_name, ra.artist_id,
       similarity(lower(f_unaccent(r.title)), lower(f_unaccent(%s))) AS title_sim,
       similarity(lower(f_unaccent(ra.artist_name)), lower(f_unaccent(%s))) AS artist_sim
FROM release r
JOIN release_artist ra ON ra.release_id = r.id AND ra.extra = 0
WHERE lower(f_unaccent(ra.artist_name)) %% lower(f_unaccent(%s))
  AND similarity(lower(f_unaccent(r.title)), lower(f_unaccent(%s))) > 0.3
ORDER BY (similarity(lower(f_unaccent(r.title)), lower(f_unaccent(%s))) * 0.6
        + similarity(lower(f_unaccent(ra.artist_name)), lower(f_unaccent(%s))) * 0.4) DESC
LIMIT 10
"""

WRONG_ARTIST_ID_SQL = """
SELECT DISTINCT ra.artist_id
FROM release_artist ra
WHERE ra.extra = 0 AND lower(left(ra.artist_name, 200)) = lower(%s)
LIMIT 1
"""

GENRE_SQL = "SELECT genre FROM release_genre WHERE release_id = %s"
STYLE_SQL = "SELECT style FROM release_style WHERE release_id = %s"


def search_by_title(
    conn: psycopg.Connection,
    title: str,
    artist_name: str,
) -> list[SearchCandidate]:
    """Search Discogs cache for releases matching a title, returning candidates.

    Strategy 1: exact artist name match (B-tree, fast) + title similarity filter.
    Strategy 2: artist name trigram (GIN) + title similarity filter (slower fallback).
    """
    # Strategy 1: exact artist name + title similarity
    with conn.cursor() as cur:
        cur.execute(ARTIST_TITLE_SEARCH_SQL, (title, artist_name, title, title))
        rows = cur.fetchall()

    if not rows:
        # Strategy 2: title trigram search (no artist filter).
        # Raise pg_trgm threshold to 0.5 to reduce candidate set on 18.9M rows.
        with conn.cursor() as cur:
            cur.execute("SET pg_trgm.similarity_threshold = 0.5")
            cur.execute(
                """
                SELECT r.id, r.title, ra.artist_name, ra.artist_id,
                       similarity(lower(f_unaccent(r.title)), lower(f_unaccent(%s))) AS title_sim,
                       similarity(lower(f_unaccent(ra.artist_name)), lower(f_unaccent(%s))) AS artist_sim
                FROM release r
                JOIN release_artist ra ON ra.release_id = r.id AND ra.extra = 0
                WHERE lower(f_unaccent(r.title)) %% lower(f_unaccent(%s))
                ORDER BY similarity(lower(f_unaccent(r.title)), lower(f_unaccent(%s))) DESC
                LIMIT 10
                """,
                (title, artist_name, title, title),
            )
            rows = cur.fetchall()
            cur.execute("SET pg_trgm.similarity_threshold = 0.3")

    return [
        SearchCandidate(
            release_id=row[0],
            title=row[1],
            artist_name=row[2],
            artist_id=row[3],
            title_sim=float(row[4]),
            artist_sim=float(row[5]),
        )
        for row in rows
    ]


def batch_get_wrong_artist_ids(
    conn: psycopg.Connection, artist_names: list[str]
) -> dict[str, int | None]:
    """Find the Discogs artist_id for each name in a single query."""
    if not artist_names:
        return {}

    result: dict[str, int | None] = {}
    # Query in batches to avoid query-size limits
    batch_size = 500
    for i in range(0, len(artist_names), batch_size):
        batch = artist_names[i : i + batch_size]
        placeholders = ", ".join(["%s"] * len(batch))
        sql = f"""
            SELECT lower(left(ra.artist_name, 200)) AS name_lower,
                   MIN(ra.artist_id) AS artist_id
            FROM release_artist ra
            WHERE ra.extra = 0
              AND lower(left(ra.artist_name, 200)) IN ({placeholders})
            GROUP BY lower(left(ra.artist_name, 200))
        """
        with conn.cursor() as cur:
            cur.execute(sql, [n.lower() for n in batch])
            for row in cur.fetchall():
                result[row[0]] = row[1]

    return result


def get_genres_styles(conn: psycopg.Connection, release_id: int) -> tuple[list[str], list[str]]:
    """Fetch genres and styles for a release."""
    with conn.cursor() as cur:
        cur.execute(GENRE_SQL, (release_id,))
        genres = [row[0] for row in cur.fetchall()]
        cur.execute(STYLE_SQL, (release_id,))
        styles = [row[0] for row in cur.fetchall()]
    return genres, styles


# ---------------------------------------------------------------------------
# Resolution logic
# ---------------------------------------------------------------------------


def resolve_artist(
    conn: psycopg.Connection,
    artist: WxycArtist,
    confidence_threshold: float,
    wrong_id: int | None = None,
) -> Resolution:
    """Attempt to resolve a WRONG_PERSON artist to the correct Discogs artist."""

    if not artist.titles:
        return Resolution(
            artist=artist,
            wrong_discogs_artist_id=wrong_id,
            status="UNRESOLVED",
        )

    # Search for each WXYC title and collect candidates
    all_candidates: list[SearchCandidate] = []
    for title in artist.titles:
        candidates = search_by_title(conn, title, artist.artist_name)
        # Exclude the wrong artist from candidates
        if wrong_id is not None:
            candidates = [c for c in candidates if c.artist_id != wrong_id]
        all_candidates.extend(candidates)

    if not all_candidates:
        return Resolution(
            artist=artist,
            wrong_discogs_artist_id=wrong_id,
            status="UNRESOLVED",
        )

    # Group candidates by artist_id to find cross-title agreement
    artist_scores: dict[int | None, list[SearchCandidate]] = {}
    for c in all_candidates:
        artist_scores.setdefault(c.artist_id, []).append(c)

    # Find the best artist_id (most title matches, then highest score)
    best_artist_id = max(
        artist_scores,
        key=lambda aid: (
            len(artist_scores[aid]),
            max(c.combined_score for c in artist_scores[aid]),
        ),
    )
    best_group = artist_scores[best_artist_id]
    best_candidate = max(best_group, key=lambda c: c.combined_score)
    matched_title_count = len(best_group)

    # Classification
    score = best_candidate.combined_score
    multi_title = matched_title_count > 1

    if score >= confidence_threshold or multi_title:
        genres, styles = get_genres_styles(conn, best_candidate.release_id)
        return Resolution(
            artist=artist,
            wrong_discogs_artist_id=wrong_id,
            status="RESOLVED",
            best_candidate=best_candidate,
            genres=genres,
            styles=styles,
            matched_title_count=matched_title_count,
        )

    # Check for ambiguity: multiple artist_ids with similar scores
    if len(artist_scores) > 1:
        sorted_aids = sorted(
            artist_scores,
            key=lambda aid: max(c.combined_score for c in artist_scores[aid]),
            reverse=True,
        )
        top_score = max(c.combined_score for c in artist_scores[sorted_aids[0]])
        second_score = max(c.combined_score for c in artist_scores[sorted_aids[1]])
        if top_score - second_score < 0.10 and score >= 0.40:
            return Resolution(
                artist=artist,
                wrong_discogs_artist_id=wrong_id,
                status="AMBIGUOUS",
                best_candidate=best_candidate,
                matched_title_count=matched_title_count,
            )

    if score >= 0.40:
        return Resolution(
            artist=artist,
            wrong_discogs_artist_id=wrong_id,
            status="AMBIGUOUS",
            best_candidate=best_candidate,
            matched_title_count=matched_title_count,
        )

    return Resolution(
        artist=artist,
        wrong_discogs_artist_id=wrong_id,
        status="UNRESOLVED",
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

OUTPUT_COLUMNS = [
    "library_code_id",
    "wxyc_artist",
    "wxyc_genre",
    "wxyc_titles",
    "wrong_discogs_artist_id",
    "resolved_discogs_release_id",
    "resolved_discogs_artist_id",
    "resolved_discogs_artist_name",
    "resolved_discogs_title",
    "resolved_genres",
    "resolved_styles",
    "match_confidence",
    "matched_title_count",
    "resolution_status",
]


def write_results(results: list[Resolution], output_path: Path) -> None:
    """Write resolution results to CSV."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for r in results:
            row = {
                "library_code_id": r.artist.library_code_id,
                "wxyc_artist": r.artist.artist_name,
                "wxyc_genre": r.artist.wxyc_genre,
                "wxyc_titles": "; ".join(r.artist.titles),
                "wrong_discogs_artist_id": r.wrong_discogs_artist_id or "",
                "resolved_discogs_release_id": "",
                "resolved_discogs_artist_id": "",
                "resolved_discogs_artist_name": "",
                "resolved_discogs_title": "",
                "resolved_genres": "",
                "resolved_styles": "",
                "match_confidence": "",
                "matched_title_count": r.matched_title_count,
                "resolution_status": r.status,
            }
            if r.best_candidate:
                row["resolved_discogs_release_id"] = r.best_candidate.release_id
                row["resolved_discogs_artist_id"] = r.best_candidate.artist_id or ""
                row["resolved_discogs_artist_name"] = r.best_candidate.artist_name
                row["resolved_discogs_title"] = r.best_candidate.title
                row["match_confidence"] = f"{r.best_candidate.combined_score:.3f}"
            if r.genres:
                row["resolved_genres"] = "; ".join(r.genres)
            if r.styles:
                row["resolved_styles"] = "; ".join(r.styles)
            writer.writerow(row)


def print_summary(results: list[Resolution]) -> None:
    """Print summary statistics."""
    total = len(results)
    resolved = sum(1 for r in results if r.status == "RESOLVED")
    ambiguous = sum(1 for r in results if r.status == "AMBIGUOUS")
    unresolved = sum(1 for r in results if r.status == "UNRESOLVED")

    print(f"\n{'=' * 60}")
    print("NAME COLLISION RESOLUTION SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total artists:  {total:>6}")
    print(f"  RESOLVED:     {resolved:>6} ({100 * resolved / total:.1f}%)")
    print(f"  AMBIGUOUS:    {ambiguous:>6} ({100 * ambiguous / total:.1f}%)")
    print(f"  UNRESOLVED:   {unresolved:>6} ({100 * unresolved / total:.1f}%)")

    if resolved > 0:
        avg_confidence = (
            sum(
                r.best_candidate.combined_score
                for r in results
                if r.status == "RESOLVED" and r.best_candidate
            )
            / resolved
        )
        print(f"\n  Avg confidence (RESOLVED): {avg_confidence:.3f}")

    # Genre breakdown of resolved entries
    genre_counts: dict[str, int] = {}
    for r in results:
        if r.status == "RESOLVED" and r.genres:
            for g in r.genres:
                genre_counts[g] = genre_counts.get(g, 0) + 1
    if genre_counts:
        print("\n  Resolved Discogs genres:")
        for genre, count in sorted(genre_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"    {genre:<30} {count:>4}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve name collisions between WXYC and Discogs artists."
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to genre-analysis-results.csv",
    )
    parser.add_argument(
        "--library-db",
        required=True,
        type=Path,
        help="Path to library.db (WXYC release titles)",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output CSV path",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL_DISCOGS"),
        help="PostgreSQL URL for Discogs cache (default: $DATABASE_URL_DISCOGS)",
    )
    parser.add_argument(
        "--confidence-threshold",
        type=float,
        default=0.55,
        help="Minimum combined score for RESOLVED status (default: 0.55)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log each artist resolution",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if not args.database_url:
        print("Error: --database-url or DATABASE_URL_DISCOGS required", file=sys.stderr)
        sys.exit(1)

    if not args.input.exists():
        print(f"Error: input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    if not args.library_db.exists():
        print(f"Error: library.db not found: {args.library_db}", file=sys.stderr)
        sys.exit(1)

    # Load input
    logger.info("Loading WRONG_PERSON entries from %s", args.input)
    artists = load_wrong_person_entries(args.input)
    logger.info("Loaded %d unique artists to resolve", len(artists))

    # Load WXYC release titles
    logger.info("Loading release titles from %s", args.library_db)
    load_wxyc_titles(args.library_db, artists)

    # Connect to Discogs cache
    logger.info("Connecting to Discogs cache: %s", args.database_url.split("@")[-1])
    conn = psycopg.connect(args.database_url)

    # Batch-lookup wrong artist IDs (uses B-tree index, fast)
    logger.info("Looking up wrong Discogs artist IDs...")
    unique_names = list({a.artist_name for a in artists.values()})
    wrong_ids = batch_get_wrong_artist_ids(conn, unique_names)
    logger.info("Found wrong IDs for %d/%d artist names", len(wrong_ids), len(unique_names))

    # Resolve each artist
    results: list[Resolution] = []
    start = time.time()
    for i, artist in enumerate(artists.values(), 1):
        wrong_id = wrong_ids.get(artist.artist_name.lower())
        resolution = resolve_artist(conn, artist, args.confidence_threshold, wrong_id)
        results.append(resolution)

        if args.verbose and resolution.best_candidate:
            logger.info(
                "  %s (%s) -> %s [%s] (%.3f)",
                artist.artist_name,
                artist.wxyc_genre,
                resolution.best_candidate.artist_name,
                resolution.status,
                resolution.best_candidate.combined_score,
            )

        if i % 50 == 0:
            elapsed = time.time() - start
            rate = i / elapsed
            remaining = (len(artists) - i) / rate
            logger.info(
                "Progress: %d/%d (%.0f/s, ~%.0fs remaining)",
                i,
                len(artists),
                rate,
                remaining,
            )

    conn.close()
    elapsed = time.time() - start
    logger.info("Resolution complete in %.1fs", elapsed)

    # Write output
    write_results(results, args.output)
    logger.info("Results written to %s", args.output)

    # Print summary
    print_summary(results)


if __name__ == "__main__":
    main()
