#!/usr/bin/env python3
"""Verify and optionally prune the Discogs cache against the WXYC library catalog.

Uses multi-index (artist, album) pair matching with three independent fuzzy
scorers that must agree, classifying each Discogs release as:

  - KEEP: release matches a library (artist, title) pair
  - PRUNE: release has no plausible library match (safe to delete)
  - REVIEW: ambiguous match requiring human confirmation

The three scorers (token_set_ratio, token_sort_ratio, two-stage artist+title)
compensate for each other's weaknesses. The two-stage scorer must participate
in any KEEP decision to prevent false positives from partial name matches.

Previously confirmed decisions are loaded from artist_mappings.json to
auto-resolve REVIEW items on subsequent runs.

Usage:
    # Dry run (default): report what would be pruned and estimate space savings
    python verify_cache.py /path/to/library.db [database_url]

    # Prune: actually delete PRUNE releases (REVIEW releases are never deleted)
    python verify_cache.py --prune /path/to/library.db [database_url]

    # Use a custom mappings file
    python verify_cache.py --mappings-file ./my_mappings.json /path/to/library.db

    database_url defaults to postgresql:///discogs
"""

from __future__ import annotations

import argparse
import asyncio
import enum
import json
import logging
import multiprocessing
import os
import re
import sqlite3
import sys
import time
import unicodedata
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import asyncpg
import psycopg

try:
    from wxyc_etl.fuzzy import batch_classify_releases as _rust_batch_classify

    _HAS_WXYC_ETL = True
except ImportError:
    _HAS_WXYC_ETL = False

from rapidfuzz import fuzz, process

sys.path.insert(0, str(Path(__file__).parent.parent))
from wxyc_etl.text import is_compilation_artist, split_artist_name_contextual

from lib.format_normalization import format_matches, normalize_library_format
from lib.observability import init_logger

logger = logging.getLogger(__name__)

# Discogs suffixes like "(2)", "(3)" for disambiguation
DISCOGS_DISAMBIGUATION_RE = re.compile(r"\s*\(\d+\)\s*$")

# Library disambiguation brackets like "[NJ noise band]", "[Scotland]"
LIBRARY_DISAMBIGUATION_RE = re.compile(r"\s*\[.*?\]\s*$")

# Title suffixes to strip: vinyl formats, CD sets, reissues, editions, LP counts
TITLE_SUFFIX_RE = re.compile(
    r"""\s*(?:
        \d*"                            # 12", 7" (vinyl inch marks)
        |\(\d+\)                        # (3) Discogs disambiguation
        |\(\d+\s*(?:cd|lp)\s*set\)      # (2 cd set), (3 lp set)
        |\((?:reissue|deluxe\s+edition|expanded\s+edition
             |anniversary\s+edition|special\s+edition
             |limited\s+edition|bonus\s+tracks
             |ep|lp)\)
        |\(\d+lp\)                      # (2lp)
    )\s*$""",
    re.IGNORECASE | re.VERBOSE,
)

# Definite articles used in Discogs comma convention across languages.
# "Beatles, The" -> "The Beatles", "Fabulosos Cadillacs, Los" -> "Los Fabulosos Cadillacs"
COMMA_ARTICLES = ("the", "los", "las", "les", "la", "le", "el", "die", "der", "das")

# All tables that store per-release data.
# With FK CASCADE on child tables, deleting from release automatically
# cleans up release_artist, release_track, release_track_artist, and cache_metadata.
RELEASE_TABLES = [
    ("release", "id"),
    ("release_artist", "release_id"),
    ("release_label", "release_id"),
    ("release_genre", "release_id"),
    ("release_style", "release_id"),
    ("release_track", "release_id"),
    ("release_track_artist", "release_id"),
    ("cache_metadata", "release_id"),
]


def format_bytes(num_bytes: int) -> str:
    """Format byte count as human-readable string."""
    n = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def strip_accents(s: str) -> str:
    """Remove accent marks (e.g. e from Bjork)."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_title(title: str) -> str:
    """Normalize an album/title for comparison.

    Strips library format suffixes (12", 7"), CD/LP set counts,
    reissue/deluxe/ep tags, and Discogs disambiguation numbers.
    """
    title = title.strip().lower()
    title = strip_accents(title)
    # Repeatedly strip suffixes (e.g. 'Album 12" (reissue)')
    prev = None
    while title != prev:
        prev = title
        title = TITLE_SUFFIX_RE.sub("", title).strip()
    return title


def normalize_artist(name: str) -> str:
    """Normalize an artist name for comparison.

    Extends normalize_for_comparison with ampersand/and normalization
    and apostrophe removal.
    """
    name = normalize_for_comparison(name)
    # Normalize "&" to "and"
    name = re.sub(r"\s*&\s*", " and ", name)
    # Remove apostrophes
    name = name.replace("'", "")
    # Collapse whitespace
    name = " ".join(name.split())
    return name


def normalize_for_comparison(name: str) -> str:
    """Aggressively normalize an artist name for fuzzy comparison.

    Handles:
    - Case folding
    - Accent stripping
    - Discogs disambiguation suffixes: "Artist (2)"
    - Library disambiguation brackets: "Artist [Scotland]"
    - Discogs comma convention: "Beatles, The" -> "The Beatles"
    """
    name = name.strip().lower()
    name = strip_accents(name)

    # Remove Discogs disambiguation: "Artist (2)" -> "Artist"
    name = DISCOGS_DISAMBIGUATION_RE.sub("", name)

    # Remove library disambiguation: "Artist [Scotland]" -> "Artist"
    name = LIBRARY_DISAMBIGUATION_RE.sub("", name)

    # Flip Discogs comma convention: "Beatles, The" -> "The Beatles"
    # Handles definite articles across languages that Discogs uses in comma format.
    for article in COMMA_ARTICLES:
        suffix = f", {article}"
        if name.endswith(suffix):
            name = f"{article} " + name[: -len(suffix)]
            break

    return name.strip()


# Separator for combined artist/title strings used in fuzzy matching
COMBINED_SEPARATOR = " ||| "


class LibraryIndex:
    """Pre-built in-memory index of library (artist, title) pairs for fast matching.

    Attributes:
        exact_pairs: Set of (normalized_artist, normalized_title) tuples for exact lookup.
        artist_to_titles: Dict mapping normalized artist -> set of normalized titles.
        artist_to_titles_list: Dict mapping normalized artist -> list of normalized titles
            (pre-computed from artist_to_titles for use with rapidfuzz).
        combined_strings: List of "artist ||| title" strings for fuzzy matching.
        combined_to_original: Dict mapping combined string -> (norm_artist, norm_title).
        all_artists: Deduplicated list of normalized artist names (excludes compilations).
        compilation_titles: Set of normalized titles from compilation entries.
    """

    def __init__(
        self,
        exact_pairs: set[tuple[str, str]],
        artist_to_titles: dict[str, set[str]],
        combined_strings: list[str],
        combined_to_original: dict[str, tuple[str, str]],
        all_artists: list[str],
        compilation_titles: set[str],
        format_by_pair: dict[tuple[str, str], set[str | None]] | None = None,
    ):
        self.exact_pairs = exact_pairs
        self.artist_to_titles = artist_to_titles
        self.artist_to_titles_list: dict[str, list[str]] = {
            artist: list(titles) for artist, titles in artist_to_titles.items()
        }
        self.combined_strings = combined_strings
        self.combined_to_original = combined_to_original
        self.all_artists = all_artists
        self.compilation_titles = compilation_titles
        self.format_by_pair: dict[tuple[str, str], set[str | None]] = format_by_pair or {}

    @classmethod
    def from_rows(
        cls, rows: list[tuple[str, str]] | list[tuple[str, str, str | None]]
    ) -> LibraryIndex:
        """Build index from (artist, title) or (artist, title, format) row tuples.

        Args:
            rows: List of (raw_artist, raw_title) or (raw_artist, raw_title, raw_format)
                tuples from the library.
        """
        exact_pairs: set[tuple[str, str]] = set()
        artist_to_titles: dict[str, set[str]] = {}
        combined_to_original: dict[str, tuple[str, str]] = {}
        artist_set: set[str] = set()
        compilation_titles: set[str] = set()
        format_by_pair: dict[tuple[str, str], set[str | None]] = {}
        has_format = len(rows) > 0 and len(rows[0]) >= 3

        for row in rows:
            raw_artist, raw_title = row[0], row[1]
            raw_format = row[2] if has_format else None
            if not raw_artist or not raw_title:
                continue

            norm_title = normalize_title(raw_title)

            # Check if this is a compilation entry
            if is_compilation_artist(raw_artist):
                compilation_titles.add(norm_title)
                continue

            norm_artist = normalize_artist(raw_artist)
            pair = (norm_artist, norm_title)

            # Build format_by_pair before dedup check — same pair may have multiple formats
            if has_format:
                norm_format = normalize_library_format(raw_format)
                format_by_pair.setdefault(pair, set()).add(norm_format)

            if pair in exact_pairs:
                continue  # deduplicate (artist+title already indexed)

            exact_pairs.add(pair)
            artist_to_titles.setdefault(norm_artist, set()).add(norm_title)
            artist_set.add(norm_artist)

            combined = f"{norm_artist}{COMBINED_SEPARATOR}{norm_title}"
            combined_to_original[combined] = pair

        combined_strings = list(combined_to_original.keys())
        all_artists = sorted(artist_set)

        # Split multi-artist entries and add synthetic component pairs.
        # Components are added to exact_pairs and artist_to_titles only,
        # NOT to all_artists or compilation_titles, to avoid polluting
        # fuzzy scorer inputs that iterate the full artist list.
        known_normalized = set(artist_set)
        split_count = 0
        for row in rows:
            raw_artist, raw_title = row[0], row[1]
            if not raw_artist or not raw_title or is_compilation_artist(raw_artist):
                continue
            components = split_artist_name_contextual(raw_artist, known_normalized)
            if not components:
                continue
            split_count += 1
            norm_title = normalize_title(raw_title)
            for component in components:
                norm_component = normalize_artist(component)
                pair = (norm_component, norm_title)
                if pair not in exact_pairs:
                    exact_pairs.add(pair)
                    artist_to_titles.setdefault(norm_component, set()).add(norm_title)

        if split_count:
            logger.info(
                "Split %d multi-artist entries into component index entries",
                split_count,
            )

        return cls(
            exact_pairs=exact_pairs,
            artist_to_titles=artist_to_titles,
            combined_strings=combined_strings,
            combined_to_original=combined_to_original,
            all_artists=all_artists,
            compilation_titles=compilation_titles,
            format_by_pair=format_by_pair if has_format else None,
        )

    @classmethod
    def from_sqlite(cls, db_path: Path) -> LibraryIndex:
        """Build index from the library SQLite database.

        Loads format column if present (3-tuples); falls back to artist+title only (2-tuples).

        Args:
            db_path: Path to library.db
        """
        logger.info(f"Building LibraryIndex from {db_path}")
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        try:
            cur.execute("SELECT artist, title, format FROM library")
        except sqlite3.OperationalError:
            cur.execute("SELECT artist, title FROM library")
        rows = cur.fetchall()
        conn.close()

        index = cls.from_rows(rows)
        logger.info(
            f"LibraryIndex built: {len(index.exact_pairs):,} pairs, "
            f"{len(index.all_artists):,} artists, "
            f"{len(index.compilation_titles):,} compilation titles"
        )
        return index


# ---------------------------------------------------------------------------
# Scorers: each returns a float 0.0-1.0 for a (norm_artist, norm_title) pair
# ---------------------------------------------------------------------------


def score_exact(norm_artist: str, norm_title: str, index: LibraryIndex) -> float:
    """Return 1.0 if the exact (artist, title) pair is in the index, else 0.0."""
    return 1.0 if (norm_artist, norm_title) in index.exact_pairs else 0.0


def score_token_set(norm_artist: str, norm_title: str, index: LibraryIndex) -> float:
    """Score using token_set_ratio on combined 'artist ||| title' strings.

    Returns the best match score (0.0-1.0) against all library combined strings.
    """
    query = f"{norm_artist}{COMBINED_SEPARATOR}{norm_title}"
    result = process.extractOne(
        query,
        index.combined_strings,
        scorer=fuzz.token_set_ratio,
    )
    if result is None:
        return 0.0
    return float(result[1]) / 100.0


def score_token_sort(norm_artist: str, norm_title: str, index: LibraryIndex) -> float:
    """Score using token_sort_ratio on combined 'artist ||| title' strings.

    More sensitive to word order than token_set_ratio.
    Returns the best match score (0.0-1.0) against all library combined strings.
    """
    query = f"{norm_artist}{COMBINED_SEPARATOR}{norm_title}"
    result = process.extractOne(
        query,
        index.combined_strings,
        scorer=fuzz.token_sort_ratio,
    )
    if result is None:
        return 0.0
    return float(result[1]) / 100.0


def score_two_stage(
    norm_artist: str,
    norm_title: str,
    index: LibraryIndex,
    artist_threshold: int = 70,
) -> float:
    """Two-stage scorer: first match artist, then match title within that artist's albums.

    1. Fuzzy-match the artist name against all library artists.
    2. If a good artist match is found, fuzzy-match the title against
       that artist's known albums.
    3. Return the geometric mean of artist and title scores.

    This scorer is most precise because it separates the two dimensions,
    preventing a strong title match from compensating for a weak artist match.
    """
    if not index.all_artists:
        return 0.0

    # Stage 1: find best matching artist
    artist_result = process.extractOne(
        norm_artist,
        index.all_artists,
        scorer=fuzz.token_set_ratio,
        score_cutoff=artist_threshold,
    )
    if artist_result is None:
        return 0.0

    matched_artist, artist_score, _ = artist_result

    # Stage 2: match title within that artist's albums
    titles_list = index.artist_to_titles_list.get(matched_artist)
    if not titles_list:
        return 0.0

    title_result = process.extractOne(
        norm_title,
        titles_list,
        scorer=fuzz.token_set_ratio,
    )
    if title_result is None:
        return 0.0

    _, title_score, _ = title_result

    # Geometric mean of the two scores (both 0-100, return 0.0-1.0)
    return float((float(artist_score) * float(title_score)) ** 0.5) / 100.0


# ---------------------------------------------------------------------------
# Multi-index agreement
# ---------------------------------------------------------------------------


class Decision(enum.Enum):
    """Classification result for a Discogs release."""

    KEEP = "keep"
    PRUNE = "prune"
    REVIEW = "review"


@dataclass
class MatchResult:
    """Result of classifying a single (artist, title) pair."""

    decision: Decision
    exact_score: float
    token_set_score: float
    token_sort_score: float
    two_stage_score: float

    @property
    def max_fuzzy_score(self) -> float:
        return max(self.token_set_score, self.token_sort_score, self.two_stage_score)


class MultiIndexMatcher:
    """Classifies (artist, title) pairs as KEEP / PRUNE / REVIEW using multi-scorer agreement.

    Thresholds:
        - Exact match -> KEEP
        - 2-of-3 fuzzy scorers >= keep_threshold -> KEEP
        - 1 scorer >= high_threshold + 1 other >= moderate_threshold -> KEEP
        - Max fuzzy score >= review_threshold -> REVIEW
        - Otherwise -> PRUNE
    """

    def __init__(
        self,
        index: LibraryIndex,
        artist_mappings: dict[str, dict[str, str | None]] | None = None,
        keep_threshold: float = 0.75,
        high_threshold: float = 0.85,
        moderate_threshold: float = 0.70,
        review_threshold: float = 0.65,
    ):
        self.index = index
        self.artist_mappings = artist_mappings or {}
        self.keep_threshold = keep_threshold
        self.high_threshold = high_threshold
        self.moderate_threshold = moderate_threshold
        self.review_threshold = review_threshold

    def classify_known_artist(self, norm_artist: str, norm_title: str) -> MatchResult:
        """Classify a release when the artist is already known to be in the library.

        Skips the expensive combined-string scorers (token_set, token_sort) and the
        artist-lookup stage of two-stage. Only does exact pair match + direct title
        fuzzy match within the artist's known albums.
        Much faster: O(artist_titles) instead of O(all_library_pairs).
        """
        # Fast path: exact pair match
        if (norm_artist, norm_title) in self.index.exact_pairs:
            return MatchResult(Decision.KEEP, 1.0, 1.0, 1.0, 1.0)

        # Direct title match within this artist's albums (skip artist lookup)
        titles_list = self.index.artist_to_titles_list.get(norm_artist)
        if not titles_list:
            return MatchResult(Decision.PRUNE, 0.0, 0.0, 0.0, 0.0)

        title_result = process.extractOne(
            norm_title,
            titles_list,
            scorer=fuzz.token_set_ratio,
        )
        if title_result is None:
            return MatchResult(Decision.PRUNE, 0.0, 0.0, 0.0, 0.0)

        title_score = float(title_result[1]) / 100.0
        if title_score >= self.keep_threshold:
            return MatchResult(Decision.KEEP, 0.0, 0.0, 0.0, title_score)

        if title_score >= self.review_threshold:
            return MatchResult(Decision.REVIEW, 0.0, 0.0, 0.0, title_score)

        return MatchResult(Decision.PRUNE, 0.0, 0.0, 0.0, title_score)

    def classify(self, norm_artist: str, norm_title: str) -> MatchResult:
        """Classify a normalized (artist, title) pair."""
        # Check artist mappings first (previously confirmed decisions)
        if norm_artist in self.artist_mappings.get("keep", {}):
            return MatchResult(Decision.KEEP, 0.0, 0.0, 0.0, 0.0)
        if norm_artist in self.artist_mappings.get("prune", {}):
            return MatchResult(Decision.PRUNE, 0.0, 0.0, 0.0, 0.0)

        # Fast path: exact match
        exact = score_exact(norm_artist, norm_title, self.index)
        if exact == 1.0:
            return MatchResult(
                decision=Decision.KEEP,
                exact_score=1.0,
                token_set_score=1.0,
                token_sort_score=1.0,
                two_stage_score=1.0,
            )

        # Run all three fuzzy scorers
        ts = score_token_set(norm_artist, norm_title, self.index)
        tso = score_token_sort(norm_artist, norm_title, self.index)
        two = score_two_stage(norm_artist, norm_title, self.index)

        scores = [ts, tso, two]

        # 2-of-3 above keep_threshold, BUT the two-stage scorer must be
        # one of the agreeing scorers. This prevents false positives from
        # token_set/token_sort both matching on partial artist names
        # (e.g. "Joy" matching "Joy Division").
        above_keep = sum(1 for s in scores if s >= self.keep_threshold)
        if above_keep >= 2 and two >= self.keep_threshold:
            return MatchResult(Decision.KEEP, exact, ts, tso, two)

        # 1 high + 1 moderate (two-stage must participate)
        has_high = any(s >= self.high_threshold for s in scores)
        above_moderate = sum(1 for s in scores if s >= self.moderate_threshold)
        if has_high and above_moderate >= 2 and two >= self.moderate_threshold:
            return MatchResult(Decision.KEEP, exact, ts, tso, two)

        # Review range
        max_score = max(scores)
        if max_score >= self.review_threshold:
            return MatchResult(Decision.REVIEW, exact, ts, tso, two)

        # Prune
        return MatchResult(Decision.PRUNE, exact, ts, tso, two)


# ---------------------------------------------------------------------------
# Compilation handling
# ---------------------------------------------------------------------------


def classify_compilation(norm_title: str, index: LibraryIndex, threshold: int = 80) -> Decision:
    """Classify a compilation release by title-only matching.

    Compilations (Various Artists, etc.) can't be matched by artist,
    so we match against known compilation titles in the library.

    Returns KEEP if the title fuzzy-matches a library compilation title,
    otherwise PRUNE.
    """
    if not index.compilation_titles:
        return Decision.PRUNE

    # Exact match
    if norm_title in index.compilation_titles:
        return Decision.KEEP

    # Fuzzy match
    result = process.extractOne(
        norm_title,
        list(index.compilation_titles),
        scorer=fuzz.token_set_ratio,
        score_cutoff=threshold,
    )
    if result is not None:
        return Decision.KEEP

    return Decision.PRUNE


# ---------------------------------------------------------------------------
# Artist mappings persistence
# ---------------------------------------------------------------------------


def load_artist_mappings(path: Path) -> dict:
    """Load artist mappings from a JSON file.

    Returns {"keep": {discogs_artist: library_artist}, "prune": {discogs_artist: None}}.
    Returns empty dicts if file doesn't exist.
    """
    if not path.exists():
        return {"keep": {}, "prune": {}}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {"keep": data.get("keep", {}), "prune": data.get("prune", {})}


def save_artist_mappings(path: Path, mappings: dict) -> None:
    """Save artist mappings to a JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(mappings, f, indent=2, ensure_ascii=False)
        f.write("\n")


async def load_discogs_releases(
    conn: asyncpg.Connection,
) -> list[tuple[int, str, str, str | None]]:
    """Load all releases with their primary artist from the Discogs cache.

    Returns list of (release_id, artist_name, title, format) tuples.
    Only includes main artists (extra = 0).
    """
    logger.info("Loading Discogs releases...")
    rows = await conn.fetch("""
        SELECT r.id, ra.artist_name, r.title, r.format
        FROM release r
        JOIN release_artist ra ON ra.release_id = r.id AND ra.extra = 0
        ORDER BY r.id
    """)

    releases = [(row["id"], row["artist_name"], row["title"], row["format"]) for row in rows]
    logger.info(f"Loaded {len(releases):,} releases")
    return releases


async def get_table_sizes(conn: asyncpg.Connection) -> dict[str, tuple[int, int]]:
    """Get row count and disk size for each release table.

    Returns dict of table_name -> (row_count, size_bytes).
    """
    sizes = {}
    for table, _ in RELEASE_TABLES:
        row = await conn.fetchrow(f"""
            SELECT
                (SELECT count(*) FROM {table}) as row_count,
                pg_total_relation_size('{table}') as size_bytes
        """)
        sizes[table] = (row["row_count"], row["size_bytes"])
    return sizes


async def count_rows_to_delete(conn: asyncpg.Connection, release_ids: set[int]) -> dict[str, int]:
    """Count how many rows in each table would be deleted.

    Returns dict of table_name -> row_count_to_delete.
    """
    if not release_ids:
        return {table: 0 for table, _ in RELEASE_TABLES}

    id_list = list(release_ids)
    counts = {}
    for table, id_col in RELEASE_TABLES:
        row = await conn.fetchrow(
            f"SELECT count(*) as cnt FROM {table} WHERE {id_col} = ANY($1::integer[])",
            id_list,
        )
        counts[table] = row["cnt"]
    return counts


async def prune_releases(conn: asyncpg.Connection, release_ids: set[int]) -> dict[str, int]:
    """Delete all data for the given release IDs.

    FK CASCADE constraints on child tables (release_artist, release_track,
    release_track_artist, cache_metadata) automatically clean up related rows
    when the parent release is deleted.

    Returns dict with release table deletion count.
    """
    if not release_ids:
        return {"release": 0}

    id_list = list(release_ids)
    result = await conn.execute(
        "DELETE FROM release WHERE id = ANY($1::integer[])",
        id_list,
    )
    # asyncpg returns "DELETE N"
    count = int(result.split()[-1])
    logger.info(f"  Deleted {count:,} releases (CASCADE cleans up child tables)")
    return {"release": count}


def prune_releases_copy_swap(
    db_url: str,
    keep_ids: set[int],
    review_ids: set[int],
) -> None:
    """Prune releases using copy-and-swap (faster than DELETE for large prune sets).

    Instead of deleting ~86% of rows with CASCADE, copies only the ~14%
    we want to keep into fresh tables, then swaps them in. Reuses the
    pattern from dedup_releases.py.

    Args:
        db_url: PostgreSQL connection URL.
        keep_ids: Release IDs classified as KEEP.
        review_ids: Release IDs classified as REVIEW (also kept).
    """
    all_ids = keep_ids | review_ids
    if not all_ids:
        logger.warning("No releases to keep — skipping copy-swap prune")
        return

    logger.info(
        "Pruning via copy-and-swap (%s KEEP + %s REVIEW = %s total)",
        f"{len(keep_ids):,}",
        f"{len(review_ids):,}",
        f"{len(all_ids):,}",
    )

    conn = psycopg.connect(db_url, autocommit=True)
    start = time.time()

    try:
        with conn.cursor() as cur:
            # Load keep IDs into a temp table for efficient joins
            cur.execute("DROP TABLE IF EXISTS _keep_ids")
            cur.execute("CREATE UNLOGGED TABLE _keep_ids (release_id integer PRIMARY KEY)")
            with cur.copy("COPY _keep_ids (release_id) FROM STDIN") as copy:
                for rid in all_ids:
                    copy.write_row((rid,))

        # Tables to copy-swap: (old_table, new_table, columns, id_column)
        tables = [
            (
                "release",
                "new_release",
                "id, title, release_year, country, artwork_url, format",
                "id",
            ),
            (
                "release_artist",
                "new_release_artist",
                "release_id, artist_id, artist_name, extra",
                "release_id",
            ),
            ("release_label", "new_release_label", "release_id, label_name", "release_id"),
            ("release_genre", "new_release_genre", "release_id, genre", "release_id"),
            ("release_style", "new_release_style", "release_id, style", "release_id"),
            (
                "release_track",
                "new_release_track",
                "release_id, sequence, position, title, duration",
                "release_id",
            ),
            (
                "release_track_artist",
                "new_release_track_artist",
                "release_id, track_sequence, artist_name",
                "release_id",
            ),
            (
                "cache_metadata",
                "new_cache_metadata",
                "release_id, cached_at, source, last_validated",
                "release_id",
            ),
        ]

        # Copy keeper rows into new tables
        for old_table, new_table, columns, id_col in tables:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {new_table}")
                cur.execute(f"""
                    CREATE TABLE {new_table} AS
                    SELECT {columns} FROM {old_table} t
                    WHERE EXISTS (
                        SELECT 1 FROM _keep_ids k WHERE k.release_id = t.{id_col}
                    )
                """)
                cur.execute(f"SELECT count(*) FROM {new_table}")
                count = cur.fetchone()[0]
            logger.info(f"  Copied {old_table} -> {new_table}: {count:,} rows")

        # Drop FK constraints before swap
        with conn.cursor() as cur:
            for stmt in [
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "ALTER TABLE release_genre DROP CONSTRAINT IF EXISTS fk_release_genre_release",
                "ALTER TABLE release_style DROP CONSTRAINT IF EXISTS fk_release_style_release",
                "ALTER TABLE release_track DROP CONSTRAINT IF EXISTS fk_release_track_release",
                "ALTER TABLE release_track_artist DROP CONSTRAINT IF EXISTS fk_release_track_artist_release",
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
            ]:
                cur.execute(stmt)

        # Swap tables
        for old_table, new_table, _, _ in tables:
            bak = f"{old_table}_old"
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE {old_table} RENAME TO {bak}")
                cur.execute(f"ALTER TABLE {new_table} RENAME TO {old_table}")
                cur.execute(f"DROP TABLE {bak} CASCADE")
            logger.info(f"  Swapped {new_table} -> {old_table}")

        # Re-add constraints and indexes
        with conn.cursor() as cur:
            # PK on release
            cur.execute("ALTER TABLE release ADD PRIMARY KEY (id)")

            # Clean orphan child rows before FK validation. The live LML
            # service writes child rows for releases NOT in the post-prune
            # subset; deleting them now keeps the ADD CONSTRAINT step below
            # from failing on validation. NOT VALID on the constraint itself
            # tolerates new orphans landing between cleanup and ADD. See
            # #211 + #188 for the parallel fix in dedup_releases.py.
            for child_table in (
                "release_artist",
                "release_label",
                "release_genre",
                "release_style",
                "release_track",
                "release_track_artist",
                "cache_metadata",
            ):
                cur.execute(
                    f"DELETE FROM {child_table} WHERE NOT EXISTS "
                    f"(SELECT 1 FROM release r WHERE r.id = {child_table}.release_id)"
                )

            # FK constraints (NOT VALID for race tolerance).
            cur.execute(
                "ALTER TABLE release_artist ADD CONSTRAINT fk_release_artist_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID"
            )
            cur.execute(
                "ALTER TABLE release_label ADD CONSTRAINT fk_release_label_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID"
            )
            cur.execute(
                "ALTER TABLE release_genre ADD CONSTRAINT fk_release_genre_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID"
            )
            cur.execute(
                "ALTER TABLE release_style ADD CONSTRAINT fk_release_style_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID"
            )
            cur.execute(
                "ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID"
            )
            cur.execute(
                "ALTER TABLE release_track_artist ADD CONSTRAINT fk_release_track_artist_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID"
            )
            cur.execute(
                "ALTER TABLE cache_metadata ADD CONSTRAINT fk_cache_metadata_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID"
            )
            cur.execute("ALTER TABLE cache_metadata ADD PRIMARY KEY (release_id)")

            # FK indexes
            cur.execute("CREATE INDEX idx_release_artist_release_id ON release_artist(release_id)")
            cur.execute("CREATE INDEX idx_release_label_release_id ON release_label(release_id)")
            cur.execute("CREATE INDEX idx_release_genre_release_id ON release_genre(release_id)")
            cur.execute("CREATE INDEX idx_release_style_release_id ON release_style(release_id)")
            cur.execute("CREATE INDEX idx_release_track_release_id ON release_track(release_id)")
            cur.execute(
                "CREATE INDEX idx_release_track_artist_release_id "
                "ON release_track_artist(release_id)"
            )

            # Trigram indexes
            cur.execute(
                "CREATE INDEX idx_release_artist_name_trgm ON release_artist "
                "USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)"
            )
            cur.execute(
                "CREATE INDEX idx_release_title_trgm ON release "
                "USING gin (lower(f_unaccent(title)) gin_trgm_ops)"
            )
            cur.execute(
                "CREATE INDEX idx_release_track_title_trgm ON release_track "
                "USING gin (lower(f_unaccent(title)) gin_trgm_ops)"
            )
            cur.execute(
                "CREATE INDEX idx_release_track_artist_name_trgm ON release_track_artist "
                "USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)"
            )

            # Cache metadata indexes
            cur.execute("CREATE INDEX idx_cache_metadata_cached_at ON cache_metadata(cached_at)")
            cur.execute("CREATE INDEX idx_cache_metadata_source ON cache_metadata(source)")

            # Cleanup
            cur.execute("DROP TABLE IF EXISTS _keep_ids")

        elapsed = time.time() - start
        logger.info(f"Copy-and-swap prune completed in {elapsed:.1f}s")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Copy to target database
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
SCHEMA_DIR = SCRIPT_DIR.parent / "schema"

# Tables and their columns to copy (post-dedup: no master_id).
# Each entry: (table_name, filter_column, columns_list)
COPY_TABLE_SPEC = [
    ("release", "id", ["id", "title", "release_year", "country", "artwork_url", "format"]),
    ("release_artist", "release_id", ["release_id", "artist_name", "extra"]),
    ("release_label", "release_id", ["release_id", "label_name"]),
    ("release_genre", "release_id", ["release_id", "genre"]),
    ("release_style", "release_id", ["release_id", "style"]),
    ("release_track", "release_id", ["release_id", "sequence", "position", "title", "duration"]),
    (
        "release_track_artist",
        "release_id",
        ["release_id", "track_sequence", "artist_name"],
    ),
    (
        "release_video",
        "release_id",
        ["release_id", "sequence", "src", "title", "duration", "embed"],
    ),
    (
        "cache_metadata",
        "release_id",
        ["release_id", "cached_at", "source", "last_validated"],
    ),
]


def _parse_db_name(db_url: str) -> str:
    """Extract the database name from a PostgreSQL connection URL."""
    # postgresql://user:pass@host:port/dbname -> dbname
    # postgresql:///dbname -> dbname
    return db_url.rsplit("/", 1)[-1]


def _admin_url(db_url: str) -> str:
    """Build a URL to the 'postgres' admin database on the same server."""
    return db_url.rsplit("/", 1)[0] + "/postgres"


def _ensure_target_database(target_url: str) -> None:
    """Create the target database if it does not already exist."""
    db_name = _parse_db_name(target_url)
    admin = _admin_url(target_url)

    conn = psycopg.connect(admin, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (db_name,),
        )
        if cur.fetchone() is None:
            # Use SQL composition to safely quote the identifier
            from psycopg import sql as psql

            cur.execute(psql.SQL("CREATE DATABASE {}").format(psql.Identifier(db_name)))
            logger.info("Created target database: %s", db_name)
        else:
            logger.info("Target database already exists: %s", db_name)
    conn.close()


def _create_target_schema(target_url: str) -> None:
    """Drop existing tables and apply the schema to the target database."""
    conn = psycopg.connect(target_url, autocommit=True)
    with conn.cursor() as cur:
        for table in (
            "cache_metadata",
            "release_track_artist",
            "release_track",
            "release_video",
            "release_style",
            "release_genre",
            "release_label",
            "release_artist",
            "release",
        ):
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        schema_sql = SCHEMA_DIR.joinpath("create_database.sql").read_text()
        cur.execute(schema_sql)
    conn.close()
    logger.info("Applied schema to target database")


def _create_target_indexes(target_url: str) -> None:
    """Create functions and indexes on the target database (without CONCURRENTLY)."""
    functions_sql = SCHEMA_DIR.joinpath("create_functions.sql").read_text()
    base_indexes_sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
    base_indexes_sql = base_indexes_sql.replace(" CONCURRENTLY", "")
    track_indexes_sql = SCHEMA_DIR.joinpath("create_track_indexes.sql").read_text()
    track_indexes_sql = track_indexes_sql.replace(" CONCURRENTLY", "")

    conn = psycopg.connect(target_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(functions_sql)
        cur.execute(base_indexes_sql)
        cur.execute(track_indexes_sql)
    conn.close()
    logger.info("Created functions and indexes on target database")


def copy_releases_to_target(
    source_url: str,
    target_url: str,
    keep_ids: set[int],
    review_ids: set[int],
) -> None:
    """Copy KEEP and REVIEW releases from source to target database.

    1. Creates the target database if it doesn't exist.
    2. Applies the schema (drops existing tables first).
    3. Loads KEEP+REVIEW IDs into a temp table on the source.
    4. Streams each table from source to target via psycopg COPY.
    5. Creates trigram indexes on the target.

    Args:
        source_url: PostgreSQL connection URL for the source (imported) database.
        target_url: PostgreSQL connection URL for the target database.
        keep_ids: Set of release IDs classified as KEEP.
        review_ids: Set of release IDs classified as REVIEW.
    """
    all_ids = keep_ids | review_ids
    if not all_ids:
        logger.warning("No releases to copy (keep=%d, review=%d)", len(keep_ids), len(review_ids))
        return

    logger.info(
        "Copying %s releases to target (%s KEEP + %s REVIEW)",
        f"{len(all_ids):,}",
        f"{len(keep_ids):,}",
        f"{len(review_ids):,}",
    )

    # Step 1: Create target database
    _ensure_target_database(target_url)

    # Step 2: Apply schema
    _create_target_schema(target_url)

    # Step 3-4: Stream data from source to target
    source_conn = psycopg.connect(source_url)
    target_conn = psycopg.connect(target_url)

    try:
        # Create temp table with IDs to copy on the source
        with source_conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE _copy_ids (release_id integer PRIMARY KEY)")
            with cur.copy("COPY _copy_ids (release_id) FROM STDIN") as copy:
                for rid in all_ids:
                    copy.write_row((rid,))
        source_conn.commit()

        total_rows = 0
        for table_name, filter_col, columns in COPY_TABLE_SPEC:
            col_list = ", ".join(columns)
            select_query = (
                f"COPY (SELECT {col_list} FROM {table_name} "
                f"WHERE {filter_col} IN (SELECT release_id FROM _copy_ids)) TO STDOUT"
            )

            row_count = 0
            with source_conn.cursor() as src_cur:
                with src_cur.copy(select_query) as src_copy:
                    with target_conn.cursor() as tgt_cur:
                        with tgt_cur.copy(f"COPY {table_name} ({col_list}) FROM STDIN") as tgt_copy:
                            for data in src_copy:
                                tgt_copy.write(data)
                                row_count += 1

            total_rows += row_count
            logger.info("  Copied %s: %s rows", table_name, f"{row_count:,}")

        target_conn.commit()
        logger.info("Copied %s total rows to target", f"{total_rows:,}")
    finally:
        source_conn.close()
        target_conn.close()

    # Step 5: Create indexes
    _create_target_indexes(target_url)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


@dataclass
class ClassificationReport:
    """Aggregated results from classifying all Discogs releases."""

    keep_ids: set[int]
    prune_ids: set[int]
    review_ids: set[int]
    # REVIEW releases grouped by Discogs artist: {norm_artist: [(release_id, title, result)]}
    review_by_artist: dict[str, list[tuple[int, str, MatchResult]]]
    # Original artist names for Discogs artists
    artist_originals: dict[str, str]
    total_releases: int


def print_report(
    report: ClassificationReport,
    index: LibraryIndex,
    table_sizes: dict[str, tuple[int, int]] | None = None,
    rows_to_delete: dict[str, int] | None = None,
    pruned: bool = False,
):
    """Print the verification/pruning report."""
    action = "PRUNING" if pruned else "VERIFICATION"
    print(f"\n{'=' * 80}")
    print(f"DISCOGS CACHE {action} REPORT (multi-index pair matching)")
    print(f"{'=' * 80}")

    print(f"\nLibrary pairs:     {len(index.exact_pairs):>8,}")
    print(f"Library artists:   {len(index.all_artists):>8,}")
    print(f"Discogs releases:  {report.total_releases:>8,}")

    print("\n--- Classification ---")
    print(f"KEEP:   {len(report.keep_ids):>8,} releases")
    print(f"PRUNE:  {len(report.prune_ids):>8,} releases")
    print(f"REVIEW: {len(report.review_ids):>8,} releases ({len(report.review_by_artist)} artists)")

    # --- Database size ---
    if table_sizes:
        total_size = sum(size for _, size in table_sizes.values())
        total_rows = sum(rows for rows, _ in table_sizes.values())
        total_deletable_rows = sum(rows_to_delete.values()) if rows_to_delete else 0

        print("\n--- Database size ---")
        print(f"{'Table':<25} {'Rows':>12} {'Size':>10}   {'Deletable rows':>14}")
        print("-" * 70)
        for table, _ in RELEASE_TABLES:
            row_count, size_bytes = table_sizes[table]
            del_count = rows_to_delete.get(table, 0) if rows_to_delete else 0
            pct = (del_count / row_count * 100) if row_count > 0 else 0
            print(
                f"{table:<25} {row_count:>12,} {format_bytes(size_bytes):>10}"
                f"   {del_count:>10,} ({pct:4.1f}%)"
            )
        print("-" * 70)

        estimated_savings = 0
        for table, _ in RELEASE_TABLES:
            row_count, size_bytes = table_sizes[table]
            del_count = rows_to_delete.get(table, 0) if rows_to_delete else 0
            if row_count > 0:
                estimated_savings += int(size_bytes * del_count / row_count)

        verb = "Freed" if pruned else "Estimated savings"
        print(
            f"{'Total':<25} {total_rows:>12,} {format_bytes(total_size):>10}"
            f"   {total_deletable_rows:>10,}"
        )
        if total_size > 0:
            print(
                f"\n{verb}: ~{format_bytes(estimated_savings)}"
                f" ({estimated_savings / total_size * 100:.1f}%"
                f" of {format_bytes(total_size)})"
            )

        if pruned:
            print("\nNote: run VACUUM FULL to reclaim disk space after pruning.")

    # --- REVIEW: artist-level decisions needed ---
    if report.review_by_artist:
        num_artists = len(report.review_by_artist)
        num_releases = len(report.review_ids)
        print(
            f"\n--- REVIEW: artist-level decisions needed "
            f"({num_artists} artists, {num_releases} releases) ---"
        )
        print("Save decisions to artist_mappings.json to auto-resolve on next run.\n")
        print(f"{'Discogs Artist':<35} {'Releases':>8}  {'Best Library Match':<30} {'Score':>5}")
        print("-" * 83)

        # Sort by release count descending
        sorted_review = sorted(
            report.review_by_artist.items(),
            key=lambda x: len(x[1]),
            reverse=True,
        )
        for norm_artist, release_list in sorted_review[:50]:
            orig = report.artist_originals.get(norm_artist, norm_artist)
            count = len(release_list)
            # Best score from any release for this artist
            best_result = max(release_list, key=lambda x: x[2].max_fuzzy_score)
            best_score = best_result[2].max_fuzzy_score
            # Find best matching library artist
            best_lib_match = ""
            if index.all_artists:
                lib_match = process.extractOne(
                    norm_artist, index.all_artists, scorer=fuzz.token_set_ratio
                )
                if lib_match:
                    best_lib_match = lib_match[0]
            print(f"{orig:<35} {count:>8}  {best_lib_match:<30} {best_score:>5.2f}")

        if len(sorted_review) > 50:
            print(f"  ... and {len(sorted_review) - 50:,} more artists")

    # --- Summary ---
    print(f"\n{'=' * 80}")
    print("SUMMARY")
    print(f"{'=' * 80}")
    if pruned:
        print(f"Releases kept:     {len(report.keep_ids):>8,}")
        print(f"Releases pruned:   {len(report.prune_ids):>8,}")
        print(f"Releases review:   {len(report.review_ids):>8,}")
    else:
        print(f"Releases to keep:  {len(report.keep_ids):>8,}")
        print(f"Releases to prune: {len(report.prune_ids):>8,}")
        print(f"Releases to review:{len(report.review_ids):>8,}")
    print(f"Review artists:    {len(report.review_by_artist):>8,}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify and optionally prune the Discogs cache against the WXYC library.",
    )
    parser.add_argument(
        "library_db",
        type=Path,
        help="Path to the WXYC library SQLite database",
    )
    parser.add_argument(
        "database_url",
        nargs="?",
        default="postgresql:///discogs",
        help="PostgreSQL connection URL for the Discogs cache (default: postgresql:///discogs)",
    )

    action = parser.add_mutually_exclusive_group()
    action.add_argument(
        "--prune",
        action="store_true",
        help="Actually delete PRUNE releases (default is dry run). "
        "REVIEW releases are never deleted.",
    )
    action.add_argument(
        "--copy-to",
        type=str,
        default=None,
        metavar="URL",
        help="Copy KEEP and REVIEW releases to a target PostgreSQL database "
        "(creates the database if it doesn't exist).",
    )

    parser.add_argument(
        "--mappings-file",
        type=Path,
        default=None,
        help="Path to artist_mappings.json (default: alongside this script)",
    )
    parser.add_argument(
        "--score-cutoff",
        type=float,
        default=0.75,
        help="Keep threshold for 2-of-3 agreement (0.0-1.0, default: 0.75)",
    )
    return parser.parse_args(argv)


def classify_artist_fuzzy(
    norm_artist: str,
    artist_releases: list[tuple[int, str, str]],
    index: LibraryIndex,
    matcher: MultiIndexMatcher,
    artist_match_threshold: int = 60,
) -> tuple[set[int], set[int], set[int], dict[str, list[tuple[int, str, MatchResult]]]]:
    """Classify all releases for a single artist using fuzzy matching.

    Pure function: reads index and matcher but does not mutate shared state.
    Returns (keep_ids, prune_ids, review_ids, review_by_artist).
    """
    keep_ids: set[int] = set()
    prune_ids: set[int] = set()
    review_ids: set[int] = set()
    review_by_artist: dict[str, list[tuple[int, str, MatchResult]]] = {}

    raw_artist = artist_releases[0][1]

    if is_compilation_artist(raw_artist):
        for release_id, _, raw_title in artist_releases:
            norm_title = normalize_title(raw_title)
            decision = classify_compilation(norm_title, index)
            if decision == Decision.KEEP:
                keep_ids.add(release_id)
            else:
                prune_ids.add(release_id)
        return keep_ids, prune_ids, review_ids, review_by_artist

    # Single artist-level fuzzy match
    artist_result = process.extractOne(
        norm_artist,
        index.all_artists,
        scorer=fuzz.token_set_ratio,
        score_cutoff=artist_match_threshold,
    )

    if artist_result is None:
        for release_id, _, _ in artist_releases:
            prune_ids.add(release_id)
        return keep_ids, prune_ids, review_ids, review_by_artist

    matched_lib_artist, artist_score, _ = artist_result
    matched_titles_list = index.artist_to_titles_list.get(matched_lib_artist)

    for release_id, _, raw_title in artist_releases:
        norm_title = normalize_title(raw_title)

        # Exact pair check (using matched library artist)
        if (matched_lib_artist, norm_title) in index.exact_pairs:
            keep_ids.add(release_id)
            continue

        if not matched_titles_list:
            prune_ids.add(release_id)
            continue

        title_result = process.extractOne(
            norm_title,
            matched_titles_list,
            scorer=fuzz.token_set_ratio,
        )

        if title_result is None:
            prune_ids.add(release_id)
            continue

        title_score = float(title_result[1])
        combined = float((float(artist_score) * title_score) ** 0.5) / 100.0

        if combined >= matcher.keep_threshold:
            keep_ids.add(release_id)
        elif combined >= matcher.review_threshold:
            review_ids.add(release_id)
            result = MatchResult(Decision.REVIEW, 0.0, 0.0, 0.0, combined)
            review_by_artist.setdefault(norm_artist, []).append((release_id, raw_title, result))
        else:
            prune_ids.add(release_id)

    return keep_ids, prune_ids, review_ids, review_by_artist


def classify_fuzzy_batch(
    artists: list[str],
    by_artist: dict[str, list[tuple[int, str, str]]],
    index: LibraryIndex,
    matcher: MultiIndexMatcher,
    artist_match_threshold: int = 60,
) -> tuple[set[int], set[int], set[int], dict[str, list[tuple[int, str, MatchResult]]]]:
    """Classify a batch of artists, aggregating results.

    Returns (keep_ids, prune_ids, review_ids, review_by_artist).
    """
    keep_ids: set[int] = set()
    prune_ids: set[int] = set()
    review_ids: set[int] = set()
    review_by_artist: dict[str, list[tuple[int, str, MatchResult]]] = {}

    for norm_artist in artists:
        a_keep, a_prune, a_review, a_review_by = classify_artist_fuzzy(
            norm_artist, by_artist[norm_artist], index, matcher, artist_match_threshold
        )
        keep_ids |= a_keep
        prune_ids |= a_prune
        review_ids |= a_review
        for k, v in a_review_by.items():
            review_by_artist.setdefault(k, []).extend(v)

    return keep_ids, prune_ids, review_ids, review_by_artist


# ---------------------------------------------------------------------------
# Process pool worker functions for Phase 4 parallelization
# ---------------------------------------------------------------------------

_pool_index: LibraryIndex | None = None
_pool_matcher: MultiIndexMatcher | None = None


def _init_fuzzy_worker(index: LibraryIndex, matcher: MultiIndexMatcher) -> None:
    """Initializer for ProcessPoolExecutor workers. Stores shared read-only state."""
    global _pool_index, _pool_matcher
    _pool_index = index
    _pool_matcher = matcher


def _classify_fuzzy_chunk(
    chunk_args: tuple[list[str], dict[str, list[tuple[int, str, str]]]],
) -> tuple[set[int], set[int], set[int], dict[str, list[tuple[int, str, MatchResult]]]]:
    """Worker function for ProcessPoolExecutor. Reads index/matcher from module globals."""
    artists, chunk_by_artist = chunk_args
    return classify_fuzzy_batch(artists, chunk_by_artist, _pool_index, _pool_matcher)


def classify_all_releases(
    releases: list[tuple[int, str, str]] | list[tuple[int, str, str, str | None]],
    index: LibraryIndex,
    matcher: MultiIndexMatcher,
) -> ClassificationReport:
    """Classify all Discogs releases and build a report.

    Groups releases by normalized artist for efficient scoring and
    artist-level REVIEW grouping.

    Accepts 3-tuples (id, artist, title) or 4-tuples (id, artist, title, format).
    When 4-tuples are provided and the library has format data, exact-match KEEP
    releases are downgraded to PRUNE if their format doesn't match the library's.
    """
    keep_ids: set[int] = set()
    prune_ids: set[int] = set()
    review_ids: set[int] = set()
    review_by_artist: dict[str, list[tuple[int, str, MatchResult]]] = {}
    artist_originals: dict[str, str] = {}

    # Extract format info if 4-tuples are provided
    release_formats: dict[int, str | None] = {}
    has_format_data = len(releases) > 0 and len(releases[0]) >= 4

    # Group by normalized artist for efficient batch processing
    by_artist: dict[str, list[tuple[int, str, str]]] = {}
    for release in releases:
        release_id, raw_artist, raw_title = release[0], release[1], release[2]
        if has_format_data:
            release_formats[release_id] = release[3]  # type: ignore[misc]
        norm_artist = normalize_artist(raw_artist)
        artist_originals.setdefault(norm_artist, raw_artist)
        by_artist.setdefault(norm_artist, []).append((release_id, raw_artist, raw_title))

    total_artists = len(by_artist)
    total_releases = len(releases)
    releases_processed = 0
    artists_exact_matched = 0
    artists_fuzzy_matched = 0
    start_time = time.monotonic()

    # Build a set of library artist names for O(1) exact lookup
    library_artist_set = set(index.all_artists)

    # Phase 1: Exact artist match — O(1) per artist.
    # Artists that exactly match a library artist (after normalization) get their
    # releases classified by exact pair lookup only, skipping expensive fuzzy scoring.
    logger.info(
        "Phase 1: Exact artist matching (%s Discogs artists vs %s library artists)...",
        f"{total_artists:,}",
        f"{len(library_artist_set):,}",
    )
    exact_artist_match: set[str] = set()
    no_artist_match: set[str] = set()
    fuzzy_needed: list[str] = []

    for norm_artist in by_artist:
        if norm_artist in library_artist_set:
            exact_artist_match.add(norm_artist)
        else:
            # Check mappings before marking as needing fuzzy
            if norm_artist in matcher.artist_mappings.get("keep", {}):
                exact_artist_match.add(norm_artist)
            elif norm_artist in matcher.artist_mappings.get("prune", {}):
                no_artist_match.add(norm_artist)
            else:
                fuzzy_needed.append(norm_artist)

    exact_releases = sum(len(by_artist[a]) for a in exact_artist_match)
    no_match_releases = sum(len(by_artist[a]) for a in no_artist_match)
    fuzzy_releases = sum(len(by_artist[a]) for a in fuzzy_needed)
    logger.info(
        f"Phase 1 complete: {len(exact_artist_match):,} exact artist matches "
        f"({exact_releases:,} releases), "
        f"{len(no_artist_match):,} mapped prune ({no_match_releases:,} releases), "
        f"{len(fuzzy_needed):,} need fuzzy matching ({fuzzy_releases:,} releases)"
    )

    # Phase 2: Process exact-match artists (fast: exact pair + two-stage only)
    # Format filtering is applied here for exact-match KEEP releases: if the library
    # has format data for the matched (artist, title) pair and the release has a format,
    # the release is downgraded to PRUNE if its format doesn't match. Fuzzy-match
    # releases skip format filtering because the matched library pair is not reliably
    # known (the fuzzy artist match may not correspond to the exact library pair).
    logger.info("Phase 2: Classifying exact-match artists by pair...")
    for norm_artist in exact_artist_match:
        artist_releases = by_artist[norm_artist]
        for release_id, _, raw_title in artist_releases:
            norm_title = normalize_title(raw_title)
            result = matcher.classify_known_artist(norm_artist, norm_title)
            if result.decision == Decision.KEEP:
                # Format filtering for exact-match KEEP releases
                rel_fmt = release_formats.get(release_id)
                lib_formats = index.format_by_pair.get((norm_artist, norm_title), set())
                if not format_matches(rel_fmt, lib_formats):
                    prune_ids.add(release_id)
                else:
                    keep_ids.add(release_id)
            elif result.decision == Decision.PRUNE:
                prune_ids.add(release_id)
            else:
                review_ids.add(release_id)
                review_by_artist.setdefault(norm_artist, []).append((release_id, raw_title, result))
        releases_processed += len(artist_releases)
        artists_exact_matched += 1

    # Process mapped-prune artists
    for norm_artist in no_artist_match:
        for release_id, _, _ in by_artist[norm_artist]:
            prune_ids.add(release_id)
        releases_processed += len(by_artist[norm_artist])

    phase2_elapsed = time.monotonic() - start_time
    logger.info(
        f"Phase 2 complete in {phase2_elapsed:.1f}s: "
        f"KEEP={len(keep_ids):,}, PRUNE={len(prune_ids):,}, REVIEW={len(review_ids):,}"
    )

    # Phase 3: Token-overlap pre-screen for fuzzy candidates.
    # Build a set of all tokens from library artist names. Discard short tokens
    # (1-2 chars) that cause false positive overlaps ("dj", "mc", "j", etc.)
    phase3_start = time.monotonic()
    min_token_len = 3
    library_tokens: set[str] = set()
    for artist in index.all_artists:
        library_tokens.update(t for t in artist.split() if len(t) >= min_token_len)

    truly_fuzzy: list[str] = []
    token_pruned = 0
    for norm_artist in fuzzy_needed:
        artist_tokens = {t for t in norm_artist.split() if len(t) >= min_token_len}
        if artist_tokens & library_tokens:
            truly_fuzzy.append(norm_artist)
        else:
            # No meaningful token overlap — prune all releases
            for release_id, _, _ in by_artist[norm_artist]:
                prune_ids.add(release_id)
            releases_processed += len(by_artist[norm_artist])
            token_pruned += 1

    token_pruned_releases = (
        sum(len(by_artist[a]) for a in fuzzy_needed if a not in set(truly_fuzzy))
        if token_pruned
        else 0
    )
    phase3_elapsed = time.monotonic() - phase3_start
    logger.info(
        f"Phase 3 pre-screen in {phase3_elapsed:.1f}s: "
        f"{token_pruned:,} artists pruned by token overlap "
        f"({token_pruned_releases:,} releases), "
        f"{len(truly_fuzzy):,} artists remain for fuzzy matching"
    )

    # Phase 4: Artist-level fuzzy matching for remaining artists.
    # Match each artist ONCE against library artists, then classify their
    # releases by title only against the matched library artist's albums.
    # This avoids the O(releases * all_library_pairs) cost of full scoring.
    logger.info(
        "Phase 4: Fuzzy artist matching for %s artists...",
        f"{len(truly_fuzzy):,}",
    )
    phase4_start = time.monotonic()

    use_rust = _HAS_WXYC_ETL and not os.environ.get("WXYC_ETL_NO_RUST")

    if use_rust:
        # Rust path: batch_classify_releases handles parallelism internally via
        # rayon, eliminating ProcessPoolExecutor fork/IPC overhead entirely.
        logger.info("  Using Rust (wxyc_etl) batch classification")

        # Flatten truly_fuzzy artists into individual releases
        flat_artists: list[str] = []
        flat_titles: list[str] = []
        flat_ids: list[int] = []
        flat_raw_artists: list[str] = []
        for norm_artist in truly_fuzzy:
            for release_id, raw_artist, raw_title in by_artist[norm_artist]:
                flat_artists.append(raw_artist)
                flat_titles.append(raw_title)
                flat_ids.append(release_id)
                flat_raw_artists.append(raw_artist)

        # batch_classify_releases (wxyc-etl >=0.1.0) accepts the library as a
        # raw list of (artist, title) pairs and builds the index internally.
        rust_pairs = [(artist, title) for artist, title in index.exact_pairs]

        decisions = _rust_batch_classify(flat_artists, flat_titles, rust_pairs)

        # A release with multiple primary artists fans out to one (artist,
        # title) entry per artist (load_discogs_releases joins release_artist
        # with extra=0). Reduce to one decision per release_id with precedence
        # KEEP > REVIEW > PRUNE so the same release_id never lands in two sets.
        precedence = {"keep": 2, "review": 1, "prune": 0}
        best_for_release: dict[int, tuple[int, str, str, str]] = {}
        for i, decision in enumerate(decisions):
            release_id = flat_ids[i]
            current = best_for_release.get(release_id)
            if current is None or precedence[decision] > precedence[current[3]]:
                best_for_release[release_id] = (i, flat_raw_artists[i], flat_titles[i], decision)

        for release_id, (_, raw_artist, raw_title, decision) in best_for_release.items():
            if decision == "keep":
                keep_ids.add(release_id)
            elif decision == "review":
                review_ids.add(release_id)
                norm_artist = normalize_artist(raw_artist)
                result = MatchResult(Decision.REVIEW, 0.0, 0.0, 0.0, 0.0)
                review_by_artist.setdefault(norm_artist, []).append((release_id, raw_title, result))
            else:
                prune_ids.add(release_id)

        artists_fuzzy_matched = len(truly_fuzzy)
        releases_processed += len(flat_ids)
        phase4_elapsed = time.monotonic() - phase4_start
        logger.info(
            f"  Rust batch classification done in {phase4_elapsed:.1f}s "
            f"({len(flat_ids):,} releases, {len(truly_fuzzy):,} artists) "
            f"| KEEP={len(keep_ids):,} PRUNE={len(prune_ids):,} REVIEW={len(review_ids):,}"
        )
    else:
        # Python fallback: ProcessPoolExecutor with fork context.
        # The Python loop overhead between rapidfuzz extractOne calls holds the
        # GIL, so threads serialize on a single core. Separate processes give
        # true multi-core parallelism. Fork context avoids the cost of
        # re-importing the module in each worker; the pipeline is single-threaded
        # before this point so fork is safe.
        num_workers = min(os.cpu_count() or 4, 8)
        # Target ~200 artists per chunk for frequent progress updates,
        # but ensure at least num_workers * 2 chunks for load balancing.
        min_chunks = num_workers * 2
        target_chunk_size = 200
        chunk_size = max(1, min(target_chunk_size, len(truly_fuzzy) // min_chunks))
        chunks = [truly_fuzzy[i : i + chunk_size] for i in range(0, len(truly_fuzzy), chunk_size)]

        logger.info(
            f"  Using Python fallback: {num_workers} workers, "
            f"{len(chunks)} chunks of ~{chunk_size} artists"
        )

        completed_chunks = 0
        global _pool_index, _pool_matcher
        _pool_index = index
        _pool_matcher = matcher
        try:
            ctx = multiprocessing.get_context("fork")
            with ProcessPoolExecutor(
                max_workers=num_workers,
                mp_context=ctx,
                initializer=_init_fuzzy_worker,
                initargs=(index, matcher),
            ) as executor:
                futures = {}
                for chunk in chunks:
                    chunk_by_artist = {a: by_artist[a] for a in chunk}
                    future = executor.submit(_classify_fuzzy_chunk, (chunk, chunk_by_artist))
                    futures[future] = chunk

                for future in as_completed(futures):
                    batch_keep, batch_prune, batch_review, batch_review_by = future.result()
                    keep_ids |= batch_keep
                    prune_ids |= batch_prune
                    review_ids |= batch_review
                    for k, v in batch_review_by.items():
                        review_by_artist.setdefault(k, []).extend(v)

                    completed_chunks += 1
                    chunk = futures[future]
                    chunk_releases = sum(len(by_artist[a]) for a in chunk)
                    releases_processed += chunk_releases
                    artists_fuzzy_matched += len(chunk)

                    elapsed = time.monotonic() - phase4_start
                    rate = artists_fuzzy_matched / elapsed if elapsed > 0 else 0
                    remaining = len(truly_fuzzy) - artists_fuzzy_matched
                    eta_str = f", ETA {remaining / rate:.0f}s" if rate > 0 else ""
                    logger.info(
                        f"  Chunk {completed_chunks}/{len(chunks)} done "
                        f"({releases_processed:,}/{total_releases:,} releases, "
                        f"{rate:.0f} artists/s{eta_str}) "
                        f"| {elapsed:.1f}s elapsed "
                        f"| KEEP={len(keep_ids):,} PRUNE={len(prune_ids):,} "
                        f"REVIEW={len(review_ids):,}"
                    )
        finally:
            _pool_index = None
            _pool_matcher = None

    elapsed = time.monotonic() - start_time
    logger.info(
        f"Classification complete in {elapsed:.1f}s: KEEP={len(keep_ids):,}, "
        f"PRUNE={len(prune_ids):,}, REVIEW={len(review_ids):,} "
        f"({artists_exact_matched:,} exact, {artists_fuzzy_matched:,} fuzzy)"
    )

    return ClassificationReport(
        keep_ids=keep_ids,
        prune_ids=prune_ids,
        review_ids=review_ids,
        review_by_artist=review_by_artist,
        artist_originals=artist_originals,
        total_releases=len(releases),
    )


async def async_main():
    args = parse_args()

    if not args.library_db.exists():
        logger.error(f"Library database not found: {args.library_db}")
        raise SystemExit(1)

    # Resolve mappings file path
    mappings_path = args.mappings_file or (Path(__file__).parent / "artist_mappings.json")

    # Step 1: Load artist mappings (previously confirmed decisions)
    mappings = load_artist_mappings(mappings_path)
    if mappings["keep"] or mappings["prune"]:
        logger.info(
            f"Loaded artist mappings: {len(mappings['keep'])} keep, {len(mappings['prune'])} prune"
        )

    # Step 2: Build LibraryIndex from SQLite
    index = LibraryIndex.from_sqlite(args.library_db)

    # Step 3: Connect to Discogs cache and load releases
    logger.info(f"Connecting to {args.database_url}")
    conn = await asyncpg.connect(args.database_url)

    try:
        # Step 4: Load all Discogs releases
        releases = await load_discogs_releases(conn)

        # Step 5: Classify each release
        logger.info("Classifying releases with multi-index matching...")
        matcher = MultiIndexMatcher(
            index,
            artist_mappings=mappings,
            keep_threshold=args.score_cutoff,
        )
        report = classify_all_releases(releases, index, matcher)

        # Step 6: Get table sizes for the report
        logger.info("Measuring table sizes...")
        table_sizes = await get_table_sizes(conn)

        if args.copy_to:
            # Copy KEEP + REVIEW releases to a separate target database
            logger.info("Copying matched releases to target database...")
            copy_releases_to_target(
                args.database_url,
                args.copy_to,
                report.keep_ids,
                report.review_ids,
            )
            # Dry-run report (source is unchanged)
            rows_to_delete = await count_rows_to_delete(conn, report.prune_ids)
            print_report(report, index, table_sizes, rows_to_delete, pruned=False)
        elif args.prune:
            # Actually prune non-matching releases (never REVIEW)
            logger.info(f"Pruning {len(report.prune_ids):,} releases...")
            if len(report.prune_ids) > 10000:
                # Large prune set: copy-and-swap is faster than CASCADE DELETE
                await conn.close()
                conn = None
                prune_releases_copy_swap(args.database_url, report.keep_ids, report.review_ids)
            else:
                await prune_releases(conn, report.prune_ids)
            print_report(report, index, table_sizes, pruned=True)

        else:
            # Dry run: count what would be deleted
            logger.info("Counting rows that would be deleted (dry run)...")
            rows_to_delete = await count_rows_to_delete(conn, report.prune_ids)
            print_report(report, index, table_sizes, rows_to_delete, pruned=False)
    finally:
        if conn is not None:
            await conn.close()


def main():
    init_logger(repo="discogs-etl", tool="discogs-etl verify_cache")
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
