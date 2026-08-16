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
from collections.abc import Iterable, Sequence
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
sys.path.insert(0, str(Path(__file__).parent))
from check_cache_drift import post_slack_alert
from wxyc_etl.text import is_compilation_artist, split_artist_name_contextual

from lib.dsn import redact_dsn
from lib.format_normalization import format_matches, normalize_library_format
from lib.keep_release_ids import parse_keep_release_ids
from lib.observability import init_logger
from lib.pg_concurrent_ddl import (
    SWAP_PATH_ATTEMPTS,
    SWAP_PATH_BACKOFF_SECONDS,
    add_constraint_safely,
    add_index_concurrently_safely,
)
from lib.scratch_namespace import drop_scratch_tables, new_scratch_suffix, scratch_name

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
        cls,
        rows: list[tuple[str, str]]
        | list[tuple[str, str, str | None]]
        | list[tuple[str, str, str | None, str | None]],
    ) -> LibraryIndex:
        """Build index from library row tuples.

        Rows may be 2-, 3-, or 4-tuples:

        - ``(raw_artist, raw_title)``
        - ``(raw_artist, raw_title, raw_format)``
        - ``(raw_artist, raw_title, raw_format, raw_alternate_artist_name)``

        When present, ``raw_alternate_artist_name`` (the library's alias / ANV
        for the same release, e.g. "Plug" for "Luke Vibert") is indexed as an
        **additional exact-match artist key** for the same title, so a Discogs
        release credited to the alias classifies KEEP rather than PRUNE. This is
        an exact-key addition, not a fuzzy-threshold change. See discogs-etl#305.

        Args:
            rows: List of library row tuples (see above).
        """
        exact_pairs: set[tuple[str, str]] = set()
        artist_to_titles: dict[str, set[str]] = {}
        combined_to_original: dict[str, tuple[str, str]] = {}
        artist_set: set[str] = set()
        compilation_titles: set[str] = set()
        format_by_pair: dict[tuple[str, str], set[str | None]] = {}
        has_format = len(rows) > 0 and len(rows[0]) >= 3
        has_alternate = len(rows) > 0 and len(rows[0]) >= 4

        def _index_pair(
            norm_artist: str,
            norm_title: str,
            raw_format: str | None,
            index_combined: bool = True,
        ) -> None:
            """Index one (artist, title) key, recording format even on dedup.

            ``norm_artist`` is always added to ``artist_set`` (``all_artists``),
            which is both the exact-match routing key in ``classify_all_releases``
            *and* the candidate list for the precise two-stage scorer — an alias
            needs to be there or a Discogs release credited to it can never match.

            ``index_combined`` gates only the ``combined_strings`` membership that
            feeds the *loose* ``token_set`` / ``token_sort`` scorers. The alias
            path passes False: a short/generic alias like "Plug" as a combined
            "plug ||| <title>" string would fuzzy-match unrelated Discogs
            releases sharing that title, inflating false KEEPs — the two-stage
            scorer (which requires artist AND title agreement) is the safe way to
            reach the alias. See discogs-etl#305.
            """
            pair = (norm_artist, norm_title)

            # Build format_by_pair before dedup check — same pair may have multiple formats
            if has_format:
                norm_format = normalize_library_format(raw_format)
                format_by_pair.setdefault(pair, set()).add(norm_format)

            if pair in exact_pairs:
                return  # deduplicate (artist+title already indexed)

            exact_pairs.add(pair)
            artist_to_titles.setdefault(norm_artist, set()).add(norm_title)
            artist_set.add(norm_artist)

            if not index_combined:
                return

            combined = f"{norm_artist}{COMBINED_SEPARATOR}{norm_title}"
            combined_to_original[combined] = pair

        for row in rows:
            raw_artist, raw_title = row[0], row[1]
            raw_format = row[2] if has_format else None
            raw_alternate = row[3] if has_alternate else None
            if not raw_artist or not raw_title:
                continue

            norm_title = normalize_title(raw_title)

            # Check if this is a compilation entry
            if is_compilation_artist(raw_artist):
                compilation_titles.add(norm_title)
                continue

            norm_artist = normalize_artist(raw_artist)
            _index_pair(norm_artist, norm_title, raw_format)

            # Index the library alias / ANV as an additional *exact* key for the
            # same title (discogs-etl#305). Skip empties, compilation-style
            # aliases, and aliases that collapse to the canonical artist.
            # index_combined=False keeps the alias out of combined_strings (the
            # loose token_set / token_sort scorers) so a short alias doesn't
            # become a fuzzy magnet for unrelated Discogs releases; it stays in
            # all_artists for exact-match routing and the precise two-stage
            # scorer.
            if raw_alternate and not is_compilation_artist(raw_alternate):
                norm_alternate = normalize_artist(raw_alternate)
                if norm_alternate and norm_alternate != norm_artist:
                    _index_pair(norm_alternate, norm_title, raw_format, index_combined=False)

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
    def from_sqlite(cls, db_path: Path, use_alternate: bool = True) -> LibraryIndex:
        """Build index from the library SQLite database.

        Prefers the widest column set available, degrading gracefully:

        - ``artist, title, format, alternate_artist_name`` (4-tuples) — the alias
          is indexed as an additional exact key (discogs-etl#305)
        - ``artist, title, format`` (3-tuples)
        - ``artist, title`` (2-tuples)

        Args:
            db_path: Path to library.db
            use_alternate: When ``True`` (default), select ``alternate_artist_name``
                and index it as an extra exact key (post-#305 behavior). When
                ``False``, skip that column entirely so ``has_alternate`` stays
                False and the alias is never indexed — reproducing pre-#305
                matching. The prune audit (discogs-etl#217 Phase 0) builds a
                second index with ``use_alternate=False`` to measure the #305
                recall uplift as a file diff.
        """
        logger.info(f"Building LibraryIndex from {db_path} (use_alternate={use_alternate})")
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        if use_alternate:
            try:
                cur.execute("SELECT artist, title, format, alternate_artist_name FROM library")
            except sqlite3.OperationalError:
                try:
                    cur.execute("SELECT artist, title, format FROM library")
                except sqlite3.OperationalError:
                    cur.execute("SELECT artist, title FROM library")
        else:
            # Pre-#305 reproduction: never select the alias column, so
            # from_rows sees at most 3-tuples and has_alternate is False.
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


def load_keep_release_ids(path: Path | None) -> set[int]:
    """Load an allowlist of release_ids exempt from pruning.

    Returns an empty set if *path* is None or the file doesn't exist --
    tolerant like load_artist_mappings, so a missing/omitted allowlist
    degrades to a no-op rather than a hard failure.
    """
    if path is None:
        return set()
    return parse_keep_release_ids(path)


def apply_release_overrides(report: ClassificationReport, override_ids: set[int]) -> None:
    """Exempt WXYC library pinned overrides from pruning.

    Moves every id in *override_ids* into report.keep_ids and out of BOTH
    report.prune_ids and report.review_ids, regardless of what fuzzy-match
    classification decided. Subtracting from review_ids too preserves the
    keep/prune/review partition: without it a pinned REVIEW release would sit
    in keep_ids and review_ids at once -- double-counted in the report, and
    ambiguous to the copy-swap prune path that rebuilds from keep_ids ∪
    review_ids. A no-op when override_ids is empty.
    """
    report.keep_ids |= override_ids
    report.prune_ids -= override_ids
    report.review_ids -= override_ids


def _write_release_ids(path: Path, ids: set[int]) -> None:
    """Write *ids* to *path*, sorted, one release_id per line."""
    with open(path, "w", encoding="utf-8") as f:
        for release_id in sorted(ids):
            f.write(f"{release_id}\n")


def _partition_report(report: ClassificationReport) -> tuple[set[int], set[int], set[int]]:
    """Resolve a report's keep/prune/review to a true partition by release_id.

    ``classify_all_releases`` classifies each release once per primary-artist
    group, so a release credited to several ``extra=0`` artists can land in more
    than one bucket — e.g. KEEP via an in-library artist and PRUNE via a junk
    co-credit. Resolve those collisions with the production precedence
    ``KEEP > REVIEW > PRUNE`` (the prune copy-swap rebuilds from
    ``keep_ids ∪ review_ids``, so any keep/review membership means the release is
    retained). The returned prune set is therefore exactly what the rebuild
    removes — the coverage gap the audit measures — not the raw per-group prunes.

    Returns ``(keep, review, prune)`` as disjoint sets covering every classified
    release_id.
    """
    keep = set(report.keep_ids)
    review = report.review_ids - keep
    prune = report.prune_ids - keep - report.review_ids
    return keep, review, prune


def dump_classification(
    out_dir: Path,
    report: ClassificationReport,
    report_no_anv: ClassificationReport,
    override_ids: set[int],
    releases: Sequence[tuple[int, str, str, str | None]],
) -> None:
    """Persist the prune classification for the coverage audit (discogs-etl#217).

    The keep/prune/review id sets exist only in memory during a rebuild and are
    otherwise reported as counts only; in ``--prune`` mode the pre-prune
    population is deleted immediately after classification, so it cannot be
    recovered post-hoc. This writes everything Phase 2 of the audit needs, all
    from state already in memory (no extra query):

    - ``keep_ids.txt`` / ``prune_ids.txt`` / ``review_ids.txt`` — sorted,
      newline-delimited ``release_id``s, resolved to a true partition by
      ``_partition_report`` (KEEP > REVIEW > PRUNE). Post-override: they reflect
      the ``apply_release_overrides`` pass, so pinned releases have already moved
      into ``keep_ids``. ``prune_ids.txt`` is exactly the set the rebuild
      removes, so it never lists a release that is retained via another credit.
    - ``override_ids.txt`` — the pinned ids that were exempted, so the
      pinned/non-pinned split needs no live join later.
    - ``prune_ids_no_anv.txt`` — the (partitioned) prune set from a second
      classification pass built *without* ``alternate_artist_name`` indexing
      (pre-#305), with the SAME pinned-override exemption applied. Because both
      sides are post-override, the #305 recall uplift is a clean file diff
      (``prune_ids_no_anv − prune_ids``) with no pin contamination. Note the
      uplift this measures is *marginal over pins*: a release that ANV rescues
      but that is also pinned is retained either way, so it is intentionally
      excluded — the diff counts releases ANV saves that a pin did not already
      save, not the total ANV alias-match count.
    - ``prune_releases.jsonl`` — one ``{"id", "artist", "title", "format"}``
      object per pruned id, so the false-negative sample can read raw
      artist/title without a DB round-trip. A release fans out to several rows in
      ``releases`` (the ``extra=0`` join); the row with the lexicographically
      smallest artist is kept so the output is deterministic across runs.
    - ``counts.json`` — headline counts plus a ``dedup_note`` slot left null for
      Phase 2 to fill with the post-prune dedup delta. ``total_release_rows`` is
      the fan-out row count (a multi-artist release counts once per credit), so
      it is ``>=`` the number of distinct ids in the partitioned sets.

    Every write is a local file; the cache database is untouched.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    keep_ids, review_ids, prune_ids = _partition_report(report)
    _, _, prune_ids_no_anv = _partition_report(report_no_anv)

    _write_release_ids(out_dir / "keep_ids.txt", keep_ids)
    _write_release_ids(out_dir / "prune_ids.txt", prune_ids)
    _write_release_ids(out_dir / "review_ids.txt", review_ids)
    _write_release_ids(out_dir / "override_ids.txt", override_ids)
    _write_release_ids(out_dir / "prune_ids_no_anv.txt", prune_ids_no_anv)

    # One (artist, title, format) per release_id. A release with multiple primary
    # artists fans out to several rows in ``releases`` (the extra=0 join in
    # load_discogs_releases, ordered only by r.id); keep the row with the
    # smallest artist name so the dump is reproducible regardless of row order.
    release_by_id: dict[int, tuple[str | None, str | None, str | None]] = {}
    for release in releases:
        rid = release[0]
        artist = release[1]
        existing = release_by_id.get(rid)
        if existing is None or (artist or "") < (existing[0] or ""):
            fmt = release[3] if len(release) > 3 else None
            release_by_id[rid] = (artist, release[2], fmt)

    with open(out_dir / "prune_releases.jsonl", "w", encoding="utf-8") as f:
        for rid in sorted(prune_ids):
            artist, title, fmt = release_by_id.get(rid, (None, None, None))
            f.write(
                json.dumps(
                    {"id": rid, "artist": artist, "title": title, "format": fmt},
                    ensure_ascii=False,
                )
                + "\n"
            )

    counts = {
        "keep": len(keep_ids),
        "prune": len(prune_ids),
        "review": len(review_ids),
        "override_applied": len(override_ids),
        "total_release_rows": report.total_releases,
        # Filled by Phase 2: (keep + review) − final `release` count, the
        # dedup_releases.py collapse that happens after the prune.
        "dedup_note": None,
    }
    with open(out_dir / "counts.json", "w", encoding="utf-8") as f:
        json.dump(counts, f, indent=2)
        f.write("\n")

    logger.info(
        "Wrote prune-audit classification to %s "
        "(keep=%d prune=%d review=%d override=%d prune_no_anv=%d)",
        out_dir,
        len(keep_ids),
        len(prune_ids),
        len(review_ids),
        len(override_ids),
        len(prune_ids_no_anv),
    )


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


# Column DEFAULTs to re-apply on each new_X table *before* the swap makes it
# live. CTAS strips DEFAULTs along with NOT NULL/CHECK; restoring DEFAULT
# *before* the swap closes the race window where an LML cache-miss insert
# (which omits cached_at) could land NULL between the swap and the
# Level-2 SET DEFAULT, then trip the Level-2 SET NOT NULL. See #256
# (sibling of #254 fixed in scripts/dedup_releases.py).
PRUNE_PRE_SWAP_COLUMN_DEFAULTS: dict[str, dict[str, str]] = {
    "new_cache_metadata": {"cached_at": "now()"},
    "new_release_artist": {"extra": "0"},
    "new_release_track_artist": {"extra": "0"},
    # LML#510: see dedup_releases.PRE_SWAP_COLUMN_DEFAULTS for context.
    "new_release": {"not_found": "false"},
}


# Columns to apply ``NOT NULL`` to pre-swap so the post-swap
# ``ADD CONSTRAINT ... PRIMARY KEY USING INDEX`` step is a brief catalog
# flip rather than a hidden full-table scan under AccessExclusive. CTAS
# strips NOT NULL alongside DEFAULT; applying it on ``new_X`` (which isn't
# live yet) costs a scan that can't conflict with LML's writes. Mirror of
# ``dedup_releases.PRE_SWAP_NOT_NULL_COLUMNS`` — kept in sync so a future
# schema change adding a new PK doesn't have to be remembered in two
# places. See WXYC/discogs-etl#286 "Prereq for the brief AccessExclusive
# claim."
PRUNE_PRE_SWAP_NOT_NULL_COLUMNS: dict[str, tuple[str, ...]] = {
    "new_release": ("id",),
    "new_cache_metadata": ("release_id",),
}


def _prune_build_scratch_tables(
    db_url: str,
    keep_ids: set[int],
    review_ids: set[int],
    suffix: str = "",
) -> None:
    """Build ``_keep_ids`` and every ``new_X`` scratch table for one prune
    invocation inside a single transaction.

    Loads ``_keep_ids``, copies keepers into `new_X`, and re-applies the
    column DEFAULTs / NOT NULLs that CTAS stripped (so the swapped-in table
    is already correct at the moment it goes live) -- all namespaced with
    ``suffix`` (empty string, the default, preserves today's unnamespaced
    table names) and all inside ONE non-autocommit transaction on a
    dedicated connection, committed only once everything has been built.

    This is what satisfies "a crashed run leaves no scratch residue"
    (WXYC/discogs-etl#356): PostgreSQL rolls back an open transaction the
    moment it detects the client connection is gone, so a killed process
    (or a terminated backend) at any point inside this function undoes
    every scratch table it created up to that point. See the symmetric
    ``dedup_releases.build_dedup_scratch_tables`` for the same shape
    applied to the dedup step.

    Idempotent (DROP IF EXISTS precedes each CREATE). See #256.
    """
    all_ids = keep_ids | review_ids
    keep_ids_table = scratch_name("_keep_ids", suffix)
    conn = psycopg.connect(db_url)  # non-autocommit: this function owns the one commit
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {keep_ids_table}")
            cur.execute(f"CREATE UNLOGGED TABLE {keep_ids_table} (release_id integer PRIMARY KEY)")
            with cur.copy(f"COPY {keep_ids_table} (release_id) FROM STDIN") as copy:
                for rid in all_ids:
                    copy.write_row((rid,))

        for old_table, new_table, columns, id_col in PRUNE_COPY_TABLES:
            physical_new_table = scratch_name(new_table, suffix)
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {physical_new_table}")
                cur.execute(f"""
                    CREATE TABLE {physical_new_table} AS
                    SELECT {columns} FROM {old_table} t
                    WHERE EXISTS (
                        SELECT 1 FROM {keep_ids_table} k WHERE k.release_id = t.{id_col}
                    )
                """)
                for column, default_sql in PRUNE_PRE_SWAP_COLUMN_DEFAULTS.get(
                    new_table, {}
                ).items():
                    cur.execute(
                        f"ALTER TABLE {physical_new_table} ALTER COLUMN {column} "
                        f"SET DEFAULT {default_sql}"
                    )
                cur.execute(f"SELECT count(*) FROM {physical_new_table}")
                count = cur.fetchone()[0]
            logger.info(f"  Copied {old_table} -> {physical_new_table}: {count:,} rows")

        # Pre-swap NOT NULL on PK columns, driven by PRUNE_PRE_SWAP_NOT_NULL_COLUMNS.
        # The new_X tables aren't live yet, so the implicit full-table scan that
        # ``ALTER COLUMN ... SET NOT NULL`` performs can't conflict with LML's
        # writes — and doing it now is the prerequisite for the post-swap
        # ``ADD CONSTRAINT ... PRIMARY KEY USING INDEX`` step in
        # ``_prune_add_base_constraints_and_indexes`` to be the brief catalog
        # flip CONCURRENTLY promises rather than a hidden AccessExclusive scan.
        # Without this, CTAS-stripped NULLability would force PG to run SET
        # NOT NULL internally under AccessExclusive, defeating the lock-conflict
        # avoidance the helper is supposed to give us. See #286 "Prereq for the
        # 'brief AccessExclusive' claim". The dict form mirrors dedup's
        # PRE_SWAP_NOT_NULL_COLUMNS so a future PK column addition only has
        # to be remembered once per script.
        with conn.cursor() as cur:
            for new_table, columns in PRUNE_PRE_SWAP_NOT_NULL_COLUMNS.items():
                physical_new_table = scratch_name(new_table, suffix)
                for column in columns:
                    cur.execute(
                        f"ALTER TABLE {physical_new_table} ALTER COLUMN {column} SET NOT NULL"
                    )

        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


class CopySwapShortfallError(RuntimeError):
    """Raised when the copy-and-swap prune copied fewer ``release`` rows than
    the KEEP/REVIEW set called for.

    WXYC/discogs-etl#357: on 2026-08-04 a prune logged ``69,768 KEEP + 2,507
    REVIEW = 71,694 total`` immediately followed by ``Copied release ->
    new_release: 44,531 rows`` -- 27,163 ids marked keep had no source row
    to copy (a concurrent peer had already swapped the source table out
    from under this invocation). Nothing compared the two counts, so the
    truncated table swapped into place anyway and the loss wasn't found
    until the next day. See :func:`_assert_release_copy_count`.

    This is the one exception type :func:`prune_releases_copy_swap` treats
    specially on its cleanup path: a shortfall abort retains
    ``_keep_ids_<suffix>`` for forensics. Anything raised as a plain
    ``RuntimeError`` gets the blanket cleanup instead, so don't widen this
    class to cover unrelated prune failures.
    """


def _assert_release_copy_count(
    db_url: str,
    keep_ids: set[int],
    review_ids: set[int],
    *,
    suffix: str,
) -> None:
    """Abort before the swap if ``new_release`` didn't get every KEEP/REVIEW id.

    Compares ``count(*) FROM new_release`` (namespaced with ``suffix``) to
    ``|keep_ids ∪ review_ids|``. Scoped to the release/new_release pair
    only: ``keep_ids``/``review_ids`` are release ids, and ``release`` is
    the only 1:1-keyed table in PRUNE_COPY_TABLES. The other seven child
    tables have a variable rows-per-release ratio and must never be
    compared against this same total -- that would abort every legitimate
    prune, not just a genuine shortfall.

    A match is a no-op. A shortfall logs both counts, the delta, and a
    sample of the missing ids, posts a best-effort Slack alert (a no-op
    when ``SLACK_MONITORING_WEBHOOK`` is unset), and raises
    :class:`CopySwapShortfallError` -- called from
    :func:`_prune_copy_swap_tables` before it opens the connection that
    drops FK constraints and runs the RENAME swap, so a shortfall never
    touches the live tables. See WXYC/discogs-etl#357.

    The missing-id sample is capped at 20, so the alert alone can't
    close an investigation on a 27,163-id shortfall. The uncapped diff is
    re-runnable afterwards only because :func:`prune_releases_copy_swap`'s
    abort path retains ``_keep_ids_<suffix>``; the message says so, and
    that retention is what makes this sample a pointer rather than the
    whole record.
    """
    all_ids = keep_ids | review_ids
    expected = len(all_ids)

    keep_ids_table = scratch_name("_keep_ids", suffix)
    physical_new_release = scratch_name("new_release", suffix)

    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {physical_new_release}")
            copied = cur.fetchone()[0]

            if copied == expected:
                return

            cur.execute(f"""
                SELECT k.release_id FROM {keep_ids_table} k
                WHERE NOT EXISTS (
                    SELECT 1 FROM {physical_new_release} n WHERE n.id = k.release_id
                )
                ORDER BY k.release_id
                LIMIT 20
            """)
            missing_sample = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

    message = (
        f"Copy-and-swap prune shortfall on release -> {physical_new_release}: "
        f"copied {copied:,} of {expected:,} expected rows (KEEP ∪ REVIEW), "
        f"delta {expected - copied:,}. Missing ids (sample, max 20): {missing_sample}. "
        f"The live tables are untouched. The abort drops this invocation's new_* table "
        f"copies but RETAINS {keep_ids_table} for forensics -- re-run the NOT EXISTS diff "
        f"against it for the full missing-id set, then DROP TABLE {keep_ids_table} when "
        f"done, since per-invocation suffixing means no later run reclaims it."
    )
    logger.error(message)
    post_slack_alert(
        webhook_url=os.environ.get("SLACK_MONITORING_WEBHOOK"),
        message=message,
    )
    raise CopySwapShortfallError(message)


def _prune_copy_swap_tables(
    db_url: str,
    keep_ids: set[int],
    review_ids: set[int],
    *,
    suffix: str = "",
) -> None:
    """CTAS + DEFAULT-restore + RENAME for every table in PRUNE_COPY_TABLES.

    Delegates scratch-table construction to :func:`_prune_build_scratch_tables`
    (namespaced with ``suffix``, crash-safe -- see that function's docstring
    for WXYC/discogs-etl#356), asserts the release copy matches the
    KEEP/REVIEW set before any live table is touched (WXYC/discogs-etl#357,
    see :func:`_assert_release_copy_count`), then drops the old FK
    constraints and swaps the tables in. Idempotent. See #256.
    """
    _prune_build_scratch_tables(db_url, keep_ids, review_ids, suffix=suffix)
    _assert_release_copy_count(db_url, keep_ids, review_ids, suffix=suffix)

    conn = psycopg.connect(db_url, autocommit=True)
    try:
        # DROP CONSTRAINT + RENAME deadlock surface — same lock-acquisition
        # mismatch as the FK-add path below, with the *broader* surface
        # (AccessExclusive instead of ShareRowExclusive). Conflicts with every
        # live LML lock mode on the involved tables, not just the
        # ShareRowExclusive ↔ RowExclusive pairing from the FK-add path.
        # Wrap each statement in :func:`add_constraint_safely` with the wider
        # SWAP_PATH_ATTEMPTS budget so a transient LML transaction doesn't
        # abort the whole prune. See #286 "Second deadlock surface in the
        # same script."
        for stmt, table in (
            (
                "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
                "release_artist",
            ),
            (
                "ALTER TABLE release_label DROP CONSTRAINT IF EXISTS fk_release_label_release",
                "release_label",
            ),
            (
                "ALTER TABLE release_genre DROP CONSTRAINT IF EXISTS fk_release_genre_release",
                "release_genre",
            ),
            (
                "ALTER TABLE release_style DROP CONSTRAINT IF EXISTS fk_release_style_release",
                "release_style",
            ),
            (
                "ALTER TABLE release_track DROP CONSTRAINT IF EXISTS fk_release_track_release",
                "release_track",
            ),
            (
                "ALTER TABLE release_track_artist DROP CONSTRAINT "
                "IF EXISTS fk_release_track_artist_release",
                "release_track_artist",
            ),
            (
                "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
                "cache_metadata",
            ),
        ):
            add_constraint_safely(
                conn,
                stmt,
                lock_tables=[table],
                attempts=SWAP_PATH_ATTEMPTS,
                backoff_seconds=SWAP_PATH_BACKOFF_SECONDS,
            )

        for old_table, new_table, _, _ in PRUNE_COPY_TABLES:
            physical_new_table = scratch_name(new_table, suffix)
            bak = scratch_name(f"{old_table}_old", suffix)
            # Atomic swap: the three statements (RENAME old → bak, RENAME
            # new → old, DROP bak CASCADE) run in one transaction so the
            # table name ``old_table`` is never momentarily unbound (which
            # would race-fail LML's writes with ``relation does not exist``)
            # AND so AccessExclusive on ``old_table`` is held coherently
            # for the full swap window — LML's writes queue on the lock
            # and land in the swapped-in table once we commit. See #286
            # "Swap path lock-contention coverage." Both names are listed
            # in the LOCK TABLE for completeness even though the surviving
            # post-swap relation reuses the ``old_table`` name.
            add_constraint_safely(
                conn,
                [
                    f"ALTER TABLE {old_table} RENAME TO {bak}",
                    f"ALTER TABLE {physical_new_table} RENAME TO {old_table}",
                    f"DROP TABLE {bak} CASCADE",
                ],
                lock_tables=[old_table, physical_new_table],
                attempts=SWAP_PATH_ATTEMPTS,
                backoff_seconds=SWAP_PATH_BACKOFF_SECONDS,
            )
            logger.info(f"  Swapped {physical_new_table} -> {old_table}")
    finally:
        conn.close()


def _prune_add_base_constraints_and_indexes(db_url: str, *, suffix: str = "") -> None:
    """Add PK / FK / indexes / NOT NULL on the post-swap tables.

    Runs orphan cleanup, FK + PK creation, FK + trigram + cache metadata
    indexes, a race-window backfill of any NULL cache_metadata.cached_at
    that slipped past PRUNE_PRE_SWAP_COLUMN_DEFAULTS, and SET NOT NULL on
    the schema-required columns. Drops ``_keep_ids`` (namespaced with
    ``suffix``, matching the value :func:`_prune_build_scratch_tables` was
    called with in this invocation -- see WXYC/discogs-etl#356) at the end.

    DEFAULT restoration happens in ``_prune_build_scratch_tables`` pre-swap
    (see PRUNE_PRE_SWAP_COLUMN_DEFAULTS); this function only handles
    constraints/indexes that have to land on the live table.
    """
    conn = psycopg.connect(db_url, autocommit=True)
    try:
        # PK on release: build CONCURRENTLY so the index scan doesn't take
        # Share on ``release`` (which conflicts with LML's RowExclusive for
        # the full minutes-long build). The post-swap ``new_release.id``
        # column was set NOT NULL pre-swap in _prune_copy_swap_tables so
        # ``ADD CONSTRAINT ... USING INDEX`` is the brief catalog flip
        # CONCURRENTLY promises — without that prereq it would silently
        # run a full-table NOT NULL scan under AccessExclusive. See #286
        # "Prereq for the brief AccessExclusive claim".
        add_index_concurrently_safely(
            conn,
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS release_pkey ON release(id)",
        )
        add_constraint_safely(
            conn,
            "ALTER TABLE release ADD CONSTRAINT release_pkey PRIMARY KEY USING INDEX release_pkey",
            lock_tables=["release"],
        )

        # Clean orphan child rows before FK validation. The live LML
        # service writes child rows for releases NOT in the post-prune
        # subset; deleting them now keeps the ADD CONSTRAINT step below
        # from failing on validation. NOT VALID on the constraint itself
        # tolerates new orphans landing between cleanup and ADD. See
        # #211 + #188 for the parallel fix in dedup_releases.py.
        with conn.cursor() as cur:
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

        # FK constraint adds. Each ``ALTER TABLE ... ADD CONSTRAINT FOREIGN
        # KEY ... REFERENCES release(id) NOT VALID`` takes
        # ShareRowExclusiveLock on **both** the child (target of the ALTER)
        # and the parent (FK target) — see PG source
        # ``tablecmds.c::ATAddForeignKeyConstraint``: ``table_openrv(
        # fkconstraint->pktable, ShareRowExclusiveLock)``. ``NOT VALID``
        # skips the existing-row scan but does NOT skip the parent-side
        # lock acquisition.
        #
        # The deadlock fixed by #286: PG's FK creation acquires the child
        # first (the ``ALTER TABLE`` target) then the parent — opposite of
        # LML's order (``write_release`` UPSERTs ``release`` first, then
        # children). RowExclusive ↔ ShareRowExclusive conflict per the PG
        # lock-mode matrix → classic cycle. The fix here: pre-acquire both
        # tables via ``LOCK TABLE <parent>, <child> IN ACCESS EXCLUSIVE
        # MODE`` in parent-first order so the happy path doesn't even
        # reach PG's deadlock detector — our transaction just waits for
        # any open LML transaction to commit, then proceeds.
        for fk_ddl, child_table in (
            (
                "ALTER TABLE release_artist ADD CONSTRAINT fk_release_artist_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                "release_artist",
            ),
            (
                "ALTER TABLE release_label ADD CONSTRAINT fk_release_label_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                "release_label",
            ),
            (
                "ALTER TABLE release_genre ADD CONSTRAINT fk_release_genre_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                "release_genre",
            ),
            (
                "ALTER TABLE release_style ADD CONSTRAINT fk_release_style_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                "release_style",
            ),
            (
                "ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                "release_track",
            ),
            (
                "ALTER TABLE release_track_artist ADD CONSTRAINT "
                "fk_release_track_artist_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                "release_track_artist",
            ),
            (
                "ALTER TABLE cache_metadata ADD CONSTRAINT fk_cache_metadata_release "
                "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE NOT VALID",
                "cache_metadata",
            ),
        ):
            add_constraint_safely(
                conn,
                fk_ddl,
                # Parent-first ordering matches LML's ``write_release``
                # acquisition order.
                lock_tables=["release", child_table],
            )

        # PK on cache_metadata via CONCURRENTLY-built index then USING
        # INDEX attach. ``new_cache_metadata.release_id`` was set NOT NULL
        # pre-swap so this is a brief catalog flip, not a hidden scan.
        add_index_concurrently_safely(
            conn,
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS cache_metadata_pkey "
            "ON cache_metadata(release_id)",
        )
        add_constraint_safely(
            conn,
            "ALTER TABLE cache_metadata ADD CONSTRAINT cache_metadata_pkey "
            "PRIMARY KEY USING INDEX cache_metadata_pkey",
            lock_tables=["cache_metadata"],
        )

        # FK indexes + trigram + cache_metadata side indexes — all built
        # CONCURRENTLY so the long index build takes only
        # ShareUpdateExclusive (no conflict with LML's RowExclusive DML)
        # rather than the Share that non-concurrent CREATE INDEX would
        # hold for the full duration. CONCURRENTLY must NOT run inside a
        # transaction block; the helper refuses to do so and the
        # connection is autocommit at this point.
        for index_ddl in (
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_artist_release_id "
            "ON release_artist(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_label_release_id "
            "ON release_label(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_genre_release_id "
            "ON release_genre(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_style_release_id "
            "ON release_style(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_release_id "
            "ON release_track(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_artist_release_id "
            "ON release_track_artist(release_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_artist_name_trgm "
            "ON release_artist USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_title_trgm "
            "ON release USING gin (lower(f_unaccent(title)) gin_trgm_ops)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_title_trgm "
            "ON release_track USING gin (lower(f_unaccent(title)) gin_trgm_ops)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_artist_name_trgm "
            "ON release_track_artist USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cache_metadata_cached_at "
            "ON cache_metadata(cached_at)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cache_metadata_source "
            "ON cache_metadata(source)",
        ):
            add_index_concurrently_safely(conn, index_ddl)

        with conn.cursor() as cur:
            # Backfill any race-window NULLs before SET NOT NULL. The
            # pre-swap DEFAULT restoration in _prune_copy_swap_tables
            # *should* keep this UPDATE a no-op in steady state; we keep
            # it as belt-and-suspenders for any LML insert that committed
            # against the renamed table before the pre-swap ALTER landed.
            # See #256.
            cur.execute("UPDATE cache_metadata SET cached_at = now() WHERE cached_at IS NULL")

        # Re-apply NOT NULL constraints stripped by ``CREATE TABLE AS
        # SELECT`` above. CTAS carries column types forward but not
        # NOT NULL / DEFAULT / CHECK. The corresponding DEFAULTs are
        # re-applied earlier — in _prune_copy_swap_tables, before the
        # RENAME makes the new table live — so LML's cache-miss
        # INSERT (which omits ``cached_at``) never lands a NULL during
        # the swap window. Pinned by
        # ``tests/integration/test_copy_swap_preserves_not_null.py``.
        #
        # ``ALTER COLUMN ... SET NOT NULL`` takes AccessExclusive and
        # performs an implicit full-table scan. Wrap each via the helper
        # so the brief lock window is shared with LML cleanly under the
        # same retry envelope as the FK adds.
        for not_null_ddl, target_table in (
            ("ALTER TABLE release ALTER COLUMN title SET NOT NULL", "release"),
            ("ALTER TABLE release ALTER COLUMN not_found SET NOT NULL", "release"),
            (
                "ALTER TABLE release_artist ALTER COLUMN release_id SET NOT NULL",
                "release_artist",
            ),
            (
                "ALTER TABLE release_artist ALTER COLUMN artist_name SET NOT NULL",
                "release_artist",
            ),
            ("ALTER TABLE release_label ALTER COLUMN release_id SET NOT NULL", "release_label"),
            ("ALTER TABLE release_label ALTER COLUMN label_name SET NOT NULL", "release_label"),
            ("ALTER TABLE release_genre ALTER COLUMN release_id SET NOT NULL", "release_genre"),
            ("ALTER TABLE release_genre ALTER COLUMN genre SET NOT NULL", "release_genre"),
            ("ALTER TABLE release_style ALTER COLUMN release_id SET NOT NULL", "release_style"),
            ("ALTER TABLE release_style ALTER COLUMN style SET NOT NULL", "release_style"),
            ("ALTER TABLE release_track ALTER COLUMN release_id SET NOT NULL", "release_track"),
            ("ALTER TABLE release_track ALTER COLUMN sequence SET NOT NULL", "release_track"),
            ("ALTER TABLE release_track ALTER COLUMN title SET NOT NULL", "release_track"),
            (
                "ALTER TABLE release_track_artist ALTER COLUMN release_id SET NOT NULL",
                "release_track_artist",
            ),
            (
                "ALTER TABLE release_track_artist ALTER COLUMN track_sequence SET NOT NULL",
                "release_track_artist",
            ),
            (
                "ALTER TABLE release_track_artist ALTER COLUMN artist_name SET NOT NULL",
                "release_track_artist",
            ),
            (
                "ALTER TABLE cache_metadata ALTER COLUMN cached_at SET NOT NULL",
                "cache_metadata",
            ),
            ("ALTER TABLE cache_metadata ALTER COLUMN source SET NOT NULL", "cache_metadata"),
        ):
            add_constraint_safely(
                conn,
                not_null_ddl,
                lock_tables=[target_table],
            )

    finally:
        # Drop ``_keep_ids`` whether or not the constraint adds succeeded.
        # A failed prune that retries (manually or via the outer pipeline)
        # would otherwise leave the UNLOGGED keep-ids table sitting in the
        # cache until the next invocation's ``_prune_build_scratch_tables``
        # call drops it -- and under per-invocation suffixing (#356) a
        # retry mints a *different* suffix, so that self-healing DROP would
        # never find this invocation's table. This is the only cleanup it
        # gets, so it's load-bearing here, not merely belt-and-suspenders:
        # the storage overhead is real (~24MB per ~6M-row keep set) and an
        # orphaned per-invocation table would sit in ``public`` forever.
        keep_ids_table = scratch_name("_keep_ids", suffix)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {keep_ids_table}")
        except Exception:  # noqa: BLE001
            logger.warning("Could not clean up %s on exit", keep_ids_table, exc_info=True)
        conn.close()


def prune_releases_copy_swap(
    db_url: str,
    keep_ids: set[int],
    review_ids: set[int],
    *,
    suffix: str | None = None,
) -> None:
    """Prune releases using copy-and-swap (faster than DELETE for large prune sets).

    Instead of deleting ~86% of rows with CASCADE, copies only the ~14%
    we want to keep into fresh tables, then swaps them in. Reuses the
    pattern from dedup_releases.py.

    Args:
        db_url: PostgreSQL connection URL.
        keep_ids: Release IDs classified as KEEP.
        review_ids: Release IDs classified as REVIEW (also kept).
        suffix: Per-invocation scratch-table namespacing
            (WXYC/discogs-etl#356). Defaults to ``None``, which mints a
            fresh suffix via :func:`lib.scratch_namespace.new_scratch_suffix`
            -- so every real call to this function (verify_cache.py's own
            ``--prune`` CLI path, and run_pipeline.py's subprocess launch
            of it) is automatically collision-safe against a concurrent
            invocation, with no call-site changes required. Pass ``""``
            explicitly to opt out (pre-#356 unnamespaced behavior) or a
            specific value to control/observe one invocation's tables
            (e.g. in a concurrency or crash-residue test).
    """
    all_ids = keep_ids | review_ids
    if not all_ids:
        logger.warning("No releases to keep — skipping copy-swap prune")
        return

    resolved_suffix = new_scratch_suffix() if suffix is None else suffix
    logger.info("Scratch table suffix for this prune invocation: %s", resolved_suffix)

    logger.info(
        "Pruning via copy-and-swap (%s KEEP + %s REVIEW = %s total)",
        f"{len(keep_ids):,}",
        f"{len(review_ids):,}",
        f"{len(all_ids):,}",
    )

    start = time.time()
    # Once _prune_build_scratch_tables commits, this invocation's scratch
    # tables are durable and no longer covered by PostgreSQL's
    # rollback-on-disconnect -- and per-invocation suffixing means no later
    # run reclaims them by name. _prune_add_base_constraints_and_indexes
    # drops _keep_ids in its own finally, but it never runs at all if
    # _prune_copy_swap_tables raises (add_constraint_safely re-raises once
    # SWAP_PATH_ATTEMPTS is exhausted, e.g. when a live LML lock outlasts
    # the retry budget). Without this handler that failure orphans
    # _keep_ids_<suffix> plus every not-yet-swapped new_X_<suffix> -- a
    # multi-GB permanent leak in `public`.
    try:
        _prune_copy_swap_tables(db_url, keep_ids, review_ids, suffix=resolved_suffix)
        _prune_add_base_constraints_and_indexes(db_url, suffix=resolved_suffix)
    except CopySwapShortfallError:
        # Forensics exemption (WXYC/discogs-etl#357). The blanket cleanup
        # below is correct for every failure whose diagnosis lives in the
        # traceback -- but a shortfall's diagnosis lives in the *data*.
        # The alert names at most 20 missing ids, sampled by a NOT EXISTS
        # diff of _keep_ids against new_release; dropping _keep_ids makes
        # that query unrunnable and the remaining ids unrecoverable, since
        # the classification that produced them is not persisted anywhere
        # else and re-deriving it means re-running the whole fuzzy-match
        # phase against a cache that has since moved.
        #
        # So: retain _keep_ids_<suffix> (one integer column -- ~24MB at the
        # ~6M-row worst case, cheap to leave behind) and still drop every
        # new_X_<suffix> copy (full table copies, the multi-GB leak the
        # cleanup exists to prevent). The cost is a real one and is stated
        # in docs/architecture.md: this table is now the operator's to drop
        # once the investigation closes, because per-invocation suffixing
        # means no later run will ever reclaim its name.
        retained = scratch_name("_keep_ids", resolved_suffix)
        logger.error(
            "Copy-and-swap prune aborted on a row-count shortfall (suffix %s); dropping this "
            "invocation's new_* table copies but RETAINING %s so the missing-id diff stays "
            "re-runnable. Investigate, then DROP TABLE %s -- nothing else will.",
            resolved_suffix,
            retained,
            retained,
        )
        _drop_prune_scratch_tables(db_url, resolved_suffix, bases=PRUNE_NEW_TABLE_SCRATCH_BASES)
        raise
    except BaseException:
        logger.warning(
            "Copy-and-swap prune failed; dropping this invocation's scratch tables (suffix %s)",
            resolved_suffix,
        )
        _drop_prune_scratch_tables(db_url, resolved_suffix)
        raise
    elapsed = time.time() - start
    logger.info(f"Copy-and-swap prune completed in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# Copy to target database
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
SCHEMA_DIR = SCRIPT_DIR.parent / "schema"

# Tables and their column lists for the two copy-swap paths in this module.
#
# Both PRUNE_COPY_TABLES and COPY_TABLE_SPEC drive ``CREATE TABLE new_X AS SELECT
# {cols} FROM X`` operations. Any column missing from a list is silently dropped
# from the live table after the RENAME swap (CTAS inherits only what the SELECT
# names, and no relic remains in pg_attribute). For that reason the column lists
# must stay in sync with ``schema/create_database.sql`` — every column declared
# in the canonical CREATE TABLE for these tables MUST appear here, or the next
# rebuild will drop it.
#
# Parallel guard: ``dedup_releases.DEDUP_TABLES`` (the upstream copy-swap that
# happens just before the prune step). Pin tests for both live in
# ``tests/integration/test_dedup.py`` and
# ``tests/integration/test_verify_cache_columns.py``.

# Used by prune_releases_copy_swap (default --prune path; the monthly rebuild
# fires this on EVERY run).
PRUNE_COPY_TABLES = [
    (
        "release",
        "new_release",
        "id, title, release_year, country, artwork_url, released, format, master_id, artwork_checked_at, not_found",
        "id",
    ),
    (
        "release_artist",
        "new_release_artist",
        "release_id, artist_id, artist_name, extra, role",
        "release_id",
    ),
    (
        "release_label",
        "new_release_label",
        "release_id, label_id, label_name, catno",
        "release_id",
    ),
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
        "release_id, track_sequence, artist_name, extra, role",
        "release_id",
    ),
    (
        "cache_metadata",
        "new_cache_metadata",
        "release_id, cached_at, source, last_validated",
        "release_id",
    ),
]


# The `new_X` table copies alone. These are full copies of the live tables
# (multi-GB on the real cache), normally consumed by the swap's RENAME, so
# they only survive when the run dies before or partway through the swap
# loop. Every failure path drops these -- there is no case in which keeping
# one is worth the storage.
PRUNE_NEW_TABLE_SCRATCH_BASES: tuple[str, ...] = tuple(new for _, new, _, _ in PRUNE_COPY_TABLES)

# Every base scratch-table name one prune invocation can create, for the
# failure-path cleanup in prune_releases_copy_swap(). ``_keep_ids`` is the
# narrow (one integer column) id table; it is the one base that a
# CopySwapShortfallError abort deliberately retains -- see
# :func:`prune_releases_copy_swap`.
PRUNE_SCRATCH_BASES: tuple[str, ...] = ("_keep_ids", *PRUNE_NEW_TABLE_SCRATCH_BASES)


def _drop_prune_scratch_tables(
    db_url: str,
    suffix: str,
    bases: Iterable[str] = PRUNE_SCRATCH_BASES,
) -> None:
    """Drop this prune invocation's scratch tables. Idempotent.

    Best-effort: a failure to clean up must never mask the original error
    that triggered the cleanup, so this swallows and logs its own
    exceptions rather than raising.

    Args:
        db_url: PostgreSQL connection URL.
        suffix: The suffix this invocation minted.
        bases: Which base names to drop. Defaults to the full set. The
            shortfall-abort path in :func:`prune_releases_copy_swap` passes
            :data:`PRUNE_NEW_TABLE_SCRATCH_BASES` instead, to retain
            ``_keep_ids`` for forensics (WXYC/discogs-etl#357).
    """
    try:
        conn = psycopg.connect(db_url, autocommit=True)
    except Exception:  # noqa: BLE001
        logger.warning("Could not connect to clean up prune scratch tables", exc_info=True)
        return
    try:
        with conn.cursor() as cur:
            drop_scratch_tables(cur, bases, suffix)
    except Exception:  # noqa: BLE001
        logger.warning("Could not clean up prune scratch tables", exc_info=True)
    finally:
        conn.close()


# Used by --copy-to (deprecated alongside --target-db-url, but still functional).
# Each entry: (table_name, filter_column, columns_list).
COPY_TABLE_SPEC = [
    (
        "release",
        "id",
        [
            "id",
            "title",
            "release_year",
            "country",
            "artwork_url",
            "released",
            "format",
            "master_id",
            "artwork_checked_at",
            "not_found",
        ],
    ),
    (
        "release_artist",
        "release_id",
        ["release_id", "artist_id", "artist_name", "extra", "role"],
    ),
    ("release_label", "release_id", ["release_id", "label_id", "label_name", "catno"]),
    ("release_genre", "release_id", ["release_id", "genre"]),
    ("release_style", "release_id", ["release_id", "style"]),
    ("release_track", "release_id", ["release_id", "sequence", "position", "title", "duration"]),
    (
        "release_track_artist",
        "release_id",
        ["release_id", "track_sequence", "artist_name", "extra", "role"],
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
    parser.add_argument(
        "--keep-release-ids",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to a newline-separated file of release_ids that must never "
        "be pruned, regardless of fuzzy-match result (e.g. WXYC library "
        "pinned overrides from lml_cache.library_release_override). "
        "Omit for today's behavior.",
    )
    parser.add_argument(
        "--dump-classification",
        type=Path,
        default=None,
        metavar="DIR",
        help="Write the prune classification to DIR for the coverage audit "
        "(discogs-etl#217 Phase 0): keep/prune/review id sets, the pruned "
        "releases' raw artist/title, the applied override ids, and a second "
        "prune set computed without alternate_artist_name indexing (pre-#305) "
        "for the #305-uplift diff. Read-only with respect to cache data (only "
        "writes local files); adds one extra classification pass. Omit for "
        "normal rebuilds.",
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
    logger.info("Connecting to %s", redact_dsn(args.database_url))
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

        override_ids = load_keep_release_ids(args.keep_release_ids)
        if override_ids:
            logger.info(f"Applying {len(override_ids):,} pinned release_id override(s)")
        apply_release_overrides(report, override_ids)

        # Step 5b (audit only): dump the prune classification for discogs-etl#217
        # Phase 0. Runs a second classification pass with alternate_artist_name
        # indexing disabled (pre-#305) so the #305 recall uplift is recoverable
        # as a pure file diff in Phase 2. Guarded by --dump-classification, so it
        # never runs on a normal rebuild. Read-only w.r.t. cache data.
        if args.dump_classification is not None:
            logger.info(
                "Prune-audit mode: running a second no-ANV classification pass "
                "for the #305 uplift diff (discogs-etl#217)"
            )
            index_no_anv = LibraryIndex.from_sqlite(args.library_db, use_alternate=False)
            matcher_no_anv = MultiIndexMatcher(
                index_no_anv,
                artist_mappings=mappings,
                keep_threshold=args.score_cutoff,
            )
            report_no_anv = classify_all_releases(releases, index_no_anv, matcher_no_anv)
            # Apply the SAME pin exemption to the no-ANV report. This keeps
            # prune_ids_no_anv − prune_ids a pure #305 (alias) signal: a pinned
            # release that would prune under BOTH passes is removed from both
            # sets and never pollutes the uplift diff.
            apply_release_overrides(report_no_anv, override_ids)
            dump_classification(
                args.dump_classification,
                report,
                report_no_anv,
                override_ids,
                releases,
            )

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
