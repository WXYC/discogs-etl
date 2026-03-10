"""Tests for verify_cache multi-index matching pipeline."""

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Load verify_cache module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
_spec = importlib.util.spec_from_file_location("verify_cache", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_vc = importlib.util.module_from_spec(_spec)
sys.modules["verify_cache"] = _vc
_spec.loader.exec_module(_vc)

# Re-export for cleaner access in tests
normalize_title = _vc.normalize_title
normalize_artist = _vc.normalize_artist
LibraryIndex = _vc.LibraryIndex
score_exact = _vc.score_exact
score_token_set = _vc.score_token_set
score_token_sort = _vc.score_token_sort
score_two_stage = _vc.score_two_stage
MultiIndexMatcher = _vc.MultiIndexMatcher
Decision = _vc.Decision
load_artist_mappings = _vc.load_artist_mappings
save_artist_mappings = _vc.save_artist_mappings
classify_compilation = _vc.classify_compilation
load_discogs_releases = _vc.load_discogs_releases

# ---------------------------------------------------------------------------
# Step 1: Normalization
# ---------------------------------------------------------------------------


class TestNormalizeTitle:
    """Test album/title normalization for matching."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ('Automanikk 12"', "automanikk"),
            ("Cobra (2 cd set)", "cobra"),
            ("OK Computer (reissue)", "ok computer"),
            ("Dummy (3)", "dummy"),  # Discogs disambiguation
            ("Abbey Road", "abbey road"),  # no-op
            ("In Utero (ep)", "in utero"),
            ("Loveless (deluxe edition)", "loveless"),
            ("  Spaced Out  ", "spaced out"),
            ('Raw Power 7"', "raw power"),
            ("Homogenic (2lp)", "homogenic"),
            ("Loveless (expanded edition)", "loveless"),
            ("OK Computer (anniversary edition)", "ok computer"),
            ("In Utero (special edition)", "in utero"),
            ("Dummy (limited edition)", "dummy"),
            ("Kid A (bonus tracks)", "kid a"),
        ],
        ids=[
            "vinyl_12_inch",
            "2_cd_set",
            "reissue",
            "discogs_disambiguation",
            "no_op",
            "ep_suffix",
            "deluxe_edition",
            "whitespace",
            "7_inch",
            "2lp",
            "expanded_edition",
            "anniversary_edition",
            "special_edition",
            "limited_edition",
            "bonus_tracks",
        ],
    )
    def test_normalize_title(self, raw, expected):
        assert normalize_title(raw) == expected


class TestNormalizeArtist:
    """Test artist name normalization."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("Radiohead", "radiohead"),
            ("Beatles, The", "the beatles"),
            ("Bjork (2)", "bjork"),  # Discogs disambiguation
            ("Artist [Scotland]", "artist"),  # Library disambiguation
            ("Simon & Garfunkel", "simon and garfunkel"),
            ("Simon And Garfunkel", "simon and garfunkel"),
            ("Guns N' Roses", "guns n roses"),
            ("  Spaced  ", "spaced"),
            ("Björk", "bjork"),  # Accent stripping
            ("E.S.T.", "e.s.t."),  # Dots preserved
        ],
        ids=[
            "simple",
            "comma_the",
            "discogs_disambiguation",
            "library_disambiguation",
            "ampersand_to_and",
            "and_normalized",
            "apostrophe",
            "whitespace",
            "accents",
            "dots_preserved",
        ],
    )
    def test_normalize_artist(self, raw, expected):
        assert normalize_artist(raw) == expected


class TestNormalizeArtistCommaConventions:
    """Test comma-article flipping for non-English definite articles."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("Fabulosos Cadillacs, Los", "los fabulosos cadillacs"),
            ("Ärzte, Die", "die arzte"),
            ("Planète Sauvage, La", "la planete sauvage"),
            ("Smiths, The", "the smiths"),  # existing behavior, should still pass
        ],
        ids=["spanish_los", "german_die", "french_la", "english_the"],
    )
    def test_comma_article_flipping(self, raw, expected):
        assert normalize_artist(raw) == expected


# ---------------------------------------------------------------------------
# Step 2: LibraryIndex
# ---------------------------------------------------------------------------

# Shared fixture: a small hand-crafted library for testing
SAMPLE_LIBRARY_ROWS = [
    ("Radiohead", "OK Computer"),
    ("Radiohead", "Kid A"),
    ("Joy Division", "Unknown Pleasures"),
    ("Joy Division", "Closer"),
    ("Aphex Twin", "Selected Ambient Works 85-92"),
    ("Aphex Twin", 'Analord 10 12"'),
    ("Beatles, The", "Abbey Road"),
    ("Simon & Garfunkel", "Bridge Over Troubled Water"),
    ("Björk", "Homogenic"),
    ("Various Artists - Compilations", "Sugar Hill"),
    ("Soundtracks - S", "Lost In Translation"),
]


@pytest.fixture
def sample_index():
    """Build a LibraryIndex from hand-crafted rows."""
    return LibraryIndex.from_rows(SAMPLE_LIBRARY_ROWS)


class TestLibraryIndex:
    """Test LibraryIndex construction and data structures."""

    def test_exact_pairs_populated(self, sample_index):
        """Normalized (artist, title) tuples are in exact_pairs."""
        assert ("radiohead", "ok computer") in sample_index.exact_pairs
        assert ("radiohead", "kid a") in sample_index.exact_pairs

    def test_exact_pairs_normalizes_titles(self, sample_index):
        """Title normalization strips suffixes before inserting into exact_pairs."""
        # 'Analord 10 12"' should become 'analord 10'
        assert ("aphex twin", "analord 10") in sample_index.exact_pairs

    def test_exact_pairs_normalizes_artists(self, sample_index):
        """Artist normalization handles ampersands and accents."""
        assert ("simon and garfunkel", "bridge over troubled water") in sample_index.exact_pairs
        assert ("bjork", "homogenic") in sample_index.exact_pairs

    def test_artist_to_titles_mapping(self, sample_index):
        """Each artist maps to the set of their normalized titles."""
        assert sample_index.artist_to_titles["radiohead"] == {"ok computer", "kid a"}
        assert "unknown pleasures" in sample_index.artist_to_titles["joy division"]

    def test_combined_strings_format(self, sample_index):
        """Combined strings use 'artist ||| title' format."""
        assert "radiohead ||| ok computer" in sample_index.combined_strings

    def test_combined_to_original_maps_back(self, sample_index):
        """combined_to_original maps the combined string back to the normalized pair."""
        key = "radiohead ||| ok computer"
        assert sample_index.combined_to_original[key] == ("radiohead", "ok computer")

    def test_all_artists_populated(self, sample_index):
        """all_artists contains deduplicated normalized artist names."""
        artists = sample_index.all_artists
        assert "radiohead" in artists
        assert "joy division" in artists
        # No duplicates
        assert len(set(artists)) == len(artists)

    def test_various_artists_excluded(self, sample_index):
        """Compilation entries are separated into compilation_index."""
        # "Various Artists - Compilations" and "Soundtracks - S" are compilations
        assert "various artists - compilations" not in sample_index.all_artists
        assert "soundtracks - s" not in sample_index.all_artists
        # But they should be in compilation_titles
        assert sample_index.compilation_titles is not None
        assert "sugar hill" in sample_index.compilation_titles

    def test_deduplication(self):
        """Duplicate rows produce unique entries."""
        rows = [
            ("Radiohead", "OK Computer"),
            ("Radiohead", "OK Computer"),  # duplicate
        ]
        idx = LibraryIndex.from_rows(rows)
        assert len(idx.exact_pairs) == 1
        assert len(idx.combined_strings) == 1


class TestLibraryIndexMultiArtistSplitting:
    """Test that LibraryIndex splits combined artist entries into components."""

    def test_comma_split_adds_component_pairs(self):
        """Comma-delimited multi-artist entries add component artist pairs."""
        rows = [("Mike Vainio, Ryoji, Alva Noto", "Live 2002")]
        idx = LibraryIndex.from_rows(rows)
        # Components should appear in exact_pairs
        assert ("mike vainio", "live 2002") in idx.exact_pairs
        assert ("ryoji", "live 2002") in idx.exact_pairs
        assert ("alva noto", "live 2002") in idx.exact_pairs

    def test_comma_split_adds_component_to_artist_to_titles(self):
        rows = [("Mike Vainio, Ryoji, Alva Noto", "Live 2002")]
        idx = LibraryIndex.from_rows(rows)
        assert "live 2002" in idx.artist_to_titles.get("alva noto", set())
        assert "live 2002" in idx.artist_to_titles_list.get("alva noto", [])

    def test_original_combined_entry_preserved(self):
        """The original combined entry should remain in the index."""
        rows = [("Mike Vainio, Ryoji, Alva Noto", "Live 2002")]
        idx = LibraryIndex.from_rows(rows)
        norm = normalize_artist("Mike Vainio, Ryoji, Alva Noto")
        assert (norm, "live 2002") in idx.exact_pairs

    def test_components_not_in_all_artists(self):
        """Synthetic component artists should NOT appear in all_artists."""
        rows = [("Mike Vainio, Ryoji, Alva Noto", "Live 2002")]
        idx = LibraryIndex.from_rows(rows)
        # all_artists should only contain the original normalized combined name
        assert "alva noto" not in idx.all_artists
        assert "mike vainio" not in idx.all_artists
        assert "ryoji" not in idx.all_artists

    def test_components_not_in_compilation_titles(self):
        """Splitting should not affect compilation_titles."""
        rows = [
            ("Various Artists", "Best of 2024"),
            ("Mike Vainio, Ryoji, Alva Noto", "Live 2002"),
        ]
        idx = LibraryIndex.from_rows(rows)
        assert "best of 2024" in idx.compilation_titles
        assert len(idx.compilation_titles) == 1

    def test_ampersand_split_with_known_standalone(self):
        """Ampersand entries split when a component exists as standalone."""
        rows = [
            ("Duke Ellington", "Money Jungle"),
            ("Duke Ellington & John Coltrane", "Duke Ellington & John Coltrane"),
        ]
        idx = LibraryIndex.from_rows(rows)
        # normalize_title doesn't convert & to "and", so title stays as-is
        norm_title = normalize_title("Duke Ellington & John Coltrane")
        # "john coltrane" should be added as a component
        assert ("john coltrane", norm_title) in idx.exact_pairs
        assert norm_title in idx.artist_to_titles.get("john coltrane", set())

    def test_ampersand_no_split_without_standalone(self):
        """Ampersand entries don't split when no component is standalone."""
        rows = [("Simon & Garfunkel", "Bridge Over Troubled Water")]
        idx = LibraryIndex.from_rows(rows)
        # Neither "simon" nor "garfunkel" should appear
        assert "simon" not in idx.artist_to_titles
        assert "garfunkel" not in idx.artist_to_titles

    def test_all_artists_count_unchanged(self):
        """all_artists count should not grow from splitting."""
        rows = [
            ("Duke Ellington", "Money Jungle"),
            ("Duke Ellington & John Coltrane", "Duke Ellington & John Coltrane"),
        ]
        idx = LibraryIndex.from_rows(rows)
        # Only 2 original artists: "duke ellington" and "duke ellington and john coltrane"
        assert len(idx.all_artists) == 2


# ---------------------------------------------------------------------------
# Step 3: Individual Scorers
# ---------------------------------------------------------------------------


class TestScoreExact:
    """Test exact pair matching scorer."""

    def test_exact_match_returns_1(self, sample_index):
        assert score_exact("radiohead", "ok computer", sample_index) == 1.0

    def test_no_match_returns_0(self, sample_index):
        assert score_exact("radiohead", "the bends", sample_index) == 0.0

    def test_normalizes_before_lookup(self, sample_index):
        """Inputs are already normalized by caller, but verify exact lookup."""
        assert score_exact("the beatles", "abbey road", sample_index) == 1.0


class TestScoreTokenSet:
    """Test token_set_ratio on combined 'artist ||| title' strings."""

    def test_high_similarity_for_exact_match(self, sample_index):
        score = score_token_set("radiohead", "ok computer", sample_index)
        assert score >= 0.95

    def test_partial_artist_with_matching_title_is_high(self, sample_index):
        """'Joy' / 'Unknown Pleasures' vs 'Joy Division' / 'Unknown Pleasures'.

        token_set_ratio is generous with subset matches -- 'joy' is a subset
        of 'joy division' tokens, so this scores very high (1.0). This is the
        known weakness that multi-index agreement compensates for.
        """
        score = score_token_set("joy", "unknown pleasures", sample_index)
        assert score >= 0.9  # token_set_ratio treats subsets generously

    def test_no_match_is_low(self, sample_index):
        score = score_token_set("zzyzx nonexistent", "fake album", sample_index)
        assert score < 0.5


class TestScoreTokenSort:
    """Test token_sort_ratio on combined strings."""

    def test_high_similarity_for_exact_match(self, sample_index):
        score = score_token_sort("radiohead", "ok computer", sample_index)
        assert score >= 0.95

    def test_no_match_is_low(self, sample_index):
        score = score_token_sort("zzyzx nonexistent", "fake album", sample_index)
        assert score < 0.5


class TestScoreTwoStage:
    """Test two-stage scorer: artist match then title match."""

    def test_exact_artist_and_title(self, sample_index):
        score = score_two_stage("radiohead", "ok computer", sample_index)
        assert score >= 0.95

    def test_penalizes_short_artist_with_wrong_title(self, sample_index):
        """'Joy' fuzzy-matches 'Joy Division' but 'Some Album' doesn't match titles.

        The two-stage scorer uses geometric mean: even if artist matches well,
        a poor title match drags the score down. Should be moderate at best.
        """
        score = score_two_stage("joy", "some album", sample_index)
        assert score < 0.75  # below the KEEP threshold

    def test_est_matches_est(self):
        """E.S.T. should match e.s.t. since dots are preserved."""
        rows = [("E.S.T.", "Tuesday Wonderland")]
        idx = LibraryIndex.from_rows(rows)
        score = score_two_stage("e.s.t.", "tuesday wonderland", idx)
        assert score >= 0.95

    def test_no_artist_match_returns_0(self, sample_index):
        score = score_two_stage("zzyzx nonexistent", "fake album", sample_index)
        assert score == 0.0


# ---------------------------------------------------------------------------
# Step 4: Multi-Index Agreement
# ---------------------------------------------------------------------------

# Module path for patching scorers
_MODULE = "verify_cache"


class TestMultiIndexMatcher:
    """Test MultiIndexMatcher.classify() with mocked scorer outputs."""

    def test_exact_match_is_keep(self, sample_index):
        """An exact pair match should always be KEEP."""
        matcher = MultiIndexMatcher(sample_index)
        result = matcher.classify("radiohead", "ok computer")
        assert result.decision == Decision.KEEP

    def test_two_of_three_above_threshold_is_keep(self, sample_index):
        """When 2 of 3 fuzzy scorers are above 0.75 (including two-stage), result is KEEP."""
        matcher = MultiIndexMatcher(sample_index)
        with (
            patch.object(_vc, "score_exact", return_value=0.0),
            patch.object(_vc, "score_token_set", return_value=0.80),
            patch.object(_vc, "score_token_sort", return_value=0.50),
            patch.object(_vc, "score_two_stage", return_value=0.78),
        ):
            result = matcher.classify("some artist", "some title")
        assert result.decision == Decision.KEEP

    def test_two_of_three_without_two_stage_is_not_keep(self, sample_index):
        """When 2 of 3 are above 0.75 but two-stage is low, NOT KEEP.

        This prevents false positives from subset matching on short names.
        """
        matcher = MultiIndexMatcher(sample_index)
        with (
            patch.object(_vc, "score_exact", return_value=0.0),
            patch.object(_vc, "score_token_set", return_value=0.80),
            patch.object(_vc, "score_token_sort", return_value=0.78),
            patch.object(_vc, "score_two_stage", return_value=0.50),
        ):
            result = matcher.classify("some artist", "some title")
        assert result.decision != Decision.KEEP

    def test_one_high_plus_one_moderate_is_keep(self, sample_index):
        """One scorer >= 0.85 plus another >= 0.70 (including two-stage) is KEEP."""
        matcher = MultiIndexMatcher(sample_index)
        with (
            patch.object(_vc, "score_exact", return_value=0.0),
            patch.object(_vc, "score_token_set", return_value=0.90),
            patch.object(_vc, "score_token_sort", return_value=0.40),
            patch.object(_vc, "score_two_stage", return_value=0.72),
        ):
            result = matcher.classify("some artist", "some title")
        assert result.decision == Decision.KEEP

    def test_all_below_near_miss_is_prune(self, sample_index):
        """When all scorers are below the REVIEW threshold, result is PRUNE."""
        matcher = MultiIndexMatcher(sample_index)
        with (
            patch.object(_vc, "score_exact", return_value=0.0),
            patch.object(_vc, "score_token_set", return_value=0.30),
            patch.object(_vc, "score_token_sort", return_value=0.25),
            patch.object(_vc, "score_two_stage", return_value=0.20),
        ):
            result = matcher.classify("zzyzx", "fake album")
        assert result.decision == Decision.PRUNE

    def test_near_miss_range_is_review(self, sample_index):
        """When max score is in the review range (0.65-0.75), result is REVIEW."""
        matcher = MultiIndexMatcher(sample_index)
        with (
            patch.object(_vc, "score_exact", return_value=0.0),
            patch.object(_vc, "score_token_set", return_value=0.70),
            patch.object(_vc, "score_token_sort", return_value=0.60),
            patch.object(_vc, "score_two_stage", return_value=0.55),
        ):
            result = matcher.classify("some artist", "some title")
        assert result.decision == Decision.REVIEW

    def test_joy_does_not_keep_as_joy_division(self, sample_index):
        """'Joy' / 'Random Album' should not match 'Joy Division' and KEEP.

        Even though token_set_ratio might be generous, the multi-index
        agreement should not let it through without title confirmation.
        """
        matcher = MultiIndexMatcher(sample_index)
        result = matcher.classify("joy", "random album")
        assert result.decision != Decision.KEEP

    def test_result_contains_scores(self, sample_index):
        """MatchResult should contain individual scorer scores."""
        matcher = MultiIndexMatcher(sample_index)
        result = matcher.classify("radiohead", "ok computer")
        assert result.exact_score == 1.0
        assert hasattr(result, "token_set_score")
        assert hasattr(result, "token_sort_score")
        assert hasattr(result, "two_stage_score")


class TestClassifyKnownArtist:
    """Test classify_known_artist — the primary Phase 2 classification path."""

    @pytest.mark.parametrize(
        "artist, title, expected_decision",
        [
            ("radiohead", "ok computer", Decision.KEEP),  # exact pair
            ("radiohead", "kid a", Decision.KEEP),  # exact pair
            ("radiohead", "ok computers", Decision.KEEP),  # fuzzy title >= keep
            ("radiohead", "nonexistent album", Decision.PRUNE),  # no title match
            ("aphex twin", "selected ambient works", Decision.KEEP),  # partial title
        ],
        ids=[
            "exact_pair",
            "exact_pair_2",
            "fuzzy_title_keep",
            "no_match_prune",
            "partial_title",
        ],
    )
    def test_classify_decisions(self, sample_index, artist, title, expected_decision):
        matcher = MultiIndexMatcher(sample_index)
        result = matcher.classify_known_artist(artist, title)
        assert result.decision == expected_decision

    def test_unknown_artist_returns_prune(self, sample_index):
        """Artist not in index at all -> PRUNE with zero scores."""
        matcher = MultiIndexMatcher(sample_index)
        result = matcher.classify_known_artist("zzyzx band", "some album")
        assert result.decision == Decision.PRUNE

    def test_exact_match_sets_all_scores_to_1(self, sample_index):
        """Exact pair match short-circuits with all scores at 1.0."""
        matcher = MultiIndexMatcher(sample_index)
        result = matcher.classify_known_artist("radiohead", "ok computer")
        assert result.exact_score == 1.0
        assert result.two_stage_score == 1.0


# ---------------------------------------------------------------------------
# Step 5: Artist Mappings Persistence
# ---------------------------------------------------------------------------


class TestArtistMappings:
    """Test loading and saving artist_mappings.json."""

    def test_load_empty_mappings(self, tmp_path):
        """Missing file returns empty keep/prune dicts."""
        mappings = load_artist_mappings(tmp_path / "nonexistent.json")
        assert mappings == {"keep": {}, "prune": {}}

    def test_load_existing_mappings(self, tmp_path):
        """Reads JSON and returns keep/prune dicts."""
        path = tmp_path / "mappings.json"
        data = {
            "keep": {"bjork (2)": "Bjork", "sunn o)))": "Sunn O)))"},
            "prune": {"joy": None},
        }
        path.write_text(json.dumps(data))
        mappings = load_artist_mappings(path)
        assert mappings["keep"]["bjork (2)"] == "Bjork"
        assert mappings["prune"]["joy"] is None

    def test_save_mappings(self, tmp_path):
        """Writes JSON that round-trips correctly."""
        path = tmp_path / "mappings.json"
        data = {
            "keep": {"bjork (2)": "Bjork"},
            "prune": {"joy": None},
        }
        save_artist_mappings(path, data)
        loaded = load_artist_mappings(path)
        assert loaded == data

    def test_mappings_override_keep(self, sample_index):
        """A REVIEW release whose artist is in keep mappings -> KEEP."""
        mappings = {"keep": {"some artist": "Some Artist"}, "prune": {}}
        matcher = MultiIndexMatcher(sample_index, artist_mappings=mappings)
        # This artist wouldn't normally be found in the index
        with (
            patch.object(_vc, "score_exact", return_value=0.0),
            patch.object(_vc, "score_token_set", return_value=0.70),
            patch.object(_vc, "score_token_sort", return_value=0.60),
            patch.object(_vc, "score_two_stage", return_value=0.55),
        ):
            result = matcher.classify("some artist", "some title")
        assert result.decision == Decision.KEEP

    def test_mappings_override_prune(self, sample_index):
        """A release whose artist is in prune mappings -> PRUNE."""
        mappings = {"keep": {}, "prune": {"joy": None}}
        matcher = MultiIndexMatcher(sample_index, artist_mappings=mappings)
        result = matcher.classify("joy", "unknown pleasures")
        assert result.decision == Decision.PRUNE


# ---------------------------------------------------------------------------
# Step 6: Compilation Handling
# ---------------------------------------------------------------------------


class TestCompilationHandling:
    """Test compilation classification by title-only matching."""

    def test_compilation_matched_by_title(self, sample_index):
        """'Various Artists' / 'Sugar Hill' matches the compilation title."""
        result = classify_compilation("sugar hill", sample_index)
        assert result == Decision.KEEP

    def test_compilation_no_match(self, sample_index):
        """Unknown compilation title -> PRUNE."""
        result = classify_compilation("unknown comp", sample_index)
        assert result == Decision.PRUNE

    def test_compilation_fuzzy_match(self):
        """Fuzzy title matching for compilations (minor spelling differences)."""
        rows = [("Various Artists - Compilations", "Lost In Translation")]
        idx = LibraryIndex.from_rows(rows)
        result = classify_compilation("lost in translation", idx)
        assert result == Decision.KEEP


# ---------------------------------------------------------------------------
# Step 7: Discogs Data Loading
# ---------------------------------------------------------------------------


class TestLoadDiscogsReleases:
    """Test loading releases from PostgreSQL with mocked asyncpg."""

    @pytest.mark.asyncio
    async def test_returns_release_tuples(self):
        """Returns list of (release_id, artist_name, title) tuples."""
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(
            return_value=[
                {"id": 28138, "title": "Confield", "artist_name": "Autechre"},
                {"id": 12345, "title": "OK Computer", "artist_name": "Radiohead"},
            ]
        )
        releases = await load_discogs_releases(mock_conn)
        assert len(releases) == 2
        assert releases[0] == (28138, "Autechre", "Confield")
        assert releases[1] == (12345, "Radiohead", "OK Computer")

    @pytest.mark.asyncio
    async def test_query_filters_extra_artists(self):
        """Query should only include main artists (extra = 0)."""
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        await load_discogs_releases(mock_conn)
        call_args = mock_conn.fetch.call_args
        query = call_args[0][0]
        assert "extra = 0" in query or "extra=0" in query

    @pytest.mark.asyncio
    async def test_empty_results(self):
        """Returns empty list when no releases found."""
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        releases = await load_discogs_releases(mock_conn)
        assert releases == []


# ---------------------------------------------------------------------------
# Step 8: Argument Parsing
# ---------------------------------------------------------------------------

format_bytes = _vc.format_bytes
ClassificationReport = _vc.ClassificationReport
MatchResult = _vc.MatchResult
print_report = _vc.print_report

classify_all_releases = _vc.classify_all_releases
classify_artist_fuzzy = _vc.classify_artist_fuzzy
classify_fuzzy_batch = _vc.classify_fuzzy_batch
prune_releases_copy_swap = _vc.prune_releases_copy_swap

parse_args = _vc.parse_args


# ---------------------------------------------------------------------------
# Step 8.5: Parallel Fuzzy Matching
# ---------------------------------------------------------------------------


class TestClassifyArtistFuzzy:
    """Test the extracted per-artist fuzzy classification function."""

    def test_high_score_match_returns_keep(self, sample_index):
        """An artist that closely matches a library artist returns keep IDs."""
        matcher = MultiIndexMatcher(sample_index)
        # "radiohead" is in library. A slight misspelling should still match.
        by_artist = {"radioheed": [(999, "Radioheed", "OK Computer")]}
        keep, prune, review, review_by = classify_artist_fuzzy(
            "radioheed", by_artist["radioheed"], sample_index, matcher
        )
        # "OK Computer" should match, so release 999 should be in keep
        assert 999 in keep

    def test_no_match_returns_prune(self, sample_index):
        """An artist with no plausible match returns prune IDs."""
        matcher = MultiIndexMatcher(sample_index)
        releases = [(888, "Zzyzx Unknownband", "Nonexistent Album")]
        keep, prune, review, review_by = classify_artist_fuzzy(
            "zzyzx unknownband", releases, sample_index, matcher
        )
        assert 888 in prune
        assert not keep

    def test_compilation_artist_uses_title_matching(self, sample_index):
        """Compilation artists should use title-only matching."""
        matcher = MultiIndexMatcher(sample_index)
        releases = [(777, "Various Artists", "Sugar Hill")]
        keep, prune, review, review_by = classify_artist_fuzzy(
            "various artists", releases, sample_index, matcher
        )
        assert 777 in keep

    def test_returns_four_collections(self, sample_index):
        """Function returns (keep_ids, prune_ids, review_ids, review_by_artist)."""
        matcher = MultiIndexMatcher(sample_index)
        releases = [(100, "Nobody", "Nothing")]
        result = classify_artist_fuzzy("nobody", releases, sample_index, matcher)
        assert len(result) == 4
        keep, prune, review, review_by = result
        assert isinstance(keep, set)
        assert isinstance(prune, set)
        assert isinstance(review, set)
        assert isinstance(review_by, dict)


class TestClassifyFuzzyBatch:
    """Test batch processing of multiple artists."""

    def test_batch_aggregates_multiple_artists(self, sample_index):
        """Batch processing aggregates results from multiple artists."""
        matcher = MultiIndexMatcher(sample_index)
        by_artist = {
            "radioheed": [(101, "Radioheed", "OK Computer")],
            "zzyzx unknownband": [(102, "Zzyzx Unknownband", "Fake Album")],
        }
        artists = ["radioheed", "zzyzx unknownband"]
        keep, prune, review, review_by = classify_fuzzy_batch(
            artists, by_artist, sample_index, matcher
        )
        assert 101 in keep
        assert 102 in prune

    def test_empty_batch_returns_empty_sets(self, sample_index):
        """Empty artist list returns empty sets."""
        matcher = MultiIndexMatcher(sample_index)
        keep, prune, review, review_by = classify_fuzzy_batch([], {}, sample_index, matcher)
        assert keep == set()
        assert prune == set()
        assert review == set()
        assert review_by == {}


class TestPruneCopySwapSQL:
    """Test prune_releases_copy_swap generates correct SQL operations."""

    def _make_mock_conn(self):
        """Create a mock psycopg connection with proper cursor context manager."""
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # fetchone returns (count,) for "SELECT count(*)" queries
        mock_cursor.fetchone.return_value = (42,)
        # copy context manager
        mock_cursor.copy.return_value.__enter__ = MagicMock()
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_cursor

    def test_creates_keep_ids_table(self):
        """Should create a temp table with keep + review IDs."""
        mock_conn, mock_cursor = self._make_mock_conn()

        with patch("verify_cache.psycopg") as mock_psycopg:
            mock_psycopg.connect.return_value = mock_conn
            prune_releases_copy_swap("postgresql:///test", keep_ids={1, 2, 3}, review_ids={4})

        # Verify _keep_ids temp table was created
        executed_sqls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any("_keep_ids" in s for s in executed_sqls)

    def test_swaps_all_release_tables(self):
        """Should swap release, release_artist, release_label, release_track,
        release_track_artist, and cache_metadata."""
        mock_conn, mock_cursor = self._make_mock_conn()

        with patch("verify_cache.psycopg") as mock_psycopg:
            mock_psycopg.connect.return_value = mock_conn
            prune_releases_copy_swap("postgresql:///test", keep_ids={1, 2}, review_ids={3})

        all_sql = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
        # All tables should be involved in the copy-swap
        for table in [
            "release",
            "release_artist",
            "release_label",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        ]:
            assert f"new_{table}" in all_sql, f"{table} should be part of copy-swap"

    def test_empty_ids_is_noop(self):
        """Empty keep + review IDs should not connect to database."""
        with patch("verify_cache.psycopg") as mock_psycopg:
            prune_releases_copy_swap("postgresql:///test", keep_ids=set(), review_ids=set())
        mock_psycopg.connect.assert_not_called()


class TestParallelMatchesSerial:
    """Verify parallel fuzzy matching produces identical results to serial."""

    def test_parallel_matches_serial(self, sample_index):
        """classify_all_releases should produce identical results regardless of threading."""
        releases = [
            (1, "Radiohead", "OK Computer"),
            (2, "Joy Division", "Unknown Pleasures"),
            (3, "Aphex Twin", "Selected Ambient Works 85-92"),
            (4, "Nobody Real", "Fake Album XYZ"),
            (5, "Another Unknown", "Phantom Record"),
        ]
        matcher = MultiIndexMatcher(sample_index)

        report = classify_all_releases(releases, sample_index, matcher)

        # Exact-match artists (radiohead, joy division, aphex twin) should be KEEP
        assert {1, 2, 3} <= report.keep_ids
        # Unknown artists should be PRUNE
        assert {4, 5} <= report.prune_ids


class TestParseArgsCopyTo:
    """Test --copy-to argument parsing and mutual exclusivity with --prune."""

    def test_copy_to_parsed(self, tmp_path):
        """--copy-to is parsed as a string."""
        lib_db = tmp_path / "library.db"
        lib_db.touch()
        args = parse_args(
            [str(lib_db), "postgresql:///discogs", "--copy-to", "postgresql:///target"]
        )
        assert args.copy_to == "postgresql:///target"
        assert not args.prune

    def test_prune_without_copy_to(self, tmp_path):
        """--prune works without --copy-to."""
        lib_db = tmp_path / "library.db"
        lib_db.touch()
        args = parse_args([str(lib_db), "--prune"])
        assert args.prune
        assert args.copy_to is None

    def test_copy_to_and_prune_mutually_exclusive(self, tmp_path):
        """--copy-to and --prune cannot be used together."""
        lib_db = tmp_path / "library.db"
        lib_db.touch()
        with pytest.raises(SystemExit):
            parse_args([str(lib_db), "--prune", "--copy-to", "postgresql:///target"])

    def test_default_no_copy_to(self, tmp_path):
        """copy_to defaults to None when not specified."""
        lib_db = tmp_path / "library.db"
        lib_db.touch()
        args = parse_args([str(lib_db)])
        assert args.copy_to is None


# ---------------------------------------------------------------------------
# format_bytes
# ---------------------------------------------------------------------------


class TestFormatBytes:
    """Test human-readable byte formatting."""

    @pytest.mark.parametrize(
        "num_bytes, expected",
        [
            (0, "0.0 B"),
            (1023, "1023.0 B"),
            (1024, "1.0 KB"),
            (1048576, "1.0 MB"),
            (1073741824, "1.0 GB"),
            (1099511627776, "1.0 TB"),
        ],
        ids=["zero", "bytes", "kilobytes", "megabytes", "gigabytes", "terabytes"],
    )
    def test_format_bytes(self, num_bytes: int, expected: str) -> None:
        assert format_bytes(num_bytes) == expected


# ---------------------------------------------------------------------------
# print_report
# ---------------------------------------------------------------------------


class TestPrintReport:
    """Test the print_report function with mock data."""

    def test_basic_report(self, sample_index, capsys: pytest.CaptureFixture[str]) -> None:
        report = ClassificationReport(
            keep_ids={1, 2, 3},
            prune_ids={4, 5},
            review_ids=set(),
            review_by_artist={},
            artist_originals={},
            total_releases=5,
        )

        print_report(report, sample_index)

        captured = capsys.readouterr()
        assert "VERIFICATION REPORT" in captured.out
        assert "KEEP:" in captured.out
        assert "PRUNE:" in captured.out
        assert "3" in captured.out  # keep count
        assert "2" in captured.out  # prune count

    def test_report_with_table_sizes(
        self, sample_index, capsys: pytest.CaptureFixture[str]
    ) -> None:
        report = ClassificationReport(
            keep_ids={1, 2},
            prune_ids={3, 4, 5},
            review_ids=set(),
            review_by_artist={},
            artist_originals={},
            total_releases=5,
        )
        table_sizes = {
            "release": (100, 1048576),
            "release_artist": (200, 2097152),
            "release_label": (150, 524288),
            "release_track": (500, 4194304),
            "release_track_artist": (300, 1048576),
            "cache_metadata": (100, 262144),
        }
        rows_to_delete = {
            "release": 60,
            "release_artist": 120,
            "release_label": 90,
            "release_track": 300,
            "release_track_artist": 180,
            "cache_metadata": 60,
        }

        print_report(report, sample_index, table_sizes=table_sizes, rows_to_delete=rows_to_delete)

        captured = capsys.readouterr()
        assert "Database size" in captured.out
        assert "Estimated savings" in captured.out
        assert "release_track" in captured.out

    def test_pruned_report(self, sample_index, capsys: pytest.CaptureFixture[str]) -> None:
        report = ClassificationReport(
            keep_ids={1, 2},
            prune_ids={3},
            review_ids=set(),
            review_by_artist={},
            artist_originals={},
            total_releases=3,
        )

        print_report(report, sample_index, pruned=True)

        captured = capsys.readouterr()
        assert "PRUNING REPORT" in captured.out
        assert "Releases kept:" in captured.out
        assert "Releases pruned:" in captured.out

    def test_report_with_review_artists(
        self, sample_index, capsys: pytest.CaptureFixture[str]
    ) -> None:
        match_result = MatchResult(
            decision=Decision.REVIEW,
            exact_score=0.0,
            token_set_score=0.70,
            token_sort_score=0.65,
            two_stage_score=0.60,
        )
        report = ClassificationReport(
            keep_ids={1},
            prune_ids={2},
            review_ids={3},
            review_by_artist={"some artist": [(3, "Some Album", match_result)]},
            artist_originals={"some artist": "Some Artist"},
            total_releases=3,
        )

        print_report(report, sample_index)

        captured = capsys.readouterr()
        assert "REVIEW" in captured.out
        assert "artist-level decisions needed" in captured.out
