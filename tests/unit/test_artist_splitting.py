"""Unit tests for lib/artist_splitting.py."""

from __future__ import annotations

import pytest

from lib.artist_splitting import (
    _split_trailing_and,
    _try_ampersand_split,
    split_artist_name,
    split_artist_name_contextual,
)

# ---------------------------------------------------------------------------
# split_artist_name (context-free)
# ---------------------------------------------------------------------------


class TestSplitArtistName:
    """Context-free splitting on unambiguous delimiters: comma, slash, plus."""

    def test_comma_split(self) -> None:
        assert split_artist_name("Mike Vainio, Ryoji, Alva Noto") == [
            "Mike Vainio",
            "Ryoji",
            "Alva Noto",
        ]

    def test_plus_split(self) -> None:
        assert split_artist_name("Mika Vainio + Ryoji Ikeda + Alva Noto") == [
            "Mika Vainio",
            "Ryoji Ikeda",
            "Alva Noto",
        ]

    def test_slash_split(self) -> None:
        assert split_artist_name("J Dilla / Jay Dee") == ["J Dilla", "Jay Dee"]

    def test_plus_deduplicates(self) -> None:
        assert split_artist_name("David + David") == ["David"]

    def test_numeric_comma_guard(self) -> None:
        """Commas in numeric contexts should not trigger splitting."""
        assert split_artist_name("10,000 Maniacs") == []

    def test_trailing_and_stripped(self) -> None:
        """'and' at the end of a comma-delimited list is part of the list."""
        assert split_artist_name("Emerson, Lake, and Palmer") == [
            "Emerson",
            "Lake",
            "Palmer",
        ]

    def test_trailing_ampersand_kept_without_context(self) -> None:
        """Trailing '& X' after comma-split stays intact without known_artists context."""
        assert split_artist_name("Crosby, Stills, Nash & Young") == [
            "Crosby",
            "Stills",
            "Nash & Young",
        ]

    def test_no_split_on_and(self) -> None:
        assert split_artist_name("Andy Human and the Reptoids") == []

    def test_no_split_on_with(self) -> None:
        assert split_artist_name("Nurse with Wound") == []

    def test_single_char_filtered(self) -> None:
        """Single-character components after splitting are filtered out."""
        assert split_artist_name("A + B") == []

    def test_no_delimiter(self) -> None:
        assert split_artist_name("Autechre") == []

    def test_empty_string(self) -> None:
        assert split_artist_name("") == []

    def test_whitespace_trimmed(self) -> None:
        assert split_artist_name("  Cat Power  /  Liz Phair  ") == [
            "Cat Power",
            "Liz Phair",
        ]

    def test_does_not_split_ampersand_alone(self) -> None:
        """Ampersand without comma context is not split by the context-free function."""
        assert split_artist_name("Duke Ellington & John Coltrane") == []

    def test_comma_with_short_numeric_component(self) -> None:
        """Guard catches numeric-looking components like '10' in '10,000 Maniacs'."""
        assert split_artist_name("808,303") == []

    def test_fred_hopkins_slash(self) -> None:
        assert split_artist_name("Fred Hopkins / Dierdre Murray") == [
            "Fred Hopkins",
            "Dierdre Murray",
        ]


# ---------------------------------------------------------------------------
# split_artist_name_contextual (with known_artists)
# ---------------------------------------------------------------------------


class TestSplitArtistNameContextual:
    """Contextual splitting adds ampersand splitting when components are known artists."""

    def test_ampersand_with_known_artist(self) -> None:
        known = {"duke ellington"}
        result = split_artist_name_contextual("Duke Ellington & John Coltrane", known)
        assert result == ["Duke Ellington", "John Coltrane"]

    def test_ampersand_without_known_artist(self) -> None:
        known: set[str] = set()
        result = split_artist_name_contextual("Simon & Garfunkel", known)
        assert result == []

    def test_ampersand_with_second_component_known(self) -> None:
        known = {"john coltrane"}
        result = split_artist_name_contextual("Duke Ellington & John Coltrane", known)
        assert result == ["Duke Ellington", "John Coltrane"]

    def test_context_free_splits_still_work(self) -> None:
        """Context-free delimiters (/, +, comma) work regardless of known_artists."""
        known: set[str] = set()
        result = split_artist_name_contextual("J Dilla / Jay Dee", known)
        assert result == ["J Dilla", "Jay Dee"]

    def test_imperfect_heuristic_13_and_god(self) -> None:
        """'13 & God' splits if 'god' is a known artist (imperfect but acceptable)."""
        known = {"god"}
        result = split_artist_name_contextual("13 & God", known)
        assert result == ["13", "God"]

    def test_known_artists_normalized(self) -> None:
        """known_artists should be pre-normalized (lowercase, no accents)."""
        known = {"bjork"}  # normalized form of "Björk"
        result = split_artist_name_contextual("Björk & Thom Yorke", known)
        assert result == ["Björk", "Thom Yorke"]

    def test_mixed_comma_and_ampersand(self) -> None:
        """Comma split happens first; ampersand in a remaining component is checked contextually."""
        known = {"young"}
        result = split_artist_name_contextual("Crosby, Stills, Nash & Young", known)
        # Comma split gives ["Crosby", "Stills", "Nash & Young"]
        # "Nash & Young" is re-checked: "young" is known -> split
        assert result == ["Crosby", "Stills", "Nash", "Young"]

    def test_mixed_comma_and_ampersand_no_known(self) -> None:
        """Without known artists, ampersand component stays intact."""
        known: set[str] = set()
        result = split_artist_name_contextual("Crosby, Stills, Nash & Young", known)
        assert result == ["Crosby", "Stills", "Nash & Young"]

    @pytest.mark.parametrize(
        "name",
        [
            "Sly and the Family Stone",
            "Andy Human and the Reptoids",
            "My Life with the Thrill Kill Kult",
            "Nurse with Wound",
        ],
    )
    def test_band_names_not_split(self, name: str) -> None:
        """Common band name patterns with 'and'/'with' should never be split."""
        known = {"sly", "andy human", "my life", "nurse"}
        assert split_artist_name_contextual(name, known) == []


# ---------------------------------------------------------------------------
# _comma_guard (numeric guard)
# ---------------------------------------------------------------------------


class TestCommaGuardNumeric:
    """The numeric guard in comma splitting prevents splitting artist names with numbers."""

    @pytest.mark.parametrize(
        "name",
        [
            "10,000 Maniacs",
            "808,303",
            "1,000 Homo DJs",
        ],
        ids=["10000-maniacs", "808-303", "1000-homo-djs"],
    )
    def test_numeric_components_block_split(self, name: str) -> None:
        assert split_artist_name(name) == []


# ---------------------------------------------------------------------------
# _split_trailing_and (single-component input)
# ---------------------------------------------------------------------------


class TestSplitTrailingAnd:
    """Direct tests for _split_trailing_and edge cases."""

    def test_single_component_returns_as_is(self) -> None:
        assert _split_trailing_and(["Autechre"]) == ["Autechre"]

    def test_empty_list_returns_as_is(self) -> None:
        assert _split_trailing_and([]) == []

    def test_two_components_with_trailing_and(self) -> None:
        assert _split_trailing_and(["Emerson", "and Palmer"]) == ["Emerson", "Palmer"]

    def test_two_components_without_trailing_and(self) -> None:
        assert _split_trailing_and(["Emerson", "Palmer"]) == ["Emerson", "Palmer"]


# ---------------------------------------------------------------------------
# _try_ampersand_split (edge cases)
# ---------------------------------------------------------------------------


class TestTryAmpersandSplit:
    """Direct tests for _try_ampersand_split edge cases."""

    def test_no_ampersand_returns_none(self) -> None:
        assert _try_ampersand_split("Autechre", {"autechre"}) is None

    def test_no_known_artist_returns_none(self) -> None:
        assert _try_ampersand_split("Simon & Garfunkel", set()) is None

    def test_single_char_components_rejected(self) -> None:
        """When all components after filtering are too short, returns None."""
        assert _try_ampersand_split("A & B", {"a"}) is None
