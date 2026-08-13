"""Tests for ``lib/backend_catalog_norm.py``.

Parameterized cases are ported from Backend-Service's
``jobs/library-etl/job.ts`` (commit ``9e841b010d527d1b32c0341bfa809516aa7560ba``)
unit tests -- ``tests/unit/jobs/library-etl.test.ts:169-206``
(``normalizeArtistName``) and ``:225-296`` (``normalizeCodeLetters``,
``parseFormatAndDiscs``) -- plus ``tests/unit/database/fold-artist-name.test.ts``
for the fold (twin of migration 0134's SQL function, commit
``79eff85ddaf7f7c10f0b5ce83bb4630abf098079``). The intervening
``toAlphabeticalName`` block (``:207-224``) is deliberately NOT ported: it
feeds ``artists.alphabetical_name``, which is not a diffed
``scripts/catalog_parity_diff.py`` column (see ``lib/backend_catalog_norm.py``'s
module docstring), and it is the sole home of the forbidden "The Beatles"
fixture name in that source file.

Incidental artist names from the Backend fixtures ("FKA twigs", "The
Beatles") are replaced with WXYC-representative artists per this repo's
CLAUDE.md convention (mirrored at ``tests/fixtures/create_fixtures.py:12-16``).
The *shape* of each case (leading/trailing whitespace, casing) is preserved
exactly -- only the incidental name changes. "The Notwist" / "Notwist"
deliberately exercises the leading-"The" fold path Part 1 of the plan calls
out: ``fold_artist_name`` must NOT collapse them together.
"""

from __future__ import annotations

import unicodedata

import pytest

from lib.backend_catalog_norm import (
    fold_artist_name,
    is_db_only_genre,
    normalize_artist_name,
    normalize_code_letters,
    parse_format_and_discs,
)


class TestIsDbOnlyGenre:
    @pytest.mark.parametrize("value", ["db_only", "DB_ONLY", "  Db_Only  "])
    def test_true_for_db_only_case_insensitive(self, value: str) -> None:
        assert is_db_only_genre(value) is True

    @pytest.mark.parametrize("value", ["rock", ""])
    def test_false_for_other_values(self, value: str) -> None:
        assert is_db_only_genre(value) is False

    def test_false_for_none(self) -> None:
        assert is_db_only_genre(None) is False


class TestNormalizeArtistName:
    def test_normalizes_various_artists_dash_suffix(self) -> None:
        result = normalize_artist_name("Various Artists - latin")
        assert result.name == "Various Artists"
        assert result.is_various is True

    def test_is_case_insensitive_for_various_artists_prefix(self) -> None:
        result = normalize_artist_name("  VARIOUS ARTISTS - Something  ")
        assert result.name == "Various Artists"
        assert result.is_various is True

    def test_returns_trimmed_name_and_is_various_false_for_regular_artists(self) -> None:
        result = normalize_artist_name("  Chuquimamani-Condori  ")
        assert result.name == "Chuquimamani-Condori"
        assert result.is_various is False

    def test_matches_bare_various(self) -> None:
        result = normalize_artist_name("various")
        assert result.name == "Various Artists"
        assert result.is_various is True

    def test_matches_various_artists(self) -> None:
        result = normalize_artist_name("various artists")
        assert result.name == "Various Artists"
        assert result.is_various is True

    def test_various_artists_rock_letter_carveout_stays_literal(self) -> None:
        """Ported verbatim from the upstream case of the same name
        (``library-etl.test.ts:200-205``): this row keeps its raw name and is
        NOT Various Artists.

        **The carve-out regex is not what makes that true, and the port has to
        stay literal anyway.** ``"various artists - rock - a"`` matches neither
        regex: the carve-out needs ``-rock`` with no space (job.ts:81), and the
        general VA pattern's optional tail is ``\\s*-\\s*[a-z]+`` -- one hyphen,
        letters only -- so it cannot consume a second ``- a``. Both fail and the
        value falls through unchanged. In fact no string satisfies both
        patterns at once (the carve-out requires two hyphens, the general
        pattern permits one), which makes the carve-out branch unreachable *as
        a decision*: deleting it changes no answer.

        That is a property of the upstream source, not a licence to
        paraphrase. The parity harness's job is to derive what Backend
        actually holds, and it can only do that by matching job.ts
        byte-for-byte -- including its dead branches, so that a *future* edit
        to either regex diverges here loudly instead of silently. Recorded
        rather than "simplified away" for the same reason.
        """
        result = normalize_artist_name("various artists - rock - a")
        assert result.name == "various artists - rock - a"
        assert result.is_various is False


class TestNormalizeCodeLetters:
    def test_return_chars_uppercased(self) -> None:
        assert normalize_code_letters("ab") == "AB"
        assert normalize_code_letters("xyz") == "XYZ"

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_returns_none_for_empty_or_none(self, value: str | None) -> None:
        assert normalize_code_letters(value) is None

    def test_trims_then_takes_first_two(self) -> None:
        assert normalize_code_letters("  xy  ") == "XY"

    def test_unanchored_z_dash_letter_maps_to_various_artists_code(self) -> None:
        """``/Z-[A-Z]/`` is unanchored -- any ``Z-<letter>`` substring matches,
        not just a whole-string match."""
        assert normalize_code_letters("Z-S") == "V/A"
        assert normalize_code_letters("xZ-Ty") == "V/A"

    def test_z_dash_dash_does_not_match_and_keeps_its_value(self) -> None:
        """``Z--`` has no letter after the hyphen, so the unanchored
        ``/Z-[A-Z]/`` does not match it. It falls through to the 3-char
        branch and returns unchanged (uppercased, but already uppercase).

        This is the split the plan's R11 revision measured: ``Z--`` is
        3,104 rows on the 2026-07-19 prod snapshot -- the largest ``Z-``
        population, larger than every genuine ``Z-[A-Z]`` code combined --
        and it belongs to a totally different population than the 6,338-row
        ``V/A`` class (which comes from the ``isVarious`` branch, not this
        function at all).
        """
        assert normalize_code_letters("Z--") == "Z--"


class TestParseFormatAndDiscs:
    def test_parses_cd_as_cd_one_disc(self) -> None:
        assert parse_format_and_discs("cd") == ("cd", 1)
        assert parse_format_and_discs("CD") == ("cd", 1)

    def test_parses_cd_x_2_as_cd_two_discs(self) -> None:
        assert parse_format_and_discs("cd x 2") == ("cd", 2)

    def test_parses_cd_box_as_cd_one_disc(self) -> None:
        assert parse_format_and_discs("cd box") == ("cd", 1)

    def test_parses_cdr_as_cdr_one_disc(self) -> None:
        assert parse_format_and_discs("cdr") == ("cdr", 1)

    def test_parses_vinyl_no_size_as_vinyl_one_disc(self) -> None:
        assert parse_format_and_discs("vinyl") == ("vinyl", 1)

    def test_parses_7_inch_as_vinyl_7(self) -> None:
        assert parse_format_and_discs('vinyl 7"') == ('vinyl 7"', 1)

    def test_parses_10_inch_as_vinyl_10(self) -> None:
        assert parse_format_and_discs('vinyl 10"') == ('vinyl 10"', 1)

    def test_parses_12_inch_or_lp_as_vinyl_12(self) -> None:
        assert parse_format_and_discs('vinyl 12"') == ('vinyl 12"', 1)
        assert parse_format_and_discs("vinyl lp") == ('vinyl 12"', 1)

    def test_parses_vinyl_x_2_as_two_discs(self) -> None:
        assert parse_format_and_discs("vinyl x 2") == ("vinyl", 2)

    def test_parses_vinyl_12_x_2_as_two_discs(self) -> None:
        assert parse_format_and_discs('vinyl 12" x 2') == ('vinyl 12"', 2)

    @pytest.mark.parametrize("value", ["cassette", "digital", ""])
    def test_returns_none_for_unsupported_format(self, value: str) -> None:
        assert parse_format_and_discs(value) is None


# --- fold_artist_name (migration 0134 twin) --------------------------------
#
# The NFD (decomposed) form of each name is derived with
# `unicodedata.normalize`, not typed as a second source literal: a
# hand-typed "distinct codepoints" literal routinely gets silently
# NFC-normalized back to the same bytes as its NFC sibling by an editor,
# clipboard, or file-write pipeline, which would make the "all variants fold
# equal" tests below pass trivially without ever exercising the NFD path.
# Deriving it programmatically guarantees the two are byte-distinct.

_NILUFER_NFC = "Nilüfer Yanya"  # ü = U+00FC
_NILUFER_NFD = unicodedata.normalize("NFD", _NILUFER_NFC)  # u + U+0308 (combining diaeresis)
_NILUFER_ASCII = "Nilufer Yanya"

_CSILLAG_NFC = "Csillagrablók"  # ó = U+00F3
_CSILLAG_NFD = unicodedata.normalize("NFD", _CSILLAG_NFC)  # o + U+0301 (combining acute)
_CSILLAG_ASCII = "Csillagrablok"

_HERMANOS_NFC = "Hermanos Gutiérrez"  # é = U+00E9
_HERMANOS_NFD = unicodedata.normalize("NFD", _HERMANOS_NFC)  # e + U+0301
_HERMANOS_ASCII = "Hermanos Gutierrez"


@pytest.mark.parametrize(
    "nfc, nfd",
    [
        (_NILUFER_NFC, _NILUFER_NFD),
        (_CSILLAG_NFC, _CSILLAG_NFD),
        (_HERMANOS_NFC, _HERMANOS_NFD),
    ],
)
def test_nfc_and_nfd_fixtures_are_byte_distinct(nfc: str, nfd: str) -> None:
    """Guards the fixtures themselves: if this fails, the NFD variant collapsed
    back to NFC somewhere and the fold-equality tests below would be vacuous."""
    assert nfc != nfd


class TestFoldArtistName:
    def test_nilufer_yanya_all_variants_fold_equal(self) -> None:
        key = fold_artist_name(_NILUFER_NFC)
        assert key == "nilufer yanya"
        assert fold_artist_name(_NILUFER_NFD) == key
        assert fold_artist_name(_NILUFER_ASCII) == key
        assert fold_artist_name("NILÜFER YANYA") == key

    def test_csillagrablok_all_form_variants_fold_equal(self) -> None:
        key = fold_artist_name(_CSILLAG_NFC)
        assert key == "csillagrablok"
        assert fold_artist_name(_CSILLAG_NFD) == key
        assert fold_artist_name(_CSILLAG_ASCII) == key

    def test_hermanos_gutierrez_all_form_variants_fold_equal(self) -> None:
        key = fold_artist_name(_HERMANOS_NFC)
        assert key == "hermanos gutierrez"
        assert fold_artist_name(_HERMANOS_NFD) == key
        assert fold_artist_name(_HERMANOS_ASCII) == key

    def test_keeps_distinct_artists_distinct(self) -> None:
        assert fold_artist_name("Stereolab") != fold_artist_name("Cat Power")
        assert fold_artist_name("Nilüfer Yanya") != fold_artist_name("Jessica Pratt")

    def test_does_not_strip_a_leading_the(self) -> None:
        """Deliberately narrower than ``normalize_artist_name``: porting the
        leading-"The" strip here would newly merge "The Notwist" with
        "Notwist"."""
        assert fold_artist_name("The Notwist") == "the notwist"
        assert fold_artist_name("The Notwist") != fold_artist_name("Notwist")

    def test_preserves_ascii_punctuation_and_separators(self) -> None:
        assert (
            fold_artist_name("Duke Ellington & John Coltrane") == "duke ellington & john coltrane"
        )

    def test_does_not_fold_eszett_to_ss(self) -> None:
        """Safe under-merge direction: these letters carry no combining-mark
        decomposition under NFD, so the fold leaves them intact -- it strips
        diacritics but never transliterates."""
        assert fold_artist_name("Straße") == "straße"

    def test_does_not_fold_l_slash_to_l(self) -> None:
        assert fold_artist_name("Łódź") == "łodz"
        assert fold_artist_name("ł") == "ł"
        assert fold_artist_name("ł") != fold_artist_name("l")

    def test_keeps_turkish_dotless_i_distinct_from_i(self) -> None:
        assert fold_artist_name("ı") == "ı"
        assert fold_artist_name("ı") != fold_artist_name("i")

    @pytest.mark.parametrize("value", [None, ""])
    def test_total_over_nullish_input(self, value: str | None) -> None:
        assert fold_artist_name(value) == ""

    def test_idempotent(self) -> None:
        once = fold_artist_name(_NILUFER_NFC)
        assert fold_artist_name(once) == once
