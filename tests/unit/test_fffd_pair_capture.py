"""Unit tests for ``lib/fffd_pair_capture.py`` (WXYC/Backend-Service#2152).

Pure logic only -- no network, no database, no subprocess. These tests are
the correctness bar for the pairing rule: a Backend
``compilation_track_artist`` row with U+FFFD is resolved against MySQL truth
only when exactly one candidate agrees at every non-U+FFFD position (and
matches byte-for-byte on any column that isn't corrupt), and every other
outcome must come back unresolved rather than guessed.

Fixtures use WXYC-representative, diacritic-bearing artists (Csillagrablók,
Nilüfer Yanya, Hermanos Gutiérrez, µ-Ziq) -- these are exactly the names
where an accent-stripped or visually-similar guess is a wrong answer, which
is the point of the rule under test.
"""

from __future__ import annotations

import dataclasses

import pytest

from lib.fffd_pair_capture import (
    CodepointEntry,
    CtaRow,
    ResolvedFffdPair,
    UnresolvedFffdPair,
    find_fffd_pairs,
    has_fffd,
    hex_codepoints,
    render_pending_cta_repair_values,
    sql_string_literal,
)


class TestHasFffd:
    def test_true_when_present(self) -> None:
        assert has_fffd("La B�te") is True

    def test_false_for_clean_ascii(self) -> None:
        assert has_fffd("Aluminum Tunes") is False

    def test_false_for_clean_unicode(self) -> None:
        assert has_fffd("Csillagrablók") is False


class TestHexCodepoints:
    def test_empty_for_pure_ascii(self) -> None:
        assert hex_codepoints("Hasty Boom Alert") == ()

    def test_records_every_non_ascii_character_in_order(self) -> None:
        cps = hex_codepoints("Reménytelen Tánc")
        assert [c.char for c in cps] == ["é", "á"]
        assert [c.codepoint for c in cps] == ["U+00E9", "U+00E1"]
        # Indices are into the actual string, not just sequential.
        assert cps[0].index == "Reménytelen Tánc".index("é")
        assert cps[1].index == "Reménytelen Tánc".index("á")

    def test_micro_sign_vs_greek_mu_are_distinct_codepoints(self) -> None:
        """The trap this whole capture mode exists to unblock (BS#2152)."""
        micro_sign = hex_codepoints("µ-Ziq")  # U+00B5 MICRO SIGN
        greek_mu = hex_codepoints("μ-Ziq")  # U+03BC GREEK SMALL LETTER MU
        assert [c.codepoint for c in micro_sign] == ["U+00B5"]
        assert [c.codepoint for c in greek_mu] == ["U+03BC"]
        assert micro_sign != greek_mu


class TestFindFffdPairs:
    def test_clean_single_candidate_match_preserves_char_length(self) -> None:
        """`La Bête` -> `La B<FFFD>te`: 7 characters on both sides."""
        backend = [CtaRow(1, "Various Artists", "La B�te")]
        mysql = [CtaRow(1, "Various Artists", "La Bête")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert len(resolved) == 1
        pair = resolved[0]
        assert pair.legacy_release_id == 1
        assert pair.current_artist_name == "Various Artists"
        assert pair.current_track_title == "La B�te"
        assert pair.true_artist_name is None  # not corrupt -- left untouched
        assert pair.true_track_title == "La Bête"
        assert pair.true_artist_name_codepoints == ()
        assert [c.codepoint for c in pair.true_track_title_codepoints] == ["U+00EA"]

    def test_multiple_u_fffd_in_a_single_string(self) -> None:
        """Two placeholders in one string resolve as one match, not two."""
        backend = [CtaRow(50340, "Csillagrablók", "Rem�nytelen T�nc")]
        mysql = [CtaRow(50340, "Csillagrablók", "Reménytelen Tánc")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert len(resolved) == 1
        pair = resolved[0]
        assert pair.true_artist_name is None  # artist_name was not corrupt
        assert pair.true_track_title == "Reménytelen Tánc"
        assert [c.codepoint for c in pair.true_track_title_codepoints] == [
            "U+00E9",
            "U+00E1",
        ]

    def test_ambiguous_multiple_candidates_is_unresolved_not_guessed(self) -> None:
        """The µ (U+00B5) vs μ (U+03BC) trap: two equally-plausible MySQL
        rows must never be silently disambiguated by picking one."""
        backend = [CtaRow(7, "�-Ziq", "Hasty Boom Alert")]
        mysql = [
            CtaRow(7, "µ-Ziq", "Hasty Boom Alert"),  # U+00B5 MICRO SIGN
            CtaRow(7, "μ-Ziq", "Hasty Boom Alert"),  # U+03BC GREEK SMALL LETTER MU
        ]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert len(unresolved) == 1
        row = unresolved[0]
        assert row.legacy_release_id == 7
        assert row.reason == "multiple_candidates"
        assert {c.artist_name for c in row.candidates} == {"µ-Ziq", "μ-Ziq"}

    def test_zero_candidates_is_unresolved(self) -> None:
        """No MySQL row for this release agrees on the non-corrupt column."""
        backend = [CtaRow(4, "Nil�fer Yanya", "Midnight Sun")]
        mysql = [CtaRow(4, "Nilüfer Yanya", "Midnight Sky")]  # different track_title

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert len(unresolved) == 1
        row = unresolved[0]
        assert row.reason == "zero_candidates"
        assert row.candidates == ()

    def test_zero_candidates_when_release_has_no_mysql_siblings_at_all(self) -> None:
        backend = [CtaRow(999, "Nil�fer Yanya", "Midnight Sun")]
        mysql = [CtaRow(1, "Nilüfer Yanya", "Midnight Sun")]  # different release

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "zero_candidates"

    def test_row_corrupt_in_both_columns_emits_as_one_row_with_both_true_values(
        self,
    ) -> None:
        backend = [
            CtaRow(2, "Herm�nos Guti�rrez", "M�xico"),
        ]
        mysql = [
            CtaRow(2, "Hermanos Gutiérrez", "México"),
            # Distractor: matches on artist_name alone, NOT track_title --
            # proves the two corrupt columns are resolved jointly (an
            # intersection), not independently, or this would also count as
            # a candidate and create a spurious ambiguity.
            CtaRow(2, "Hermanos Gutiérrez", "Something Else Entirely"),
            # Distractor: matches on track_title alone, NOT artist_name.
            CtaRow(2, "A Totally Different Artist", "México"),
        ]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert len(resolved) == 1
        pair = resolved[0]
        assert pair.true_artist_name == "Hermanos Gutiérrez"
        assert pair.true_track_title == "México"
        assert [c.codepoint for c in pair.true_artist_name_codepoints] == ["U+00E9"]
        assert [c.codepoint for c in pair.true_track_title_codepoints] == ["U+00E9"]

    def test_non_corrupt_backend_rows_are_skipped_entirely(self) -> None:
        backend = [CtaRow(1, "Stereolab", "Miss Modular")]
        mysql = [CtaRow(1, "Stereolab", "Miss Modular")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved == []

    def test_non_corrupt_column_must_match_byte_for_byte_to_anchor_identity(self) -> None:
        """A corrupt track_title with a MISMATCHED (but uncorrupted) artist_name
        must not match -- the non-corrupt column anchors row identity."""
        backend = [CtaRow(1, "Stereolab", "Miss M�dular")]
        mysql = [CtaRow(1, "Cat Power", "Miss Modular")]  # wrong artist_name

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "zero_candidates"

    def test_track_positions_lookup_is_applied_by_backend_current_values(self) -> None:
        backend = [CtaRow(50340, "Csillagrablók", "Rem�nytelen T�nc")]
        mysql = [CtaRow(50340, "Csillagrablók", "Reménytelen Tánc")]
        positions = {(50340, "Csillagrablók", "Rem�nytelen T�nc"): "3"}

        resolved, _ = find_fffd_pairs(backend, mysql, track_positions=positions)

        assert resolved[0].track_position == "3"

    def test_track_position_is_none_when_lookup_has_no_entry(self) -> None:
        backend = [CtaRow(1, "Various Artists", "La B�te")]
        mysql = [CtaRow(1, "Various Artists", "La Bête")]

        resolved, _ = find_fffd_pairs(backend, mysql)

        assert resolved[0].track_position is None

    def test_dataclasses_are_frozen(self) -> None:
        entry = CodepointEntry(index=0, char="é", codepoint="U+00E9")
        pair = ResolvedFffdPair(
            legacy_release_id=1,
            track_position=None,
            current_artist_name="a",
            current_track_title="b",
            true_artist_name=None,
            true_track_title="c",
        )
        unresolved = UnresolvedFffdPair(
            legacy_release_id=1,
            track_position=None,
            current_artist_name="a",
            current_track_title="b",
            reason="zero_candidates",
        )
        for obj, attr in ((entry, "index"), (pair, "true_artist_name"), (unresolved, "reason")):
            with pytest.raises(dataclasses.FrozenInstanceError):
                setattr(obj, attr, "mutated")


class TestSqlStringLiteral:
    def test_none_renders_as_bare_null(self) -> None:
        assert sql_string_literal(None) == "NULL"

    def test_plain_string_is_single_quoted(self) -> None:
        assert sql_string_literal("Csillagrablók") == "'Csillagrablók'"

    def test_embedded_single_quote_is_doubled(self) -> None:
        assert sql_string_literal("Guns N' Roses") == "'Guns N'' Roses'"

    def test_empty_string_is_not_null(self) -> None:
        assert sql_string_literal("") == "''"


class TestRenderPendingCtaRepairValues:
    def test_empty_input_renders_as_empty_string(self) -> None:
        assert render_pending_cta_repair_values([]) == ""

    def test_single_row_matches_consumer_column_order(self) -> None:
        row = ResolvedFffdPair(
            legacy_release_id=50340,
            track_position="3",
            current_artist_name="Csillagrablók",
            current_track_title="Rem�nytelen T�nc",
            true_artist_name=None,
            true_track_title="Reménytelen Tánc",
        )

        rendered = render_pending_cta_repair_values([row])

        assert rendered == (
            "  (50340, '3', 'Csillagrablók', 'Rem�nytelen T�nc', NULL, 'Reménytelen Tánc')"
        )

    def test_multiple_rows_are_comma_newline_joined_with_no_trailing_comma(self) -> None:
        rows = [
            ResolvedFffdPair(1, None, "a", "b", None, "true-b"),
            ResolvedFffdPair(2, None, "c", "d", "true-c", None),
        ]

        rendered = render_pending_cta_repair_values(rows)

        lines = rendered.split("\n")
        assert len(lines) == 2
        assert lines[0].endswith(",")
        assert not lines[1].endswith(",")
        # Splicing this between "VALUES" and the block's trailing ";" must
        # yield syntactically valid SQL -- two tuples, one comma between them.
        assert rendered.count("),\n  (") == 1

    def test_both_columns_corrupt_row_carries_both_true_values_not_two_rows(self) -> None:
        row = ResolvedFffdPair(
            legacy_release_id=2,
            track_position=None,
            current_artist_name="Herm�nos Guti�rrez",
            current_track_title="M�xico",
            true_artist_name="Hermanos Gutiérrez",
            true_track_title="México",
        )

        rendered = render_pending_cta_repair_values([row])

        assert rendered.count("\n") == 0  # exactly one row
        assert "'Hermanos Gutiérrez'" in rendered
        assert "'México'" in rendered
