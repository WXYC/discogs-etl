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

    def test_mysql_row_carrying_its_own_fffd_is_never_emitted_as_truth(self) -> None:
        """The MySQL side is the source of truth ONLY while it is itself clean.

        Position-wise agreement treats a Backend U+FFFD as matching anything,
        so a MySQL value holding a U+FFFD at the same position agrees with it
        trivially. Emitting that as ``true_track_title`` would write the
        corruption straight into the repair script while reporting success --
        the one failure mode this module's "never approximate" contract
        cannot tolerate, because the consumer applies it as an UPDATE.
        """
        backend = [CtaRow(50340, "Csillagrablók", "Rem�nytelen T�nc")]
        mysql = [CtaRow(50340, "Csillagrablók", "Rem�nytelen T�nc")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "corrupt_candidates"
        assert unresolved[0].candidates == (mysql[0],)

    def test_corrupt_candidates_is_distinct_from_zero_candidates(self) -> None:
        """ "tubafrenzy lost the byte too" needs a different remedy from "no row
        matched" -- the former means no re-import recovers it, so the reasons
        must not collapse into one."""
        backend = [CtaRow(1, "Various Artists", "La B�te")]

        _, no_match = find_fffd_pairs(backend, [CtaRow(1, "Various Artists", "Fade Away")])
        _, source_corrupt = find_fffd_pairs(backend, [CtaRow(1, "Various Artists", "La B�te")])

        assert no_match[0].reason == "zero_candidates"
        assert no_match[0].candidates == ()
        assert source_corrupt[0].reason == "corrupt_candidates"

    def test_a_clean_candidate_still_resolves_alongside_a_corrupt_sibling(self) -> None:
        """The guard rejects corrupt candidates; it does not poison the release."""
        backend = [CtaRow(1, "Nilüfer Yanya", "Th� Dealer")]
        clean = CtaRow(1, "Nilüfer Yanya", "Thé Dealer")
        mysql = [CtaRow(1, "Nilüfer Yanya", "Th� Dealer"), clean]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_track_title == "Thé Dealer"

    def test_byte_identical_mysql_siblings_resolve_because_the_truth_is_unambiguous(
        self,
    ) -> None:
        """Duplicate CTA rows are ordinary in this catalog (documented
        double-entry rate), and two byte-identical candidates carry ONE
        answer -- counting them as ambiguous would strand a row whose true
        value is not in doubt."""
        backend = [CtaRow(1, "Hermanos Guti�rrez", "El Bueno Y El Malo")]
        mysql = [
            CtaRow(1, "Hermanos Gutiérrez", "El Bueno Y El Malo"),
            CtaRow(1, "Hermanos Gutiérrez", "El Bueno Y El Malo"),
        ]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert len(resolved) == 1
        assert resolved[0].true_artist_name == "Hermanos Gutiérrez"

    def test_siblings_differing_in_the_corrupt_column_stay_ambiguous(self) -> None:
        """Deduplication is byte-exact, so it cannot launder a genuine
        two-answer case into a resolution."""
        backend = [CtaRow(1, "Various Artists", "L� Vie")]
        mysql = [CtaRow(1, "Various Artists", "Là Vie"), CtaRow(1, "Various Artists", "Lá Vie")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "multiple_candidates"
        assert len(unresolved[0].candidates) == 2

    def test_duplicate_backend_rows_each_emit_their_own_pending_row(self) -> None:
        """Both corrupt copies need repairing, and the consumer's UPDATE keys
        on the current values -- so one pending row per corrupt Backend row,
        not one per distinct value."""
        backend = [CtaRow(1, "Various Artists", "La B�te"), CtaRow(1, "Various Artists", "La B�te")]
        mysql = [CtaRow(1, "Various Artists", "La Bête")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert len(resolved) == 2
        assert {row.true_track_title for row in resolved} == {"La Bête"}

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
