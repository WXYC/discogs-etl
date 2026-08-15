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
    _double_encoded_fffd_match,
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


class TestForwardSimulatedDoubleEncoding:
    """discogs-etl#386: the live BS#2152 rows are double-encoded mojibake
    (WXYC/Backend-Service#1996) with U+FFFD damage layered on top, not the
    substitution-only shape ``TestFindFffdPairs`` covers. A length-preserving
    positional rule can never match them -- double-encoding inflates
    character count before U+FFFD is even considered, and an uncorrupted
    anchor column can itself be mojibake with no U+FFFD in sight.

    The fix does not invert the corruption (proven impossible in the ticket:
    splitting ``DÃ<FFFD><FFFD>calÃ© Chinois`` on U+FFFD and un-mojibaking the
    fragment ``DÃ`` raises ``UnicodeDecodeError``, because ``"DÃ"`` cp1252-
    encodes to a lone UTF-8 lead byte with no continuation). Instead it
    forward-simulates: double-encode each MySQL candidate the same way
    Backend's corruption would have, and accept the candidate only if that
    reproduces the observed Backend string exactly, with divergence permitted
    only inside a U+FFFD run -- and there only for as many UTF-8 bytes as the
    run is wide (see ``TestGapWidthIsByteBounded``).

    Every ``backend`` fixture in this class is a real production value off the
    14-row table on WXYC/Backend-Service#2152. The ``mysql`` side is a
    *reconstruction*, not an observation -- there is no MySQL ground truth in
    this environment (every ``candidates`` array in the live artifact is
    empty) -- but each one is forced: it is the value whose forward simulation
    reproduces the observed string byte-for-byte outside the run and to the
    exact byte width inside it, and it reads as the real title/name it claims
    to be.
    """

    def test_layered_double_encoding_and_fffd_resolves_to_real_mysql_truth(self) -> None:
        """Row 53042 from the live capture, byte-for-byte off the ticket
        table. ``DÃ<FFFD><FFFD>calÃ© Chinois`` is `Décalé Chinois` double-
        encoded via cp1252 with its lone `©` (the second byte of `é`'s
        double-encoded form) eaten by a 2-wide U+FFFD run -- not the 1
        U+FFFD-per-character shape the old rule assumed."""
        backend = [CtaRow(53042, "Wanda Sá", "DÃ��calÃ© Chinois")]
        mysql = [CtaRow(53042, "Wanda Sá", "Décalé Chinois")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert len(resolved) == 1
        pair = resolved[0]
        assert pair.true_artist_name is None  # artist_name was not corrupt
        assert pair.true_track_title == "Décalé Chinois"
        assert [c.codepoint for c in pair.true_track_title_codepoints] == [
            "U+00E9",
            "U+00E9",
        ]

    def test_anchor_column_can_be_clean_double_encoded_mojibake(self) -> None:
        """Row 8844, both columns straight off the ticket table: the anchor
        column (``artist_name``) is ITSELF double-encoded mojibake with no
        U+FFFD -- `Wanda SÃ¡` on Backend against MySQL's clean `Wanda Sá` --
        so byte-for-byte equality can never anchor this row's identity."""
        backend = [CtaRow(8844, "Wanda SÃ¡", "SÃ³ DanÃ��o Samba = Jazz 'N' Samba")]
        mysql = [CtaRow(8844, "Wanda Sá", "Só Danço Samba = Jazz 'N' Samba")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert len(resolved) == 1
        pair = resolved[0]
        assert pair.current_artist_name == "Wanda SÃ¡"  # untouched -- see Correction 4
        assert pair.true_artist_name is None  # anchor column, not corrupt
        assert pair.true_track_title == "Só Danço Samba = Jazz 'N' Samba"

    def test_three_wide_fffd_run_resolves_a_greek_true_value(self) -> None:
        """Row 11615's ``track_title``, the ticket's 3-adjacent-U+FFFD case.
        The run is 3 wide because the destroyed character in the mojibake is
        `†` (U+2020, the cp1252 reading of `φ`'s second UTF-8 byte 0x86),
        which is 3 UTF-8 bytes -- run width tracks bytes, not characters."""
        backend = [CtaRow(11615, "Chuquimamani-Condori", "Î”ÎµÎ»Ï†Î¯Î½Î¹ Î”ÎµÎ»Ï���Î¹Î½Î¬ÎºÎ¹")]
        mysql = [CtaRow(11615, "Chuquimamani-Condori", "Δελφίνι Δελφινάκι")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_track_title == "Δελφίνι Δελφινάκι"

    def test_leading_fffd_run_resolves_a_greek_artist_name(self) -> None:
        """Row 11615's ``artist_name`` opens with the run, so there is no
        prefix to anchor on at all -- only the byte-width bound and the
        suffix keep this from matching half the release."""
        backend = [CtaRow(11615, "��¤Î¬ÏƒÎ¿Ï‚ Î§Î±Î»ÎºÎ¹Î¬Ï‚", "Sirtaki")]
        mysql = [CtaRow(11615, "Τάσος Χαλκιάς", "Sirtaki")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_artist_name == "Τάσος Χαλκιάς"

    def test_two_wide_fffd_run_resolves_a_japanese_true_value(self) -> None:
        """Row 59194's ``track_title`` (Japanese, 3-byte-per-character UTF-8)
        -- confirms the forward-simulation is not Latin-alphabet-specific,
        and carries a U+008F that only the MySQL flavour of the codec can
        produce (see ``TestProducerCodecIsMysqlLatin1``)."""
        backend = [CtaRow(59194, "Various Artists", "ãƒˆãƒ¬ãƒ¼ãƒ‹ãƒ³ã‚°Â·��ƒ\x8fã‚¦ã‚¹")]
        mysql = [CtaRow(59194, "Various Artists", "トレーニング·ハウス")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_track_title == "トレーニング·ハウス"

    def test_length_inflating_substitution_only_row_resolves(self) -> None:
        """Row 11704 is NOT double-encoded -- its surviving text is pure
        ASCII, and the observed value is just `Huldreslåtten...` with `å`'s
        TWO UTF-8 bytes replaced by two U+FFFD. That inflates the character
        count by one, so the length-preserving substitution-only rule cannot
        see it either; only the identity simulation plus a 2-byte gap does.
        """
        title = "Huldresl��tten = The Wood Nymph Tune"
        backend = [CtaRow(11704, "Various Artists", title)]
        mysql = [CtaRow(11704, "Various Artists", "Huldreslåtten = The Wood Nymph Tune")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_track_title == "Huldreslåtten = The Wood Nymph Tune"

    def test_never_approximate_rejects_a_candidate_that_diverges_outside_the_gap(self) -> None:
        """`Décalé Chinoises` (trailing extra `s`) double-encodes to a string
        that agrees with row 53042's corrupt value everywhere up to the
        gap, then diverges right after it. The gap absorbs unknown content
        ONLY inside itself -- a mismatch anywhere else must still fail the
        match, or 'forward-simulate' degrades into a similarity score."""
        backend = [CtaRow(53042, "Wanda Sá", "DÃ��calÃ© Chinois")]
        mysql = [CtaRow(53042, "Wanda Sá", "Décalé Chinoises")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "zero_candidates"

    def test_two_candidates_differing_only_inside_the_gap_stay_ambiguous(self) -> None:
        """`Décalé` and `Dûcalé` double-encode to strings that agree with
        row 53042's corrupt value everywhere OUTSIDE the gap and differ only
        in the single character the gap swallows -- exactly the case the
        gap's width is supposed to make un-decidable. Both are legitimate
        candidates; picking either would be a guess."""
        backend = [CtaRow(53042, "Wanda Sá", "DÃ��calÃ© Chinois")]
        mysql = [
            CtaRow(53042, "Wanda Sá", "Décalé Chinois"),
            CtaRow(53042, "Wanda Sá", "Dûcalé Chinois"),
        ]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "multiple_candidates"
        assert {c.track_title for c in unresolved[0].candidates} == {
            "Décalé Chinois",
            "Dûcalé Chinois",
        }

    def test_current_values_stay_byte_identical_to_the_corrupt_backend_value(self) -> None:
        """Correction 4: ``current_*`` must match Backend's raw corrupt value
        exactly, byte for byte, even though resolving this row required
        computing a decoded/forward-simulated form internally --
        Backend-Service's repair SQL joins on ``current_artist_name`` /
        ``current_track_title`` matching the LIVE corrupt row, and a cleaned
        value here would silently zero out that join."""
        backend = [CtaRow(53042, "Wanda Sá", "DÃ��calÃ© Chinois")]
        mysql = [CtaRow(53042, "Wanda Sá", "Décalé Chinois")]

        resolved, _ = find_fffd_pairs(backend, mysql)

        assert resolved[0].current_artist_name.encode("utf-8") == backend[0].artist_name.encode(
            "utf-8"
        )
        assert resolved[0].current_track_title.encode("utf-8") == backend[0].track_title.encode(
            "utf-8"
        )
        assert resolved[0].current_track_title == "DÃ��calÃ© Chinois"

    def test_substitution_only_regression_still_resolves_unchanged(self) -> None:
        """The shape the module was originally written for -- no double
        encoding, one U+FFFD per destroyed character -- must keep resolving
        through the exact same length-preserving path, unaffected by the new
        forward-simulation fallback. (BS#2114's precedent; same fixture as
        ``TestFindFffdPairs.test_clean_single_candidate_match_preserves_char_length``.)

        The forward-simulation arm would NOT accept this pair: `ê` is 2 UTF-8
        bytes against a 1-wide run, so the byte-width bound rejects it. The
        assertion below therefore genuinely exercises the original path.
        """
        backend = [CtaRow(1, "µ-Ziq", "La B�te")]
        mysql = [CtaRow(1, "µ-Ziq", "La Bête")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_track_title == "La Bête"
        assert _double_encoded_fffd_match("La B�te", "La Bête") is False


class TestGapWidthIsByteBounded:
    """A U+FFFD gap is NOT a free wildcard -- it consumes exactly as many
    UTF-8 bytes of the simulated string as the run is wide.

    This is the difference between a constraint and a prefix/suffix filter.
    Two of the twelve live corrupt columns carry their run at index 0
    (11615's ``artist_name``, 53478's ``artist_name``) and one carries it at
    the very end (56717), so an unbounded gap would reduce those rows to
    "any candidate at this release that ends/starts the right way" -- and a
    single wrong candidate that clears that bar resolves the row, silently,
    with no ``multiple_candidates`` to catch it. The width bound is derived,
    not guessed: on every one of the seven reconstructible live rows the run
    width equals the UTF-8 byte length of the destroyed span exactly, which
    is what a per-byte ``errors="replace"`` decode of a UTF-8 stream does.
    """

    def test_trailing_run_does_not_swallow_an_arbitrary_suffix(self) -> None:
        """Row 56717's shape (`Ã³lÃ<FFFD><FFFD>`, run at the end). Without
        the byte bound this candidate matches on the prefix alone and gets
        written into a production UPDATE as truth."""
        backend = [CtaRow(56717, "Various Artists", "Ã³lÃ��")]
        mysql = [CtaRow(56717, "Various Artists", "ólé porque me quieres tanto")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "zero_candidates"

    def test_a_two_byte_trailing_gap_still_resolves(self) -> None:
        """The bound must not simply refuse trailing runs: the same shape
        with a candidate whose gap really is 2 UTF-8 bytes still resolves."""
        backend = [CtaRow(56717, "Various Artists", "Ã³lÃ��")]
        mysql = [CtaRow(56717, "Various Artists", "ólé")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_track_title == "ólé"

    def test_leading_run_does_not_swallow_an_arbitrary_prefix(self) -> None:
        """Row 11615's ``artist_name`` opens with the run. `Ο Τάσος Χαλκιάς`
        forward-simulates to a string ending in the observed suffix, so an
        unbounded gap accepts it -- and it is the only candidate, so it
        resolves. Its gap is 8 UTF-8 bytes against a 2-wide run."""
        backend = [CtaRow(11615, "��¤Î¬ÏƒÎ¿Ï‚ Î§Î±Î»ÎºÎ¹Î¬Ï‚", "Sirtaki")]
        mysql = [CtaRow(11615, "Ο Τάσος Χαλκιάς", "Sirtaki")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "zero_candidates"

    def test_mid_string_run_does_not_swallow_an_arbitrary_infix(self) -> None:
        """Row 53042 again, with a candidate that agrees on both sides of
        the gap but stuffs 22 extra characters into it."""
        backend = [CtaRow(53042, "Wanda Sá", "DÃ��calÃ© Chinois")]
        mysql = [CtaRow(53042, "Wanda Sá", "Déxxxxxxxxxxxxxxxxxxxxxxécalé Chinois")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "zero_candidates"

    def test_a_gap_may_not_end_mid_character(self) -> None:
        """A 2-wide run cannot be satisfied by a 3-byte character: the bound
        is exact-byte, not at-most-byte, so the run must land on a character
        boundary of the simulated string."""
        title = "Huldresl��tten = The Wood Nymph Tune"
        backend = [CtaRow(11704, "Various Artists", title)]
        mysql = [CtaRow(11704, "Various Artists", "Huldresl★tten = The Wood Nymph Tune")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "zero_candidates"

    def test_the_irreducible_one_byte_ambiguity_is_reported_not_resolved(self) -> None:
        """What the bound CANNOT remove: the destroyed byte itself. Every
        `D<U+00C0..U+00FF>calé Chinois` forward-simulates into row 53042's
        observed value, because they differ only in the one byte the gap
        stands for. When two of them are in the candidate set that is
        ``multiple_candidates``; when only a wrong one is, this module has no
        signal left to distinguish it -- which is why the PR body flags the
        residue rather than claiming it away."""
        backend = [CtaRow(53042, "Wanda Sá", "DÃ��calÃ© Chinois")]
        mysql = [
            CtaRow(53042, "Wanda Sá", "Décalé Chinois"),
            CtaRow(53042, "Wanda Sá", "Dàcalé Chinois"),
        ]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "multiple_candidates"


class TestProducerCodecIsMysqlLatin1:
    """The codec that produced this mojibake is MySQL's ``latin1`` -- which
    is windows-1252, and maps the five bytes cp1252 leaves unassigned
    (0x81, 0x8D, 0x8F, 0x90, 0x9D) to the C1 controls U+0081, U+008D, U+008F,
    U+0090, U+009D rather than raising.

    Python has neither codec: strict ``cp1252`` raises on those five bytes,
    and ``latin1`` maps ALL of 0x80-0x9F to C1 controls, which disagrees with
    the `‚`/`ƒ`/`„`/`†`/`Œ` visible in the live artifact. At least five of the
    twelve live corrupt columns (49848, 58487, 59002, 59194, 67454) contain
    one of the five bytes, so under a cp1252-or-latin1 transform set they can
    never resolve no matter what MySQL holds. These two rows are the proof.
    """

    def test_persian_row_needs_the_mysql_flavour_of_windows_1252(self) -> None:
        """Row 49848: `ف` is UTF-8 D9 81, and 0x81 is exactly the byte strict
        cp1252 refuses. The C1 control the ticket's table renders as ``^A``
        is U+0081, spelled ``\\x81`` here."""
        backend = [CtaRow(49848, "Ø¹Ø¨Ø¯Ø§Ù„Ù†Ù‚ÛŒ Ø§��\x81Ø´Ø§Ø±Ù†ÛŒØ§", "Nava")]
        mysql = [CtaRow(49848, "عبدالنقی افشارنیا", "Nava")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_artist_name == "عبدالنقی افشارنیا"

    def test_japanese_row_needs_the_mysql_flavour_of_windows_1252(self) -> None:
        """Row 59194: `ハ` is UTF-8 E3 83 8F, and 0x8F is another of the five.
        Strict cp1252 raises on it; ISO-8859-1 renders 0x83 as U+0083 instead
        of the `ƒ` the live value actually carries."""
        backend = [CtaRow(59194, "Various Artists", "ãƒˆãƒ¬ãƒ¼ãƒ‹ãƒ³ã‚°Â·��ƒ\x8fã‚¦ã‚¹")]
        mysql = [CtaRow(59194, "Various Artists", "トレーニング·ハウス")]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_track_title == "トレーニング·ハウス"

    def test_siblings_differing_only_in_the_anchor_column_carry_one_answer(self) -> None:
        """Fallout of the anchor widening: tubafrenzy holds its own BS#1996
        mojibake, so a release can carry the same credit twice -- once clean,
        once double-encoded -- and both now clear the anchor. They agree on
        the ONLY column being repaired, so they are one answer, not an
        ambiguity; deduplication keys on the corrupt column(s) for exactly
        this reason."""
        backend = [CtaRow(8844, "Wanda SÃ¡", "SÃ³ DanÃ��o Samba = Jazz 'N' Samba")]
        mysql = [
            CtaRow(8844, "Wanda Sá", "Só Danço Samba = Jazz 'N' Samba"),
            CtaRow(8844, "Wanda SÃ¡", "Só Danço Samba = Jazz 'N' Samba"),
        ]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert unresolved == []
        assert resolved[0].true_track_title == "Só Danço Samba = Jazz 'N' Samba"

    def test_siblings_differing_in_the_corrupt_column_are_still_two_answers(self) -> None:
        """The dedupe key narrows to the corrupt column; it must not narrow
        past it. A clean and a mojibake reading of the *repaired* column are
        two different values to write, so they stay ambiguous."""
        backend = [CtaRow(8844, "Wanda SÃ¡", "SÃ³ DanÃ��o Samba = Jazz 'N' Samba")]
        mysql = [
            CtaRow(8844, "Wanda Sá", "Só Danço Samba = Jazz 'N' Samba"),
            CtaRow(8844, "Wanda Sá", "SÃ³ DanÃ§o Samba = Jazz 'N' Samba"),
        ]

        resolved, unresolved = find_fffd_pairs(backend, mysql)

        assert resolved == []
        assert unresolved[0].reason == "multiple_candidates"

    def test_every_simulation_of_a_candidate_emits_that_same_candidate(self) -> None:
        """Trying several transforms cannot manufacture two different
        answers: the emitted ``true_*`` is always the MySQL row itself, never
        a simulated form, so "matched under transform A" and "matched under
        transform B" are the same resolution, not competing ones."""
        backend = [CtaRow(53042, "Wanda Sá", "DÃ��calÃ© Chinois")]
        mysql = [CtaRow(53042, "Wanda Sá", "Décalé Chinois")]

        resolved, _ = find_fffd_pairs(backend, mysql)

        assert resolved[0].true_track_title == mysql[0].track_title


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
