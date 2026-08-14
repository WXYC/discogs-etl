"""Pure U+FFFD pair-capture logic for WXYC/Backend-Service#2152.

The corruption this pairs against has one specific, narrow shape: MySQL
(tubafrenzy) returned latin1 bytes for a `compilation_track_artist` row, some
consumer decoded them as UTF-8, and every undecodable byte became one U+FFFD
REPLACEMENT CHARACTER in what Backend-Service now stores. Because the
substitution is one-byte-in, one-codepoint-out, **character length is
preserved** across the corruption (`La Bête` -> `La B<U+FFFD>te`, both 7
characters) and every position that isn't U+FFFD is untouched. That is the
whole pairing rule: for a Backend value containing U+FFFD, the matching
MySQL value is the same length and agrees with the Backend value at every
non-U+FFFD position.

This module contains only that rule and the formatting it feeds -- no
network, no database, no subprocess. ``scripts/catalog_parity_diff.py``
fetches the two sides' CTA rows (Backend over HTTP, MySQL over the `mysql`
CLI, both already-existing producers -- see that script's ``--capture-fffd-cta-pairs``
mode) and hands them to :func:`find_fffd_pairs` here. Kept separate so the
matching logic -- the part with a correctness bar high enough that a wrong
answer silently corrupts data -- is testable with plain Python values and
carries no I/O to mock.

**Never approximate.** A Backend value with a U+FFFD has, by construction,
already lost the original byte; there is no way to recover it except by
reading the true character from a source that still has it (MySQL, here).
:func:`find_fffd_pairs` requires *exactly one* MySQL candidate for a Backend
row to resolve it -- zero or more than one is reported unresolved, with the
candidates considered, never guessed. This is what protects the diacritic
traps (`Csillagrablók`, `Bête`, `µ-Ziq` -- see the codepoint fields below):
an accent-stripped or visually-similar substitute is a *wrong* answer, not
an approximately-right one, and this module has no path that can produce
one.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

FFFD = "�"


@dataclass(frozen=True)
class CtaRow:
    """One ``compilation_track_artist`` row, from either side.

    ``legacy_release_id`` is tubafrenzy's ``LIBRARY_RELEASE.ID`` -- the join
    key on both sides. On the Backend side it comes from
    ``library.legacy_release_id`` (``compilation_track_artist.library_id`` is
    a Backend-internal serial and is never this row's own field); on the
    MySQL side it is ``COMPILATION_TRACK_ARTIST.LIBRARY_RELEASE_ID`` as-is.
    """

    legacy_release_id: int
    artist_name: str
    track_title: str


@dataclass(frozen=True)
class CodepointEntry:
    """One non-ASCII character in a resolved true value, named explicitly.

    Not decoration: this is what lets a reviewer see ``µ`` U+00B5 MICRO SIGN
    was captured rather than the visually-identical ``μ`` U+03BC GREEK SMALL
    LETTER MU (or any other NFC/NFKC-foldable near-miss) without having to
    trust their editor's font rendering.
    """

    index: int
    char: str
    codepoint: str  # "U+00E9" -- 4+ hex digits, zero-padded, uppercase


@dataclass(frozen=True)
class ResolvedFffdPair:
    """A Backend CTA row whose true value(s) were found, unambiguously.

    ``true_artist_name`` / ``true_track_title`` is ``None`` on whichever
    column was NOT corrupt on this row -- matching
    ``scripts/audit/bs_replacement_char_cta.sql``'s ``pending_cta_repair``
    contract (leave the untouched column's ``true_*`` NULL), and its
    ``guard-post-fix-fffd`` companion (a row corrupt in both columns must
    carry BOTH ``true_*`` values on this one row, never split across two).
    """

    legacy_release_id: int
    track_position: str | None
    current_artist_name: str
    current_track_title: str
    true_artist_name: str | None
    true_track_title: str | None
    true_artist_name_codepoints: tuple[CodepointEntry, ...] = ()
    true_track_title_codepoints: tuple[CodepointEntry, ...] = ()


@dataclass(frozen=True)
class UnresolvedFffdPair:
    """A Backend CTA row whose true value could NOT be pinned down.

    ``reason`` is ``"zero_candidates"`` (no MySQL row at this
    ``legacy_release_id`` agrees at every non-U+FFFD position, on every
    corrupt column, while matching every non-corrupt column exactly) or
    ``"multiple_candidates"`` (more than one MySQL row does). ``candidates``
    lists every MySQL row that was considered a match for the corrupt
    column(s) -- empty for ``zero_candidates``, 2+ for
    ``"multiple_candidates"`` -- so a human can resolve it by hand instead of
    the harness guessing.
    """

    legacy_release_id: int
    track_position: str | None
    current_artist_name: str
    current_track_title: str
    reason: str
    candidates: tuple[CtaRow, ...] = ()


def has_fffd(value: str) -> bool:
    """True when ``value`` contains at least one U+FFFD REPLACEMENT CHARACTER."""
    return FFFD in value


def hex_codepoints(value: str) -> tuple[CodepointEntry, ...]:
    """Every non-ASCII character in ``value``, in order.

    ASCII-only input (no diacritics, no U+FFFD -- true values never carry
    one) returns an empty tuple. This is deliberately the full record for
    *every* non-ASCII character, not merely a summary -- the byte-exact
    assertion this feeds (µ U+00B5 vs μ U+03BC) needs the codepoint of each
    character named individually, since a single wrong character among
    several correct ones would otherwise hide in an aggregate.
    """
    return tuple(
        CodepointEntry(index=i, char=ch, codepoint=f"U+{ord(ch):04X}")
        for i, ch in enumerate(value)
        if ord(ch) > 127
    )


def _column_agrees(backend_value: str, mysql_value: str, corrupt: bool) -> bool:
    """Does ``mysql_value`` satisfy this column's half of the pairing rule?

    A column with no U+FFFD anchors the row's identity and must match
    byte-for-byte -- this is what keeps a "row corrupt in only one column"
    match from wandering onto the wrong track within the same release. A
    corrupt column instead needs equal character length and agreement at
    every position the Backend side did not lose to U+FFFD; multiple
    placeholders in one string (`Rem�nytelen T�nc`) fall out of
    the same position-by-position check with no special-casing.
    """
    if not corrupt:
        return backend_value == mysql_value
    if len(backend_value) != len(mysql_value):
        return False
    return all(b == FFFD or b == m for b, m in zip(backend_value, mysql_value))


def find_fffd_pairs(
    backend_rows: Iterable[CtaRow],
    mysql_rows: Iterable[CtaRow],
    track_positions: Mapping[tuple[int, str, str], str] | None = None,
) -> tuple[list[ResolvedFffdPair], list[UnresolvedFffdPair]]:
    """Pair every U+FFFD-corrupted Backend CTA row against its MySQL truth.

    ``backend_rows`` with no U+FFFD in either column are silently skipped --
    this function's job is capturing corruption, not re-deriving the whole
    CTA diff the rest of the harness already computes.

    ``track_positions`` is an optional ``(legacy_release_id, artist_name,
    track_title) -> track_position`` lookup, keyed on the Backend row's OWN
    (corrupt) values -- Backend's ``compilation_track_artist.track_position``
    is not itself corrupted by this bug (it carries no non-ASCII content),
    so a caller with access to it can pass it straight through; a caller
    without it (or a row this lookup doesn't cover) gets ``track_position =
    None`` on the output, which is a legal value for
    ``pending_cta_repair.track_position`` (see that table's own header --
    the column participates in no matching predicate there).

    A single Backend row corrupt in BOTH columns produces exactly ONE
    ``ResolvedFffdPair`` (or one ``UnresolvedFffdPair``) carrying both
    columns' state -- never two separate entries for the two corrupt
    columns. That is what the consumer script's own capture procedure
    requires (step 4, and the `guard-post-fix-fffd` / `guard-ambiguous-match`
    guards it exists to satisfy): splitting one corrupt row into two pending
    rows leaves each row's untouched column still corrupt in its computed
    post-fix tuple.
    """
    mysql_by_release: dict[int, list[CtaRow]] = {}
    for row in mysql_rows:
        mysql_by_release.setdefault(row.legacy_release_id, []).append(row)

    positions = track_positions or {}
    resolved: list[ResolvedFffdPair] = []
    unresolved: list[UnresolvedFffdPair] = []

    for row in backend_rows:
        artist_corrupt = has_fffd(row.artist_name)
        title_corrupt = has_fffd(row.track_title)
        if not artist_corrupt and not title_corrupt:
            continue

        siblings = mysql_by_release.get(row.legacy_release_id, [])
        candidates = [
            sibling
            for sibling in siblings
            if _column_agrees(row.artist_name, sibling.artist_name, artist_corrupt)
            and _column_agrees(row.track_title, sibling.track_title, title_corrupt)
        ]
        position = positions.get((row.legacy_release_id, row.artist_name, row.track_title))

        if len(candidates) == 1:
            match = candidates[0]
            true_artist = match.artist_name if artist_corrupt else None
            true_title = match.track_title if title_corrupt else None
            resolved.append(
                ResolvedFffdPair(
                    legacy_release_id=row.legacy_release_id,
                    track_position=position,
                    current_artist_name=row.artist_name,
                    current_track_title=row.track_title,
                    true_artist_name=true_artist,
                    true_track_title=true_title,
                    true_artist_name_codepoints=(
                        hex_codepoints(true_artist) if true_artist is not None else ()
                    ),
                    true_track_title_codepoints=(
                        hex_codepoints(true_title) if true_title is not None else ()
                    ),
                )
            )
        else:
            unresolved.append(
                UnresolvedFffdPair(
                    legacy_release_id=row.legacy_release_id,
                    track_position=position,
                    current_artist_name=row.artist_name,
                    current_track_title=row.track_title,
                    reason="zero_candidates" if not candidates else "multiple_candidates",
                    candidates=tuple(candidates),
                )
            )

    return resolved, unresolved


def sql_string_literal(value: str | None) -> str:
    """A PostgreSQL string literal for ``value``, or the bare word ``NULL``.

    Single-quote doubling only -- no other escaping, no Unicode
    normalization. ``pending_cta_repair``'s own header ("Unicode
    normalization: deliberately NOT applied") is the same posture: these
    values are copied byte-for-byte out of tubafrenzy, and normalizing here
    would risk the same NFC/NFD twin-detection miss that guard exists to
    reject at capture time.
    """
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def render_pending_cta_repair_values(rows: Sequence[ResolvedFffdPair]) -> str:
    """Render resolved rows as ``pending_cta_repair`` VALUES tuples.

    Column order matches ``scripts/audit/bs_replacement_char_cta.sql``'s
    ``pending_cta_repair`` table and its `insert-pending-rows` block exactly:
    ``(legacy_release_id, track_position, current_artist_name,
    current_track_title, true_artist_name, true_track_title)``. Rows are
    joined with ``",\\n"`` -- comma-then-newline between rows, no trailing
    comma after the last one -- so the caller can paste the result directly
    between that block's ``VALUES`` line and its closing ``;``, replacing the
    placeholder row. Empty input renders as ``""``, matching neither the
    placeholder row nor a real row -- the caller (or an operator) is
    responsible for leaving the shipped all-NULL placeholder in place when
    there is nothing to paste, exactly as the consumer script's own header
    describes for a zero-row capture.
    """
    lines = [
        "  ("
        + ", ".join(
            (
                str(row.legacy_release_id),
                sql_string_literal(row.track_position),
                sql_string_literal(row.current_artist_name),
                sql_string_literal(row.current_track_title),
                sql_string_literal(row.true_artist_name),
                sql_string_literal(row.true_track_title),
            )
        )
        + ")"
        for row in rows
    ]
    return ",\n".join(lines)
