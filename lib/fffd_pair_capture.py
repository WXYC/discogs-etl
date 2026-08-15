"""Pure U+FFFD pair-capture logic for WXYC/Backend-Service#2152.

The corruption this pairs against comes in two layered shapes, and every one
of the 14 live rows found by the first production dispatch (discogs-etl#386)
turned out to be the *second* one -- the module originally only handled the
first:

1. **Substitution-only.** MySQL (tubafrenzy) returned latin1 bytes for a
   `compilation_track_artist` row, some consumer decoded them as UTF-8, and
   every undecodable byte became one U+FFFD REPLACEMENT CHARACTER. Because
   the substitution is one-byte-in, one-codepoint-out, **character length is
   preserved** (`La Bête` -> `La B<U+FFFD>te`, both 7 characters) and every
   non-U+FFFD position is untouched. A same-length, position-agreeing MySQL
   row is the match.
2. **Byte-run substitution, with or without double-encoding
   (discogs-etl#386).** Every one of the 14 live rows lost a *multi-byte*
   span, not a single byte: the run is one U+FFFD per byte the decoder
   could not use, so a single destroyed character surfaces as a run of 2 or
   3 adjacent U+FFFD and the character count stops being preserved. On most
   of them the value had *also* been run through the
   WXYC/Backend-Service#1996 double-encoding class first -- MySQL's correct
   UTF-8 bytes misread once as MySQL ``latin1`` (`Décalé` -> `DÃ©calÃ©`) --
   which inflates the character count again before U+FFFD even enters the
   picture. Undoing any of this is impossible: splitting on U+FFFD and
   un-mojibaking each fragment fails on the row this module is named after,
   whose fragment ends in a lone UTF-8 lead byte with no continuation, so
   decoding it raises `UnicodeDecodeError` by construction.

   The fix instead **forward-simulates**: apply the corruption to each MySQL
   candidate and check whether the result reproduces the observed Backend
   string exactly, with each U+FFFD run standing for exactly that many UTF-8
   *bytes* of the simulated value. The byte width is the load-bearing part
   -- an unbounded gap on a short field is close to matching anything with
   the right prefix and suffix, and two of the live corrupt columns carry
   their run at index 0, where "prefix" is nothing at all. See
   ``_forward_simulations`` and ``_fffd_gap_match`` below, and
   ``docs/architecture.md``'s worked example.

Both shapes can appear on the same row, and even on the same *column* --
their acceptance is a plain OR: a candidate that agrees under the
substitution-only rule or the forward-simulation rule is accepted either
way.

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
one. The forward-simulation rule keeps this bar: a U+FFFD gap absorbs
*unknown* content, but only as many bytes as the run is wide, and every
character outside a gap must still agree exactly -- so two candidates
differing only inside a gap are correctly reported ambiguous rather than one
being picked (see the "differing only inside the gap" test).

What the rule **cannot** rule out, and what the consumer must keep in mind:
the destroyed bytes themselves. Every `D<U+00C0..U+00FF>calé Chinois`
simulates into row 53042's observed value, because they differ only in the
byte the gap stands for. When two such values are both in the candidate set
that is ``multiple_candidates`` and nothing is emitted; when only a wrong one
is present and the right one is absent, no signal in the Backend string can
tell them apart, and the row resolves to that wrong value. That residue is
irreducible for this corruption -- it is the information the corruption
destroyed -- so a resolved row is a *proof of consistency with the observed
damage*, not a proof of identity.

``current_artist_name`` / ``current_track_title`` on every emitted pair are
the raw Backend value, untouched -- never the decoded or forward-simulated
form, even when resolving the row required computing one internally.
Backend-Service's repair SQL joins on those columns matching the *live*
corrupt row (``bs_replacement_char_cta.sql``); a cleaned value here would
silently zero out that join.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

FFFD = "�"
_FFFD_RUN = re.compile(f"{FFFD}+")

# The five bytes Python's strict ``cp1252`` leaves unassigned. MySQL's
# ``latin1`` -- the codec that actually produced this mojibake -- maps them to
# the matching C1 control codepoints instead of raising, exactly as the
# WHATWG windows-1252 index does. The live artifact proves it: row 49848
# carries U+0081 (from `ف` = D9 81) and row 59194 carries U+008F (from `ハ` =
# E3 83 8F), sitting alongside `‚` / `ƒ` / `„` / `Œ` that only the cp1252 half
# of the table produces. Neither of Python's stock codecs can spell that: the
# strict one raises, and ISO-8859-1 renders the whole 0x80-0x9F range as C1
# controls.
_CP1252_UNASSIGNED = frozenset({0x81, 0x8D, 0x8F, 0x90, 0x9D})
_MYSQL_LATIN1_TABLE = "".join(
    chr(b) if b in _CP1252_UNASSIGNED else bytes((b,)).decode("cp1252") for b in range(256)
)


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

    ``reason`` is one of three, and they need different remedies:

    - ``"zero_candidates"`` -- no MySQL row at this ``legacy_release_id``
      satisfies the pairing rule (neither the substitution-only shape nor the
      forward-simulated one) on every corrupt column while also anchoring on
      every non-corrupt one. The row may have changed in tubafrenzy since the
      corruption, or its true value may simply not be in this snapshot.
    - ``"multiple_candidates"`` -- more than one does, and they disagree on a
      column being repaired. Resolvable by hand from ``track_position`` and
      ``candidates``; never by the harness picking one.
    - ``"corrupt_candidates"`` -- the only rows that fit carry U+FFFD
      themselves, so tubafrenzy lost the bytes too. **Unrecoverable**; a
      re-import cannot supply what neither side still holds.

    ``candidates`` lists every MySQL row that was considered a match for the
    corrupt column(s) -- empty for ``zero_candidates``, 2+ for
    ``multiple_candidates``, 1+ (all corrupt) for ``corrupt_candidates`` --
    so a human can resolve it by hand instead of the harness guessing.
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


def _mysql_latin1(utf8: bytes) -> str:
    """Decode ``utf8`` the way MySQL's ``latin1`` (== windows-1252) does.

    Identical to Python's ``cp1252`` everywhere ``cp1252`` is defined, and
    ``chr(byte)`` on the five bytes it is not -- see ``_CP1252_UNASSIGNED``.
    """
    return "".join(_MYSQL_LATIN1_TABLE[b] for b in utf8)


def _forward_simulations(value: str) -> tuple[str, ...]:
    """Every plausible pre-U+FFFD form of ``value``, deduplicated, in a fixed
    order: the value itself, then its MySQL-``latin1`` double-encoding, then
    its ISO-8859-1 one.

    The identity comes first because not every corrupt row is double-encoded:
    row 11704 (`Huldresl<FFFD><FFFD>tten = The Wood Nymph Tune`) is clean
    UTF-8 whose `å` simply lost both its bytes, and its surviving text is
    pure ASCII, so nothing in it is mojibake at all. The two misread-codec
    forms model the WXYC/Backend-Service#1996 class applied forward:
    ``value``'s correct UTF-8 bytes read once through a single-byte codec
    upstream of Backend (`Décalé` -> `DÃ©calÃ©`).

    ISO-8859-1 is kept as the second misread codec because BS#1996's
    ``repair()`` recipe names it, but nothing in the live artifact needs it;
    MySQL-``latin1`` explains every reconstructible row. Order is not a
    tie-break: whichever simulation matches, the value emitted as ``true_*``
    is ``value`` itself, so two simulations matching the same candidate are
    one resolution, not two competing ones.
    """
    utf8 = value.encode("utf-8")
    simulations: list[str] = [value]
    for candidate in (_mysql_latin1(utf8), utf8.decode("latin1")):
        if candidate not in simulations:
            simulations.append(candidate)
    return tuple(simulations)


def _fffd_gap_match(observed: str, simulated: str) -> bool:
    """Does ``simulated`` reproduce ``observed`` once every maximal run of
    adjacent U+FFFD in ``observed`` is treated as a gap standing for exactly
    that many UTF-8 BYTES of ``simulated``?

    This is the forward-simulation half of discogs-etl#386, and the byte
    bound is the part that keeps it from being a wildcard. A run's *character*
    count is not recoverable -- the double-encoding layer inflates bytes per
    source character, so one destroyed character can surface as 2 or 3
    adjacent U+FFFD -- but its *byte* count is, because the run is one
    U+FFFD per byte the decoder could not use. Every reconstructible live row
    agrees: `©` (2 bytes) -> 2 wide on row 53042, `†` (3 bytes) -> 3 wide on
    row 11615, `å` (2 bytes) -> 2 wide on row 11704.

    So a gap consumes the unique prefix of the remaining ``simulated`` whose
    UTF-8 length is exactly the run width -- unique because prefix byte
    length is strictly increasing in character count, which also means this
    match needs no backtracking. A run that would end mid-character fails.
    Without the bound, a run at index 0 or at the end degenerates into a
    suffix- or prefix-only filter, and a single wrong candidate clearing that
    filter resolves the row with no ``multiple_candidates`` to catch it.

    Every position outside a gap must still match ``simulated`` exactly,
    character for character; that is what keeps this a proof of consistency
    rather than a similarity score.
    """
    position = 0
    index = 0
    length = len(observed)
    while index < length:
        run = _FFFD_RUN.match(observed, index)
        if run is not None:
            width = len(run.group())
            consumed = 0
            while position < len(simulated) and consumed < width:
                consumed += len(simulated[position].encode("utf-8"))
                position += 1
            if consumed != width:
                return False
            index = run.end()
            continue
        next_fffd = observed.find(FFFD, index)
        end = length if next_fffd == -1 else next_fffd
        literal = observed[index:end]
        if not simulated.startswith(literal, position):
            return False
        position += len(literal)
        index = end
    return position == len(simulated)


def _double_encoded_fffd_match(backend_value: str, mysql_value: str) -> bool:
    """Does some plausible forward simulation of ``mysql_value`` reproduce
    ``backend_value``'s corruption exactly, modulo byte-bounded U+FFFD gaps?

    The forward-simulate half of the pairing rule (discogs-etl#386
    Correction 3): rather than trying to strip ``backend_value``'s
    corruption -- proven unrecoverable, see the module docstring -- apply
    the same corruption forward to ``mysql_value`` and require an exact
    reproduction.
    """
    return any(
        _fffd_gap_match(backend_value, simulation)
        for simulation in _forward_simulations(mysql_value)
    )


def _column_agrees(
    backend_value: str,
    mysql_value: str,
    corrupt: bool,
    *,
    allow_corrupt_source: bool = False,
) -> bool:
    """Does ``mysql_value`` satisfy this column's half of the pairing rule?

    A column with no U+FFFD anchors the row's identity, and must equal one
    of ``mysql_value``'s forward simulations -- the identity (byte-for-byte,
    the original rule) or one of the misread-codec forms, because an anchor
    column can itself be clean double-encoded mojibake with no data loss and
    hence no U+FFFD (discogs-etl#386 Correction 1; row 8844's anchor is
    `Wanda SÃ¡` against MySQL's clean `Wanda Sá`). The widening is exact, not
    fuzzy: there is no gap to absorb anything, so the anchor still has to
    reproduce ``backend_value`` character for character, and a different
    track on the same release still fails it. This is what keeps a "row
    corrupt in only one column" match from wandering within the release.

    A corrupt column accepts either of two rules, tried in order:

    1. **Substitution-only** (the shape the module was originally written
       for): equal character length and agreement at every position the
       Backend side did not lose to U+FFFD. Multiple placeholders in one
       string (`Rem�nytelen T�nc`) fall out of the same
       position-by-position check with no special-casing.
    2. **Forward-simulated** (discogs-etl#386): some simulation of
       ``mysql_value`` reproduces ``backend_value`` exactly once each U+FFFD
       run is read as exactly that many UTF-8 bytes -- see
       ``_double_encoded_fffd_match``. Tried only when rule 1 does not
       already resolve it; a row that satisfies rule 1 stays on the
       unchanged, length-preserving path (BS#2114's precedent), and rule 2
       would in fact reject that shape, since a 1-wide run cannot stand for
       a 2-byte `ê`.

    **A U+FFFD on the MySQL side disqualifies the row** unless
    ``allow_corrupt_source``. Position-wise agreement and gap-matching both
    treat the Backend placeholder as absorbing unknown content, so a MySQL
    value corrupt at the same place would agree and be emitted as "true" --
    writing the corruption into the repair while reporting success. MySQL is
    the source of truth only while it is itself clean; the relaxed form
    exists solely so the caller can *report* that distinction (see
    ``corrupt_candidates`` in :func:`find_fffd_pairs`), never to resolve on.
    """
    if not corrupt:
        return backend_value in _forward_simulations(mysql_value)
    if not allow_corrupt_source and has_fffd(mysql_value):
        return False
    if len(backend_value) == len(mysql_value) and all(
        b == FFFD or b == m for b, m in zip(backend_value, mysql_value)
    ):
        return True
    return _double_encoded_fffd_match(backend_value, mysql_value)


def _row_agrees(
    backend_row: CtaRow,
    sibling: CtaRow,
    artist_corrupt: bool,
    title_corrupt: bool,
    *,
    allow_corrupt_source: bool,
) -> bool:
    """Does ``sibling`` satisfy the pairing rule on BOTH columns?"""
    return _column_agrees(
        backend_row.artist_name,
        sibling.artist_name,
        artist_corrupt,
        allow_corrupt_source=allow_corrupt_source,
    ) and _column_agrees(
        backend_row.track_title,
        sibling.track_title,
        title_corrupt,
        allow_corrupt_source=allow_corrupt_source,
    )


def _dedupe_candidates(
    candidates: Sequence[CtaRow], artist_corrupt: bool, title_corrupt: bool
) -> list[CtaRow]:
    """Collapse candidates that carry the same answer, preserving order.

    Duplicate ``compilation_track_artist`` rows are ordinary in this catalog
    (see the double-entry rate on discogs-etl#346), and two MySQL rows that
    agree on the columns being repaired carry exactly ONE answer -- counting
    them as ambiguous would strand a row whose true value is not in doubt.

    The key is the **corrupt** column(s) only, not both raw columns, because
    the anchor column is never emitted: ``true_artist_name`` /
    ``true_track_title`` is ``None`` on whichever column was not corrupt. Two
    siblings differing only in the anchor is a real shape once that anchor
    accepts a double-encoding -- tubafrenzy carries its own WXYC/Backend-
    Service#1996 mojibake, so the same credit can appear both clean and
    double-encoded on one release -- and both spellings answer the repair
    identically.

    The comparison stays byte-exact on the columns it does key on, so it
    cannot collapse a genuine two-answer case: candidates differing in a
    corrupt column still count separately and still report
    ``multiple_candidates``.
    """
    seen: set[tuple[str, ...]] = set()
    unique: list[CtaRow] = []
    for row in candidates:
        key = tuple(
            value
            for value, corrupt in (
                (row.artist_name, artist_corrupt),
                (row.track_title, title_corrupt),
            )
            if corrupt
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


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
        candidates = _dedupe_candidates(
            [
                s
                for s in siblings
                if _row_agrees(row, s, artist_corrupt, title_corrupt, allow_corrupt_source=False)
            ],
            artist_corrupt,
            title_corrupt,
        )
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
        elif candidates:
            unresolved.append(
                UnresolvedFffdPair(
                    legacy_release_id=row.legacy_release_id,
                    track_position=position,
                    current_artist_name=row.artist_name,
                    current_track_title=row.track_title,
                    reason="multiple_candidates",
                    candidates=tuple(candidates),
                )
            )
        else:
            corrupt_candidates = _dedupe_candidates(
                [
                    s
                    for s in siblings
                    if _row_agrees(row, s, artist_corrupt, title_corrupt, allow_corrupt_source=True)
                ],
                artist_corrupt,
                title_corrupt,
            )
            unresolved.append(
                UnresolvedFffdPair(
                    legacy_release_id=row.legacy_release_id,
                    track_position=position,
                    current_artist_name=row.artist_name,
                    current_track_title=row.track_title,
                    reason="corrupt_candidates" if corrupt_candidates else "zero_candidates",
                    candidates=tuple(corrupt_candidates),
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
