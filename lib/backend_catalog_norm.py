"""Python twin of Backend-Service's ``jobs/library-etl/job.ts`` normalizers.

Ported from ``Backend-Service/jobs/library-etl/job.ts`` at commit
``9e841b010d527d1b32c0341bfa809516aa7560ba`` (the porting commit --
``git -C Backend-Service log -1 --format=%H -- jobs/library-etl/job.ts``):

- ``normalizeArtistName`` (``job.ts:79-89``)
- ``normalizeCodeLetters`` (``job.ts:102-113``)
- ``parseFormatAndDiscs`` (``job.ts:115-146``)
- ``isDbOnlyGenre`` (``job.ts:75-77``)

Plus the artist-name fold, ported from ``Backend-Service/shared/database/src/
fold-artist-name.ts`` at commit ``79eff85ddaf7f7c10f0b5ce83bb4630abf098079``
-- the TypeScript twin of the SQL function ``wxyc_schema.fold_artist_name(text)``
defined in migration 0134.

**Why this exists**: ``scripts/catalog_parity_diff.py`` (discogs-etl#346/#370)
compares a MySQL-sourced ``library.db`` against a Backend-Service-sourced one.
Most of the field-level differences between the two are not defects -- they
are these exact functions running during Backend's ``library-etl`` import.
Replaying them here lets the harness derive what Backend *should* hold from
a MySQL row, rather than pattern-matching the difference after the fact
(which both undercounts and overcounts -- see the plan's BS#2116 discussion).

**Port the literals, not the names.** Every load-bearing string in this
module is byte-exact against the TypeScript source: the VA carve-out regex,
the unanchored ``Z-[A-Z]`` code-letters regex, the 3-char-uppercase vs
first-2-uppercase branch, and the fold's Combining Diacritical Marks range.
A reimplementation from a description of the behavior (rather than a literal
port) is exactly the kind of drift this module exists to prevent -- see
``tests/unit/test_backend_catalog_norm.py`` for the ported fixture cases and
the carve-outs a paraphrase would miss.

**Why this is NOT ``lib/format_normalization.py``.** That module's
``normalize_library_format`` maps a raw format string into a *broad category*
space (``"Vinyl"``, ``"CD"``, ``"Cassette"``, ``"7\""``, ``"Digital"``) for
dedup/prune partitioning, where track listings are typically identical within
a category. ``parse_format_and_discs`` in *this* module maps into Backend's
own storage space (``"vinyl 12\""``, not ``"Vinyl"``) because that is the
literal string Backend's ``format`` table stores and the catalog export
emits -- the parity diff needs to derive *that* value, not a coarser one. The
two modules solve different problems with overlapping-looking output; do not
consolidate them.

**The ``artist`` column's denormalized copy.** The catalog export reads
``COALESCE(library.artist_name, artists.artist_name)``
(``catalog-export.service.ts:220``) -- ``library.artist_name`` is a
denormalized copy of ``artists.artist_name`` frozen at insert time
(``job.ts:1094``; ``shared/database/src/schema.ts:604``, nullable until the
A.2 backfill). For every row this ETL itself writes, the two columns agree,
so the fold tier in ``scripts/catalog_parity_diff.py`` holds against today's
data. But a later rename of the canonical ``artists.artist_name`` (e.g. via
the dj-site catalog editor) does not propagate to the frozen
``library.artist_name`` copy, and the COALESCE means the *stale* copy wins
the export. Naming both sources here keeps the fold tier's actual scope
(and its blind spot) explicit rather than assumed.
"""

from __future__ import annotations

import re
import unicodedata
from typing import NamedTuple

VARIOUS_ARTISTS_NAME = "Various Artists"

# job.ts:81 -- literal, including the lack of a `\s*` between `-` and `rock`
# (so a "- rock" with a space does NOT satisfy this branch; see the module's
# test file for why that still produces the right overall answer).
_VARIOUS_ROCK_LETTER_RE = re.compile(r"various\s*artists\s*-rock\s*-[a-z]", re.IGNORECASE)

# job.ts:84
_VARIOUS_RE = re.compile(r"various(?:\s+artists(?:\s*-\s*[a-z]+)?)?", re.IGNORECASE)

# job.ts:106 -- deliberately UNANCHORED (no `^`/`$`): any `Z-<letter>`
# substring matches, not just a whole-string match. `Z--` (hyphen, not a
# letter, after the `Z-`) does NOT match and falls through to the 3-char
# branch below.
_Z_CODE_RE = re.compile(r"Z-[A-Z]")


class ArtistNameInfo(NamedTuple):
    """Port of job.ts's `{ name, isVarious }` return shape."""

    name: str
    is_various: bool


def normalize_artist_name(name: str) -> ArtistNameInfo:
    """Port of `normalizeArtistName` (job.ts:79-89).

    Trims the input, then classifies it as Various Artists (VA) or a regular
    artist name. The VA carve-out for "various artists -rock -<letter>"
    style codes is checked FIRST and always returns `is_various=False`
    regardless of whether the general VA pattern would also match -- ported
    literally, not restated, because the exact shape of both regexes governs
    which real library rows get folded into "Various Artists".
    """
    trimmed = name.strip()
    if _VARIOUS_ROCK_LETTER_RE.fullmatch(trimmed):
        return ArtistNameInfo(trimmed, False)
    if _VARIOUS_RE.fullmatch(trimmed):
        return ArtistNameInfo(VARIOUS_ARTISTS_NAME, True)
    return ArtistNameInfo(trimmed, False)


def normalize_code_letters(code: str | None) -> str | None:
    """Port of `normalizeCodeLetters` (job.ts:102-113).

    None/empty/whitespace-only input returns None. A trimmed value matching
    the unanchored `Z-[A-Z]` pattern returns the Various Artists code letters
    ("V/A") -- this is a DIFFERENT population than the `isVarious` branch's
    own "V/A" writes (see the module docstring and the plan's R11 `Z--`
    measurement). A 3-character value uppercases as-is; anything else takes
    the first two characters, uppercased.
    """
    if not code:
        return None
    trimmed = code.strip()
    if len(trimmed) == 0:
        return None
    if _Z_CODE_RE.search(trimmed):
        return "V/A"
    if len(trimmed) == 3:
        return trimmed.upper()
    return trimmed[:2].upper()


class FormatInfo(NamedTuple):
    """Port of job.ts's `{ formatName, discQuantity }` return shape."""

    format_name: str
    disc_quantity: int


# job.ts:118
_CD_RE = re.compile(r"cd(?:\s*x\s*(\d+))?(?:\s*box)?")
# job.ts:124
_CDR_RE = re.compile(r"cdr")
# job.ts:133
_X_RE = re.compile(r"\bx\s*(\d+)\b")


def parse_format_and_discs(format_text: str) -> FormatInfo | None:
    """Port of `parseFormatAndDiscs` (job.ts:115-146).

    Lowercases and trims the input, then matches CD (with optional `x N`
    disc count and/or trailing "box"), CD-R, or a vinyl variant (7", 10",
    12"/LP, else bare "vinyl") with an optional `x N` disc count anywhere in
    the string. Returns None for anything else (cassette, digital, empty) --
    Backend's ETL skips these rows entirely (job.ts:976), which is a
    row-skip rule for Part 2 of the parity plan, not this module's concern.
    """
    normalized = format_text.lower().strip()

    cd_match = _CD_RE.fullmatch(normalized)
    if cd_match:
        disc_quantity = int(cd_match.group(1)) if cd_match.group(1) else 1
        return FormatInfo("cd", disc_quantity)

    if _CDR_RE.fullmatch(normalized):
        return FormatInfo("cdr", 1)

    if not normalized.startswith("vinyl"):
        return None

    x_match = _X_RE.search(normalized)
    disc_quantity = int(x_match.group(1)) if x_match else 1

    format_name = "vinyl"
    if '7"' in normalized:
        format_name = 'vinyl 7"'
    elif '10"' in normalized:
        format_name = 'vinyl 10"'
    elif '12"' in normalized or "lp" in normalized:
        format_name = 'vinyl 12"'

    return FormatInfo(format_name, disc_quantity)


def is_db_only_genre(genre_ref: str | None) -> bool:
    """Port of `isDbOnlyGenre` (job.ts:75-77).

    NOT a bare `== 'db_only'`: the comparison is case-insensitive and
    trims surrounding whitespace first.
    """
    return genre_ref is not None and genre_ref.strip().lower() == "db_only"


# Combining Diacritical Marks block: U+0300-U+036F. Ported verbatim from
# fold-artist-name.ts; covers the accents in every WXYC-canonical diacritic
# name (diaeresis U+0308, acute U+0301, grave, tilde U+0303, cedilla U+0327,
# caron, etc.). Written with explicit \uXXXX escapes, not the literal glyphs,
# so the source stays unambiguous in editors/diffs that render combining
# characters invisibly -- same rationale as `lib/library_db.py`'s
# `FTS_TOKENIZER` ZWJ escape.
_COMBINING_DIACRITICAL_MARKS = re.compile("[\u0300-\u036f]")


def fold_artist_name(value: str | None) -> str:
    """Port of `foldArtistName` (`shared/database/src/fold-artist-name.ts`).

    NFD-normalizes, strips the Combining Diacritical Marks block, then
    lowercases. Total over None/empty input (mirrors the SQL function's
    `coalesce(input, '')`).

    Deliberately NARROWER than `normalize_artist_name`: it does NOT strip a
    leading "The " -- porting that here would newly merge "The Notwist" with
    "Notwist". See `tests/unit/test_backend_catalog_norm.py`'s
    `test_does_not_strip_a_leading_the` for the pinned case.
    """
    coalesced = value or ""
    decomposed = unicodedata.normalize("NFD", coalesced)
    stripped = _COMBINING_DIACRITICAL_MARKS.sub("", decomposed)
    return stripped.lower()
