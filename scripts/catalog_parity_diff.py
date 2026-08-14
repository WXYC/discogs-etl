"""Diff-two-files core of the discogs-etl#346 catalog-parity harness.

discogs-etl builds the production ``library.db`` nightly from tubafrenzy's
MySQL database (``scripts/sync-library.sh``). To retire that MySQL
dependency ahead of the 2026-08-31 tubafrenzy turndown, the daily build must
move to Backend-Service as its catalog source -- but only once a
Backend-sourced ``library.db`` is proven equivalent to the MySQL-sourced one.

This script is the comparison core of that proof: given two already-built
``library.db`` SQLite files, it diffs them field-by-field and reports where
they diverge, so an operator can drive the migration to zero drift (or an
explicitly accepted residue).

It also carries the **producer** half (WXYC/discogs-etl#351): given a live
source instead of a prebuilt file, ``--mysql-source`` / ``--backend-source``
build the corresponding ``library.db`` first, so a single invocation can
build both sides and diff them.

- ``--mysql-source mysql://user@host:port/dbname`` reproduces the daily
  build's own read path -- the ``mysql`` CLI in batch/raw mode, running the
  exact SELECTs from ``scripts/sync-library.sh`` (a source-grep test pins
  them together), parsed by the same TSV parser production uses. The CLI, not
  a Python driver, because tubafrenzy's MySQL 4.1 auth breaks those drivers.
  The password comes from ``$LIBRARY_DB_PASSWORD``: put in the DSN it would
  sit in this process's own argv, visible to ``ps`` for the whole run.
- ``--backend-source https://api.wxyc.org`` is the migration target
  (decision D3 / Option B, 2026-08-03): the gzipped-NDJSON exports ``GET
  /library/catalog`` + ``GET /library/catalog/compilation-tracks``
  (WXYC/Backend-Service#1965) read over HTTPS with a service-account bearer
  token -- no prod-DB credentials, which is the whole point of Option B over
  a direct-Postgres producer. Plain http to anything but a loopback address
  is refused, and so is any cross-origin redirect, which urllib would
  otherwise follow *carrying that token*.

  The token is minted per run from ``$BACKEND_CATALOG_EMAIL`` +
  ``$BACKEND_CATALOG_PASSWORD`` (WXYC/discogs-etl#365), because the JWT
  Backend-Service accepts lives 15 minutes and so cannot be a stored CI
  secret for a soak that runs 7+ consecutive days -- what CI stores is the
  service account's password. ``$BACKEND_CATALOG_TOKEN`` still short-circuits
  all of that for a one-off run with a token already in hand. See
  ``_TokenSource`` for why a refresh re-exchanges rather than re-signs-in.

Both producers write **only** to the path named by the matching ``--*-db``
flag, and refuse to write to a path that already exists: this harness must
never be able to clobber a real ``library.db``. Each builds into a scratch
file beside its target and renames it into place at the end, so a build that
dies partway leaves nothing behind to refuse on the next parity day.

**Still out of scope**: the operational cutover -- running the 7 clean parity
days, flipping ``sync-library.sh``'s source, and taking ``/wxycdb`` dark --
is WXYC/discogs-etl#346, deliberately human-gated.

Schema note: the ``library`` table's 12 columns (``id, title, artist,
call_letters, artist_call_number, release_call_number, genre, format,
alternate_artist_name, album_artist, label, cross_reference_names``) are
**imported** from ``lib/library_db.py`` -- the authoritative daily-sync
shape, shared with ``scripts/tsv_to_sqlite.py`` so both producers build the
same database. Imported rather than restated, so a column added there widens
this diff automatically instead of becoming an undiffed blind spot.
``label`` is always NULL in production (nothing ever inserts it -- see that
module's docstring), so it is excluded from the diffed column set below
rather than compared as a trivially-always-equal no-op.

**Field-level modeling (discogs-etl#370).** A field-by-field byte compare
overcounts: most of what looks like drift between the two sides is actually
Backend's own ``library-etl`` ETL running deterministic transforms on the way
in (VA-folding an artist name, uppercasing code letters, coercing a NULL
call number to ``0``, ...). ``COLUMN_MODELS`` (keyed by column name, one
entry per ``DIFF_COLUMNS`` member -- see the drift-guard test asserting set
equality between the two) replays those transforms, ported to Python in
``lib/backend_catalog_norm.py``, to derive what Backend *should* hold from
the raw mysql-sourced value. Each matched row's field then classifies as
"agree" (byte-identical), "normalized" (Backend equals the *derived*
expectation, not the raw mysql value -- a deliberate, counted-not-drift
transform), or "mismatch" (neither -- a genuine defect). **This redefines
``field_mismatches``**: it now counts only the "mismatch" tier, not every
byte-level difference -- a column that differs solely by a deliberate
normalization (case folding, VA collapsing, a coerced NULL) no longer counts
toward it. The normalized-tier counts themselves are tallied by
``_classify_matched_rows`` (column -> class -> count) and are reported on
``ParityDiff.normalizations`` (and the ``--json`` contract) -- reported,
never gating. ``_print_human`` does not carry them, so an INFO log line
remains their only surface for the default (non-``--json``) invocation.

**Fold-collapse (discogs-etl#346, plan step 9).** One class of divergence is
NOT a property of the row pair at all. tubafrenzy identifies an artist by
``LIBRARY_CODE`` row and 295 presentation names have more than one; Backend
identifies by ``artists`` row via ``fold_artist_name``, so those collapse
into one row carrying a single ``code_letters`` and a single
``artist_genre_code`` per genre. Releases under the losing code then diverge
against a value that is legitimate but belongs to a sibling.
``FOLD_COLLAPSE_COLUMNS`` (``call_letters``, ``artist_call_number``) resolve
against the folded artist's whole mysql row group in
``_classify_matched_rows`` -- which already holds both full maps, so
``COLUMN_MODELS`` stays per-pair and pure -- and report ``fold_collapsed``.
``cross_reference_names`` collapses identically but as a set union, so it
needs no group context and expresses the same widening per row as
``cardinality_gain``. The resolution is deliberately narrow: Backend's value
must be one the mysql side itself supplies, so a defect on a duplicated
artist is still gated.

**Row-level expected/unexplained classification and the ``clean`` verdict
(discogs-etl#370, ``ResidueLedger``).** ``missing_in_backend`` /
``extra_in_backend`` count every id-level divergence, including known,
documented residue (a duplicate tubafrenzy row Backend's ETL correctly
collapsed, a BS#1963-minted id with no tubafrenzy counterpart, a row
Backend's ETL deliberately skipped). ``--residue-ledger`` (defaulting to the
vendored ``vendor/parity-residue/ledger.json``) splits each into *expected*
(explained by a rule or the vendored enumeration) and *unexplained* (a
genuine divergence). ``clean`` is ``True`` only when every unexplained count
is zero, ``field_mismatches`` is all-zero, and CTA drift is within its
documented baseline; it is ``False`` on any of those failing, and ``None``
-- absent, not failed -- when no ledger was supplied at all (``--residue-ledger
none``).

Usage::

    python scripts/catalog_parity_diff.py \\
        --mysql-db /path/to/mysql-sourced/library.db \\
        --backend-db /path/to/backend-sourced/library.db \\
        --json

Exit codes:

- ``0`` -- ran successfully. A nonzero diff count is still exit 0 (unless
  ``--fail-on-drift`` is given and the verdict is not clean -- see ``4``
  below); the operator reads the counts (and, in ``--json`` mode, the id
  lists and ``clean``) to judge parity.
- ``2`` -- bad arguments (missing required flags, or ``--fail-on-drift``
  combined with ``--residue-ledger none``).
- ``3`` -- source/read error: missing file, unreadable database, a required
  table (``library``) absent from one of the inputs, a malformed input
  (duplicate ``library.id``, which a valid library.db's primary key forbids),
  a missing/unreadable/malformed ``--residue-ledger``, or **any** producer
  failure (unreachable source, missing credentials, a refused overwrite, an
  inconsistent snapshot, an empty catalog export, a contract-violating row, a
  missing ``mysql`` binary, a malformed DSN).
- ``4`` -- ``--fail-on-drift`` was given and the verdict is not clean.
- ``1`` -- reserved for an uncaught crash (the blanket ``except Exception``
  around both the producer and diff phases exists precisely so this doesn't
  happen for a known failure mode -- see those two ``try`` blocks in
  ``main``). Deliberately never returned for a diff *result*: reusing it for
  "not clean" would make an interpreter crash and a drift verdict
  indistinguishable to a CI runner, which is why drift gets its own code (4)
  instead.

Never writes to either input: both files are opened as read-only SQLite
connections (``mode=ro``).
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import zlib
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    Request,
    build_opener,
    pathname2url,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.backend_catalog_norm import (  # noqa: E402
    fold_artist_name,
    is_db_only_genre,
    normalize_artist_name,
    normalize_code_letters,
    parse_format_and_discs,
)
from lib.library_db import (  # noqa: E402
    CROSS_REFERENCE_SEPARATOR,
    LIBRARY_COLUMNS,
    build_library_db,
    parse_compilation_track_tsv,
    parse_library_tsv,
)
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)


class SourceError(RuntimeError):
    """A library.db side is unusable.

    Either an input file (missing, unreadable, missing a required table, or
    malformed -- e.g. a duplicate id) or a producer that could not build one
    (bad DSN/URL, missing credentials, a refused overwrite, an unreachable
    source, or an inconsistent snapshot).
    """


# Columns actually diffed field-by-field: every library column except `id`
# (the join key, not a diffable field) and `label` (always NULL in prod --
# `lib/library_db.py`'s insert omits it entirely -- so it is trivially
# NULL==NULL on both sides and carries no signal; excluded rather than
# reported as a permanently-zero mismatch bucket).
#
# Derived from the imported `LIBRARY_COLUMNS` rather than a local copy: a
# column added to `lib/library_db.py` must widen the diff automatically, or
# it becomes a permanently-undiffed blind spot in the tool that certifies
# the cutover.
#
# Since #370 that widening also *demands* a `COLUMN_MODELS` entry: an
# unmodelled column raises `KeyError` out of `classify_field` rather than
# quietly falling back to a byte compare. That is deliberate -- a silent
# fallback is the blind spot in a different costume, and it would be a
# comparison nobody chose, inside the tool certifying the cutover. The loud
# failure is caught in CI by `TestColumnModelsDriftGuard`, which is where a
# newly-added column is discovered, not on an operator's console.
DIFF_COLUMNS = tuple(c for c in LIBRARY_COLUMNS if c not in ("id", "label"))

# compilation_track_artist has no primary key of its own; it is compared as
# a (library_release_id, artist_name, track_title) multiset.
CTA_COLUMNS = ("library_release_id", "artist_name", "track_title")


@dataclass(frozen=True)
class ParityDiff:
    """Outcome of diffing two library.db files.

    Field order matches the CLI's ``--json`` contract (``dataclasses.asdict``
    preserves declaration order). ``matched`` through ``cta_extra`` are
    unchanged from before discogs-etl#370; the six new fields
    (``clean`` through ``normalizations``) are appended after ``cta_extra``
    rather than leading with ``clean``, so the existing key *set* keeps its
    order. ``normalizations`` needs a ``default_factory`` (it can be empty),
    which is why it comes after the five new plain-typed fields rather than
    among them -- Python forbids a non-default field after a defaulted one,
    and the two id lists (also ``default_factory``) already sit last.

    ``clean``, ``missing_expected``, ``missing_unexplained``,
    ``extra_expected``, and ``extra_unexplained`` require a
    ``ResidueLedger`` (see ``diff_library_dbs`` / ``run_diff``). Without one,
    the *_expected counts are 0, the *_unexplained counts equal the raw
    missing/extra counts, and ``clean`` is ``None`` -- absent, not failed.
    ``normalizations`` is populated unconditionally either way (field tiering
    has no dependency on a ledger); it is reported, never gating.
    """

    matched: int
    missing_in_backend: int
    extra_in_backend: int
    field_mismatches: dict[str, int]
    cta_missing: int
    cta_extra: int
    clean: bool | None
    missing_unexplained: int
    missing_expected: int
    extra_unexplained: int
    extra_expected: int
    normalizations: dict[str, dict[str, int]] = field(default_factory=dict)
    missing_in_backend_ids: list[int] = field(default_factory=list)
    extra_in_backend_ids: list[int] = field(default_factory=list)


@dataclass(frozen=True)
class ResidueLedger:
    """The vendored discogs-etl#370 expected-residue ledger
    (``vendor/parity-residue/ledger.json``, pinned by
    ``parity-residue-pin.txt``).

    ``collapsed_ids`` is the ONE genuinely frozen enumeration: the 599
    ``findExistingRelease`` duplicate-collapse mysql ids (skip path 7 in the
    plan's Rule B table), which cannot be re-derived from a library.db row.
    Everything else Rule A/B classify (a minted backend id, a db_only genre,
    an unparseable format, an empty artist name or album title) is computed
    directly from row data and needs no ledger entry at all -- see
    ``_rule_b_missing_reason`` and ``_is_minted_id``.

    ``normalizations_baseline`` / ``cta_missing_baseline`` /
    ``cta_extra_baseline`` are step 6's concern (the plan's CTA-baseline
    commit) and are ``{}`` / ``None`` / ``None`` until that step populates
    them -- see ``vendor/parity-residue/ledger.json``'s ``baselines`` block
    and ``scripts/vendor_parity_residue.py``.
    """

    collapsed_ids: frozenset[int]
    normalizations_baseline: dict[str, dict[str, int]]
    cta_missing_baseline: int | None
    cta_extra_baseline: int | None
    measured_date: str | None


def load_residue_ledger(path: str | Path) -> ResidueLedger:
    """Load and validate a ``ResidueLedger`` from a vendored ``ledger.json``.

    Raises ``SourceError`` for a missing file, unreadable/malformed JSON, a
    payload missing the required ``collapsed_mysql_ids`` key, or a
    structurally wrong ``baselines`` block -- the same contract as an
    unreadable ``library.db`` (exit 3 at the CLI).

    **Every shape check happens here rather than at the point of use**, and
    that placement is the contract. Two failure modes motivate it: a truthy
    non-dict ``baselines`` (``"oops"``) used to reach ``.get`` and raise
    ``AttributeError``, which escaped ``main``'s ``except SourceError`` and
    surfaced as **exit 1** -- the code deliberately reserved for an uncaught
    crash, so a CI runner could no longer tell a corrupt ledger from an
    interpreter fault. And a non-integer CTA baseline used to survive the
    load and fail later at ``cta_missing <= "0"`` inside ``diff_library_dbs``:
    the right exit code by luck, from the wrong place, with a message
    pointing at the diff instead of at the file the operator has to fix.
    """
    p = Path(path)
    if not p.is_file():
        raise SourceError(f"residue ledger not found: {p}")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise SourceError(f"residue ledger unreadable or malformed: {p} ({exc})") from exc
    if not isinstance(data, dict):
        raise SourceError(f"residue ledger malformed: {p} (top level is not an object)")
    try:
        collapsed_raw = data["collapsed_mysql_ids"]
        collapsed_ids = frozenset(int(k) for k in collapsed_raw)
        measured_date = data.get("measured_date")
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceError(f"residue ledger malformed: {p} ({exc})") from exc

    baselines = data.get("baselines") or {}
    if not isinstance(baselines, dict):
        raise SourceError(f"residue ledger malformed: {p} (baselines is not an object)")
    normalizations_baseline = baselines.get("normalizations", {})
    if not isinstance(normalizations_baseline, dict):
        raise SourceError(
            f"residue ledger malformed: {p} (baselines.normalizations is not an object)"
        )
    cta_missing_baseline = baselines.get("cta_missing")
    cta_extra_baseline = baselines.get("cta_extra")
    for key, value in (("cta_missing", cta_missing_baseline), ("cta_extra", cta_extra_baseline)):
        # bool is an int subclass, and `True <= 5` would silently compare as
        # 1 -- an unpopulated-looking baseline that quietly gates.
        if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
            raise SourceError(
                f"residue ledger malformed: {p} (baselines.{key} is not an integer or null)"
            )
    return ResidueLedger(
        collapsed_ids=collapsed_ids,
        normalizations_baseline=normalizations_baseline,
        cta_missing_baseline=cta_missing_baseline,
        cta_extra_baseline=cta_extra_baseline,
        measured_date=measured_date,
    )


def _default_residue_ledger_path() -> Path:
    """The vendored ledger's path, resolved from this module's own location
    -- NEVER from the current working directory.

    A cwd-relative default would fail every invocation launched from
    anywhere but the repo root, starting with the ``mktemp -d`` block in
    ``docs/architecture.md``'s "Catalog parity producers" section that an
    operator copies verbatim to run the soak. Precedent:
    ``alembic/versions/0004_wxyc_identity_match_fns.py``'s ``_REPO_ROOT`` and
    ``tests/integration/test_wxyc_identity_match_parity.py``'s ``REPO_ROOT``.
    """
    return Path(__file__).resolve().parent.parent / "vendor" / "parity-residue" / "ledger.json"


# BS#1963 mints extra_in_backend ids from here up; those releases have no
# tubafrenzy counterpart by construction. The set grows during the soak as
# librarians add through dj-site, so it is a RULE (Rule A), never an
# enumeration -- enumerating it would go stale within a day.
_MINTED_ID_FLOOR = 1_000_000


def _is_minted_id(backend_id: int) -> bool:
    """Rule A: a backend-only id at or above the minted floor is expected
    residue by construction, with no ledger entry required."""
    return backend_id >= _MINTED_ID_FLOOR


def _rule_b_missing_reason(mysql_row: Mapping[str, object]) -> str | None:
    """Rule B, the row-derivable half only (skip paths 1/3/5/6 -- ``db_only``
    genre, an unparseable format, an empty artist name, an empty album
    title). Checked in ``job.ts``'s own skip order, though only for
    legibility: just the None/not-None result is consumed today, and the
    reason string is never reported, so no output depends on the ordering.

    Returns ``None`` for paths 2/4 (a genre/format id absent from Backend's
    OWN lookup tables) and paths 7/8 (the 599 duplicate-collapse and a
    Backend-side parse failure) -- none of those are computable from a
    mysql-sourced library.db row alone, so they correctly fall through as
    unexplained rather than being guessed at.

    **The artist and title checks are spelled ``_normalize(value) is None``,
    and the reason is the producer's wire format -- not a preference for
    reusing the comparator.** ``_normalize`` also collapses the literal
    4-character string ``"NULL"``, which looks like over-matching (an album
    genuinely titled ``NULL`` is a legal row Backend imports rather than
    skips, so forgiving its absence would file real drift as expected). It is
    nonetheless the only correct predicate here, because for *these two
    columns* the wire cannot tell the two apart:

    - ``mysql -B -N`` **on this server prints a genuine SQL NULL as the
      literal text ``"NULL"``**, not as the ``\\N`` sentinel -- verified in
      prod and documented at ``scripts/sync-library.sh``'s SELECT comment,
      which fixed exactly this for ``ALBUM_ARTIST`` (64,780 rows had leaked a
      literal ``'NULL'`` into ``library_fts``).
    - ``parse_library_tsv`` maps only ``\\N`` to ``None``. It has no handling
      for the literal string, so that mapping never fires for these columns.
    - ``TITLE`` and ``PRESENTATION_NAME`` are the two columns
      ``LIBRARY_SELECT_SQL`` leaves **unwrapped** by ``IFNULL`` (unlike
      ``ALTERNATE_ARTIST_NAME`` / ``ALBUM_ARTIST`` / the crossref subquery).

    So a SQL-NULL title arrives here as the string ``"NULL"``, and
    ``value is None or value.strip() == ""`` would miss it -- leaving the
    ledger's six empty-``TITLE`` ids (``residue-ledger.md`` Set 2)
    permanently unexplained and ``clean`` permanently unreachable, which is
    the failure this whole predicate exists to prevent.

    ``_load_cta_counts`` uses the narrower ``str.strip() == ""`` for what
    reads like the same rule, and the two are **correctly** different rather
    than an inconsistency: ``COMPILATION_TRACK_ARTIST.ARTIST_NAME`` is
    documented ``NOT NULL`` and also unwrapped, so a ``"NULL"`` there can
    only ever be a genuine artist name -- never a SQL NULL. The predicate
    tracks each column's nullability, not one house style.

    The real fix is at the SQL layer, where ``sync-library.sh`` already put
    it for the other columns: ``IFNULL``-wrap ``TITLE`` and
    ``PRESENTATION_NAME`` in both SELECTs so a SQL NULL arrives as ``''`` and
    this can narrow to ``.strip() == ""``. That changes the pinned
    production SELECTs, so it is a follow-up rather than part of #370, and
    plan step 8's prod run settles which representation the six ids actually
    take with a single query.
    """
    genre = mysql_row.get("genre")
    if is_db_only_genre(genre if isinstance(genre, str) else None):
        return "db_only_genre"
    fmt = mysql_row.get("format")
    if parse_format_and_discs(fmt if isinstance(fmt, str) else "") is None:
        return "unparseable_format"
    if _normalize(mysql_row.get("artist")) is None:
        return "empty_artist_name"
    if _normalize(mysql_row.get("title")) is None:
        return "empty_album_title"
    return None


def _normalize(value: object) -> object:
    """Normalize a single field value for comparison.

    SQL NULL, the empty string, and the literal string ``"NULL"`` (a known
    transient artifact of the MySQL export pipeline, being fixed at the
    source separately -- see WXYC/discogs-etl#346) are all treated as equal,
    and collapse to ``None``. Surrounding whitespace is stripped. No other
    transform is applied: no case folding, no accent folding, no internal
    whitespace collapsing -- that stays true of THIS function specifically.
    The per-column expectation model above (``COLUMN_MODELS`` /
    ``classify_field``) layers case folding, accent folding, and other
    tiered transforms on top of ``_normalize``'s output; see that section's
    comment block for what a given column actually tolerates end to end.
    """
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "" or stripped == "NULL":
            return None
        return stripped
    return value


# --- Per-column expectation model (discogs-etl#370, plan Part 1) ----------
#
# Each diffed column classifies into one of three tiers, computed by
# comparing the Backend-sourced value against what Backend's own ETL
# (`lib/backend_catalog_norm.py`, ported from `job.ts`) would derive from the
# raw mysql-sourced value:
#
#   - "agree"      -- backend == mysql, byte-for-byte (under `_normalize`).
#   - "normalized" -- backend == expected(mysql) != mysql. A deliberate,
#     row-derivable Backend transform -- counted, not drift.
#   - "mismatch"   -- neither of the above. Backend disagrees with its own
#     ETL's spec: a genuine defect.
#
# `artist` (and the two multiset/VA-coupled columns downstream of it,
# `call_letters` and `cross_reference_names`) get a fourth wrinkle folded
# into the "normalized" tier: `ensureArtist` can return an existing row's
# STORED spelling on a fold match, which is not reproducible by replaying
# `normalize_artist_name` alone. A backend value that is fold-equal (but not
# byte-equal) to the derived expectation is still "normalized", tagged with
# its own class so the count is visible separately.


def _tab_nl_sub(value: object) -> object:
    """Mirror Backend's ``REPLACE(REPLACE(col, '\\t', ' '), '\\n', ' ')``.

    Four of the diffed columns (``title``, ``artist``, ``alternate_artist_name``,
    ``album_artist``) are wrapped in this at extraction on Backend's side
    (``job.ts:281-296``); the harness's own ``LIBRARY_SELECT_SQL`` has no such
    wrapper, so the mysql side is where the byte survives. Applied to the raw
    mysql value *before* any further derivation, since the SQL-level replace
    runs first in production.

    **Inert until WXYC/discogs-etl#371 lands, which makes that a merge-order
    dependency and not just a "no overlapping files" one.** ``mysql -B -N``
    escapes an embedded tab into the two characters ``\\`` + ``t`` before the
    TSV is ever written, and ``lib/library_db.parse_library_tsv`` passes that
    through unchanged today -- so the mysql-sourced ``library.db`` holds the
    escape sequence, not the byte, and this substitution matches nothing. The
    ``tab_newline_substituted`` class therefore measures zero and rows carrying
    embedded tabs stay counted as mismatches (#371 measured that population at
    0 rows on the 2026-07-19 prod snapshot, so this is prophylactic rather than
    an active miscount). #371 is the producer-side unescape that turns the
    sequence back into the byte; until it merges, this function is correct but
    unreachable.

    A no-op for anything that isn't a string (``None``, an int call number).
    """
    if not isinstance(value, str):
        return value
    return value.replace("\t", " ").replace("\n", " ")


def _case_insensitive_equal(a: object, b: object) -> bool:
    """True when both sides are non-None and equal ignoring case."""
    if a is None or b is None:
        return False
    return str(a).lower() == str(b).lower()


def _classify_artist(
    mysql_row: Mapping[str, object], backend_row: Mapping[str, object]
) -> tuple[str, str | None]:
    """``artist``: TAB/NL sub -> ``normalize_artist_name`` -> fold tier."""
    mysql_raw = mysql_row["artist"]
    backend_value = backend_row["artist"]
    if _normalize(backend_value) == _normalize(mysql_raw):
        return ("agree", None)

    substituted = _tab_nl_sub(mysql_raw)
    info = normalize_artist_name(substituted if isinstance(substituted, str) else "")
    expected = info.name
    if _normalize(backend_value) == _normalize(expected):
        return ("normalized", "various_artists" if info.is_various else "trimmed_or_substituted")

    norm_backend = _normalize(backend_value)
    norm_expected = _normalize(expected)
    if (
        norm_backend is not None
        and norm_expected is not None
        and fold_artist_name(str(norm_backend)) == fold_artist_name(str(norm_expected))
    ):
        return ("normalized", "fold_equal")

    return ("mismatch", None)


def _classify_call_letters(
    mysql_row: Mapping[str, object], backend_row: Mapping[str, object]
) -> tuple[str, str | None]:
    """``call_letters``: ``normalize_code_letters(...) or '??'``, VA override
    from ``artist``, compared case-insensitively.

    On a VA row ``normalize_code_letters`` is never called at all -- the
    ``isVarious`` branch short-circuits straight to ``"V/A"`` (``job.ts:993-996``)
    -- so the VA class belongs to a different population than the ordinary
    uppercase/'??' classes below it.
    """
    mysql_raw = mysql_row["call_letters"]
    backend_value = backend_row["call_letters"]
    if _normalize(backend_value) == _normalize(mysql_raw):
        return ("agree", None)

    expected, cls = _derive_call_letters(mysql_row)

    if _case_insensitive_equal(_normalize(backend_value), _normalize(expected)):
        return ("normalized", cls)
    return ("mismatch", None)


def _derive_call_letters(mysql_row: Mapping[str, object]) -> tuple[str | None, str]:
    """Replay Backend's ``code_letters`` derivation for ONE mysql row.

    Extracted so ``_fold_group_values`` can ask the same question of a
    *sibling* row without restating the rule -- the fold-collapse resolution
    is only sound if the value it accepts is one this same derivation would
    have produced.
    """
    mysql_raw = mysql_row["call_letters"]
    artist_substituted = _tab_nl_sub(mysql_row["artist"])
    artist_info = normalize_artist_name(
        artist_substituted if isinstance(artist_substituted, str) else ""
    )
    if artist_info.is_various:
        expected: str | None = "V/A"
        cls = "various"
    else:
        derived = normalize_code_letters(mysql_raw if isinstance(mysql_raw, str) else None)
        expected = derived or "??"
        if derived is None:
            cls = "fallback_unknown"
        elif derived == "V/A":
            # The unanchored `/Z-[A-Z]/` branch -- a THIRD population, distinct
            # from both plain uppercasing and the `isVarious` branch above.
            # `Z--` does not reach it (no letter after the hyphen), which is
            # the split the plan's R11 measurement turns on, and these class
            # names are the key space the residue ledger's `baselines` block
            # shares -- so it needs its own name or the ledger cannot size the
            # Z-code cohort at all.
            cls = "various_artists_code"
        else:
            cls = "uppercased"

    return expected, cls


def _classify_genre(
    mysql_row: Mapping[str, object], backend_row: Mapping[str, object]
) -> tuple[str, str | None]:
    """``genre``: lookup-resolved, compared case-insensitively.

    ``genreMap`` is keyed on ``.toLowerCase()`` (``job.ts:951``, ``:965``)
    while the export emits the stored ``genres.genre_name``
    (``catalog-export.service.ts:233``) -- so a case-only difference is a
    deliberate normalization, and a genuine rename on either side stays
    invisible to this harness (recorded limitation, not fixed here).
    """
    mysql_raw = mysql_row["genre"]
    backend_value = backend_row["genre"]
    if _normalize(backend_value) == _normalize(mysql_raw):
        return ("agree", None)
    if _case_insensitive_equal(_normalize(backend_value), _normalize(mysql_raw)):
        return ("normalized", "case_folded")
    return ("mismatch", None)


def _classify_format(
    mysql_row: Mapping[str, object], backend_row: Mapping[str, object]
) -> tuple[str, str | None]:
    """``format``: ``parse_format_and_discs``, then compared case-insensitively.

    Same lookup-resolved shape as ``genre`` (``formatMap`` keyed on
    ``.toLowerCase()``, ``job.ts:954``, ``:980``) layered on top of the
    format-string derivation.
    """
    mysql_raw = mysql_row["format"]
    backend_value = backend_row["format"]
    if _normalize(backend_value) == _normalize(mysql_raw):
        return ("agree", None)

    parsed = parse_format_and_discs(mysql_raw if isinstance(mysql_raw, str) else "")
    if parsed is None:
        # Backend's own ETL would have skipped this row entirely (job.ts:976)
        # rather than write an unresolvable format -- on a row that DID make
        # it into both sides, an unparseable mysql format can't explain
        # whatever backend actually holds.
        return ("mismatch", None)

    if _case_insensitive_equal(_normalize(backend_value), _normalize(parsed.format_name)):
        return ("normalized", "format_derived")
    return ("mismatch", None)


def _classify_artist_call_number(
    mysql_row: Mapping[str, object], backend_row: Mapping[str, object]
) -> tuple[str, str | None]:
    """``artist_call_number``: ``0`` when VA, else ``mysql ?? 0``.

    ``job.ts:996``: ``isVarious ? VARIOUS_ARTISTS_CODE_NUMBER : (artist_call_numbers ?? 0)``.

    The ``?? 0`` fires on more than a SQL NULL: Backend reads this column
    through ``toNullableNumber`` (``job.ts:69-74``), which maps empty/blank
    text *and* anything ``Number()`` cannot make finite -- the literal
    ``"NULL"`` included -- to null first. That is exactly ``_normalize``'s own
    equivalence class, so the coalesce branches on it rather than on
    ``is not None``; branching on the raw value files the artifact shapes
    ``_normalize`` exists to absorb as defects.

    The ``ensureGenreArtistCrossref`` last-write-wins coupling on
    ``(artist_id, genre_id)`` (``job.ts:456-470``) means a delta here is not
    automatically a defect -- this classifier ships the simple model; sizing
    that residual is a later step's concern, not this one's.
    """
    mysql_raw = mysql_row["artist_call_number"]
    backend_value = backend_row["artist_call_number"]
    if _normalize(backend_value) == _normalize(mysql_raw):
        return ("agree", None)

    expected, cls = _derive_artist_call_number(mysql_row)

    if _normalize(backend_value) == _normalize(expected):
        return ("normalized", cls)
    return ("mismatch", None)


def _derive_artist_call_number(mysql_row: Mapping[str, object]) -> tuple[object, str]:
    """Replay Backend's ``artist_genre_code`` derivation for ONE mysql row.

    Extracted for the same reason as ``_derive_call_letters``: the
    fold-collapse resolution has to ask what a *sibling* row would have
    written into the shared ``(artist_id, genre_id)`` crossref, and the
    ``?? 0`` coalesce runs per row -- a sibling holding NULL contributes
    ``0`` to the group, not NULL.
    """
    mysql_raw = mysql_row["artist_call_number"]
    artist_substituted = _tab_nl_sub(mysql_row["artist"])
    artist_info = normalize_artist_name(
        artist_substituted if isinstance(artist_substituted, str) else ""
    )
    if artist_info.is_various:
        return 0, "various"
    expected = mysql_raw if _normalize(mysql_raw) is not None else 0
    return expected, "null_coalesced_zero"


def _classify_release_call_number(
    mysql_row: Mapping[str, object], backend_row: Mapping[str, object]
) -> tuple[str, str | None]:
    """``release_call_number``: ``mysql ?? 0`` -- no VA branch (``job.ts:1100``).

    Same ``toNullableNumber`` coercion as ``artist_call_number`` above, so the
    coalesce keys on ``_normalize`` rather than on ``is not None``.
    """
    mysql_raw = mysql_row["release_call_number"]
    backend_value = backend_row["release_call_number"]
    if _normalize(backend_value) == _normalize(mysql_raw):
        return ("agree", None)
    expected = mysql_raw if _normalize(mysql_raw) is not None else 0
    if _normalize(backend_value) == _normalize(expected):
        return ("normalized", "null_coalesced_zero")
    return ("mismatch", None)


def _make_tab_nl_classifier(
    column: str,
) -> Callable[[Mapping[str, object], Mapping[str, object]], tuple[str, str | None]]:
    """``title`` / ``alternate_artist_name`` / ``album_artist``: TAB/NL sub,
    then byte-identical. No further transform -- these three (plus ``artist``,
    modeled separately above) are the four columns ``buildReleaseQuery`` wraps
    in ``REPLACE(REPLACE(...), '\\t', ' '), '\\n', ' ')`` (``job.ts:281-296``).
    """

    def classify(
        mysql_row: Mapping[str, object], backend_row: Mapping[str, object]
    ) -> tuple[str, str | None]:
        mysql_raw = mysql_row[column]
        backend_value = backend_row[column]
        if _normalize(backend_value) == _normalize(mysql_raw):
            return ("agree", None)
        expected = _tab_nl_sub(mysql_raw)
        if _normalize(backend_value) == _normalize(expected):
            return ("normalized", "tab_newline_substituted")
        return ("mismatch", None)

    return classify


def _split_cross_refs(value: object) -> list[str]:
    """Split a ``cross_reference_names`` field on the imported separator.

    ``_normalize`` first, so NULL / '' / whitespace-only all yield an empty
    list rather than a single-element list containing an empty string.
    """
    normalized = _normalize(value)
    if normalized is None:
        return []
    return [item for item in str(normalized).split(CROSS_REFERENCE_SEPARATOR) if item != ""]


def _classify_cross_reference_names(
    mysql_row: Mapping[str, object], backend_row: Mapping[str, object]
) -> tuple[str, str | None]:
    """``cross_reference_names``: multiset compare under the fold tier.

    MySQL's ``GROUP_CONCAT(DISTINCT ...)`` has no ``ORDER BY``
    (``catalog_parity_diff.py``'s own ``LIBRARY_SELECT_SQL``), and Backend's
    export is an ordered array -- so order is never significant here, only
    membership under the fold.

    Cardinality loss is reported as a residual, not gated: Backend can hold
    FEWER fold-distinct aliases than MySQL for two byte-indistinguishable
    reasons (a MySQL-side fold-collapse the ``array_agg(DISTINCT ...)`` also
    performs, or a crossref ``importReleaseCrossrefs`` never imported because
    an artist/genre/album lookup missed). Nothing in the row distinguishes
    the two, so a backend fold-set that is a SUBSET of the derived mysql
    fold-set is "normalized", not "mismatch".

    **Cardinality GAIN is reported the same way, for the same reason
    (WXYC/discogs-etl#346, plan step 9).** An earlier revision called a
    superset "a genuine defect"; the 2026-08-13 prod run showed that is
    wrong, and wrong structurally rather than by degree.

    The asymmetry is in the two sides' join grain.
    ``LIBRARY_CODE_CROSS_REFERENCE`` is keyed by *code*, and
    ``LIBRARY_SELECT_SQL``'s correlated subquery can only return aliases
    attached to a code that carries a *release* -- a code with zero releases
    is not in this harness's input at all. Backend has no such restriction:
    its ``artists`` table is deduplicated onto ``fold_artist_name`` (see
    ``_fold_identity_key``), so several codes -- release-carrying or not --
    resolve to one ``artists`` row, and ``artist_crossreference`` hangs off
    that row carrying the union of their aliases. Measured on prod: of the 6
    distinct (artist, surplus-alias) pairs, 4 come from a duplicate code with
    **zero** releases (``Odd Nosdam``, ``Kendra Smith``, ``Tom Carter``
    twice), which ``LIBRARY_SELECT_SQL`` can never surface however healthy
    both sides are.

    So gating on a superset measures the query's blind spot, not the
    migration. All 11 such rows on prod were strict supersets; zero were any
    other shape.

    What stays gated is a set that is neither subset nor superset: Backend
    dropping one alias *and* gaining another. No fold-collapse or
    invisible-duplicate story produces that, so it remains the shape this
    column can still catch.
    """
    mysql_raw = mysql_row["cross_reference_names"]
    backend_value = backend_row["cross_reference_names"]
    if _normalize(backend_value) == _normalize(mysql_raw):
        return ("agree", None)

    mysql_fold_keys: set[str] = set()
    for item in _split_cross_refs(mysql_raw):
        substituted = _tab_nl_sub(item)
        info = normalize_artist_name(substituted if isinstance(substituted, str) else "")
        mysql_fold_keys.add(fold_artist_name(info.name))

    backend_fold_keys = {fold_artist_name(item) for item in _split_cross_refs(backend_value)}

    if backend_fold_keys == mysql_fold_keys:
        return ("normalized", "fold_equal")
    if backend_fold_keys < mysql_fold_keys:
        return ("normalized", "cardinality_loss")
    if backend_fold_keys > mysql_fold_keys:
        return ("normalized", "cardinality_gain")
    return ("mismatch", None)


# Keyed by column name (never a positional/ordered table) so
# `TestColumnModelsDriftGuard` can assert `set(COLUMN_MODELS) == set(DIFF_COLUMNS)`
# -- set equality, not containment, so an added OR removed diffed column
# fails this test rather than silently falling through unmodelled.
COLUMN_MODELS: dict[
    str, Callable[[Mapping[str, object], Mapping[str, object]], tuple[str, str | None]]
] = {
    "title": _make_tab_nl_classifier("title"),
    "artist": _classify_artist,
    "call_letters": _classify_call_letters,
    "artist_call_number": _classify_artist_call_number,
    "release_call_number": _classify_release_call_number,
    "genre": _classify_genre,
    "format": _classify_format,
    "alternate_artist_name": _make_tab_nl_classifier("alternate_artist_name"),
    "album_artist": _make_tab_nl_classifier("album_artist"),
    "cross_reference_names": _classify_cross_reference_names,
}


def classify_field(
    col: str, mysql_row: Mapping[str, object], backend_row: Mapping[str, object]
) -> tuple[str, str | None]:
    """Classify one column's value pair into ``(tier, normalization_class)``.

    ``tier`` is one of ``"agree"``, ``"normalized"``, ``"mismatch"``.
    ``normalization_class`` names which of Part 1's baseline classes explains
    the difference, and is only non-``None`` when ``tier == "normalized"``.

    Raises ``KeyError`` on a column with no model. Unguarded on purpose -- see
    the ``DIFF_COLUMNS`` comment above for why a ``.get()`` fallback to a bare
    byte compare would be worse than the exception.
    """
    return COLUMN_MODELS[col](mysql_row, backend_row)


# Columns whose divergence can be explained by a row OTHER than the one being
# compared -- see `_is_fold_collapsed`. Deliberately narrow: these are the two
# attributes Backend stores once per folded artist (`artists.code_letters`) or
# once per folded artist and genre (`genre_artist_crossreference.
# artist_genre_code`), so a duplicate `LIBRARY_CODE` forces one value to win.
# `cross_reference_names` collapses the same way but needs no group context --
# it is a set union, so the widening is expressible per row as
# `cardinality_gain` in `_classify_cross_reference_names`.
FOLD_COLLAPSE_COLUMNS = ("call_letters", "artist_call_number")


def _fold_identity_key(row: Mapping[str, object]) -> str:
    """The key Backend's ``artists`` table currently collapses a mysql row's
    artist onto: ``fold_artist_name(normalize_artist_name(...))``, matching
    the ``artist`` / ``call_letters`` classifiers' own derivation.

    **This is an empirical property of the live data, NOT a restatement of
    what ``ensureArtist`` would do today, and the difference matters.**
    ``ensureArtist`` (``job.ts:388-427``) matches on
    ``fold_artist_name(...)`` *and* ``lower(artists.code_letters)`` (plus
    ``(genre_id, artist_genre_code)`` off ``genre_artist_crossreference`` for
    a non-various artist), and INSERTs a fresh row when any of those miss --
    read literally, two ``LIBRARY_CODE`` rows sharing a name but differing in
    ``CALL_LETTERS`` would get two ``artists`` rows and would never collapse
    at all. Prod says otherwise, because the table has since been
    deduplicated onto the fold (the ``fold_artist_name`` work behind
    migration 0134): measured 2026-08-13, **1** fold name out of 23,882
    ``artists`` rows has more than one row, and **0** ``(fold name,
    code_letters)`` pairs do. The lone exception (``Markolino Dimond``,
    ``MA``/``DI``, one release each) does not appear in the resolved set.

    So the key models the state the harness actually compares against. The
    standing assumption is that the dedup holds; if ``artists`` is ever
    allowed to re-accumulate fold-duplicates, a release linked to the *wrong*
    one of two rows would be silently absorbed here rather than reported. Any
    change to `ensureArtist`'s matching should re-measure those two counts
    before trusting this resolution -- library.db carries no artist id, so
    the harness cannot check it from its own inputs.
    """
    artist = _tab_nl_sub(row.get("artist"))
    info = normalize_artist_name(artist if isinstance(artist, str) else "")
    return fold_artist_name(info.name)


def _build_fold_groups(
    mysql_rows: Mapping[int, Mapping[str, object]],
) -> dict[str, list[Mapping[str, object]]]:
    """Index every IMPORTABLE mysql row by its Backend artist-fold key.

    Built from all mysql rows rather than only matched ones: the sibling that
    supplied the winning value is often a row Backend collapsed away entirely
    (it is in ``missing_in_backend``), and excluding it would leave the very
    collapse this resolution exists to explain unexplained.

    But rows Backend's ETL *skips* are excluded, because they can never have
    written the value being explained. ``job.ts:959-990`` ``continue``s past a
    ``db_only`` genre, an unresolvable genre, an unparseable format and an
    empty artist name **before** reaching ``ensureArtist`` /
    ``ensureGenreArtistCrossref``, so such a row contributes no
    ``code_letters`` and no ``artist_genre_code``. Admitting one would weaken
    the gate this resolution rests on -- "the value has to be one the mysql
    side itself supplies" -- to "...or one a never-imported row happens to
    carry". ``_rule_b_missing_reason`` is exactly that predicate, already
    used for row-level expectation.

    Inert on the 2026-08-13 prod pair (0 of 92 resolutions change), which is
    the argument for landing it while it is free rather than after a skipped
    sibling coincides with a real defect.
    """
    groups: dict[str, list[Mapping[str, object]]] = {}
    for row in mysql_rows.values():
        if _rule_b_missing_reason(row) is not None:
            continue
        groups.setdefault(_fold_identity_key(row), []).append(row)
    return groups


def _fold_group_values(
    column: str, siblings: Iterable[Mapping[str, object]], mysql_row: Mapping[str, object]
) -> set[object]:
    """Every value Backend could legitimately hold for ``column`` given the
    folded artist's mysql rows.

    ``artist_call_number`` is scoped to the row's own genre and
    ``call_letters`` is not, and that asymmetry is the whole correctness
    argument: ``ensureGenreArtistCrossref`` keys on ``(artist_id, genre_id)``
    (``job.ts:456-470``) so the collapse happens *within* a genre, while
    ``artists.code_letters`` is one column on the artist and collapses across
    all of them. Widening the call-number scope past the genre would accept a
    value no crossref row could have carried -- a real defect, laundered.
    """
    values: set[object] = set()
    if column == "call_letters":
        for sibling in siblings:
            expected, _ = _derive_call_letters(sibling)
            normalized = _normalize(expected)
            values.add(str(normalized).upper() if isinstance(normalized, str) else normalized)
        return values
    if column == "artist_call_number":
        # Case-insensitive, like `_classify_genre` and for the same reason:
        # Backend resolves a genre through `genreMap.get(name.toLowerCase())`
        # (`job.ts:951,965`), so two `GENRE.REFERENCE_NAME`s differing only in
        # case land on ONE `genre_id` and therefore one crossref row. A
        # byte-exact filter would drop that sibling and report a genuinely
        # collapsed value as a mismatch.
        #
        # `_case_insensitive_equal` is False when either side is None, so the
        # both-absent case is spelled out: two genre-less rows share a genre
        # as surely as two rows both reading "Rock" do, and silently dropping
        # that pairing would be the same bug in the other direction.
        genre = _normalize(mysql_row.get("genre"))
        for sibling in siblings:
            sibling_genre = _normalize(sibling.get("genre"))
            same_genre = (
                sibling_genre is None
                if genre is None
                else _case_insensitive_equal(sibling_genre, genre)
            )
            if not same_genre:
                continue
            expected, _ = _derive_artist_call_number(sibling)
            values.add(_normalize(expected))
        return values
    raise ValueError(f"no fold-collapse model for column {column!r}")


def _is_fold_collapsed(
    column: str,
    mysql_row: Mapping[str, object],
    backend_row: Mapping[str, object],
    siblings: Iterable[Mapping[str, object]],
) -> bool:
    """True when Backend's value is one the folded artist's own mysql rows supply.

    Note what this is NOT: it never accepts a value merely because the artist
    has duplicates. The value has to be present in the group, which is what
    keeps a genuine defect on a duplicated artist gated.
    """
    backend_value = _normalize(backend_row[column])
    if column == "call_letters" and isinstance(backend_value, str):
        backend_value = backend_value.upper()
    return backend_value in _fold_group_values(column, siblings, mysql_row)


def _classify_matched_rows(
    mysql_rows: Mapping[int, Mapping[str, object]],
    backend_rows: Mapping[int, Mapping[str, object]],
    matched_ids: Iterable[int],
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """Classify every ``DIFF_COLUMNS`` field on every matched id.

    Returns ``(field_mismatches, normalizations)``:

    - ``field_mismatches`` counts only the ``"mismatch"`` tier, fully keyed
      over ``DIFF_COLUMNS`` with zeros included (``_print_human`` subscripts
      it directly for every column with no ``.get()``, so a tiering change
      that drops a zero-count key would turn the default invocation into a
      ``KeyError``).
    - ``normalizations`` counts the ``"normalized"`` tier, keyed
      column -> class -> count. Not yet wired into ``ParityDiff`` -- that
      lands with the dataclass field-order change in a later step; this
      function is where the counting itself lives so that step can wire it
      in without re-deriving the classification.
    """
    field_mismatches: dict[str, int] = dict.fromkeys(DIFF_COLUMNS, 0)
    normalizations: dict[str, dict[str, int]] = {}
    # Built lazily: only a run that actually produces a fold-collapse
    # candidate pays for indexing the whole mysql side.
    fold_groups: dict[str, list[Mapping[str, object]]] | None = None
    for id_ in matched_ids:
        mrow = mysql_rows[id_]
        brow = backend_rows[id_]
        for col in DIFF_COLUMNS:
            tier, cls = classify_field(col, mrow, brow)
            if tier == "mismatch" and col in FOLD_COLLAPSE_COLUMNS:
                if fold_groups is None:
                    fold_groups = _build_fold_groups(mysql_rows)
                siblings = fold_groups.get(_fold_identity_key(mrow), ())
                if _is_fold_collapsed(col, mrow, brow, siblings):
                    tier, cls = "normalized", "fold_collapsed"
            if tier == "mismatch":
                field_mismatches[col] += 1
            elif tier == "normalized":
                bucket = normalizations.setdefault(col, {})
                key = cls or "unspecified"
                bucket[key] = bucket.get(key, 0) + 1
    return field_mismatches, normalizations


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _require_table(conn: sqlite3.Connection, table: str, label: str) -> None:
    if not _table_exists(conn, table):
        raise SourceError(f"{label} database is missing required table '{table}'")


def _open_readonly(path: str, label: str) -> sqlite3.Connection:
    """Open ``path`` as a read-only SQLite connection.

    Raises ``SourceError`` (never writes, never creates) when the file is
    missing or is not a readable SQLite database.
    """
    p = Path(path)
    if not p.is_file():
        raise SourceError(f"{label} database not found: {path}")
    # Percent-encode the path before splicing it into the file: URI. A raw path
    # containing a URI-significant character (?, #, or a space) would otherwise
    # be misparsed -- e.g. a `?` in the path prematurely starts the query string,
    # silently dropping the `?mode=ro` read-only guard and/or opening the wrong
    # file. pathname2url encodes those characters while leaving `/` intact.
    uri = f"file:{pathname2url(str(p.resolve()))}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        # Force a read of the database header/schema so a "file is not a
        # database" error surfaces here (and is wrapped as SourceError) rather
        # than leaking as a raw sqlite3.DatabaseError from a later query. A bare
        # `SELECT 1` is a constant expression that never touches the file, so it
        # does NOT validate the header on every SQLite build (it passes on macOS
        # but not on the Linux CI build); querying sqlite_master does.
        conn.execute("SELECT count(*) FROM sqlite_master")
    except sqlite3.Error as exc:
        raise SourceError(f"{label} database unreadable: {path} ({exc})") from exc
    return conn


def _load_library_rows(conn: sqlite3.Connection, label: str) -> dict[int, dict[str, object]]:
    """Read every ``library`` row, keyed by ``id``.

    A valid daily-sync ``library.db`` has ``id INTEGER PRIMARY KEY`` (see
    ``lib/library_db.py``), so ids are unique. If two rows share an id
    the input is malformed; keying into a dict would silently keep only the
    last (hiding a row-count divergence this parity harness exists to catch),
    so we raise ``SourceError`` instead of under-counting.
    """
    cols = ", ".join(LIBRARY_COLUMNS)
    rows = conn.execute(f"SELECT {cols} FROM library").fetchall()
    result: dict[int, dict[str, object]] = {}
    for row in rows:
        record = dict(zip(LIBRARY_COLUMNS, row, strict=True))
        row_id = int(record["id"])
        if row_id in result:
            raise SourceError(
                f"{label} database has a duplicate library.id ({row_id}); "
                "a valid library.db has a unique id primary key"
            )
        result[row_id] = record
    return result


def _load_cta_counts(conn: sqlite3.Connection) -> Counter[tuple[object, ...]]:
    """Read compilation_track_artist as a normalized multiset.

    Returns an empty Counter (never raises) when the table is absent --
    the table is optional, matching tsv_to_sqlite.py's graceful-degradation
    handling of pre-V008 fixtures / Backend-Service-sourced catalogs.

    Two legacy-ETL transforms are replayed, and both are applied to **either**
    side, not just the mysql one:

    - the TAB/NL substitution Backend's own extraction SQL applies to
      ``ARTIST_NAME`` / ``TRACK_TITLE`` at import time (``job.ts:727-729``);
      the harness's own ``COMPILATION_TRACK_SELECT_SQL`` has no such wrapper
      (and stays that way -- see ``test_select_statements_match_sync_library_sh``).
    - the row-drop for an empty ``artist_name``, mirroring
      ``parseLegacyCompilationTrackRows`` (``job.ts:710-711``) -- Backend's
      importer never writes such a row, so a mysql-side one is
      expected-missing rather than genuine drift.

    **Symmetry is the whole point, and getting it wrong inverts the tool.**
    Applying either transform to one counter only deletes a row from that side
    while the other keeps it, so two byte-identical rows report as ``cta_extra``
    -- the harness manufacturing drift out of agreement, in a gate whose job is
    to certify seven consecutive clean days. Where the "backend already
    reflects this" assumption holds, applying it to both sides is a no-op;
    where it does not, applying it to both is what keeps agreement reading as
    agreement.

    The drop test is ``str.strip() == ""``, the ported rule -- deliberately NOT
    ``_normalize``, which also collapses the literal string ``"NULL"``. That is
    a legal 4-character artist name Backend keeps, so screening the drop
    through ``_normalize`` would file a genuinely-missing "NULL" row as
    expected.
    """
    if not _table_exists(conn, "compilation_track_artist"):
        return Counter()
    cols = ", ".join(CTA_COLUMNS)
    rows = conn.execute(f"SELECT {cols} FROM compilation_track_artist").fetchall()
    counter: Counter[tuple[object, ...]] = Counter()
    for row in rows:
        # Keyed by name off CTA_COLUMNS rather than unpacked positionally, so
        # a widened CTA shape widens the multiset instead of raising a bare
        # unpacking ValueError -- the same auto-widening property DIFF_COLUMNS
        # has.
        record = dict(zip(CTA_COLUMNS, row, strict=True))
        record["artist_name"] = _tab_nl_sub(record["artist_name"])
        record["track_title"] = _tab_nl_sub(record["track_title"])
        artist_name = record["artist_name"]
        if isinstance(artist_name, str) and artist_name.strip() == "":
            continue
        counter[tuple(_normalize(record[col]) for col in CTA_COLUMNS)] += 1
    return counter


def _classify_row_expectations(
    mysql_rows: Mapping[int, Mapping[str, object]],
    missing_ids: Sequence[int],
    extra_ids: Sequence[int],
    ledger: ResidueLedger,
) -> tuple[int, int, int, int]:
    """Split ``missing_ids`` / ``extra_ids`` into expected vs. unexplained.

    An id is expected-missing when Rule B's row-derivable predicate matches
    its mysql row (``_rule_b_missing_reason``) OR it is one of the ledger's
    enumerated 599 collapse ids -- either is sufficient, so a stale ledger
    entry that Rule B would ALSO have explained does not double count.

    An id is expected-extra when Rule A's minted-id predicate matches (there
    is no enumerated extra-id set yet -- see ``ResidueLedger``'s docstring).

    A ledger id that does not appear in ``missing_ids`` at all (already
    resolved, or never was missing) is silently a no-op here: this function
    only ever looks UP an actual missing/extra id in the ledger, never the
    other way around, so a stale ledger entry cannot raise.

    Returns ``(missing_expected, missing_unexplained, extra_expected,
    extra_unexplained)``.
    """
    missing_expected = 0
    for id_ in missing_ids:
        if id_ in ledger.collapsed_ids or _rule_b_missing_reason(mysql_rows[id_]) is not None:
            missing_expected += 1
    missing_unexplained = len(missing_ids) - missing_expected

    extra_expected = sum(1 for id_ in extra_ids if _is_minted_id(id_))
    extra_unexplained = len(extra_ids) - extra_expected

    return missing_expected, missing_unexplained, extra_expected, extra_unexplained


def diff_library_dbs(
    mysql_conn: sqlite3.Connection,
    backend_conn: sqlite3.Connection,
    ledger: ResidueLedger | None = None,
) -> ParityDiff:
    """Compute the full parity diff between two already-open library.db connections.

    Assumes both connections have a ``library`` table (callers -- ``run_diff``
    -- are responsible for validating that up front via ``_require_table``).

    ``ledger`` gates only the ROW-LEVEL expected/unexplained classification
    and ``clean`` (see ``ParityDiff``'s docstring) -- field tiering
    (``field_mismatches`` / ``normalizations``) runs identically either way,
    with or without one.
    """
    mysql_rows = _load_library_rows(mysql_conn, "mysql")
    backend_rows = _load_library_rows(backend_conn, "backend")

    mysql_ids = set(mysql_rows)
    backend_ids = set(backend_rows)

    matched_ids = mysql_ids & backend_ids
    missing_ids = sorted(mysql_ids - backend_ids)
    extra_ids = sorted(backend_ids - mysql_ids)

    field_mismatches, normalizations = _classify_matched_rows(mysql_rows, backend_rows, matched_ids)
    if normalizations:
        # `ParityDiff.normalizations` now carries these, but only `--json`
        # renders that field -- `_print_human` does not -- so this line stays
        # as the counts' surface for the default invocation. INFO, not DEBUG:
        # `init_logger` pins the root level at INFO and this CLI has no
        # verbosity flag, so a DEBUG record here would be unreachable in every
        # invocation the harness actually has -- "logged rather than
        # discarded" would be discarded. One line per run, and on stderr, so
        # `--json`'s one-object-on-stdout contract is untouched.
        logger.info("catalog parity normalizations", extra={"normalizations": normalizations})

    mysql_cta = _load_cta_counts(mysql_conn)
    backend_cta = _load_cta_counts(backend_conn)
    cta_missing = sum((mysql_cta - backend_cta).values())
    cta_extra = sum((backend_cta - mysql_cta).values())

    if ledger is None:
        clean = None
        missing_expected = 0
        missing_unexplained = len(missing_ids)
        extra_expected = 0
        extra_unexplained = len(extra_ids)
    else:
        (
            missing_expected,
            missing_unexplained,
            extra_expected,
            extra_unexplained,
        ) = _classify_row_expectations(mysql_rows, missing_ids, extra_ids, ledger)

        cta_within_baseline = (
            ledger.cta_missing_baseline is not None
            and ledger.cta_extra_baseline is not None
            and cta_missing <= ledger.cta_missing_baseline
            and cta_extra <= ledger.cta_extra_baseline
        )
        clean = (
            missing_unexplained == 0
            and extra_unexplained == 0
            and sum(field_mismatches.values()) == 0
            and cta_within_baseline
        )

    return ParityDiff(
        matched=len(matched_ids),
        missing_in_backend=len(missing_ids),
        extra_in_backend=len(extra_ids),
        field_mismatches=field_mismatches,
        cta_missing=cta_missing,
        cta_extra=cta_extra,
        clean=clean,
        missing_unexplained=missing_unexplained,
        missing_expected=missing_expected,
        extra_unexplained=extra_unexplained,
        extra_expected=extra_expected,
        normalizations=normalizations,
        missing_in_backend_ids=missing_ids,
        extra_in_backend_ids=extra_ids,
    )


def run_diff(mysql_db: str, backend_db: str, ledger: ResidueLedger | None = None) -> ParityDiff:
    """Open both library.db files read-only, validate schema, and diff them.

    Raises ``SourceError`` for any file/table problem on either side. Never
    writes to either input.
    """
    mysql_conn = _open_readonly(mysql_db, "mysql")
    try:
        _require_table(mysql_conn, "library", "mysql")
        backend_conn = _open_readonly(backend_db, "backend")
        try:
            _require_table(backend_conn, "library", "backend")
            return diff_library_dbs(mysql_conn, backend_conn, ledger=ledger)
        finally:
            backend_conn.close()
    finally:
        mysql_conn.close()


# --- Producers: build a library.db from a live source (#351) --------------

# One row of either output table, already mapped out of its wire dict: a
# `library` row in LIBRARY_INSERT_COLUMNS order, or a (library_release_id,
# artist_name, track_title) triple. Element 0 is the library release id in
# both, which is what the snapshot-consistency check keys on.
_Row = Sequence[object]

# Env var holding a pre-minted Backend-Service service-account JWT. Decision
# D3 (Option B) deliberately picked an HTTP/contract-governed producer over a
# direct-Postgres one precisely so that no prod-DB credential has to exist in
# GitHub Actions.
#
# This is the *manual* route: a JWT from better-auth lives 15 minutes (the
# plugin default, which Backend-Service does not override), so it suits an
# operator running a one-off with a token already in hand and cannot be a
# stored CI secret for a soak measured in days. Unattended runs use the
# credential pair below and mint per run.
BACKEND_TOKEN_ENV = "BACKEND_CATALOG_TOKEN"

# The service account's own credentials (#365) -- what CI actually stores.
# `catalog-parity@wxyc.invalid` holds the `member` org role, the least-privileged
# role carrying `catalog:read`.
BACKEND_EMAIL_ENV = "BACKEND_CATALOG_EMAIL"
BACKEND_PASSWORD_ENV = "BACKEND_CATALOG_PASSWORD"

# Optional overrides. The auth base URL defaults to `<--backend-source>/auth`,
# which is where prod serves better-auth (https://api.wxyc.org/auth), so a
# normal invocation names one URL rather than two.
BACKEND_AUTH_URL_ENV = "BACKEND_AUTH_URL"
BACKEND_AUTH_ORIGIN_ENV = "BACKEND_AUTH_ORIGIN"

# better-auth's CSRF guard rejects a sign-in with no Origin header
# (MISSING_OR_NULL_ORIGIN) -- which is every non-browser caller, this one
# included. The value has to be one of the auth server's
# BETTER_AUTH_TRUSTED_ORIGINS; dj-site's is the one every headless WXYC
# client sends (wxyc-canary does the same).
_DEFAULT_AUTH_ORIGIN = "https://dj.wxyc.org"

_SIGN_IN_PATH = "/sign-in/email"
_EXCHANGE_PATH = "/token"
_SIGN_OUT_PATH = "/sign-out"

# Cleanup budget. Deliberately not _HTTP_TIMEOUT_SECONDS: the export is
# already done by the time this runs, and a hung revocation must not hold a
# finished run open for five minutes.
_SIGN_OUT_TIMEOUT_SECONDS = 30

# better-auth's default JWT life, which Backend-Service does not override.
# Only a fallback: it schedules the next refresh when a token's own `exp` is
# unreadable, so a claim-format change upstream costs one assumption rather
# than an exchange per call.
_ASSUMED_JWT_TTL_SECONDS = 900

# Sign-ins per process. Two: the first, plus one recovery for a session that
# turns out to be dead. The refresh path re-exchanges instead (see
# `_TokenSource`), so a long run does not spend these -- and a credential that
# is simply wrong fails after two rather than hammering a rate limiter shared
# with every DJ logging in. Counted per sign-in, not per HTTP attempt: the
# 429 retry below belongs to the sign-in that provoked it.
_MAX_SIGN_INS = 2

# Attempts per exchange: one against the cached session, and -- if that session
# turns out to be dead -- one against a fresh one. Deliberately its own budget
# rather than a share of _MAX_SIGN_INS, because an exhausted sign-in allowance
# must not disable refreshing against a session that still works. That
# conflation is what made a long soak run unable to refresh at all.
_MAX_EXCHANGE_ATTEMPTS = 2

# One retry on a rate-limited sign-in, waiting at most this long. Two limiters
# sit in front of that path: the express one (10 per 15 min, draft-7
# `Retry-After`) and better-auth's own (3 per 10s, `X-Retry-After`). The cap
# has to be able to clear the shorter window, so it is 10s and not
# wxyc-canary's 5s. A hint longer than this -- or missing entirely -- is not
# waited out; see `_sign_in`.
_SIGN_IN_RETRY_CAP_SECONDS = 10
_SIGN_IN_RETRY_FLOOR_SECONDS = 1

# Env var holding the tubafrenzy MySQL password. It is read from the
# environment rather than from the ``--mysql-source`` DSN because a DSN
# password sits in *this* process's argv for the whole run -- readable by any
# `ps`, and echoed by a `set -x` or a GitHub Actions command trace. (It is
# still accepted inside the DSN for local one-offs; the env var wins.)
MYSQL_PASSWORD_ENV = "LIBRARY_DB_PASSWORD"

_CATALOG_PATH = "/library/catalog"
_COMPILATION_TRACKS_PATH = "/library/catalog/compilation-tracks"

# The full catalog is ~2.6 MB gzipped and the CTA export ~3.2 MB (measured
# against the 2026-07-19 prod snapshot, api.yaml BS#1965 notes), both served
# from a per-watermark in-memory cache -- but a cold cache has to build the
# whole body first, so the budget is generous.
_HTTP_TIMEOUT_SECONDS = 300

# Refresh a JWT this far before its `exp` rather than after. A fetch can run
# for the whole timeout above, so a token with less life than that left cannot
# safely start one: it is "expired" for our purposes while the clock still
# says otherwise. The 401 retry would recover, but at the price of re-fetching
# an entire export.
_JWT_REFRESH_MARGIN_SECONDS = _HTTP_TIMEOUT_SECONDS

# GET /library/catalog and GET /library/catalog/compilation-tracks are two
# requests, not one transaction. They form a consistent snapshot only while
# the shared library_watermark holds still across both; api.yaml's
# CROSS-ENDPOINT CONSISTENCY note says to treat a change as "re-fetch both".
# A catalog write mid-fetch is rare and self-healing, so a couple of retries
# is plenty -- and failing after them is right, because the alternative is
# diffing a torn snapshot and calling the tear a parity defect.
_SNAPSHOT_ATTEMPTS = 3

# Loopback only: anywhere else, plain HTTP would put the service-account
# bearer token on the wire in the clear.
_PLAINTEXT_OK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

_DEFAULT_PORTS = {"http": 80, "https": 443}


def _origin(url: str) -> tuple[str, str, int]:
    """(scheme, host, port) with the scheme's default port made explicit."""
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    return (scheme, (parts.hostname or "").lower(), parts.port or _DEFAULT_PORTS.get(scheme, 0))


class _SameOriginRedirectHandler(HTTPRedirectHandler):
    """Refuse any redirect that would carry the bearer token to a new origin.

    ``urllib``'s default handler copies every header except Content-Length /
    Content-Type into the redirected request -- ``Authorization`` included.
    So a 302 from a proxy, a misconfigured CDN, or hijacked DNS would replay
    the service-account JWT to a foreign host, and to a plaintext one, which
    is exactly what ``_resolve_backend_base_url``'s https check exists to
    prevent. That check only ever sees the *first* URL, so the guard has to
    live here as well.

    Stopping (rather than stripping the header and following the hop) keeps
    the failure diagnosable: a silently-unauthenticated request would come
    back as a bare 401 from an origin the operator never named.
    """

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Request | None:
        if _origin(req.full_url) != _origin(newurl):
            raise SourceError(
                f"refusing to follow the {code} redirect from {req.full_url} to {newurl}: "
                "it crosses origins, and urllib would replay the parity service-account "
                f"credentials to the new host -- the bearer token on a catalog fetch, the "
                f"${BACKEND_PASSWORD_ENV} sign-in password on a mint. Point --backend-source "
                f"(or ${BACKEND_AUTH_URL_ENV}) at the origin that actually serves it."
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


# Module-level opener so every catalog fetch goes through the redirect guard
# above; the bare `urllib.request.urlopen` would use the permissive default.
_opener = build_opener(_SameOriginRedirectHandler())

# The library SELECT production runs every day, copied verbatim from
# scripts/sync-library.sh. tests/unit/test_catalog_parity_diff.py lifts every
# `-e "SELECT ..."` out of that script and asserts (whitespace-insensitively)
# that the set is exactly these two -- equality, not containment, so neither
# an appended `ORDER BY`/`LIMIT` on the shell side nor a third divergent
# query can slip past. A baseline built from a *different* query would make
# the parity diff measure the harness rather than the migration. Change one,
# change both.
LIBRARY_SELECT_SQL = (
    "SELECT r.ID, r.TITLE, lc.PRESENTATION_NAME, lc.CALL_LETTERS, lc.CALL_NUMBERS,"
    " r.CALL_NUMBERS, g.REFERENCE_NAME, f.REFERENCE_NAME,"
    " IFNULL(r.ALTERNATE_ARTIST_NAME, ''), IFNULL(r.ALBUM_ARTIST, ''),"
    " IFNULL((SELECT GROUP_CONCAT(DISTINCT xlc.PRESENTATION_NAME SEPARATOR ' | ')"
    " FROM LIBRARY_CODE_CROSS_REFERENCE xcr, LIBRARY_CODE xlc"
    " WHERE xlc.ID = CASE WHEN xcr.CROSS_REFERENCING_ARTIST_ID = lc.ID"
    " THEN xcr.CROSS_REFERENCED_LIBRARY_CODE_ID"
    " WHEN xcr.CROSS_REFERENCED_LIBRARY_CODE_ID = lc.ID"
    " THEN xcr.CROSS_REFERENCING_ARTIST_ID ELSE NULL END"
    " AND (xcr.CROSS_REFERENCING_ARTIST_ID = lc.ID"
    " OR xcr.CROSS_REFERENCED_LIBRARY_CODE_ID = lc.ID)"
    " AND xlc.ID != lc.ID), '')"
    " FROM LIBRARY_RELEASE r JOIN LIBRARY_CODE lc ON r.LIBRARY_CODE_ID = lc.ID"
    " JOIN FORMAT f ON r.FORMAT_ID = f.ID JOIN GENRE g ON lc.GENRE_ID = g.ID"
)

# Likewise the compilation-track SELECT. Supplementary to LIBRARY_RELEASE: on
# a source with no COMPILATION_TRACK_ARTIST table this query fails and the
# build continues without the table, exactly as sync-library.sh does.
COMPILATION_TRACK_SELECT_SQL = (
    "SELECT LIBRARY_RELEASE_ID, ARTIST_NAME, IFNULL(TRACK_TITLE, '')"
    " FROM COMPILATION_TRACK_ARTIST ORDER BY LIBRARY_RELEASE_ID"
)


def _require_absent(output_path: str, label: str) -> None:
    """Refuse to build over an existing file.

    The harness builds scratch copies; the one thing it must never do is
    overwrite a real ``library.db`` (or the prebuilt file the operator passed
    as the other side of the diff).
    """
    if Path(output_path).exists():
        raise SourceError(
            f"refusing to build the {label} library.db at {output_path}: the path "
            f"already exists. This harness only ever writes fresh scratch copies -- "
            f"point --{label}-db at a new path (or delete that one deliberately)."
        )
    parent = Path(output_path).parent
    if not parent.is_dir():
        raise SourceError(
            f"cannot build the {label} library.db at {output_path}: its parent "
            f"directory {str(parent)!r} does not exist"
        )


@contextmanager
def _atomic_output(output_path: str, label: str) -> Iterator[str]:
    """Yield a scratch path to build into, published only on success.

    ``_require_absent`` refuses any path that already exists, so a build that
    dies partway -- a duplicate id, a full disk, a killed process -- must not
    leave a stub behind: it would refuse every subsequent run of a
    seven-clean-day parity soak until an operator deleted it by hand. Build
    beside the target (same filesystem, so the publish is a rename) and
    ``os.replace`` it into place at the end.
    """
    _require_absent(output_path, label)
    target = Path(output_path)
    fd, scratch = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".partial", dir=target.parent)
    os.close(fd)
    try:
        yield scratch
    except BaseException:
        Path(scratch).unlink(missing_ok=True)
        raise
    os.replace(scratch, output_path)


def _build_into(
    output_path: str,
    label: str,
    library_rows: Iterable[_Row],
    cta_rows: Iterable[_Row] | None,
) -> int:
    """Build a library.db at ``output_path`` atomically, as a ``SourceError`` seam.

    SQLite write failures (a duplicate id from a malformed export, a full
    disk) are producer failures like any other, so they surface as
    ``SourceError`` -- exit 3 -- rather than a raw traceback.
    """
    try:
        with _atomic_output(output_path, label) as scratch:
            return build_library_db(scratch, library_rows, cta_rows, report=_report)
    except sqlite3.Error as exc:
        raise SourceError(
            f"failed to write the {label} library.db at {output_path}: {exc}"
        ) from exc


def _report(message: str) -> None:
    """Emit a producer progress line on stderr.

    Deliberately not stdout: under ``--json`` stdout carries exactly one JSON
    object, and a progress line there would break every machine consumer of
    this harness. (``tsv_to_sqlite.py``, whose stdout is a human log, keeps
    the default.)
    """
    print(message, file=sys.stderr)


def _empty_if_none(value: object) -> object:
    """Map a null wire value to ''.

    sync-library.sh wraps alternate_artist_name / album_artist /
    cross_reference_names in ``IFNULL(<col>, '')``, so production's
    library.db holds '' -- never NULL -- for those three. The Backend build
    must match: after the cutover this output *is* library.db, and the
    difference is observable in the FTS content and in LML's pipe-split of
    cross_reference_names, not just in the diff (which normalizes them equal).
    """
    return "" if value is None else value


# The CatalogExportRow fields api.yaml marks `required` and this producer
# writes straight into a library column. A bare `.get()` on any of them would
# turn a field rename or a partial server-side regression into a SQL NULL for
# every row: after the cutover that empties the row's FTS content and breaks
# LML search, and before it, it reads as an ordinary field mismatch --
# indistinguishable from real catalog drift. (`id`, also required, is
# deliberately absent: it is never written, only quoted in diagnostics.)
_REQUIRED_CATALOG_FIELDS = (
    "album_title",
    "artist_name",
    "code_letters",
    "code_artist_number",
    "code_number",
    "genre_name",
    "format_name",
)


def _require_legacy_release_id(row: dict[str, Any], what: str) -> int:
    """Read ``legacy_release_id`` as an int, or raise ``SourceError``."""
    legacy_release_id = row.get("legacy_release_id")
    if legacy_release_id is None:
        raise SourceError(
            f"{what} export row has no legacy_release_id "
            f"(Backend serial id {row.get('id')!r}): either this Backend predates "
            "WXYC/Backend-Service#1965 or the BS#1963 mint/backfill is incomplete. "
            "Refusing to write a library.db row with a null id."
        )
    try:
        return int(legacy_release_id)
    except (TypeError, ValueError) as exc:
        raise SourceError(
            f"{what} export row has a non-integer legacy_release_id "
            f"({legacy_release_id!r}); library.db's id is an INTEGER PRIMARY KEY"
        ) from exc


def _catalog_row_to_library_row(row: dict[str, Any]) -> list[object]:
    """Map one CatalogExportRow onto a daily-sync ``library`` row tuple."""
    legacy_release_id = _require_legacy_release_id(row, "catalog")
    for name in _REQUIRED_CATALOG_FIELDS:
        if row.get(name) is None:
            raise SourceError(
                f"catalog export row for legacy_release_id {legacy_release_id} is missing "
                f"the required field {name!r}; api.yaml's CatalogExportRow marks it required, "
                "and writing it as NULL would corrupt library.db rather than show up as drift"
            )

    cross_reference_names = row.get("cross_reference_names") or []
    if not isinstance(cross_reference_names, list):
        # A string is truthy and joinable, so without this check a scalar
        # would be split character-by-character into phantom aliases
        # ('S | t | e | r | e | o | l | a | b') that LML then pipe-splits
        # straight into the live search index.
        raise SourceError(
            f"catalog export row for legacy_release_id {legacy_release_id} has "
            f"cross_reference_names of type {type(cross_reference_names).__name__}, "
            "not the array api.yaml specifies"
        )

    return [
        legacy_release_id,
        row["album_title"],
        row["artist_name"],
        row["code_letters"],
        row["code_artist_number"],
        row["code_number"],
        row["genre_name"],
        row["format_name"],
        _empty_if_none(row.get("alternate_artist_name")),
        _empty_if_none(row.get("album_artist")),
        # The wire carries an array precisely so no artist name can be split
        # into phantom aliases by the delimiter; the join happens here, at the
        # one place that writes the SQLite column.
        CROSS_REFERENCE_SEPARATOR.join(cross_reference_names),
    ]


def _catalog_cta_row_to_library_row(row: dict[str, Any]) -> tuple[object, ...]:
    """Map one CatalogCompilationTrackRow onto a ``compilation_track_artist`` row."""
    legacy_release_id = _require_legacy_release_id(row, "compilation-track")
    artist_name = row.get("artist_name")
    if artist_name is None:
        # `required` in the api.yaml schema and NOT NULL in the column
        # beneath it, so this is a contract violation, not a data gap.
        raise SourceError(
            "compilation-track export row is missing the required field 'artist_name' "
            f"(legacy_release_id={legacy_release_id})"
        )
    return (
        legacy_release_id,
        artist_name,
        _empty_if_none(row.get("track_title")),
    )


def _resolve_https_base_url(source: str, *, source_label: str, secret_description: str) -> str:
    """Validate and normalize a base URL that a secret is about to travel to.

    Two URLs now carry a secret -- the catalog exports (a bearer token) and
    the auth service (the sign-in password) -- and they are named by different
    things and protect different secrets, so the caller supplies both labels.
    A message that says "bearer token" when the password is what's at risk
    sends the operator to the wrong env var.
    """
    parts = urlsplit(source)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise SourceError(
            f"{source_label} must be an http(s) base URL "
            f"(e.g. https://api.wxyc.org), got {source!r}"
        )
    if parts.scheme == "http" and (parts.hostname or "") not in _PLAINTEXT_OK_HOSTS:
        raise SourceError(
            f"refusing to send {secret_description} over plaintext http "
            f"to {parts.hostname!r} (from {source_label}) -- use https (plain http is "
            "allowed only for a loopback address, for local testing)"
        )
    return f"{parts.scheme}://{parts.netloc}{parts.path.rstrip('/')}"


def _resolve_backend_base_url(source: str) -> str:
    """Validate and normalize the Backend base URL."""
    return _resolve_https_base_url(
        source,
        source_label="--backend-source",
        secret_description=(
            f"the service-account bearer token (${BACKEND_TOKEN_ENV}, or one minted "
            f"from ${BACKEND_EMAIL_ENV})"
        ),
    )


def _default_auth_url(base_url: str) -> str:
    """Where better-auth lives relative to the Backend base URL.

    Production serves it at https://api.wxyc.org/auth, i.e. the same origin as
    the catalog exports -- so the common case needs no second flag, and
    ``$BACKEND_AUTH_URL`` exists for the environments where that isn't true.
    """
    return f"{base_url.rstrip('/')}/auth"


class _AuthStatusError(Exception):
    """An auth-service call that came back with a status, not a token.

    Internal to the mint path: `_TokenSource` turns each of these into either
    a retry (429 on sign-in, 401 on an exchange) or a ``SourceError``. It
    carries the response headers so the 429 branch can honor a retry hint.
    """

    def __init__(self, code: int, detail: str, headers: Any) -> None:
        super().__init__(f"HTTP {code}: {detail}")
        self.code = code
        self.detail = detail
        self.headers = headers

    def retry_hint_seconds(self) -> float | None:
        """The server's own wait hint, or None when it did not give a usable one.

        The express limiter sends draft-7 ``Retry-After``; better-auth's own
        limiter sends ``X-Retry-After`` and nothing else. Reading only one of
        the two means ignoring the hint from half the limiters that can
        produce this response.

        Returned unclamped, because the caller has to be able to tell a hint
        it can wait out from one it cannot: silently truncating a 15-minute
        window to a 10-second sleep buys a certain second refusal.
        """
        for header in ("Retry-After", "X-Retry-After"):
            raw = self.headers.get(header) if self.headers is not None else None
            if raw is None:
                continue
            try:
                return max(float(str(raw).strip()), 0.0)
            except ValueError:
                continue  # HTTP-date form: treat as "no usable hint"
        return None


def _jwt_expiry_epoch(token: str) -> float:
    """The ``exp`` claim, or the assumed life when it cannot be read.

    A local, signature-unverified decode: this only schedules the *next*
    refresh, and the authority on whether a token is good remains the 401 from
    Backend-Service. An unreadable claim therefore falls back to better-auth's
    documented 15 minutes rather than to "expired" -- the latter would make
    every single ``token()`` call an exchange, six-plus per run, against a
    limiter that allows three every ten seconds.
    """
    try:
        segment = token.split(".")[1]
        padded = segment + "=" * (-len(segment) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
        return float(claims["exp"])
    except Exception:  # noqa: BLE001 - any malformed shape falls back
        return time.time() + _ASSUMED_JWT_TTL_SECONDS


class _TokenSource:
    """Supplies a currently-valid bearer for the catalog exports.

    Two modes:

    - **Static** (``$BACKEND_CATALOG_TOKEN``): hand back what the operator
      supplied. A 401 is terminal -- there is nothing here to refresh from,
      and saying so beats retrying a token that cannot improve.
    - **Credential** (``$BACKEND_CATALOG_EMAIL`` + ``$BACKEND_CATALOG_PASSWORD``):
      sign in once, then exchange that session for a 15-minute JWT as often
      as needed.

    The asymmetry between those two cached values is the whole design, and it
    is dictated by where the rate limiters sit. ``/auth/sign-in`` is capped at
    10 per 15 minutes (express, keyed on the caller's IP) and 3 per 10 seconds
    (better-auth's own); ``/auth/token`` is exempt from the first and
    generously bounded by the second. A parity run can outlive 15 minutes --
    three snapshot attempts over two exports, each with a 300-second budget --
    so refreshes must be exchanges, not sign-ins. The session behind them is
    good for a year server-side, so one sign-in covers the whole run, and only
    an exchange that 401s spends another.
    """

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url
        self._static = os.environ.get(BACKEND_TOKEN_ENV) or None
        self._email = os.environ.get(BACKEND_EMAIL_ENV) or None
        self._password = os.environ.get(BACKEND_PASSWORD_ENV) or None
        self._session: str | None = None
        self._jwt: str | None = None
        self._jwt_expires_at = 0.0
        self._sign_ins = 0
        self._auth_url = ""
        self._origin = ""

        if self._static:
            # Credentials, when they are also set, are this token's fallback
            # rather than dead weight -- but resolving the auth URL now would
            # refuse a plaintext one the run may never touch.
            return
        self._enter_credential_mode()

    def _enter_credential_mode(self) -> None:
        """Validate the credential pair and pin the URL the password goes to."""
        if not (self._email and self._password):
            raise SourceError(
                "no Backend service-account credentials: set "
                f"${BACKEND_EMAIL_ENV} + ${BACKEND_PASSWORD_ENV} (the unattended route -- "
                "the harness signs in and mints a fresh token per run), or "
                f"${BACKEND_TOKEN_ENV} with a JWT you already hold (a one-off; better-auth "
                "JWTs expire after 15 minutes). Either way the principal needs catalog:read."
            )
        self._auth_url = _resolve_https_base_url(
            os.environ.get(BACKEND_AUTH_URL_ENV) or _default_auth_url(self._base_url),
            source_label=f"${BACKEND_AUTH_URL_ENV}",
            secret_description=f"the ${BACKEND_PASSWORD_ENV} sign-in password",
        )
        self._origin = os.environ.get(BACKEND_AUTH_ORIGIN_ENV) or _DEFAULT_AUTH_ORIGIN
        self._static = None

    def token(self) -> str:
        """A bearer valid now -- minting or refreshing if it isn't."""
        if self._static:
            return self._static
        if self._jwt and time.time() < self._jwt_expires_at - _JWT_REFRESH_MARGIN_SECONDS:
            return self._jwt
        self._jwt = self._exchange()
        self._jwt_expires_at = _jwt_expiry_epoch(self._jwt)
        return self._jwt

    def invalidate(self) -> None:
        """Discard the cached JWT after Backend-Service rejected it.

        Drops the *JWT* only. The session it came from is almost certainly
        still good -- a 15-minute token against a year-long session -- and
        re-signing-in here is what would put a long run on a collision course
        with the sign-in limiter.
        """
        if self._static:
            if not (self._email and self._password):
                raise SourceError(
                    f"the ${BACKEND_TOKEN_ENV} token was rejected (401) and cannot be "
                    "refreshed: better-auth JWTs expire after 15 minutes. Mint a fresh one, "
                    f"or set ${BACKEND_EMAIL_ENV} + ${BACKEND_PASSWORD_ENV} so the harness "
                    "can mint per run -- which is what an unattended soak needs."
                )
            # Both are set, which is what an unattended run inherits if the
            # secret #365 originally specified is left in place. Stranding it
            # on a 15-minute-old JWT -- while holding everything needed to
            # mint a fresh one -- would be a self-inflicted outage.
            logger.warning(
                "the pre-minted token was rejected; falling back to the credential pair",
                extra={"step": "backend_producer"},
            )
            _report(
                f"WARNING: ${BACKEND_TOKEN_ENV} was rejected (401); minting from "
                f"${BACKEND_EMAIL_ENV} instead"
            )
            self._enter_credential_mode()
            return
        self._jwt = None
        self._jwt_expires_at = 0.0

    def close(self) -> None:
        """Revoke the session this run minted, if it minted one.

        Backend-Service pins ``session.expiresIn`` to a year, so a session
        left behind is a standalone catalog:read credential with a year to
        run -- and ``admin/set-user-password`` revokes nothing, so a password
        rotation would not clear it. Unattended runs would accumulate one per
        run.

        Best-effort by design: the export has already happened, and the worst
        case of a failed revocation is the state every run had before this
        existed. A token supplied through ``$BACKEND_CATALOG_TOKEN`` is not
        this run's to revoke and is left alone.
        """
        session, self._session = self._session, None
        self._jwt = None
        self._jwt_expires_at = 0.0
        if not session:
            return
        url = self._auth_url + _SIGN_OUT_PATH
        request = Request(
            url,
            data=b"{}",
            headers={
                "Authorization": f"Bearer {session}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Origin": self._origin,
            },
        )
        try:
            with _opener.open(request, timeout=_SIGN_OUT_TIMEOUT_SECONDS):
                pass
        except Exception as exc:  # noqa: BLE001 - see below: nothing may escape here
            # Deliberately every exception, not just OSError. This runs from a
            # `finally`, where anything raised *replaces* the exception in
            # flight -- so a redirect on the cleanup call (SourceError, a
            # RuntimeError) would overwrite the torn-snapshot or empty-export
            # diagnosis the operator actually needs, and on the success path
            # would fail a build that had already finished.
            #
            # Named loudly: the leftover session outlives this process by a
            # year, so an operator who sees this may want to revoke it by hand
            # (POST /auth/admin/revoke-user-sessions).
            logger.warning(
                "could not sign out the parity session; it stays valid server-side",
                extra={"step": "backend_producer", "error": str(exc)},
            )
            _report(f"WARNING: sign-out at {url} failed ({exc}); the session was not revoked")

    def _exchange(self) -> str:
        """Trade the cached session for a JWT, re-signing-in at most once.

        Bounded by its own attempt budget. Gating this loop on the sign-in
        allowance instead would mean that once a run had spent that allowance
        -- on a rate-limited first sign-in, or on one legitimate dead-session
        recovery -- every later refresh would raise without issuing a single
        exchange, against a session that was still perfectly good.
        """
        url = self._auth_url + _EXCHANGE_PATH
        last: _AuthStatusError | None = None
        for _attempt in range(_MAX_EXCHANGE_ATTEMPTS):
            session = self._session or self._sign_in()
            request = Request(
                url,
                headers={
                    "Authorization": f"Bearer {session}",
                    "Origin": self._origin,
                    "Accept": "application/json",
                },
            )
            try:
                return _auth_token_from(request, "the token exchange")
            except _AuthStatusError as exc:
                if exc.code != 401:
                    raise SourceError(
                        f"the token exchange at {url} failed with HTTP {exc.code}: {exc.detail}"
                    ) from exc
                # The session is dead (rotated, revoked, expired). That is the
                # one condition worth another sign-in.
                last = exc
                self._session = None
        raise SourceError(
            f"the token exchange at {url} returned HTTP 401 on every one of "
            f"{_MAX_EXCHANGE_ATTEMPTS} attempts as ${BACKEND_EMAIL_ENV}: "
            f"{last.detail if last else 'no detail'}. Check that the service account "
            "exists, is not banned, and holds catalog:read."
        )

    def _sign_in(self) -> str:
        """Exchange the password for a session token. One retry on a 429.

        Costs one unit of the sign-in allowance however many HTTP attempts it
        takes -- the 429 retry is part of this sign-in, not a second one.
        """
        url = self._auth_url + _SIGN_IN_PATH
        if self._sign_ins >= _MAX_SIGN_INS:
            raise SourceError(
                f"signing in as ${BACKEND_EMAIL_ENV} at {url} already ran "
                f"{self._sign_ins} times this run and the session it returns keeps coming "
                "back dead. Check that the service account exists, is not banned, and that "
                f"${BACKEND_PASSWORD_ENV} is current."
            )
        self._sign_ins += 1
        body = json.dumps({"email": self._email, "password": self._password}).encode("utf-8")
        for attempt in (1, 2):
            request = Request(
                url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    # Absent, better-auth's CSRF guard 400s with
                    # MISSING_OR_NULL_ORIGIN before ever checking the password.
                    "Origin": self._origin,
                },
            )
            try:
                session = _auth_token_from(request, "sign-in")
            except _AuthStatusError as exc:
                if exc.code == 429 and attempt == 1:
                    hint = exc.retry_hint_seconds()
                    if hint is None:
                        # Both limiters in front of this path send a hint, so
                        # a 429 without one came from something else (a proxy,
                        # a WAF) whose window we cannot guess. Retrying blind
                        # into what may be the express limiter's 15 minutes
                        # spends a second slot from a budget shared with every
                        # DJ signing in, and learns nothing.
                        raise SourceError(
                            f"sign-in as ${BACKEND_EMAIL_ENV} at {url} was rate limited "
                            "(HTTP 429) with no usable Retry-After/X-Retry-After hint, so there "
                            "is no way to tell a "
                            "10-second window from a 15-minute one. Re-run later, and check "
                            "whether something else is signing in as this service account."
                        ) from exc
                    if hint > _SIGN_IN_RETRY_CAP_SECONDS:
                        # The express limiter's window is 15 minutes. Sleeping
                        # the cap and retrying into a window this long is a
                        # guaranteed second refusal; the operator wants the
                        # real number, not a truncated one.
                        raise SourceError(
                            f"sign-in as ${BACKEND_EMAIL_ENV} at {url} was rate limited "
                            f"(HTTP 429) and asks for {hint:g}s, longer than the "
                            f"{_SIGN_IN_RETRY_CAP_SECONDS}s "
                            "this run will wait. Re-run after that window, and check whether "
                            "something else is signing in as this service account."
                        ) from exc
                    # A floor, because `Retry-After: 0` is not an invitation to
                    # retry inside the same tick of whatever window refused us.
                    delay = max(hint, _SIGN_IN_RETRY_FLOOR_SECONDS)
                    logger.warning(
                        "sign-in was rate limited; retrying once",
                        extra={"step": "backend_producer", "delay_seconds": delay},
                    )
                    time.sleep(delay)
                    continue
                raise SourceError(
                    f"sign-in as ${BACKEND_EMAIL_ENV} at {url} failed with "
                    f"HTTP {exc.code}: {exc.detail}"
                ) from exc
            self._session = session
            return session
        raise AssertionError("unreachable: the retry loop either returns or raises")


def _auth_token_from(request: Request, what: str) -> str:
    """Run one auth-service call and return its ``token`` field.

    Both auth responses are ``{"token": ...}`` -- a session from sign-in, a JWT
    from the exchange. Non-2xx bodies are surfaced (truncated) because they
    carry a diagnosis and never a credential; 2xx bodies are *not*, because
    they carry exactly the credential this whole path exists to protect.
    """
    try:
        with _opener.open(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
            body = response.read()
    except SourceError:
        raise  # the cross-origin redirect guard
    except HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", "replace")[:200].replace("\n", " ").strip()
        except Exception:  # noqa: BLE001 - a body we cannot read is not the failure
            pass
        raise _AuthStatusError(exc.code, detail, exc.headers) from exc
    except (URLError, OSError) as exc:
        raise SourceError(f"{what} at {request.full_url} failed: {exc}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SourceError(f"{what} at {request.full_url} returned a non-JSON body") from exc
    token = payload.get("token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token:
        raise SourceError(
            f"{what} at {request.full_url} returned 200 with no token field; "
            "the auth response shape has changed"
        )
    return token


def _map_ndjson_lines(
    url: str, stream: Any, mapper: Callable[[dict[str, Any]], _Row]
) -> list[_Row]:
    """Parse an NDJSON byte stream line by line, mapping each row as it arrives.

    Deliberately incremental: the two prod exports are 28.5 MB + 13.1 MB
    across 64,815 + 144,778 rows, and reading the whole body, decoding it,
    splitting it into a line list, and keeping every raw dict alive would
    hold four copies at once for a job whose logical working set is one row.
    Mapping here also drops each parsed dict as soon as its library row is
    built.
    """
    rows: list[_Row] = []
    for lineno, raw in enumerate(stream, start=1):
        line = raw.decode("utf-8").strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SourceError(f"{url} line {lineno} is not valid JSON: {exc}") from exc
        rows.append(mapper(payload))
    return rows


def _fetch_ndjson(
    url: str, token_source: _TokenSource, mapper: Callable[[dict[str, Any]], _Row]
) -> tuple[list[_Row], str]:
    """GET a gzipped-NDJSON export; return its mapped rows and the watermark.

    Retries exactly once on a 401, after asking ``token_source`` for a fresh
    bearer: a 15-minute JWT can expire between the two exports of a snapshot
    pair, or partway through a re-fetch, and re-reading the catalog is far
    cheaper than failing a parity day over a token turnover. Once, not in a
    loop -- a second 401 is an authorization problem, not an expiry, and
    should say so instead of retrying until the limiter notices.
    """
    for attempt in (1, 2):
        try:
            return _fetch_ndjson_once(url, token_source.token(), mapper)
        except _AuthStatusError as exc:
            if exc.code == 401 and attempt == 1:
                logger.info(
                    "catalog export returned 401; refreshing the service-account token",
                    extra={"step": "backend_producer", "url": url},
                )
                token_source.invalidate()
                continue
            raise SourceError(f"failed to fetch {url}: HTTP {exc.code}: {exc.detail}") from exc
    raise AssertionError("unreachable: the retry loop either returns or raises")


def _fetch_ndjson_once(
    url: str, token: str, mapper: Callable[[dict[str, Any]], _Row]
) -> tuple[list[_Row], str]:
    """One export GET. Raises ``_AuthStatusError`` so the caller can refresh."""
    # Scheme is validated to http/https in _resolve_backend_base_url, so this
    # cannot be tricked into a file:// or other local-handler fetch, and
    # _opener refuses any cross-origin redirect that would carry the token on.
    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/x-ndjson",
            "Accept-Encoding": "gzip",
        },
    )
    try:
        with _opener.open(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
            watermark = response.headers.get("Last-Modified")
            if not watermark:
                # Without it the cross-endpoint consistency rule below is
                # unenforceable, and silently pairing two unrelated snapshots
                # is worse than stopping.
                raise SourceError(
                    f"{url} returned no Last-Modified header; the catalog exports must carry "
                    "the library_watermark for the two-request snapshot to be checkable"
                )
            encoding = (response.headers.get("Content-Encoding") or "").lower()
            stream = gzip.GzipFile(fileobj=response) if encoding == "gzip" else response
            rows = _map_ndjson_lines(url, stream, mapper)
    except SourceError:
        raise
    except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
        raise SourceError(
            f"{url} declared Content-Encoding: gzip but did not decompress: {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise SourceError(f"{url} returned a body that is not valid UTF-8: {exc}") from exc
    except HTTPError as exc:
        # Must precede the URLError clause below -- HTTPError subclasses it, so
        # the generic branch would swallow the status and with it any chance of
        # telling "the token just expired" apart from "the export is broken".
        detail = ""
        try:
            detail = exc.read().decode("utf-8", "replace")[:200].replace("\n", " ").strip()
        except Exception:  # noqa: BLE001 - a body we cannot read is not the failure
            pass
        raise _AuthStatusError(exc.code, detail, exc.headers) from exc
    except (URLError, OSError) as exc:
        raise SourceError(f"failed to fetch {url}: {exc}") from exc
    return rows, watermark


def _fetch_consistent_snapshot(
    base_url: str, token_source: _TokenSource
) -> tuple[list[_Row], list[_Row]]:
    """Fetch both exports, retrying until they describe the same catalog snapshot.

    Returns rows already mapped into ``library`` / ``compilation_track_artist``
    shape, so both id sets are plain ints and nothing raw survives the fetch.

    Note that ``Last-Modified`` is an HTTP-date, i.e. whole seconds: two
    distinct watermarks inside the same second compare equal, so a write
    landing between the two GETs in that window slips through. The
    dangling-id check below catches the CTA->catalog direction of that tear;
    the other direction (a catalog row added mid-pair) would show up as a
    single spurious ``extra_in_backend`` and self-heals on the next run,
    because the exports are served from a per-watermark cache.
    """
    reason = "no attempt was made"
    for attempt in range(1, _SNAPSHOT_ATTEMPTS + 1):
        catalog_rows, catalog_watermark = _fetch_ndjson(
            base_url + _CATALOG_PATH, token_source, _catalog_row_to_library_row
        )
        cta_rows, cta_watermark = _fetch_ndjson(
            base_url + _COMPILATION_TRACKS_PATH, token_source, _catalog_cta_row_to_library_row
        )

        if catalog_watermark != cta_watermark:
            reason = (
                "the catalog watermark advanced between the two exports "
                f"({catalog_watermark!r} -> {cta_watermark!r})"
            )
        else:
            catalog_ids = {row[0] for row in catalog_rows}
            dangling = sorted({row[0] for row in cta_rows} - catalog_ids)
            if dangling:
                # Server-side row eligibility should make this impossible within
                # one snapshot, so seeing it means the pair is torn in a way the
                # watermark didn't reveal. Same remedy: re-fetch.
                reason = (
                    "the compilation-track export references legacy_release_id(s) "
                    f"{dangling[:5]} with no row in the catalog export "
                    f"({len(dangling)} total)"
                )
            else:
                if attempt > 1:
                    logger.info(
                        "catalog snapshot settled",
                        extra={"step": "backend_producer", "attempt": attempt},
                    )
                return catalog_rows, cta_rows

        logger.warning(
            "re-fetching the catalog snapshot",
            extra={"step": "backend_producer", "attempt": attempt, "reason": reason},
        )

    raise SourceError(
        f"could not read a consistent catalog snapshot from {base_url} in "
        f"{_SNAPSHOT_ATTEMPTS} attempts: {reason}"
    )


def _build_library_db_from_backend(source: str, output_path: str) -> None:
    """Build a daily-sync-shaped library.db from Backend-Service over HTTP.

    Args:
        source: Backend base URL, e.g. ``https://api.wxyc.org``.
        output_path: Where to write the SQLite database. Must not exist.

    Raises:
        SourceError: on a refused overwrite, a bad/plaintext URL, a
            cross-origin redirect, missing credentials (neither
            ``$BACKEND_CATALOG_TOKEN`` nor the ``$BACKEND_CATALOG_EMAIL`` /
            ``$BACKEND_CATALOG_PASSWORD`` pair), a sign-in or token-exchange
            failure, a fetch or decode failure, a torn snapshot, an empty
            catalog, or a catalog row that violates the api.yaml contract.
    """
    _require_absent(output_path, "backend")
    base_url = _resolve_backend_base_url(source)
    token_source = _TokenSource(base_url)
    try:
        _build_from_backend_snapshot(base_url, output_path, token_source)
    finally:
        # The failure path is the one that would leak most: a run that dies
        # mid-export has still minted a year-long session.
        token_source.close()


def _build_from_backend_snapshot(
    base_url: str, output_path: str, token_source: _TokenSource
) -> None:
    """Fetch a consistent snapshot and write it, with the token source live."""
    library_rows, compilation_rows = _fetch_consistent_snapshot(base_url, token_source)
    if not library_rows:
        # A broken export query, an over-narrow token scope, or a truncated
        # cached buffer all surface as a 200 with no rows. Building from it
        # would report the entire catalog as missing_in_backend -- an
        # operator reading that sees catastrophic drift, not a producer that
        # read nothing. And post-cutover this producer IS the daily build.
        raise SourceError(
            f"{base_url}{_CATALOG_PATH} returned no rows; a catalog export is never "
            "legitimately empty, so this is a producer failure (check the token's scope "
            "and the export query) rather than total drift"
        )
    if not compilation_rows:
        # Supplementary, and genuinely absent on some sources -- so a warning
        # rather than a failure, matching the MySQL side's graceful
        # degradation. Loud, because in prod it is ~144k rows and its absence
        # would otherwise land in the report as cta_missing.
        logger.warning(
            "the compilation-track export returned no rows; building without the table",
            extra={"step": "backend_producer", "source": base_url},
        )
        _report(
            f"WARNING: {base_url}{_COMPILATION_TRACKS_PATH} returned no rows; "
            "building without a compilation_track_artist table"
        )

    count = _build_into(output_path, "backend", library_rows, compilation_rows or None)
    logger.info(
        "built Backend-sourced library.db",
        extra={
            "step": "backend_producer",
            "rows": count,
            "compilation_track_rows": len(compilation_rows),
            "output": output_path,
        },
    )
    _report(f"Exported {count} rows to {output_path} (source: {base_url})")


def _default_mysql_runner(argv: Sequence[str], env: dict[str, str], stdout_path: str) -> bool:
    """Run the ``mysql`` CLI, capturing stdout to ``stdout_path``.

    Returns True on exit 0. stderr is echoed so a failure is diagnosable
    (sync-library.sh does the same, appending it to the ETL log).
    """
    with open(stdout_path, "wb") as stdout:
        # argv is built by _mysql_invocation; no shell, no interpolation.
        completed = subprocess.run(
            list(argv),
            env={**os.environ, **env},
            stdout=stdout,
            stderr=subprocess.PIPE,
            check=False,
        )
    if completed.stderr:
        sys.stderr.write(completed.stderr.decode("utf-8", errors="replace"))
    return completed.returncode == 0


# Module-level seam so tests can substitute the CLI. Rebound, not wrapped, so
# `_build_library_db_from_mysql` picks it up at call time.
_mysql_runner = _default_mysql_runner


def _mysql_invocation(source: str) -> tuple[list[str], dict[str, str]]:
    """Build the ``mysql`` argv + env from a ``mysql://`` DSN.

    Mirrors sync-library.sh: batch (``-B``) + raw (``-N``) mode over the CLI
    rather than a Python driver, because tubafrenzy runs a MySQL old enough
    that the drivers can't authenticate against it. The password reaches the
    CLI through ``MYSQL_PWD``, never its argv.

    Prefer ``$LIBRARY_DB_PASSWORD`` over a password embedded in the DSN. The
    DSN reaches *this* process on the command line, so an embedded password
    is visible to `ps` for the whole run and echoed by any ``set -x`` or
    GitHub Actions command trace -- and a password containing ``/``, ``#``,
    ``?`` or ``%`` has to be percent-encoded or ``urlsplit`` silently
    mis-slices the DSN around it.
    """
    parts = urlsplit(source)
    if parts.scheme not in ("mysql", "mysql+pymysql"):
        raise SourceError(
            f"--mysql-source must be a mysql:// DSN (mysql://user@host:port/dbname), got {source!r}"
        )
    database = parts.path.lstrip("/")
    if not parts.hostname or not database:
        raise SourceError(f"--mysql-source is missing a host and/or database name: {source!r}")
    try:
        port = parts.port or 3306
    except ValueError as exc:
        # Reached whenever the netloc's `:`-suffix isn't numeric -- most often
        # an un-encoded `/` in the password, which moves the real host:port
        # into the path and leaves the password fragment where the port
        # belongs. Percent-encode it, or (better) use $LIBRARY_DB_PASSWORD.
        raise SourceError(
            f"--mysql-source has a malformed port in {source!r} ({exc}). If the password "
            f"contains '/', '#', '?' or '%' it must be percent-encoded -- or supply it via "
            f"${MYSQL_PASSWORD_ENV} and leave it out of the DSN entirely."
        ) from exc

    argv = ["mysql", "-h", parts.hostname, "-P", str(port)]
    if parts.username:
        argv += ["-u", unquote(parts.username)]
    argv += ["--default-character-set=utf8", "-B", "-N", database]
    password = os.environ.get(MYSQL_PASSWORD_ENV) or unquote(parts.password or "")
    return argv, {"MYSQL_PWD": password}


def _build_library_db_from_mysql(source: str, output_path: str) -> None:
    """Build the baseline library.db from tubafrenzy MySQL, the way prod does.

    Args:
        source: ``mysql://user@host:port/dbname``, with the password in
            ``$LIBRARY_DB_PASSWORD``. Point it at a local port when
            tunnelling, as sync-library.sh does.
        output_path: Where to write the SQLite database. Must not exist.

    Raises:
        SourceError: on a refused overwrite, a malformed DSN, or a failed
            library export query. A failed *compilation-track* query is
            tolerated (that table is supplementary and absent on some
            sources), matching sync-library.sh.
    """
    _require_absent(output_path, "mysql")
    argv, env = _mysql_invocation(source)

    scratch = tempfile.mkdtemp(prefix="catalog-parity-mysql-")
    try:
        library_tsv = os.path.join(scratch, "library.tsv")
        if not _mysql_runner([*argv, "-e", LIBRARY_SELECT_SQL], env, library_tsv):
            raise SourceError(
                "the MySQL library export query failed (see stderr above); "
                "no library.db was written"
            )

        cta_tsv = os.path.join(scratch, "compilation_track_artist.tsv")
        cta_rows = None
        if _mysql_runner([*argv, "-e", COMPILATION_TRACK_SELECT_SQL], env, cta_tsv):
            if os.path.getsize(cta_tsv) > 0:
                cta_rows = parse_compilation_track_tsv(cta_tsv)
        else:
            logger.warning(
                "compilation-track export unavailable; building without it",
                extra={"step": "mysql_producer"},
            )

        count = _build_into(output_path, "mysql", parse_library_tsv(library_tsv), cta_rows)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    logger.info(
        "built MySQL-sourced library.db",
        extra={"step": "mysql_producer", "rows": count, "output": output_path},
    )
    _report(f"Exported {count} rows to {output_path} (source: MySQL)")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Diff two already-built library.db SQLite files (a MySQL-sourced "
            "daily build vs. a Backend-sourced build) for the discogs-etl#346 "
            "catalog-parity harness. Compares library rows (keyed by id), the "
            "compilation_track_artist table, and row-set membership. "
            "Read-only -- never writes to either input."
        ),
    )
    p.add_argument(
        "--mysql-db",
        default=None,
        help="Path to the MySQL-sourced (daily-sync) library.db.",
    )
    p.add_argument(
        "--backend-db",
        default=None,
        help="Path to the Backend-sourced library.db.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON object on stdout (machine-readable).",
    )
    p.add_argument(
        "--mysql-source",
        default=None,
        metavar="DSN",
        help=(
            "Build the MySQL-sourced library.db first, from this "
            "mysql://user@host:port/dbname DSN, writing it to --mysql-db "
            f"(which must not already exist). Password via ${MYSQL_PASSWORD_ENV} -- "
            "putting it in the DSN leaves it in this process's argv."
        ),
    )
    p.add_argument(
        "--backend-source",
        default=None,
        metavar="URL",
        help=(
            "Build the Backend-sourced library.db first, from this base URL "
            f"(e.g. https://api.wxyc.org), writing it to --backend-db (which must "
            f"not already exist). Needs a catalog:read service account: "
            f"${BACKEND_EMAIL_ENV} + ${BACKEND_PASSWORD_ENV} (minted per run), or "
            f"${BACKEND_TOKEN_ENV} for a one-off with a JWT already in hand."
        ),
    )
    p.add_argument(
        "--residue-ledger",
        default=None,
        metavar="PATH",
        help=(
            "Path to the vendored discogs-etl#370 residue ledger JSON. Defaults "
            "to vendor/parity-residue/ledger.json, resolved relative to this "
            "script's own location (never the cwd), so the soak needs no "
            "argument. Pass the literal 'none' to run without a ledger -- "
            "row-level expected/unexplained classification and `clean` are "
            "then unavailable (`clean` reports null rather than false)."
        ),
    )
    p.add_argument(
        "--fail-on-drift",
        action="store_true",
        help=(
            "Exit 4 when the verdict is not clean. Cannot be combined with "
            "--residue-ledger none (exit 2) -- that combination asks to fail "
            "on drift while refusing the definition of expected drift."
        ),
    )
    return p


def _print_human(result: ParityDiff) -> None:
    print(f"matched:            {result.matched:>10}")
    print(f"missing_in_backend: {result.missing_in_backend:>10}")
    if result.missing_in_backend_ids:
        print(f"  ids: {result.missing_in_backend_ids}")
    print(f"extra_in_backend:   {result.extra_in_backend:>10}")
    if result.extra_in_backend_ids:
        print(f"  ids: {result.extra_in_backend_ids}")
    print("field_mismatches:")
    for col in DIFF_COLUMNS:
        print(f"  {col:<24} {result.field_mismatches[col]:>6}")
    print(f"cta_missing:        {result.cta_missing:>10}")
    print(f"cta_extra:          {result.cta_extra:>10}")
    print(f"clean:              {result.clean!s:>10}")
    print(f"missing_expected:   {result.missing_expected:>10}")
    print(f"missing_unexplained:{result.missing_unexplained:>10}")
    print(f"extra_expected:     {result.extra_expected:>10}")
    print(f"extra_unexplained:  {result.extra_unexplained:>10}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    init_logger(repo="discogs-etl", tool="discogs-etl catalog_parity_diff")

    # Each --*-source needs its matching --*-db as the output path, so the
    # required-flags check runs before any build.
    for source, output, flag in (
        (args.mysql_source, args.mysql_db, "mysql"),
        (args.backend_source, args.backend_db, "backend"),
    ):
        if source is not None and not output:
            print(
                f"error: --{flag}-source requires --{flag}-db, the path to write the "
                "built library.db to.",
                file=sys.stderr,
            )
            parser.print_usage(sys.stderr)
            return 2

    if not args.mysql_db or not args.backend_db:
        print("error: --mysql-db and --backend-db are both required.", file=sys.stderr)
        parser.print_usage(sys.stderr)
        return 2

    residue_ledger_arg = args.residue_ledger
    ledger_disabled = (
        residue_ledger_arg is not None and residue_ledger_arg.strip().lower() == "none"
    )
    if args.fail_on_drift and ledger_disabled:
        print(
            "error: --fail-on-drift cannot be combined with --residue-ledger none -- "
            "that asks to fail on drift while refusing the definition of expected drift.",
            file=sys.stderr,
        )
        parser.print_usage(sys.stderr)
        return 2

    try:
        # Validate BOTH output paths before either build runs: checking the
        # backend path only when its turn comes would burn the entire MySQL
        # export before refusing a path the operator already had on disk.
        if args.mysql_source is not None:
            _require_absent(args.mysql_db, "mysql")
        if args.backend_source is not None:
            _require_absent(args.backend_db, "backend")

        if args.mysql_source is not None:
            _build_library_db_from_mysql(args.mysql_source, args.mysql_db)
        if args.backend_source is not None:
            _build_library_db_from_backend(args.backend_source, args.backend_db)
    except SourceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:
        # Exit 3 is the documented contract for *any* producer failure. Without
        # this, a `mysql` binary that isn't on PATH, an unreadable output
        # directory, or a malformed DSN escapes as a raw traceback and exit 1.
        logger.exception("catalog parity producer failed")
        print(f"error: {exc}", file=sys.stderr)
        return 3

    try:
        if ledger_disabled:
            ledger: ResidueLedger | None = None
        else:
            ledger_path = (
                Path(residue_ledger_arg)
                if residue_ledger_arg is not None
                else _default_residue_ledger_path()
            )
            ledger = load_residue_ledger(ledger_path)
    except SourceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3

    try:
        result = run_diff(args.mysql_db, args.backend_db, ledger=ledger)
    except SourceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:  # pragma: no cover - unexpected read-time failure
        logger.exception("catalog parity diff failed")
        print(f"error: {exc}", file=sys.stderr)
        return 3

    if args.json:
        print(json.dumps(asdict(result)))
    else:
        _print_human(result)

    logger.info(
        "catalog parity diff complete",
        extra={
            "step": "catalog_parity_diff",
            "matched": result.matched,
            "missing_in_backend": result.missing_in_backend,
            "extra_in_backend": result.extra_in_backend,
            "cta_missing": result.cta_missing,
            "cta_extra": result.cta_extra,
            "clean": result.clean,
            "missing_expected": result.missing_expected,
            "missing_unexplained": result.missing_unexplained,
            "extra_expected": result.extra_expected,
            "extra_unexplained": result.extra_unexplained,
        },
    )

    if args.fail_on_drift and not result.clean:
        return 4
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
