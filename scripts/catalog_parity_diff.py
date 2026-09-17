"""Diff-two-files core of the discogs-etl#346 catalog-parity harness.

discogs-etl builds the production ``library.db`` nightly from tubafrenzy's
MySQL database (``scripts/sync-library.sh``). To retire that MySQL
dependency ahead of the 2026-09-07 tubafrenzy turndown, the daily build must
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
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile

# Kept despite having no caller left in this module: the producers moved to
# lib/, but tests/unit/test_catalog_parity_diff.py still reaches the stdlib
# clock through this module (`monkeypatch.setattr(mod.time, "sleep", ...)`) to
# keep the sign-in retry tests instant. Patching a module object mutates it
# globally, so the producer honours it from its new home -- but only if this
# name still resolves here.
import time  # noqa: F401
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import (
    Request,
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
from lib.backend_library_source import _DEFAULT_AUTH_ORIGIN as _DEFAULT_AUTH_ORIGIN  # noqa: E402
from lib.backend_library_source import (  # noqa: E402
    _HTTP_TIMEOUT_SECONDS,
    BACKEND_EMAIL_ENV,
    BACKEND_PASSWORD_ENV,
    BACKEND_TOKEN_ENV,
    _AuthStatusError,
    _build_library_db_from_backend,
    _fetch_consistent_snapshot,
    _opener,
    _require_legacy_release_id,
    _resolve_backend_base_url,
    _TokenSource,
)
from lib.backend_library_source import (  # noqa: E402
    _JWT_REFRESH_MARGIN_SECONDS as _JWT_REFRESH_MARGIN_SECONDS,
)

# Re-exported with redundant aliases: these four are not referenced by the code
# below, but the harness's tests reach them through this module rather than the
# one that now defines them, so dropping them would break the very tests that
# prove this extraction changed nothing. The ``X as X`` spelling is the
# explicit-re-export convention, and it keeps the linter from deleting them again.
from lib.backend_library_source import BACKEND_AUTH_URL_ENV as BACKEND_AUTH_URL_ENV  # noqa: E402
from lib.backend_library_source import _default_auth_url as _default_auth_url  # noqa: E402

# The two producers were split out in WXYC/discogs-etl#346 so the daily sync can
# build a Backend-sourced library.db without importing this harness. They are
# re-exported here under their original private names because this module's
# tests -- and the MySQL producer below -- already call them that way, which
# keeps the extraction reviewable as a move rather than a rewrite.
from lib.catalog_source_common import (  # noqa: E402
    SourceError,
    _build_into,
    _report,
    _require_absent,
)
from lib.fffd_pair_capture import (  # noqa: E402
    CtaRow,
    ResolvedFffdPair,
    UnresolvedFffdPair,
    find_fffd_pairs,
    has_fffd,
    render_pending_cta_repair_values,
)
from lib.library_db import (  # noqa: E402
    CROSS_REFERENCE_SEPARATOR,
    LIBRARY_COLUMNS,
    parse_compilation_track_tsv,
    parse_library_tsv,
)
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)


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

    **The artist and title checks are spelled as an explicit empty check
    (``value is None or value.strip() == ""``), not ``_normalize(value) is
    None``.** Measured 2026-08-14 against prod tubafrenzy MySQL (#375):
    ``TITLE IS NULL`` and ``PRESENTATION_NAME IS NULL`` both count 0, and
    neither column holds the uppercase string ``"NULL"`` either (``BINARY
    TITLE = 'NULL'`` / ``BINARY PRESENTATION_NAME = 'NULL'`` both count 0).
    There is no SQL NULL arriving as the literal text ``"NULL"`` for these
    two columns to catch -- the premise the broad ``_normalize`` reuse was
    written against does not hold, so routing through ``_normalize`` was
    over-matching: an album or artist genuinely titled ``"NULL"`` is a legal
    row Backend imports rather than skips, and folding it into
    ``empty_artist_name`` / ``empty_album_title`` would silently forgive
    real drift instead of reporting it.

    The six ledger ids (``residue-ledger.md`` Set 2 -- ``21107, 39290,
    51871, 52374, 65301, 66329``) are byte-exact empty strings in prod
    (``LENGTH(TITLE) = 0``), not SQL NULLs and not whitespace, so the
    narrower ``.strip() == ""`` check still catches all six -- nothing is
    lost by dropping the ``_normalize`` reuse. Two rows (ids 18930, 55924)
    hold the mixed-case string ``"Null"``; ``_normalize`` compares
    case-sensitively (``stripped == "NULL"``, not case-folded), so neither
    the old predicate nor this one ever matched them -- their classification
    is unchanged by this narrowing.

    This narrowing is **inert on current production data**: zero rows in
    either column hold the uppercase string ``"NULL"``, so zero
    classifications change today. What it buys is forward-looking -- if a
    SQL NULL ever does appear in these columns, ``mysql -B -N`` on this
    server prints it as the literal text ``"NULL"`` (verified in prod,
    documented at ``scripts/sync-library.sh``'s SELECT comment, which
    ``IFNULL``-wrapped ``ALBUM_ARTIST`` for exactly this reason after 64,780
    rows leaked a literal ``'NULL'`` into ``library_fts``), and
    ``parse_library_tsv`` has no handling for that literal (it only maps
    ``\\N``). After this narrowing such a row is reported as unexplained
    drift -- loud, and correct -- rather than silently forgiven as expected
    residue, which is the point at which an ``IFNULL`` wrap on ``TITLE`` /
    ``PRESENTATION_NAME`` in ``LIBRARY_SELECT_SQL`` and
    ``scripts/sync-library.sh``'s SELECT would become worth landing. Neither
    pinned SELECT is touched here.

    ``_load_cta_counts`` already uses this same narrower ``str.strip() ==
    ""`` check, and the two predicates are consistent for the same
    underlying reason now confirmed for both: ``COMPILATION_TRACK_ARTIST.
    ARTIST_NAME`` is documented ``NOT NULL``, and ``TITLE`` /
    ``PRESENTATION_NAME`` are measured to hold zero SQL NULLs -- in both
    cases a ``"NULL"`` in the column can only be a genuine name/title, never
    a SQL NULL wearing a disguise.

    **``_normalize`` itself deliberately keeps the ``"NULL"`` collapse, so
    the harness is asymmetric on purpose and this is what that costs.** The
    comparison path still routes through it -- ``_make_tab_nl_classifier``
    opens with ``_normalize(backend_value) == _normalize(mysql_raw)`` -- so a
    matched row whose mysql ``title`` is the literal ``"NULL"`` and whose
    Backend ``title`` is ``''`` still reports ``agree`` rather than
    ``mismatch``. That is not an oversight left behind by this narrowing: the
    two predicates answer different questions. Classification asks whether
    *this row's own* value is absent, where a literal ``"NULL"`` is a real
    value and forgiving it hides drift. Comparison asks whether the two sides
    agree, and the Backend side still holds rows written before
    ``sync-library.sh``'s ``ALBUM_ARTIST`` ``IFNULL`` fix -- 64,780 of them
    leaked the literal text ``'NULL'`` into ``library_fts`` -- so collapsing
    it there is what lets a corrected mysql side compare equal to a
    not-yet-rewritten Backend side. Narrowing ``_normalize`` would change all
    ~14 of its call sites and manufacture drift across every column, so it
    stays broad until those Backend-side values are rewritten.
    """
    genre = mysql_row.get("genre")
    if is_db_only_genre(genre if isinstance(genre, str) else None):
        return "db_only_genre"
    fmt = mysql_row.get("format")
    if parse_format_and_discs(fmt if isinstance(fmt, str) else "") is None:
        return "unparseable_format"
    artist = mysql_row.get("artist")
    if artist is None or (isinstance(artist, str) and artist.strip() == ""):
        return "empty_artist_name"
    title = mysql_row.get("title")
    if title is None or (isinstance(title, str) and title.strip() == ""):
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


def _load_cta_rows(conn: sqlite3.Connection, label: str) -> list[CtaRow]:
    """Read ``compilation_track_artist`` as individual rows, for
    ``--capture-fffd-cta-pairs`` (WXYC/Backend-Service#2152).

    Unlike ``_load_cta_counts``, this returns each row's own identity rather
    than a normalized multiset, and applies NO normalization at all -- no
    ``_normalize``, no ``_tab_nl_sub``. The FFFD pairing rule
    (``lib/fffd_pair_capture.find_fffd_pairs``) is a literal,
    position-by-position comparison against the raw MySQL-sourced string;
    the general harness's normalization passes are calibrated for the
    field-mismatch diff, not for this, and applying one here would risk
    silently absorbing the very corruption this mode exists to capture.

    Returns ``[]`` (with a loud warning, not a silent one) when the table is
    absent -- matching ``_load_cta_counts``'s graceful degradation for the
    general diff, but logged here because an empty capture on an otherwise
    successful run reads as "nothing to do" while a missing table usually
    means the wrong ``--mysql-db`` path was passed.
    """
    if not _table_exists(conn, "compilation_track_artist"):
        logger.warning(
            "library.db has no compilation_track_artist table; the FFFD capture will "
            "find zero candidates for every corrupt Backend row",
            extra={"step": "fffd_cta_capture", "side": label},
        )
        return []
    cols = ", ".join(CTA_COLUMNS)
    rows = conn.execute(f"SELECT {cols} FROM compilation_track_artist").fetchall()
    return [
        CtaRow(legacy_release_id=int(row[0]), artist_name=row[1], track_title=row[2])
        for row in rows
    ]


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


# Env var holding the tubafrenzy MySQL password. It is read from the
# environment rather than from the ``--mysql-source`` DSN because a DSN
# password sits in *this* process's argv for the whole run -- readable by any
# `ps`, and echoed by a `set -x` or a GitHub Actions command trace. (It is
# still accepted inside the DSN for local one-offs; the env var wins.)
MYSQL_PASSWORD_ENV = "LIBRARY_DB_PASSWORD"


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


def _catalog_row_to_id_map_entry(row: dict[str, Any]) -> tuple[int, int]:
    """Map one CatalogExportRow to ``(legacy_release_id, Backend serial id)``.

    Needed only by ``--capture-fffd-cta-pairs``. The bulk CTA export
    deliberately omits both the CTA row id and ``track_position``
    (api.yaml's ``CatalogCompilationTrackRow`` docstring: "shipping them
    would break parity" with library.db's 3-column CTA table), so
    ``track_position`` has to come from the per-release
    ``GET /library/{id}/compilation-tracks`` endpoint instead
    (``CompilationTrack``, BS#1964) -- which is keyed on Backend's own
    serial id, not ``legacy_release_id``. ``_catalog_row_to_library_row``
    reads that serial id only to quote it in a diagnostic and then discards
    it; this sibling mapper exists to keep the mapping around.
    """
    legacy_release_id = _require_legacy_release_id(row, "catalog")
    backend_id = row.get("id")
    if backend_id is None:
        raise SourceError(
            f"catalog export row for legacy_release_id {legacy_release_id} has no 'id' "
            "field; api.yaml's CatalogExportRow marks it required, and "
            "--capture-fffd-cta-pairs needs it to resolve track_position via "
            "GET /library/{id}/compilation-tracks"
        )
    try:
        return legacy_release_id, int(backend_id)
    except (TypeError, ValueError) as exc:
        raise SourceError(
            f"catalog export row for legacy_release_id {legacy_release_id} has a "
            f"non-integer 'id' ({backend_id!r})"
        ) from exc


# --- WXYC/Backend-Service#2152 U+FFFD pair capture --------------------------
#
# A narrow, one-off sibling of the two producers above, not a general-purpose
# third one: it never writes a library.db at all. It fetches the same
# Backend catalog snapshot the general producer does, but keeps the fields
# that one discards (the Backend serial id, each CTA row's own identity)
# because they are exactly what this mode needs and the general shape
# doesn't carry.


def _fetch_backend_fffd_capture_inputs(
    base_url: str, token_source: _TokenSource
) -> tuple[dict[int, int], list[CtaRow]]:
    """Fetch the Backend catalog snapshot in the shape ``--capture-fffd-cta-pairs``
    needs: a ``legacy_release_id -> Backend serial id`` map (for the
    track_position follow-up below) and every CTA row as its own ``CtaRow``
    (for FFFD pairing) -- the same torn-snapshot-safe fetch
    ``_fetch_consistent_snapshot`` already does for the general producer,
    just keeping the Backend serial id on the catalog side via
    ``_catalog_row_to_id_map_entry``, and wrapped into ``CtaRow`` on the CTA
    side after the fetch returns (``_fetch_consistent_snapshot`` keeps its
    CTA mapper fixed -- see that function's docstring for why).
    """
    id_map_rows, cta_rows = _fetch_consistent_snapshot(
        base_url,
        token_source,
        catalog_mapper=_catalog_row_to_id_map_entry,
    )
    id_map = dict(id_map_rows)
    backend_rows = [CtaRow(*row) for row in cta_rows]
    return id_map, backend_rows


def _fetch_compilation_tracks_once(
    base_url: str, backend_id: int, token: str
) -> list[dict[str, Any]]:
    """One ``GET /library/{id}/compilation-tracks``. Raises ``_AuthStatusError``
    so the caller can refresh, matching ``_fetch_ndjson_once``'s shape --
    this is a single small JSON object, not gzipped NDJSON, so it does not
    reuse that function directly.
    """
    url = f"{base_url}/library/{backend_id}/compilation-tracks"
    request = Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    try:
        with _opener.open(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
            body = response.read()
    except HTTPError as exc:
        # Must precede the URLError clause below, same reasoning as
        # _fetch_ndjson_once: HTTPError subclasses URLError.
        detail = ""
        try:
            detail = exc.read().decode("utf-8", "replace")[:200].replace("\n", " ").strip()
        except Exception:  # noqa: BLE001 - a body we cannot read is not the failure
            pass
        raise _AuthStatusError(exc.code, detail, exc.headers) from exc
    except (URLError, OSError) as exc:
        raise SourceError(f"failed to fetch {url}: {exc}") from exc
    try:
        payload = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SourceError(f"{url} returned a body that could not be parsed as JSON: {exc}") from exc
    tracks = payload.get("tracks") if isinstance(payload, dict) else None
    if not isinstance(tracks, list):
        raise SourceError(f"{url} returned no 'tracks' array (the CompilationTrackList contract)")
    return tracks


def _fetch_compilation_tracks(
    base_url: str, backend_id: int, token_source: _TokenSource
) -> list[dict[str, Any]]:
    """``GET /library/{id}/compilation-tracks`` with one 401-refresh retry,
    matching ``_fetch_ndjson``'s shape. A 404 (the release vanished between
    the catalog snapshot and this follow-up call -- rare, but the snapshot
    and this call are not one transaction) degrades to an empty list rather
    than failing the whole capture: the affected row(s) simply come back
    with ``track_position = None``, which is a legal, not fatal, output.
    """
    url = f"{base_url}/library/{backend_id}/compilation-tracks"
    for attempt in (1, 2):
        try:
            return _fetch_compilation_tracks_once(base_url, backend_id, token_source.token())
        except _AuthStatusError as exc:
            if exc.code == 401 and attempt == 1:
                logger.info(
                    "compilation-tracks fetch returned 401; refreshing the service-account token",
                    extra={"step": "fffd_cta_capture", "url": url},
                )
                token_source.invalidate()
                continue
            if exc.code == 404:
                return []
            raise SourceError(f"failed to fetch {url}: HTTP {exc.code}: {exc.detail}") from exc
    raise AssertionError("unreachable: the retry loop either returns or raises")


def _resolve_fffd_track_positions(
    base_url: str,
    token_source: _TokenSource,
    id_map: Mapping[int, int],
    corrupt_rows: Sequence[CtaRow],
) -> dict[tuple[int, str, str], str]:
    """Fetch ``track_position`` for every corrupt row's release, keyed on
    each Backend row's own (still-corrupt) ``(legacy_release_id,
    artist_name, track_title)`` tuple.

    The bulk CTA export never carries ``track_position`` at all (see
    ``_catalog_row_to_id_map_entry``'s docstring), so this is the one place
    ``--capture-fffd-cta-pairs`` needs a second kind of Backend call beyond
    the two bulk exports the rest of the harness already makes.
    ``track_position`` is not itself corrupted by the U+FFFD bug -- it
    carries no non-ASCII content -- so Backend's own live value is
    trustworthy without any MySQL cross-check; the pairing rule in
    ``lib/fffd_pair_capture`` never touches this value, only reports it.

    One fetch per distinct affected release (deduplicated on Backend's own
    serial id, not on ``legacy_release_id``, since that is what the HTTP
    path is keyed on), and each fetch's response fills in EVERY track on
    that release -- not just the one row that triggered it -- so a
    compilation with several corrupt tracks costs one call, not several.

    A release this cannot resolve a position for (missing from ``id_map`` --
    only reachable if the catalog snapshot tore between the bulk fetch and
    here, since both mappers key on the same ``legacy_release_id``; the
    compilation-tracks fetch 404ing; or no track in the response matching
    the row's own corrupt tuple, e.g. a concurrent edit) is simply absent
    from the returned mapping. ``find_fffd_pairs`` treats a missing key as
    ``track_position = None``, a legal value, not a failure.
    """
    positions: dict[tuple[int, str, str], str] = {}
    fetched_backend_ids: set[int] = set()
    for row in corrupt_rows:
        backend_id = id_map.get(row.legacy_release_id)
        if backend_id is None or backend_id in fetched_backend_ids:
            continue
        fetched_backend_ids.add(backend_id)
        tracks = _fetch_compilation_tracks(base_url, backend_id, token_source)
        for track in tracks:
            artist_name = track.get("artist_name")
            position = track.get("track_position")
            if artist_name is None or position is None:
                continue
            track_title = track.get("track_title")
            track_title = track_title if track_title is not None else ""
            positions[(row.legacy_release_id, artist_name, track_title)] = position
    return positions


def _resolved_fffd_pair_to_dict(pair: ResolvedFffdPair) -> dict[str, Any]:
    return {
        "legacy_release_id": pair.legacy_release_id,
        "track_position": pair.track_position,
        "current_artist_name": pair.current_artist_name,
        "current_track_title": pair.current_track_title,
        "true_artist_name": pair.true_artist_name,
        "true_track_title": pair.true_track_title,
        "true_artist_name_codepoints": [
            {"index": c.index, "char": c.char, "codepoint": c.codepoint}
            for c in pair.true_artist_name_codepoints
        ],
        "true_track_title_codepoints": [
            {"index": c.index, "char": c.char, "codepoint": c.codepoint}
            for c in pair.true_track_title_codepoints
        ],
    }


def _unresolved_fffd_pair_to_dict(pair: UnresolvedFffdPair) -> dict[str, Any]:
    return {
        "legacy_release_id": pair.legacy_release_id,
        "track_position": pair.track_position,
        "current_artist_name": pair.current_artist_name,
        "current_track_title": pair.current_track_title,
        "reason": pair.reason,
        "candidates": [
            {"artist_name": c.artist_name, "track_title": c.track_title} for c in pair.candidates
        ],
    }


def capture_fffd_cta_pairs(mysql_db: str, backend_source: str) -> dict[str, Any]:
    """Run the WXYC/Backend-Service#2152 U+FFFD pair-capture mode end to end.

    Reads MySQL truth from ``mysql_db``'s ``compilation_track_artist`` table
    (already built, or built fresh by the caller via ``--mysql-source`` --
    see ``main``'s ``--capture-fffd-cta-pairs`` branch), fetches Backend's
    live CTA rows plus the per-release ``track_position`` follow-up over HTTP
    (``backend_source``, the same service-account credentials
    ``--backend-source`` already uses elsewhere in this harness), pairs them
    (``lib/fffd_pair_capture.find_fffd_pairs``), and returns the ``--json``
    contract::

        {"resolved": [...], "unresolved": [...], "sql_values": "..."}

    ``sql_values`` is every resolved row rendered as
    ``pending_cta_repair`` VALUES tuples
    (``scripts/audit/bs_replacement_char_cta.sql`` in Backend-Service),
    ready to paste in place of that block's placeholder row.

    Raises ``SourceError`` for any source/read/fetch failure, matching every
    other producer in this file -- caught by ``main`` as exit 3.
    """
    mysql_conn = _open_readonly(mysql_db, "mysql")
    try:
        mysql_rows = _load_cta_rows(mysql_conn, "mysql")
    finally:
        mysql_conn.close()

    base_url = _resolve_backend_base_url(backend_source)
    token_source = _TokenSource(base_url)
    try:
        id_map, backend_rows = _fetch_backend_fffd_capture_inputs(base_url, token_source)
        corrupt_rows = [
            row for row in backend_rows if has_fffd(row.artist_name) or has_fffd(row.track_title)
        ]
        positions = _resolve_fffd_track_positions(base_url, token_source, id_map, corrupt_rows)
    finally:
        token_source.close()

    resolved, unresolved = find_fffd_pairs(backend_rows, mysql_rows, track_positions=positions)
    return {
        "resolved": [_resolved_fffd_pair_to_dict(pair) for pair in resolved],
        "unresolved": [_unresolved_fffd_pair_to_dict(pair) for pair in unresolved],
        "sql_values": render_pending_cta_repair_values(resolved),
    }


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
    p.add_argument(
        "--capture-fffd-cta-pairs",
        default=None,
        metavar="PATH",
        help=(
            "Run the WXYC/Backend-Service#2152 U+FFFD pair-capture mode instead of "
            "the normal diff: pair every Backend compilation_track_artist row "
            "containing a literal U+FFFD against its MySQL truth, and write the "
            "result as JSON to PATH ('-' for stdout). Requires --backend-source "
            "(the bulk CTA export omits track_position, so this mode also calls "
            "GET /library/{id}/compilation-tracks per affected release) and either "
            "--mysql-source or a pre-built --mysql-db. Never guesses: an ambiguous "
            "or unmatched row is reported unresolved, not silently approximated."
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


def _run_capture_fffd_cta_pairs_cli(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """``main``'s ``--capture-fffd-cta-pairs`` branch.

    Deliberately its own validation, not threaded through the general
    per-source ``--*-source requires --*-db`` loop below: this mode never
    touches ``--backend-db`` at all (see ``capture_fffd_cta_pairs``'s
    docstring for why), so that loop's ``--backend-source requires
    --backend-db`` check would wrongly refuse a perfectly valid invocation.
    """
    if not args.backend_source:
        print(
            "error: --capture-fffd-cta-pairs requires --backend-source (the bulk CTA "
            "export omits track_position, so this mode needs a live Backend to fetch "
            "it via GET /library/{id}/compilation-tracks).",
            file=sys.stderr,
        )
        parser.print_usage(sys.stderr)
        return 2
    if args.mysql_source is not None and not args.mysql_db:
        print(
            "error: --mysql-source requires --mysql-db, the path to write the built library.db to.",
            file=sys.stderr,
        )
        parser.print_usage(sys.stderr)
        return 2
    if not args.mysql_source and not args.mysql_db:
        print(
            "error: --capture-fffd-cta-pairs requires --mysql-source or a pre-built --mysql-db.",
            file=sys.stderr,
        )
        parser.print_usage(sys.stderr)
        return 2
    if args.capture_fffd_cta_pairs != "-":
        # Pre-flight, before a single byte is fetched. This mode only runs in
        # CI (repo-secret credentials + a working mariadb-client), so a path
        # error discovered after the MySQL export and every Backend fetch
        # throws away a report that is expensive to re-take -- and the retry
        # then trips _require_absent on the half-built --mysql-db.
        parent = Path(args.capture_fffd_cta_pairs).parent
        if not parent.is_dir():
            print(
                f"error: --capture-fffd-cta-pairs directory does not exist: {parent}",
                file=sys.stderr,
            )
            parser.print_usage(sys.stderr)
            return 2

    try:
        if args.mysql_source is not None:
            _require_absent(args.mysql_db, "mysql")
            _build_library_db_from_mysql(args.mysql_source, args.mysql_db)
        report = capture_fffd_cta_pairs(args.mysql_db, args.backend_source)
    except SourceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:
        logger.exception("fffd cta pair capture failed")
        print(f"error: {exc}", file=sys.stderr)
        return 3

    payload = json.dumps(report)
    write_failed = False
    if args.capture_fffd_cta_pairs == "-":
        print(payload)
    else:
        try:
            Path(args.capture_fffd_cta_pairs).write_text(payload + "\n", encoding="utf-8")
        except OSError as exc:
            # Everything expensive already happened; the capture is in hand and
            # only the destination failed. Emit it on stdout rather than let it
            # die with the process -- the operator can redirect and retry.
            write_failed = True
            print(
                f"error: could not write {args.capture_fffd_cta_pairs} ({exc}); "
                "the capture follows on stdout",
                file=sys.stderr,
            )
            print(payload)

    resolved_count = len(report["resolved"])
    unresolved_count = len(report["unresolved"])
    _report(f"FFFD capture: {resolved_count} resolved, {unresolved_count} unresolved")
    logger.info(
        "fffd cta pair capture complete",
        extra={
            "step": "fffd_cta_capture",
            "resolved": resolved_count,
            "unresolved": unresolved_count,
        },
    )
    if write_failed:
        return 3
    # Exit 4 carries the same meaning it does for the diff: the run itself was
    # fine and the ANSWER is not clean. An unresolved row is as actionable as a
    # crash and, at exit 0, just as invisible -- and this mode exists to hand
    # WXYC/Backend-Service#2152 a specific set of pairs, so "captured nothing
    # usable" must not read as success. The report is written either way; the
    # unresolved rows and their candidates are the whole diagnostic.
    return 4 if unresolved_count else 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    init_logger(repo="discogs-etl", tool="discogs-etl catalog_parity_diff")

    if args.capture_fffd_cta_pairs is not None:
        return _run_capture_fffd_cta_pairs_cli(args, parser)

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
