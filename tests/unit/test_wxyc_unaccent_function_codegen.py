"""Unit tests for ``lib.unaccent_codegen`` — the pure-SQL backing for the
``wxyc_unaccent`` text-search dictionary that alembic migration 0004 used to
require at ``$SHAREDIR/tsearch_data/`` (see WXYC/discogs-etl#223). The codegen
helper reads the vendored ``vendor/wxyc-etl/wxyc_unaccent.rules`` file and
emits a ``wxyc_unaccent_text(text)`` plpgsql function whose output is
byte-equivalent to the dictionary's, no server-side filesystem dependency.

Two test surfaces:

1. Parsing + partitioning + invariant — pure-Python; exercised against the
   live vendored rules file. Failure here means a wxyc-etl re-vendoring
   added a rule that breaks the two-pass codegen equivalence and the
   alembic migration needs to be revisited before the pin SHA is bumped.

2. Codegen output shape — assert the emitted SQL contains the expected
   structural pieces (CREATE OR REPLACE FUNCTION header, ``translate(`` call,
   one REPLACE per multi-char-destination rule). Not a parity assertion;
   that's the integration test's job.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lib import unaccent_codegen

REPO_ROOT = Path(__file__).resolve().parents[2]
VENDOR_RULES = REPO_ROOT / "vendor" / "wxyc-etl" / "wxyc_unaccent.rules"
VENDOR_FUNCTIONS = REPO_ROOT / "vendor" / "wxyc-etl" / "wxyc_identity_match_functions.sql"


# ---------------------------------------------------------------------------
# parse_rules_file
# ---------------------------------------------------------------------------


def test_parse_rules_file_yields_pairs() -> None:
    rules = unaccent_codegen.parse_rules_file(VENDOR_RULES)
    assert len(rules) > 0, "expected non-empty rules file"
    for src, dst in rules:
        assert len(src) >= 1, f"empty src in rule {(src, dst)!r}"
        assert len(dst) >= 1, f"empty dst in rule {(src, dst)!r}"


def test_parse_rules_file_skips_comments_and_blanks(tmp_path: Path) -> None:
    f = tmp_path / "fake.rules"
    f.write_text(
        "# comment line\n\nà\ta\n  \né\te\n",
        encoding="utf-8",
    )
    rules = unaccent_codegen.parse_rules_file(f)
    assert rules == [("à", "a"), ("é", "e")]


def test_parse_rules_file_raises_on_malformed_line(tmp_path: Path) -> None:
    f = tmp_path / "bad.rules"
    f.write_text("à\ta\nnotabtab here\né\te\n", encoding="utf-8")
    with pytest.raises(ValueError, match="malformed"):
        unaccent_codegen.parse_rules_file(f)


# ---------------------------------------------------------------------------
# partition_rules
# ---------------------------------------------------------------------------


def test_partition_rules_splits_by_dst_width() -> None:
    rules = [("à", "a"), ("é", "e"), ("æ", "ae"), ("œ", "oe"), ("¼", "1⁄4")]
    single, multi = unaccent_codegen.partition_rules(rules)
    assert single == [("à", "a"), ("é", "e")]
    assert multi == [("æ", "ae"), ("œ", "oe"), ("¼", "1⁄4")]


def test_partition_rules_rejects_multi_char_src(tmp_path: Path) -> None:
    # All real unaccent rules are 1-char source. Any deviation is upstream
    # corruption — fail loudly so a future re-vendor surfaces it instead of
    # silently dropping the rule.
    rules = [("à", "a"), ("ae", "x")]
    with pytest.raises(ValueError, match="multi-char source"):
        unaccent_codegen.partition_rules(rules)


# ---------------------------------------------------------------------------
# assert_no_overlap_invariant
# ---------------------------------------------------------------------------


def test_invariant_passes_on_vendored_rules() -> None:
    """The current vendored rules must satisfy the two-pass equivalence
    invariant. If this fails, a re-vendoring of ``wxyc_unaccent.rules`` added
    a rule whose multi-char destination overlaps with the single-char-src
    set — the two-pass codegen would produce different output than the
    PG dictionary's single-pass semantics. Block the pin bump until the
    codegen can handle the new shape."""
    rules = unaccent_codegen.parse_rules_file(VENDOR_RULES)
    single, multi = unaccent_codegen.partition_rules(rules)
    unaccent_codegen.assert_no_overlap_invariant(single, multi)


def test_invariant_detects_overlap() -> None:
    # Fabricate a violating pair: 'æ → ae' produces 'a', and 'a → x' is in the
    # single-char-src set. Two-pass would expand æ to ae, then translate 'a'
    # in 'ae' to 'x' (and the pre-existing 'a' in input would also become 'x'),
    # diverging from single-pass unaccent which would never re-fold the 'a'
    # produced by æ-expansion.
    single = [("a", "x")]
    multi = [("æ", "ae")]
    with pytest.raises(ValueError, match="overlap"):
        unaccent_codegen.assert_no_overlap_invariant(single, multi)


# ---------------------------------------------------------------------------
# build_unaccent_function_sql
# ---------------------------------------------------------------------------


def test_emitted_sql_has_function_header() -> None:
    sql = unaccent_codegen.build_unaccent_function_sql(VENDOR_RULES)
    assert "CREATE OR REPLACE FUNCTION wxyc_unaccent_text(" in sql
    assert "LANGUAGE sql" in sql
    assert "IMMUTABLE" in sql
    assert "PARALLEL SAFE" in sql
    assert "STRICT" in sql


def test_emitted_sql_uses_translate_for_single_char_rules() -> None:
    sql = unaccent_codegen.build_unaccent_function_sql(VENDOR_RULES)
    assert "translate(" in sql, "expected a translate() call for 1:1 rules"


def test_emitted_sql_has_one_replace_per_multi_char_rule() -> None:
    rules = unaccent_codegen.parse_rules_file(VENDOR_RULES)
    _, multi = unaccent_codegen.partition_rules(rules)
    sql = unaccent_codegen.build_unaccent_function_sql(VENDOR_RULES)
    # The emitted SQL contains a nested REPLACE chain; count occurrences.
    assert sql.count("REPLACE(") == len(multi), (
        f"expected {len(multi)} REPLACE calls (one per multi-char-dst rule); "
        f"got {sql.count('REPLACE(')}"
    )


# ---------------------------------------------------------------------------
# patch_canonical_sql — the substitution that swaps the dict call for the
# function call without modifying the vendored .sql on disk.
# ---------------------------------------------------------------------------


CANONICAL_SENTINEL = "r := unaccent('wxyc_unaccent', r);"


def test_canonical_sql_contains_substitution_target_exactly_once() -> None:
    """Pins the substitution target so a re-vendoring that drops, moves, or
    duplicates the call site fails this test instead of producing a broken
    deploy."""
    canonical = VENDOR_FUNCTIONS.read_text(encoding="utf-8")
    assert canonical.count(CANONICAL_SENTINEL) == 1, (
        f"expected exactly one {CANONICAL_SENTINEL!r} in vendored canonical SQL; "
        f"found {canonical.count(CANONICAL_SENTINEL)}. Re-vendoring may have "
        "moved or duplicated the unaccent call site; update lib/unaccent_codegen.py."
    )


def test_patch_canonical_sql_replaces_unaccent_call() -> None:
    canonical = VENDOR_FUNCTIONS.read_text(encoding="utf-8")
    patched = unaccent_codegen.patch_canonical_sql(canonical)
    assert CANONICAL_SENTINEL not in patched
    assert "r := wxyc_unaccent_text(r);" in patched
    # Substitution count = 1; everything else unchanged.
    assert canonical.replace(CANONICAL_SENTINEL, "r := wxyc_unaccent_text(r);") == patched


def test_patch_canonical_sql_raises_when_target_missing() -> None:
    with pytest.raises(ValueError, match="substitution target"):
        unaccent_codegen.patch_canonical_sql("-- empty\n")
