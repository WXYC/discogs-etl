"""Pure-SQL backing for the ``wxyc_unaccent`` text-search dictionary.

Background — discogs-etl#223
----------------------------

Alembic migration 0004 originally created a Postgres text-search dictionary
named ``wxyc_unaccent`` from a rules file at
``$SHAREDIR/tsearch_data/wxyc_unaccent.rules``. On Railway-managed Postgres
that path is owned by ``root`` and the ``postgres`` OS user has no write
permission, so the dictionary create step fails with
``ConfigFileError: could not open unaccent file`` even with the
``pg_write_server_files`` role granted.

This module replaces the dictionary with an equivalent pure-SQL function
``wxyc_unaccent_text(text)`` that bakes the same rules into Postgres via
``translate()`` (for the 1-char → 1-char rules) and a small ``REPLACE``
chain (for the multi-char-destination rules). No server-side filesystem
access required.

Two-pass equivalence to ``unaccent`` dictionary semantics
---------------------------------------------------------

The PG ``unaccent`` template applies rules in a single left-to-right pass:
each character is looked up, replaced if matched, and the *output is not
re-translated*. Our two-pass implementation does the REPLACE chain first
and then translate() over the result, so any character produced by a
multi-char REPLACE that *also* appears as a single-char-source would get
re-translated — diverging from the single-pass semantics.

The ``assert_no_overlap_invariant`` check enforces that no such overlap
exists in the current rules file. For the WXYC v0.4.0 rules, the
intersection is empty, so the two-pass output is byte-identical to the
single-pass dictionary's. If a future re-vendoring adds a rule that
violates this invariant, the codegen fails loudly and the pin SHA bump
in ``wxyc-etl-pin.txt`` is blocked until the codegen is reworked.

Substitution contract
---------------------

The vendored ``wxyc_identity_match_functions.sql`` calls
``unaccent('wxyc_unaccent', r)`` from one site inside ``wxyc_match_form``.
``patch_canonical_sql`` rewrites that single call to
``wxyc_unaccent_text(r)`` without modifying the file on disk — the parity
test still SHA-pins the vendored bytes, the deployed function bodies use
the substitution. If re-vendoring moves or duplicates the call site, the
substitution count check raises a re-vendoring hint.
"""

from __future__ import annotations

from pathlib import Path

# The line in vendor/wxyc-etl/wxyc_identity_match_functions.sql that our
# function family swap targets. Public so the unit tests can pin it.
SUBSTITUTION_TARGET = "r := unaccent('wxyc_unaccent', r);"
SUBSTITUTION_REPLACEMENT = "r := wxyc_unaccent_text(r);"


def parse_rules_file(path: Path) -> list[tuple[str, str]]:
    """Parse a Postgres unaccent ``.rules`` file into ``(src, dst)`` pairs.

    Tab-separated, one rule per line. Blank lines and lines starting with
    ``#`` are skipped. Any line that does not contain exactly one tab raises
    ``ValueError`` so corruption surfaces at codegen time, not at deploy.
    """
    rules: list[tuple[str, str]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        parts = raw.split("\t")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(
                f"malformed rule at {path}:{line_no}: {raw!r} "
                "(expected exactly one tab between non-empty src and dst)"
            )
        rules.append((parts[0], parts[1]))
    return rules


def partition_rules(
    rules: list[tuple[str, str]],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Split rules into ``(single_char_dst, multi_char_dst)``.

    All rule sources must be exactly one character. Multi-char source is
    not a shape the PG ``unaccent`` template emits, so we reject upstream
    drift rather than silently coercing.
    """
    single: list[tuple[str, str]] = []
    multi: list[tuple[str, str]] = []
    for src, dst in rules:
        if len(src) != 1:
            raise ValueError(
                f"multi-char source not supported: {(src, dst)!r}. PG unaccent "
                "rules are 1-char source; re-vendoring may have introduced a "
                "different shape."
            )
        if len(dst) == 1:
            single.append((src, dst))
        else:
            multi.append((src, dst))
    return single, multi


def assert_no_overlap_invariant(
    single: list[tuple[str, str]],
    multi: list[tuple[str, str]],
) -> None:
    """Raise ``ValueError`` if any multi-char-dst output character also
    appears as a single-char-src — the case where two-pass codegen diverges
    from single-pass ``unaccent`` semantics.
    """
    multi_dst_chars = {c for _, dst in multi for c in dst}
    single_src_chars = {src for src, _ in single}
    overlap = multi_dst_chars & single_src_chars
    if overlap:
        # Report each offender so a re-vendoring fix can be targeted.
        details = []
        for c in sorted(overlap):
            translates_to = next(d for s, d in single if s == c)
            produced_by = [(s, d) for s, d in multi if c in d]
            details.append(
                f"  {c!r} (U+{ord(c):04X}): produced by {produced_by!r}, "
                f"single-rule maps to {translates_to!r}"
            )
        raise ValueError(
            "two-pass codegen invariant violated: characters produced by "
            "multi-char-dst rules overlap with single-char-src set. The "
            "REPLACE-then-translate pipeline would re-fold these characters, "
            "diverging from PG's single-pass unaccent dictionary semantics. "
            "Rework lib/unaccent_codegen.py (e.g. apply translate() first "
            "on a partitioned input, or emit a per-character lookup loop) "
            "before bumping wxyc-etl-pin.txt.\n" + "\n".join(details)
        )


def _sql_quote(s: str) -> str:
    """Quote a string for embedding inside a single-quoted SQL literal."""
    return "'" + s.replace("'", "''") + "'"


def build_unaccent_function_sql(rules_path: Path) -> str:
    """Emit the ``CREATE OR REPLACE FUNCTION wxyc_unaccent_text(text)`` SQL.

    The emitted function applies the multi-char-dst rules via REPLACE first
    (innermost wrapping the input ``s``), then translate() over the result
    for the single-char-dst rules. Output is byte-equivalent to
    ``unaccent('wxyc_unaccent', s)`` against the same rules file, provided
    ``assert_no_overlap_invariant`` holds (called here defensively).
    """
    rules = parse_rules_file(rules_path)
    single, multi = partition_rules(rules)
    assert_no_overlap_invariant(single, multi)

    src_chars = "".join(s for s, _ in single)
    dst_chars = "".join(d for _, d in single)

    replace_chain = "s"
    for src, dst in multi:
        replace_chain = f"REPLACE({replace_chain}, {_sql_quote(src)}, {_sql_quote(dst)})"

    body = (
        f"SELECT translate(\n"
        f"    {replace_chain},\n"
        f"    {_sql_quote(src_chars)},\n"
        f"    {_sql_quote(dst_chars)}\n"
        f"  )"
    )

    return (
        "CREATE OR REPLACE FUNCTION wxyc_unaccent_text(s text) RETURNS text\n"
        "LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT\n"
        "AS $wxyc_unaccent_body$\n"
        f"  {body}\n"
        "$wxyc_unaccent_body$;\n"
    )


def patch_canonical_sql(canonical: str) -> str:
    """Substitute the ``unaccent('wxyc_unaccent', r)`` call with
    ``wxyc_unaccent_text(r)``. Raises if the target is not present exactly
    once — a re-vendoring sentinel."""
    count = canonical.count(SUBSTITUTION_TARGET)
    if count != 1:
        raise ValueError(
            f"substitution target {SUBSTITUTION_TARGET!r} not found exactly "
            f"once in canonical SQL (count={count}). Re-vendoring may have "
            "moved or duplicated the unaccent call site; update "
            "lib/unaccent_codegen.py to track the new shape."
        )
    return canonical.replace(SUBSTITUTION_TARGET, SUBSTITUTION_REPLACEMENT)
