-- Canonical SQL implementation of the cross-cache-identity match form.
--
-- Vendored verbatim into every cache repo (discogs-etl, musicbrainz-cache,
-- wikidata-cache) and Backend-Service. The four function bodies must produce
-- byte-identical output to the corresponding Rust entry points in
-- `wxyc_etl::text::identity`:
--
--   wxyc_identity_match_artist            <-> to_identity_match_form
--   wxyc_identity_match_title             <-> to_identity_match_form_title
--   wxyc_identity_match_with_punctuation  <-> to_identity_match_form_with_punctuation
--   wxyc_identity_match_with_disambiguator_strip
--                                         <-> to_identity_match_form_with_disambiguator_strip
--
-- Parity is asserted by `wxyc-etl/tests/postgres_parity_test.rs` against the
-- 252-row fixture in `wxyc-etl/tests/fixtures/identity_normalization_cases.csv`.
--
-- Required Postgres version: 16+ (Unicode property classes, `normalize()`,
-- stable regex behavior). Required extension: `unaccent` configured with the
-- `wxyc_unaccent` text-search dictionary installed from
-- `data/wxyc_unaccent.rules`.
--
-- Vendoring contract: each consumer carries `wxyc-etl-pin.txt` recording the
-- SHA-256 of `data/wxyc_unaccent.rules` and the version header read from the
-- file's first comment line. Mismatch fails CI. See
-- `wxyc-etl/docs/postgres-analog-vendoring.md`.

DO $$
BEGIN
  IF current_setting('server_version_num')::int < 160000 THEN
    RAISE EXCEPTION 'wxyc identity-match functions require Postgres 16+; got %',
      current_setting('server_version');
  END IF;
END $$;

-- The wxyc_unaccent dictionary must be created before this file loads.
-- Consumer migrations do:
--   CREATE EXTENSION IF NOT EXISTS unaccent;
--   CREATE TEXT SEARCH DICTIONARY wxyc_unaccent (
--     TEMPLATE = unaccent, RULES = 'wxyc_unaccent'
--   );
-- followed by the rules-file SHA verification block (see vendoring docs).

-- ---------------------------------------------------------------------------
-- Base match-form pipeline.
--
-- Mirror of `wxyc_etl::text::to_match_form` after the storage-form pass
-- (no mojibake repair — callers responsible for storing pre-cleaned bytes).
-- Pipeline:
--   normalize NFKC -> lower -> wxyc_unaccent dictionary -> strip-Cf-except-ZWJ
--   -> collapse-ASCII-space + trim.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wxyc_match_form(s text)
  RETURNS text
  LANGUAGE plpgsql
  IMMUTABLE
  PARALLEL SAFE
AS $$
DECLARE
  r text;
  zwj text := chr(8205);     -- U+200D
  cf_pattern text;
BEGIN
  IF s IS NULL THEN RETURN NULL; END IF;
  r := normalize(s, NFKC);
  r := lower(r);
  r := unaccent('wxyc_unaccent', r);
  -- Strip Cf (format) characters except U+200D ZWJ (emoji integrity), matching
  -- `strip_cf_except_zwj` in the Rust pipeline. Postgres regex has no
  -- `\p{Cf}` and no char-class subtraction; build the class from explicit
  -- BMP Cf codepoints split around ZWJ. Supplementary-plane Cf (U+E0001 etc.)
  -- is rare in music-catalog data and intentionally not handled here.
  cf_pattern :=
       '['
    || chr(173)                                  -- U+00AD soft hyphen
    || chr(1564)                                 -- U+061C ALM
    || chr(1757)                                 -- U+06DD ARABIC END OF AYAH
    || chr(1807)                                 -- U+070F SYRIAC ABBREV MARK
    || chr(2274)                                 -- U+08E2 ARABIC DISPUTED END OF AYAH
    || chr(6158)                                 -- U+180E MONG VOWEL SEP
    || chr(8203) || '-' || chr(8204)             -- U+200B-U+200C  (200D ZWJ skipped)
    || chr(8206) || '-' || chr(8207)             -- U+200E-U+200F
    || chr(8234) || '-' || chr(8238)             -- U+202A-U+202E
    || chr(8288) || '-' || chr(8303)             -- U+2060-U+206F
    || chr(65279)                                -- U+FEFF BOM
    || chr(65529) || '-' || chr(65531)           -- U+FFF9-U+FFFB
    || ']';
  -- ZWJ is excluded from the class above, so no placeholder swap needed.
  r := regexp_replace(r, cf_pattern, '', 'g');
  -- Collapse runs of ASCII space + trim. Other whitespace (TAB etc.) preserved.
  r := regexp_replace(r, ' +', ' ', 'g');
  r := regexp_replace(r, '^ | $', '', 'g');
  RETURN r;
END
$$;

-- ---------------------------------------------------------------------------
-- Helper: strip a single trailing (...) or [...] group.
--
-- Mirror of `strip_trailing_parens` in `wxyc_etl::text::identity`. Returns
-- input unchanged when: no trailing close-bracket, brackets unbalanced,
-- or the matching open is at position 0 (would reduce stem to empty).
-- One pass only.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wxyc_strip_trailing_parens(s text)
  RETURNS text
  LANGUAGE plpgsql
  IMMUTABLE
  PARALLEL SAFE
AS $$
DECLARE
  trimmed text;
  open_chr char;
  close_chr char;
  ch char;
  depth int := 0;
  open_idx int := -1;
  i int;
  stem text;
BEGIN
  IF s IS NULL THEN RETURN NULL; END IF;
  trimmed := regexp_replace(s, ' +$', '');
  IF length(trimmed) = 0 THEN RETURN s; END IF;
  ch := right(trimmed, 1);
  IF ch = ')' THEN
    open_chr := '('; close_chr := ')';
  ELSIF ch = ']' THEN
    open_chr := '['; close_chr := ']';
  ELSE
    RETURN s;
  END IF;
  -- Scan right-to-left for the matching open.
  FOR i IN REVERSE length(trimmed)..1 LOOP
    ch := substr(trimmed, i, 1);
    IF ch = close_chr THEN
      depth := depth + 1;
    ELSIF ch = open_chr THEN
      depth := depth - 1;
      IF depth = 0 THEN
        open_idx := i;
        EXIT;
      END IF;
    END IF;
  END LOOP;
  IF open_idx < 0 OR open_idx = 1 THEN
    -- Unbalanced or full-string brackets — preserve.
    RETURN s;
  END IF;
  stem := substr(trimmed, 1, open_idx - 1);
  stem := regexp_replace(stem, ' +$', '');
  RETURN stem;
END
$$;

-- ---------------------------------------------------------------------------
-- Helper: drop a leading article or trailing comma-form article.
--
-- Mirror of `drop_articles` in `wxyc_etl::text::identity`. At most one
-- match is consumed. The leading form requires the article followed by
-- ASCII space (`the `, `a `, `an `); `theater` does not match. The comma
-- form requires `, the` / `, a` / `, an` at end-of-string with a
-- non-empty stem; `Beatles, the Best Of` does not match.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wxyc_drop_articles(s text)
  RETURNS text
  LANGUAGE plpgsql
  IMMUTABLE
  PARALLEL SAFE
AS $$
DECLARE
  art text;
  stripped text;
BEGIN
  IF s IS NULL THEN RETURN NULL; END IF;
  FOREACH art IN ARRAY ARRAY['the ', 'a ', 'an '] LOOP
    IF starts_with(s, art) THEN
      RETURN substr(s, length(art) + 1);
    END IF;
  END LOOP;
  FOREACH art IN ARRAY ARRAY[', the', ', a', ', an'] LOOP
    -- Suffix check via `right()` rather than `LIKE '%' || art` so a future
    -- article containing `%` or `_` doesn't trigger wildcard semantics.
    IF length(s) >= length(art) AND right(s, length(art)) = art THEN
      stripped := substr(s, 1, length(s) - length(art));
      IF length(stripped) > 0 THEN
        RETURN stripped;
      END IF;
    END IF;
  END LOOP;
  RETURN s;
END
$$;

-- ---------------------------------------------------------------------------
-- Helper: identity baseline (steps 4 + 5).
--
-- Mirror of `identity_baseline` in `wxyc_etl::text::identity`. The shared
-- body of artist + title entry points.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wxyc_identity_baseline(s text)
  RETURNS text
  LANGUAGE plpgsql
  IMMUTABLE
  PARALLEL SAFE
AS $$
DECLARE
  r text;
BEGIN
  IF s IS NULL THEN RETURN NULL; END IF;
  r := wxyc_match_form(s);
  r := wxyc_strip_trailing_parens(r);
  r := wxyc_drop_articles(r);
  r := regexp_replace(r, ' +', ' ', 'g');
  r := regexp_replace(r, '^ | $', '', 'g');
  RETURN r;
END
$$;

-- ---------------------------------------------------------------------------
-- Public entry point: artist identity match.
-- Mirror of `wxyc_etl::text::to_identity_match_form`.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wxyc_identity_match_artist(s text)
  RETURNS text
  LANGUAGE plpgsql
  IMMUTABLE
  PARALLEL SAFE
AS $$
BEGIN
  RETURN wxyc_identity_baseline(s);
END
$$;

-- ---------------------------------------------------------------------------
-- Public entry point: title identity match.
-- Mirror of `wxyc_etl::text::to_identity_match_form_title`. Same body as
-- artist today; separate function so callers type-distinguish at the call
-- site and a future step-6 promotion does not silently change titles that
-- would not benefit (`Side A/2` etc.).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wxyc_identity_match_title(s text)
  RETURNS text
  LANGUAGE plpgsql
  IMMUTABLE
  PARALLEL SAFE
AS $$
BEGIN
  RETURN wxyc_identity_baseline(s);
END
$$;

-- ---------------------------------------------------------------------------
-- Public entry point: identity match + opt-in punctuation collapse (step 6).
-- Mirror of `wxyc_etl::text::to_identity_match_form_with_punctuation`.
-- Each run of one-or-more non-letter, non-number, non-whitespace codepoints
-- becomes a single ASCII space; result is re-collapsed and re-trimmed.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wxyc_identity_match_with_punctuation(s text)
  RETURNS text
  LANGUAGE plpgsql
  IMMUTABLE
  PARALLEL SAFE
AS $$
DECLARE
  r text;
BEGIN
  IF s IS NULL THEN RETURN NULL; END IF;
  r := wxyc_match_form(s);
  r := wxyc_strip_trailing_parens(r);
  r := wxyc_drop_articles(r);
  -- Step 6: replace each run of non-{Letter,Number,Whitespace} with one space.
  -- Postgres regex doesn't support `\p{L}` directly, but POSIX `[:alpha:]` /
  -- `[:digit:]` / `[:space:]` are locale-aware (en_US.UTF-8 collation =
  -- full Unicode coverage).
  r := regexp_replace(r, '[^[:alpha:][:digit:][:space:]]+', ' ', 'g');
  r := regexp_replace(r, ' +', ' ', 'g');
  r := regexp_replace(r, '^ | $', '', 'g');
  RETURN r;
END
$$;

-- ---------------------------------------------------------------------------
-- Public entry point: identity match + opt-in `/N` disambiguator strip (step 8).
-- Mirror of `wxyc_etl::text::to_identity_match_form_with_disambiguator_strip`.
--
-- Artists only. The leading whitespace before `/` is REQUIRED (`John Smith /1`
-- strips; `Track 1/12` does not — matches Rust's `\s+/\d+$` not `\s*`).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wxyc_identity_match_with_disambiguator_strip(s text)
  RETURNS text
  LANGUAGE plpgsql
  IMMUTABLE
  PARALLEL SAFE
AS $$
DECLARE
  r text;
BEGIN
  IF s IS NULL THEN RETURN NULL; END IF;
  r := wxyc_identity_baseline(s);
  r := regexp_replace(r, ' +/\d+$', '');
  RETURN r;
END
$$;
