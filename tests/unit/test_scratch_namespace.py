"""Unit tests for lib/scratch_namespace.py's per-invocation scratch-table naming.

See WXYC/discogs-etl#356: the rebuild pipeline's working tables
(``dedup_delete_ids``, ``keep_release_ids``, ``_keep_ids``, ``new_release*``)
are unqualified, globally-named tables in the shared ``public`` schema.
Two concurrent invocations collide on these names -- the exact mechanism
behind the 2026-08-04 incident (WXYC/discogs-etl#352). ``scratch_name``
namespaces a table name with a per-invocation suffix; ``new_scratch_suffix``
mints that suffix.
"""

from __future__ import annotations

import re

from lib.scratch_namespace import new_scratch_suffix, scratch_name


class TestScratchName:
    def test_appends_suffix_with_underscore(self) -> None:
        assert scratch_name("new_release", "a1b2c3d4") == "new_release_a1b2c3d4"

    def test_empty_suffix_returns_base_name_unchanged(self) -> None:
        """Backward-compat: omitting the suffix must be byte-identical to
        today's unnamespaced behavior, so every existing call site that
        doesn't opt in keeps working unmodified."""
        assert scratch_name("new_release", "") == "new_release"

    def test_works_for_every_in_scope_base_name(self) -> None:
        # The exact set called out in WXYC/discogs-etl#356.
        bases = [
            "dedup_delete_ids",
            "keep_release_ids",
            "_keep_ids",
            "new_release",
            "new_release_artist",
            "new_release_label",
            "new_release_genre",
            "new_release_style",
            "new_release_track",
            "new_release_track_artist",
            "new_cache_metadata",
        ]
        for base in bases:
            assert scratch_name(base, "deadbeef") == f"{base}_deadbeef"


class TestNewScratchSuffix:
    def test_returns_nonempty_string(self) -> None:
        suffix = new_scratch_suffix()
        assert isinstance(suffix, str)
        assert suffix

    def test_only_contains_safe_identifier_characters(self) -> None:
        """The suffix is interpolated directly into f-string DDL (matching
        the existing unqualified-identifier convention throughout
        dedup_releases.py / verify_cache.py), so it must never contain
        anything but [0-9a-f] -- no quoting/escaping is done at the call
        site."""
        suffix = new_scratch_suffix()
        assert re.fullmatch(r"[0-9a-f]+", suffix), suffix

    def test_successive_calls_are_unique(self) -> None:
        suffixes = {new_scratch_suffix() for _ in range(64)}
        assert len(suffixes) == 64

    def test_suffix_is_a_valid_unquoted_postgres_identifier_suffix(self) -> None:
        """A leading digit would still be legal appended after ``base_``
        (the full identifier starts with a letter/underscore from the base
        name), but guard against anything that would ever need quoting."""
        suffix = new_scratch_suffix()
        assert re.fullmatch(r"[a-z0-9]+", suffix)
