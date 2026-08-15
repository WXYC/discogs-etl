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

    def test_longest_base_name_stays_under_postgres_identifier_limit(self) -> None:
        """PostgreSQL silently TRUNCATES identifiers past 63 bytes (NAMEDATALEN-1).

        A truncated suffix would re-collide -- two invocations whose names
        got cut back to the same 63 bytes would share a table again, which
        is precisely the failure #356 exists to prevent. The longest base
        name in either script is ``new_release_track_artist`` (24 chars);
        with ``_`` + an 8-char hex suffix that is 33.
        """
        longest_base = "new_release_track_artist"
        name = scratch_name(longest_base, new_scratch_suffix())
        assert len(name.encode()) == 33
        assert len(name.encode()) < 63


class TestDropScratchTables:
    """#356: suffixing removed the old self-healing DROP, so each invocation
    must drop its own tables. This helper is that shared step."""

    def test_drops_every_base_with_the_suffix_applied(self) -> None:
        from unittest.mock import MagicMock

        from lib.scratch_namespace import drop_scratch_tables

        cur = MagicMock()
        drop_scratch_tables(cur, ["dedup_delete_ids", "new_release"], "ab12cd34")

        executed = [call.args[0] for call in cur.execute.call_args_list]
        assert executed == [
            "DROP TABLE IF EXISTS dedup_delete_ids_ab12cd34",
            "DROP TABLE IF EXISTS new_release_ab12cd34",
        ]

    def test_uses_if_exists_so_it_is_idempotent(self) -> None:
        """The success path already consumed the ``new_X`` tables via the
        swap's RENAME, so cleanup routinely runs against tables that are
        already gone and must not raise."""
        from unittest.mock import MagicMock

        from lib.scratch_namespace import drop_scratch_tables

        cur = MagicMock()
        drop_scratch_tables(cur, ["new_release"], "ab12cd34")
        assert "IF EXISTS" in cur.execute.call_args_list[0].args[0]

    def test_empty_suffix_drops_the_unnamespaced_names(self) -> None:
        from unittest.mock import MagicMock

        from lib.scratch_namespace import drop_scratch_tables

        cur = MagicMock()
        drop_scratch_tables(cur, ["_keep_ids"], "")
        assert cur.execute.call_args_list[0].args[0] == "DROP TABLE IF EXISTS _keep_ids"
