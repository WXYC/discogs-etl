"""Unit tests for lib/matching.py."""

from __future__ import annotations

import pytest

from lib.matching import is_compilation_artist


class TestIsCompilationArtist:
    """Compilation artist detection."""

    @pytest.mark.parametrize(
        "artist, expected",
        [
            ("Various Artists", True),
            ("various", True),
            ("Soundtrack", True),
            ("Original Motion Picture Soundtrack", True),
            ("V/A", True),
            ("v.a.", True),
            ("Compilation Hits", True),
            ("Stereolab", False),
            ("Juana Molina", False),
            ("Cat Power", False),
            ("", False),
        ],
        ids=[
            "various-artists",
            "various-lowercase",
            "soundtrack",
            "soundtrack-in-phrase",
            "v-slash-a",
            "v-dot-a",
            "compilation-keyword",
            "stereolab",
            "juana-molina",
            "cat-power",
            "empty-string",
        ],
    )
    def test_is_compilation_artist(self, artist: str, expected: bool) -> None:
        assert is_compilation_artist(artist) == expected
