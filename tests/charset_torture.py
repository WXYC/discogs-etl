"""Loader for the cross-repo charset-torture corpus. See WXYC/docs#15."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, TypedDict, cast

_CORPUS_PATH = Path(__file__).parent / "fixtures" / "charset-torture.json"


class CharsetTortureEntry(TypedDict):
    category: str
    input: str
    expected_storage: str
    expected_match_form: str | None
    expected_ascii_form: str | None
    notes: str


def load_corpus() -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(_CORPUS_PATH.read_text(encoding="utf-8")))


def iter_entries() -> Iterator[CharsetTortureEntry]:
    corpus = load_corpus()
    for category, entries in corpus["categories"].items():
        for entry in entries:
            yield cast(CharsetTortureEntry, {**entry, "category": category})


def entry_id(entry: CharsetTortureEntry) -> str:
    truncated = entry["input"][:24].replace("\n", "\\n")
    return f"{entry['category']}:{truncated}"
