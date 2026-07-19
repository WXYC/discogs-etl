"""Pin that ``scripts/rebuild-cache.sh`` fetches the masters XML dump so the
converter's ``process_masters`` path runs and ``import_masters`` has a
``master.csv`` to load.

Background — WXYC/discogs-etl#317.

The prod discogs-cache ``master`` / ``master_artist`` tables are empty: the
converter (``process_masters``), the loader (``import_csv.py::import_masters``,
already wired into ``--base-only``), and the DDL all exist, but the monthly
rebuild never fetched the masters dump, so ``master.csv`` was never produced and
``import_masters`` no-oped on the missing file.

The masters fetch mirrors the artists precedent (LML#497) — same Discogs URL
convention (``data/<YYYY>/discogs_<YYYY><MM>01_masters.xml.gz``), same ``#181``
resilience flags, dropped into ``$WORK_DIR`` so directory-mode auto-dispatch
picks it up. It differs in one way: masters is the least time-critical of the
four dumps, so its fetch is **decoupled** from the releases/artists month gate
(``both_reachable``) — a masters publish lag must never drag the time-sensitive
releases/artists rebuild back a month (#317, review finding #4). The masters
fetch is therefore best-effort: probe reachability, download if present, skip
(warn) if not; ``import_masters`` leaves the tables untouched when ``master.csv``
is absent.

These tests are static-structural: they parse the script and assert the
relevant fragments exist. They do not execute curl.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache.sh"


@pytest.fixture(scope="module")
def script_text() -> str:
    return SCRIPT_PATH.read_text()


def _non_comment_lines(lines: list[str]) -> list[str]:
    return [line for line in lines if not line.lstrip().startswith("#")]


def test_main_download_spools_masters_xml_gz(script_text: str) -> None:
    """The script must spool ``$WORK_DIR/masters.xml.gz``.

    Directory mode (``main.rs::run_directory``) detects each file by its XML
    root element, so the basename only needs to end in ``.xml.gz``;
    ``masters.xml.gz`` is the conventional name.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert '-o "$WORK_DIR/masters.xml.gz"' in code, (
        "rebuild-cache.sh must spool the masters dump to "
        "'$WORK_DIR/masters.xml.gz' so the converter's directory-mode "
        "process_masters path runs and import_masters has a master.csv. See #317."
    )


def test_masters_download_uses_continue_at_and_retry_all_errors(script_text: str) -> None:
    """The #181 resilience flags must apply to the masters fetch too.

    Parse the script into individual curl invocations and verify the flags
    live in the block that writes masters.xml.gz (a naive char-window would
    bleed across the artists-curl block above).
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    curl_blocks = [b for b in code.split("\n\n") if "curl " in b]
    masters_blocks = [b for b in curl_blocks if '-o "$WORK_DIR/masters.xml.gz"' in b]
    assert len(masters_blocks) == 1, (
        "rebuild-cache.sh must contain exactly one curl block writing "
        f"'$WORK_DIR/masters.xml.gz' — got {len(masters_blocks)}."
    )
    block = masters_blocks[0]
    assert "--continue-at -" in block, (
        "the curl invocation writing 'masters.xml.gz' must use '--continue-at -' "
        f"so a mid-stream HTTP/2 reset is resumable. See #181. Block:\n{block}"
    )
    assert "--retry-all-errors" in block, (
        "the curl invocation writing 'masters.xml.gz' must use '--retry-all-errors' "
        f"so mid-stream failures trigger a retry. See #181. Block:\n{block}"
    )


def test_masters_download_asserts_min_size(script_text: str) -> None:
    """A truncated/partial masters download must fail loudly, same as the other
    dumps. ``assert_min_size`` on the spooled masters file is the floor."""
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert 'assert_min_size "$WORK_DIR/masters.xml.gz"' in code, (
        "rebuild-cache.sh must assert a minimum on-disk size for "
        "'$WORK_DIR/masters.xml.gz' so a truncated download fails loudly. See #317."
    )


def test_masters_url_uses_same_yyyymmdd_pattern(script_text: str) -> None:
    """The masters URL must follow Discogs's
    ``data/<YYYY>/discogs_<YYYY><MM>01_masters.xml.gz`` convention — same shape
    as releases/artists, derived via the ``dump_url`` helper for the resolved
    month (both the current-month and previous-month fallback branches)."""
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert 'dump_url "$year" "${year}${month}" masters' in code, (
        "rebuild-cache.sh must derive the current-month masters URL by passing "
        "'masters' as the kind argument to its dump_url helper. See #317."
    )
    assert 'dump_url "$prev_year" "$prev" masters' in code, (
        "rebuild-cache.sh must also derive the fallback (previous-month) masters "
        "URL so masters tracks whichever month the releases/artists gate resolves "
        "to. See #317."
    )


def test_masters_not_in_releases_artists_reachability_gate(script_text: str) -> None:
    """Masters must NOT be folded into the ``both_reachable`` month gate.

    The gate stays a two-arg releases+artists probe; a masters publish lag must
    not drag the time-sensitive releases/artists rebuild back a month. Masters
    is fetched best-effort against whichever month the gate resolves to.
    See #317 (review finding #4).
    """
    lines = _non_comment_lines(script_text.splitlines())
    assert any('both_reachable "$url" "$artists_url"' in ln for ln in lines), (
        "rebuild-cache.sh must keep the releases+artists month gate as "
        '\'both_reachable "$url" "$artists_url"\' (two args). See #317.'
    )
    offending = [ln for ln in lines if "both_reachable" in ln and "masters" in ln.lower()]
    assert not offending, (
        "the masters URL must not appear in a both_reachable call — masters is "
        f"best-effort, not part of the month gate. Offending lines: {offending}. See #317."
    )


def test_masters_fetch_is_best_effort_reachability_probe(script_text: str) -> None:
    """The masters download must be guarded by its own reachability probe so a
    'not published yet' masters dump skips the phase instead of failing the run.

    A ``curl -sIfL`` HEAD against ``$masters_url`` inside an ``if`` is non-fatal
    under ``set -e``; only a reachable-but-failing GET fails hard.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    probe_lines = [ln for ln in code.splitlines() if "-sIfL" in ln and "masters_url" in ln]
    assert probe_lines, (
        "rebuild-cache.sh must probe the masters URL with a non-fatal "
        "'curl -sIfL ... \"$masters_url\"' HEAD before downloading, so an "
        "unpublished masters dump skips the phase rather than failing the "
        "releases/artists rebuild. See #317."
    )


def test_smoke_mode_validates_masters_url(script_text: str) -> None:
    """REBUILD_SMOKE=1 must exercise the masters URL alongside releases/artists,
    so a fresh-host smoke test surfaces a masters reachability problem before any
    DB write. Best-effort (warn), matching the phase's decoupled posture."""
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert "masters smoke" in code, (
        "rebuild-cache.sh's REBUILD_SMOKE path must validate the masters URL "
        "(echoing a 'masters smoke' status line), mirroring the releases/artists "
        "smoke checks. See #317."
    )
