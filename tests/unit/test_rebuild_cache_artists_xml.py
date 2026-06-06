"""Pin that ``scripts/rebuild-cache.sh`` fetches the artists XML dump and
hands the run_pipeline.py invocation a directory containing both the releases
and artists dumps.

Background — LML/library-metadata-lookup#497.

The monthly cache rebuild was only downloading
``discogs_<YYYY><MM>01_releases.xml.gz``. The converter's ``process_artists``
path (``main.rs``: ``run_directory``) exists and writes
``artist.csv``/``artist_alias.csv``/``artist_name_variation.csv``/
``artist_member.csv`` to the data dir — but only runs when the input is a
directory and that directory contains an artists XML. Because the rebuild
passed a single releases file via ``--xml``, ``process_artists`` was never
invoked. The downstream effect: ``artist.profile`` ends up 97.9% NULL in the
LML cache (only the runtime ``api_fetch`` slice is populated) and the iOS
detail view silently omits artist bios.

These tests are static-structural: they parse the script and assert the
relevant fragments exist. They do not execute curl. The Discogs URL pattern
is identical for releases and artists (``data/<YYYY>/discogs_<YYYY><MM>01_*
.xml.gz``), so the new fetch piggybacks on the existing ``url`` derivation
with a substituted basename.
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


def test_main_download_spools_artists_xml_gz(script_text: str) -> None:
    """The script must spool ``$WORK_DIR/artists.xml.gz`` alongside releases.xml.gz.

    The converter's directory mode (``main.rs::run_directory``) detects each
    file by reading its XML root element, so the basename only needs to end in
    ``.xml`` or ``.xml.gz`` — ``artists.xml.gz`` is the conventional name and
    is what ``run_pipeline.py``'s ``--xml`` doc string already references.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert '-o "$WORK_DIR/artists.xml.gz"' in code, (
        "rebuild-cache.sh must spool the artists dump to "
        "'$WORK_DIR/artists.xml.gz' so the converter's directory-mode "
        "process_artists path runs alongside releases. See LML#497."
    )


def test_main_download_artists_uses_continue_at_resume(script_text: str) -> None:
    """Same #181 resilience flags must apply to the artists fetch.

    The artists dump is ~2 GB compressed — a fraction of the releases dump,
    but still big enough that a CDN flake would force a re-download without
    ``--continue-at``. Apply the same resume + retry-all-errors invariants to
    keep both fetches resilient.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    # Locate the artists download command — anchored on the unique '-o' target.
    artists_block_idx = code.find('-o "$WORK_DIR/artists.xml.gz"')
    assert artists_block_idx >= 0, (
        "rebuild-cache.sh must include a curl invocation that writes "
        "'$WORK_DIR/artists.xml.gz' (precondition for this test)."
    )
    # Take a window of 400 chars around the target — large enough to include
    # the preceding `curl` invocation's flags but tight enough not to bleed
    # into the releases fetch.
    start = max(0, artists_block_idx - 400)
    end = min(len(code), artists_block_idx + 200)
    window = code[start:end]
    assert "--continue-at -" in window, (
        "the curl invocation that writes 'artists.xml.gz' must use "
        "'--continue-at -' so a mid-stream HTTP/2 reset is resumable. See #181."
    )
    assert "--retry-all-errors" in window, (
        "the curl invocation that writes 'artists.xml.gz' must use "
        "'--retry-all-errors' so mid-stream failures trigger a retry. See #181."
    )


def test_pipeline_invocation_passes_work_dir_not_releases_file(script_text: str) -> None:
    """The pipeline invocation must hand run_pipeline.py the directory, not the
    single releases file.

    Directory-mode is the only way the converter's ``run_directory`` path
    invokes ``process_artists``. Single-file mode (``--xml releases.xml.gz``)
    skips artist processing entirely. See ``main.rs::run_directory`` vs
    ``main.rs::run_single_file``.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert '--xml "$WORK_DIR"' in code, (
        "rebuild-cache.sh must invoke run_pipeline.py with '--xml \"$WORK_DIR\"' "
        "(the directory containing both releases.xml.gz and artists.xml.gz), "
        "not the single releases file. Directory mode is what triggers the "
        "converter's process_artists path. See LML#497."
    )
    assert '--xml "$WORK_DIR/releases.xml.gz"' not in code, (
        "rebuild-cache.sh must no longer pass '$WORK_DIR/releases.xml.gz' as "
        "the --xml argument — that's single-file mode and skips artist "
        "processing. Pass the directory instead. See LML#497."
    )


def test_pipeline_invocation_drops_xml_type_releases(script_text: str) -> None:
    """In directory mode the converter auto-detects each file's root element,
    so ``--xml-type releases`` (which only describes one file) is wrong.

    The xml-type flag was load-bearing for the FIFO path (which was already
    removed in #181); now that the input is a directory of regular files,
    auto-detection works for every file. Forwarding ``--xml-type releases``
    would lock the converter into single-file mode and re-introduce the
    artist-skip bug. See LML#497.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    # The flag pattern in the existing script is `--xml-type releases`
    # (with a space, on its own line in the heredoc-ish multi-line invocation).
    assert "--xml-type releases" not in code, (
        "rebuild-cache.sh must not pass '--xml-type releases' to "
        "run_pipeline.py — directory mode requires per-file auto-detection. "
        "See LML#497."
    )


def test_artists_url_uses_same_yyyymmdd_pattern(script_text: str) -> None:
    """The artists dump URL must follow Discogs's
    ``data/<YYYY>/discogs_<YYYY><MM>01_artists.xml.gz`` convention — the same
    pattern as releases, only the basename differs.

    Pin the pattern so a stray rename can't silently break the URL
    derivation. The fallback-to-previous-month logic already handles the
    case where the current month isn't published yet.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert "discogs_${year}${month}01_artists.xml.gz" in code, (
        "rebuild-cache.sh must derive the artists URL via "
        "'discogs_${year}${month}01_artists.xml.gz' "
        "(same shape as releases). See LML#497."
    )
