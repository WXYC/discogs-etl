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
    # Parse the script into individual curl invocations (each starts with a
    # `curl` token and ends at the next blank line), then locate the one that
    # writes artists.xml.gz and verify the flags live IN THAT BLOCK. A naive
    # char-window approach would bleed across the releases-curl block
    # immediately above (the two blocks are ~290 chars apart in the stripped
    # script — well inside any reasonable lookback) and pass even if the
    # artists block lacked the flags.
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    curl_blocks = [b for b in code.split("\n\n") if "curl " in b]
    artists_blocks = [b for b in curl_blocks if '-o "$WORK_DIR/artists.xml.gz"' in b]
    assert len(artists_blocks) == 1, (
        "rebuild-cache.sh must contain exactly one curl block writing "
        f"'$WORK_DIR/artists.xml.gz' — got {len(artists_blocks)}."
    )
    artists_block = artists_blocks[0]
    assert "--continue-at -" in artists_block, (
        "the curl invocation that writes 'artists.xml.gz' must use "
        f"'--continue-at -' so a mid-stream HTTP/2 reset is resumable. See #181. Block:\n{artists_block}"
    )
    assert "--retry-all-errors" in artists_block, (
        "the curl invocation that writes 'artists.xml.gz' must use "
        f"'--retry-all-errors' so mid-stream failures trigger a retry. See #181. Block:\n{artists_block}"
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
    ``data/<YYYY>/discogs_<YYYY><MM>01_artists.xml.gz`` convention — the
    same shape as releases, only the basename differs. Test against the
    actual URL Bash will build by running the printf template the script
    uses, since the script now factors the URL into a `dump_url` helper.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    # The script's dump_url helper must reference 'artists' as a `kind` arg
    # at least once (for the current-month URL derivation).
    assert 'dump_url "$year" "${year}${month}" artists' in code, (
        "rebuild-cache.sh must derive the artists URL by passing 'artists' "
        "as the kind argument to its dump_url helper. See LML#497."
    )
    # The discogs URL convention (`data/YYYY/discogs_YYYYMM01_<kind>.xml.gz`)
    # is encoded in the printf template inside dump_url. Pin it.
    assert "discogs_%s01_%s.xml.gz" in code, (
        "rebuild-cache.sh's dump_url helper must use the Discogs URL "
        "convention `discogs_<YYYYMM>01_<kind>.xml.gz`. See LML#497."
    )
