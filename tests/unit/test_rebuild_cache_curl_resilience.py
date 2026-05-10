"""Pin the curl resilience invariants of ``scripts/rebuild-cache.sh``.

The Discogs CDN (``data.discogs.com``, Cloudflare-fronted) occasionally resets
the HTTP/2 stream partway through the multi-tens-of-GB releases dump. Run #3
on 2026-05-10 (instance ``i-0af07e0f56910ab9a``) hit this ~9 minutes into a
~14 minute download:

    curl: (92) HTTP/2 stream 1 was not closed cleanly: INTERNAL_ERROR (err 2)

The original script piped curl through a named pipe (``mkfifo``) into the
converter so the multi-tens-of-GB compressed dump never hit disk -- a
workaround for the Backend-Service EC2's ~14 GB free-disk budget. The new
ephemeral t3.medium has a 100 GB gp3 volume, so disk-spooling is feasible and
sidesteps the FIFO failure surface entirely (a mid-stream curl reset against
a FIFO is unrecoverable -- the converter has already consumed bytes 0..N and
cannot seek). Once spooled to disk, ``curl --continue-at - --retry-all-errors``
resumes mid-download natively. (#181)

These tests are static-structural: they parse the script and assert the
relevant fragments exist. They do not execute curl -- the live behavior is
covered by the next ephemeral rebuild's S3 log archive.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "rebuild-cache.sh"


@pytest.fixture(scope="module")
def script_text() -> str:
    return SCRIPT_PATH.read_text()


@pytest.fixture(scope="module")
def script_lines() -> list[str]:
    return SCRIPT_PATH.read_text().splitlines()


def _non_comment_lines(lines: list[str]) -> list[str]:
    return [line for line in lines if not line.lstrip().startswith("#")]


def test_main_download_uses_continue_at_resume(script_text: str) -> None:
    """#181: a partial download must be resumable via Range request.

    ``curl --continue-at -`` reads the existing on-disk size and asks the
    server to resume from that byte offset. Without this flag, a retry
    starts over from byte 0 -- which on a 14-minute download means we
    pay the full transfer cost on every flake.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert "--continue-at -" in code, (
        "rebuild-cache.sh must invoke curl with '--continue-at -' for the "
        "main dump download so a mid-stream HTTP/2 reset can resume from "
        "the byte offset already on disk. See #181."
    )


def test_main_download_uses_retry_all_errors(script_text: str) -> None:
    """#181: --retry only retries the *initial* connection by default.

    ``curl --retry`` without ``--retry-all-errors`` does not retry on a
    mid-stream HTTP/2 INTERNAL_ERROR (exit 92) -- it treats it as a
    fatal protocol error. ``--retry-all-errors`` widens the retry-on
    matrix to any non-zero curl exit, which is what we want for a
    flaky CDN endpoint.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert "--retry-all-errors" in code, (
        "rebuild-cache.sh must invoke curl with '--retry-all-errors' so "
        "mid-stream failures (exit 92, exit 18, etc.) trigger a retry. "
        "Plain '--retry N' only handles initial-connection failures and "
        "specific HTTP status codes. See #181."
    )


def test_main_download_does_not_use_fifo(script_text: str) -> None:
    """#181: spool the dump to disk, not through a FIFO.

    A FIFO is unseekable; once the converter has consumed bytes 0..N,
    a mid-stream curl reset cannot resume. The original FIFO design
    was a workaround for the Backend-Service EC2's ~14 GB disk budget.
    The ephemeral t3.medium has 100 GB gp3, so disk-spooling fits
    (compressed dump is ~10 GB) and removes the unrecoverable failure
    surface.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert "mkfifo" not in code, (
        "rebuild-cache.sh must not use 'mkfifo' for the dump download. "
        "Spool to a regular file via 'curl -o' so '--continue-at -' can "
        "resume mid-download. See #181."
    )


def test_main_download_runs_curl_synchronously(script_lines: list[str]) -> None:
    """#181: with disk-spool, curl must complete before the pipeline starts.

    The old FIFO design backgrounded curl ('curl ... &') and ran the
    converter concurrently against the FIFO. Disk-spool is a sequential
    two-step: download finishes, then converter consumes the file. The
    backgrounded ``CURL_PID`` + ``wait`` machinery is no longer needed
    and would break ``--continue-at`` accounting (the spool file
    wouldn't be at its final size when the pipeline tried to read).
    """
    code = "\n".join(_non_comment_lines(script_lines))
    assert "CURL_PID=$!" not in code, (
        "rebuild-cache.sh's main download path must not background curl "
        "via '&' + CURL_PID -- disk-spool is sequential. See #181."
    )
    assert 'wait "$CURL_PID"' not in code and "wait $CURL_PID" not in code, (
        "rebuild-cache.sh must not 'wait $CURL_PID' in the main download "
        "path (curl is synchronous now). See #181."
    )


def test_main_download_spools_to_xml_gz_file(script_text: str) -> None:
    """#181: the spool target must be ``$WORK_DIR/releases.xml.gz``.

    ``run_pipeline.py --xml`` is then invoked against that file. Pin the
    name so a stray rename can't silently break the handoff.
    """
    code = "\n".join(_non_comment_lines(script_text.splitlines()))
    assert '-o "$WORK_DIR/releases.xml.gz"' in code, (
        "rebuild-cache.sh must spool the dump to '$WORK_DIR/releases.xml.gz' "
        "(the path that run_pipeline.py reads via --xml). See #181."
    )
