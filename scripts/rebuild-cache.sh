#!/usr/bin/env bash
# Monthly Discogs cache rebuild — invoked by EC2 cron at 06:00 UTC on the
# 4th of each month. Spools the dump from data.discogs.com to disk (with
# resumable retry on mid-stream errors), runs the pipeline against
# $DATABASE_URL_DISCOGS, notifies Slack on outcome.
#
# Setup runbook: docs/ec2-rebuild-runbook.md
#
# Required env (from /etc/discogs-rebuild.env or equivalent):
#   DATABASE_URL_DISCOGS         destination Postgres URL (Railway public proxy)
#   REPO_DIR                     local clone of discogs-etl (default /opt/discogs-etl)
#   CONVERTER_DIR                local clone of discogs-xml-converter
#                                (default /opt/discogs-xml-converter)
#   LOG_DIR                      where to write per-run logs (default /var/log/discogs-rebuild)
#
# Optional env:
#   SENTRY_DSN                   forwarded to wxyc_etl.logger
#   SLACK_MONITORING_WEBHOOK     posts a one-line status when set
#   GH_TOKEN                     used by `gh release download` (any token with
#                                read scope on WXYC/library-metadata-lookup)
#   DRIFT_MIN_RATIO              watchdog threshold (default 0.7)
#   REBUILD_SMOKE                when set to 1, exercise everything that can
#                                fail at host setup (env, gh auth, git pulls,
#                                cargo build, library.db download, dump URL
#                                reachability via a 64 KiB Range request) and
#                                exit 0 *before* writing anything to
#                                $DATABASE_URL_DISCOGS. Use to validate a
#                                fresh EC2 setup without touching prod.

set -euo pipefail

REPO_DIR="${REPO_DIR:-/opt/discogs-etl}"
CONVERTER_DIR="${CONVERTER_DIR:-/opt/discogs-xml-converter}"
LOG_DIR="${LOG_DIR:-/var/log/discogs-rebuild}"
DRIFT_MIN_RATIO="${DRIFT_MIN_RATIO:-0.7}"

mkdir -p "$LOG_DIR"
TS="$(date -u +%Y-%m-%dT%H%MZ)"
LOG_FILE="$LOG_DIR/$TS.log"
exec > >(tee -a "$LOG_FILE") 2>&1

# Single-instance lock. Two cron ticks landing on a still-running rebuild
# would clobber each other's COPY work.
LOCK_FD=200
LOCK_FILE="${LOCK_FILE:-$LOG_DIR/discogs-rebuild.lock}"
exec 200>"$LOCK_FILE"
if ! flock -n "$LOCK_FD"; then
    echo "[$(date -u +%H:%M:%SZ)] another rebuild is already running; exiting"
    exit 0
fi

notify_slack() {
    local emoji="$1" message="$2"
    if [ -z "${SLACK_MONITORING_WEBHOOK:-}" ]; then
        return 0
    fi
    curl -sS -X POST "$SLACK_MONITORING_WEBHOOK" \
        -H 'Content-Type: application/json' \
        -d "{\"text\":\"${emoji} Discogs cache rebuild: ${message}\"}" \
        --max-time 10 || true
}

# Trap surfaces unexpected exits to Slack with the failing line context.
on_error() {
    local exit_code=$?
    local line=$1
    notify_slack ":warning:" "failed at line ${line} (exit ${exit_code}). Log: ${LOG_FILE}"
    exit "$exit_code"
}
trap 'on_error $LINENO' ERR

echo "[$(date -u +%H:%M:%SZ)] starting rebuild — log: $LOG_FILE"

# ---------------------------------------------------------------------------
# 1. Refresh code (discogs-etl + discogs-xml-converter)
# ---------------------------------------------------------------------------
echo "[$(date -u +%H:%M:%SZ)] git fetch + reset --hard origin/main"
git -C "$REPO_DIR" fetch --quiet origin main
git -C "$REPO_DIR" reset --quiet --hard origin/main
git -C "$CONVERTER_DIR" fetch --quiet origin main
git -C "$CONVERTER_DIR" reset --quiet --hard origin/main

# ---------------------------------------------------------------------------
# 2. Refresh deps. Cheap when nothing changed — pip is no-op, cargo build
#    only re-links if Cargo.lock or sources moved.
# ---------------------------------------------------------------------------
echo "[$(date -u +%H:%M:%SZ)] refresh Python venv + Rust converter binary"
# shellcheck disable=SC1091
source "$REPO_DIR/.venv/bin/activate"
pip install --quiet -e "${REPO_DIR}[dev]"
(cd "$CONVERTER_DIR" && cargo build --release --quiet)
export PATH="$CONVERTER_DIR/target/release:$PATH"

# ---------------------------------------------------------------------------
# 3. Pull daily-fresh library.db produced by sync-library workflow
# ---------------------------------------------------------------------------
WORK_DIR="$(mktemp -d "$REPO_DIR/data-rebuild.XXXXXX")"
trap 'rm -rf "$WORK_DIR"; on_error $LINENO' ERR
trap 'rm -rf "$WORK_DIR"' EXIT

echo "[$(date -u +%H:%M:%SZ)] download library.db from LML release artifact"
gh release download streaming-data-v1 \
    --repo WXYC/library-metadata-lookup \
    --pattern library.db \
    --output "$WORK_DIR/library.db" \
    --clobber
echo "    library.db: $(du -h "$WORK_DIR/library.db" | cut -f1)"

# ---------------------------------------------------------------------------
# 4. Resolve dump URL — try current month, fall back to previous if 404/403
# ---------------------------------------------------------------------------
echo "[$(date -u +%H:%M:%SZ)] resolve Discogs dump URL"
year=$(date -u +%Y)
month=$(date -u +%m)
url="https://data.discogs.com/?download=data%2F${year}%2Fdiscogs_${year}${month}01_releases.xml.gz"
if ! curl -sIfL --max-time 15 -o /dev/null "$url"; then
    prev=$(date -u -d "1 month ago" +%Y%m 2>/dev/null \
        || date -u -v-1m +%Y%m)
    prev_year=${prev:0:4}
    url="https://data.discogs.com/?download=data%2F${prev_year}%2Fdiscogs_${prev}01_releases.xml.gz"
    echo "    current-month dump not yet available; falling back to ${prev}"
fi
echo "    dump URL: $url"

# ---------------------------------------------------------------------------
# 5. Spool dump to disk + run pipeline
# ---------------------------------------------------------------------------
# The compressed releases dump is ~10 GB. The ephemeral t3.medium has 100 GB
# gp3, so we spool to a regular file and pass the path to the converter. An
# earlier FIFO design was load-bearing on the Backend-Service EC2's ~14 GB
# disk budget; that constraint no longer applies on the ephemeral host
# (#181). FIFO was unrecoverable on a mid-stream HTTP/2 reset (run #3,
# 2026-05-10, instance i-0af07e0f56910ab9a hit
# 'curl: (92) HTTP/2 stream 1 was not closed cleanly: INTERNAL_ERROR')
# because once the converter has consumed bytes 0..N from a FIFO, no resume
# is possible. Disk-spool with --continue-at + --retry-all-errors recovers
# from a CDN flake by resuming from the last byte already on disk.
#
# --library-db is forwarded straight to the converter; the converter does
# its own pair-wise (artist, title) filter inside the streaming scanner,
# so release output is ~50K rows from the start instead of ~4M. No
# library_artists.txt pre-build is required (the converter no longer needs
# it on this path). See WXYC/discogs-xml-converter#45.

if [ "${REBUILD_SMOKE:-}" = "1" ]; then
    # Smoke mode validates the URL is reachable. A 64 KiB Range request
    # confirms DNS, TLS, Cloudflare reachability, and the gzip magic in
    # the first chunk -- without paying the full 10 GB transfer.
    echo "[$(date -u +%H:%M:%SZ)] REBUILD_SMOKE=1 — validating dump URL reachability"
    smoke_file="$WORK_DIR/releases.xml.gz.smoke"
    curl -fL --max-time 30 -r 0-65535 -o "$smoke_file" "$url"
    smoke_bytes=$(wc -c < "$smoke_file" | tr -d ' ')
    if [ "$smoke_bytes" -lt 1024 ]; then
        echo "::error:: smoke mode read only ${smoke_bytes} bytes" >&2
        exit 1
    fi
    echo "    smoke OK: read ${smoke_bytes} bytes from the dump URL"
    notify_slack ":mag:" "smoke test passed (no DB write performed)"
    exit 0
fi

# --continue-at - resumes from the size already on disk if a prior attempt
# left a partial. --retry-all-errors widens curl's retry-on matrix to any
# non-zero exit, including the mid-stream HTTP/2 INTERNAL_ERROR (exit 92)
# that plain --retry refuses to retry. Five attempts at 30s spacing covers
# a transient CDN incident without unbounded re-cost.
echo "[$(date -u +%H:%M:%SZ)] download dump → $WORK_DIR/releases.xml.gz"
curl -fL --continue-at - --retry 5 --retry-delay 30 --retry-all-errors \
    -o "$WORK_DIR/releases.xml.gz" \
    "$url"
echo "    download complete ($(du -h "$WORK_DIR/releases.xml.gz" | cut -f1))"

echo "[$(date -u +%H:%M:%SZ)] start pipeline"
python "$REPO_DIR/scripts/run_pipeline.py" \
    --xml "$WORK_DIR/releases.xml.gz" \
    --xml-type releases \
    --library-db "$WORK_DIR/library.db"

# ---------------------------------------------------------------------------
# 6. Drift watchdog — same library.db the pipeline just filtered against
# ---------------------------------------------------------------------------
echo "[$(date -u +%H:%M:%SZ)] cache-drift watchdog (min-ratio=$DRIFT_MIN_RATIO)"
python "$REPO_DIR/scripts/check_cache_drift.py" \
    --library-db "$WORK_DIR/library.db" \
    --min-ratio "$DRIFT_MIN_RATIO"

echo "[$(date -u +%H:%M:%SZ)] rebuild complete"
notify_slack ":white_check_mark:" "rebuilt successfully (log: ${LOG_FILE})"

# ---------------------------------------------------------------------------
# 7. Trim old logs (keep ~6 months of tick history)
# ---------------------------------------------------------------------------
find "$LOG_DIR" -maxdepth 1 -name '*.log' -mtime +180 -delete 2>/dev/null || true
