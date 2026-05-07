#!/usr/bin/env bash
# Monthly Discogs cache rebuild — invoked by EC2 cron at 06:00 UTC on the
# 4th of each month. Streams the dump from data.discogs.com directly into
# the converter via a FIFO, runs the pipeline against $DATABASE_URL_DISCOGS,
# notifies Slack on outcome.
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
#                                cargo build, library.db download, dump URL,
#                                FIFO + curl handshake) and exit 0 *before*
#                                writing anything to $DATABASE_URL_DISCOGS.
#                                Use to validate a fresh EC2 setup without
#                                touching prod.

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
# 5. Stream dump into converter via FIFO + run pipeline
# ---------------------------------------------------------------------------
# Backend-Service EC2 only has ~14 GB free, so we cannot spool the ~10 GB
# compressed dump to disk alongside the ~5–10 GB of intermediate filtered
# CSVs the converter writes. Instead we stream curl through a named pipe
# directly into the converter.
#
# run_pipeline.py forwards --xml-type=releases to discogs-xml-converter,
# which skips the per-file root-element auto-detection that would
# otherwise open-and-close the FIFO once before the real scan,
# SIGPIPE-killing the upstream curl. Without this the EC2 path fails
# (verified 2026-05-06).
#
# --library-db is forwarded straight to the converter; the converter does
# its own pair-wise (artist, title) filter inside the streaming scanner,
# so release output is ~50K rows from the start instead of ~4M. No
# library_artists.txt pre-build is required (the converter no longer needs
# it on this path). See WXYC/discogs-xml-converter#45.

if [ "${REBUILD_SMOKE:-}" = "1" ]; then
    # Smoke mode validates the URL is reachable and the FIFO machinery works.
    # We read only ~64 KB from the FIFO, which is enough to confirm DNS, TLS,
    # Cloudflare reachability, and the gzip magic in the first chunk. head
    # closing its read end gives curl a SIGPIPE, which we silence.
    echo "[$(date -u +%H:%M:%SZ)] REBUILD_SMOKE=1 — validating curl→FIFO handshake"
    mkfifo "$WORK_DIR/releases.xml.gz"
    curl -fL --max-time 30 \
        -o "$WORK_DIR/releases.xml.gz" \
        "$url" &
    CURL_PID=$!
    head_bytes=$(head -c 65536 "$WORK_DIR/releases.xml.gz" | wc -c)
    wait "$CURL_PID" 2>/dev/null || true
    if [ "$head_bytes" -lt 1024 ]; then
        echo "::error:: smoke mode read only ${head_bytes} bytes from the FIFO" >&2
        exit 1
    fi
    echo "    smoke OK: read ${head_bytes} bytes from the streamed dump"
    notify_slack ":mag:" "smoke test passed (no DB write performed)"
    exit 0
fi

mkfifo "$WORK_DIR/releases.xml.gz"

echo "[$(date -u +%H:%M:%SZ)] start streaming download → pipeline"
curl -fL --retry 3 --retry-delay 30 \
    -o "$WORK_DIR/releases.xml.gz" \
    "$url" &
CURL_PID=$!

python "$REPO_DIR/scripts/run_pipeline.py" \
    --xml "$WORK_DIR/releases.xml.gz" \
    --xml-type releases \
    --library-db "$WORK_DIR/library.db"

# Curl is normally already done by here. Wait surfaces any non-zero curl exit
# so a streaming network failure isn't masked by the pipeline succeeding on
# partial input.
wait "$CURL_PID"

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
