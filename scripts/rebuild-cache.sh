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
# exit_code reads $2 first, falling back to $?. Multi-command ERR traps
# that need to run cleanup before on_error MUST snapshot $? themselves
# (the cleanup would otherwise clobber it) and pass it as $2. See #269 —
# the failure mode this guards against is a silent exit-0 report when a
# preceding successful `rm -rf` (or similar) hides a real failure.
on_error() {
    local exit_code=${2:-$?}
    local line=$1
    notify_slack ":warning:" "failed at line ${line} (exit ${exit_code}). Log: ${LOG_FILE}"
    exit "$exit_code"
}
trap 'on_error $LINENO' ERR

# fail <message> — same observability as ERR-trapped failures: post to Slack,
# then exit non-zero. Use this for explicit-failure paths that ERR doesn't
# catch (`exit N` doesn't fire ERR; neither does the non-zero side of an `if`).
fail() {
    notify_slack ":warning:" "$1. Log: ${LOG_FILE}"
    exit 1
}

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

# Try the prebuilt converter binary published by WXYC/discogs-xml-converter's
# Release Binary workflow. Skips the ~20-30 min cargo build on EC2. Falls back
# to source build on any failure (release not published yet, asset missing,
# checksum mismatch, network error) so the rebuild stays resilient against
# converter-side publishing problems. The function returns non-zero on any
# step that fails; the caller decides whether to fall back.
download_prebuilt_converter() {
    local target_dir="$1"
    mkdir -p "$target_dir"
    # gh works against public repos without auth and honors GH_TOKEN when set.
    if ! gh release download \
            --repo WXYC/discogs-xml-converter \
            --pattern 'discogs-xml-converter-linux-x86_64.tar.gz' \
            --pattern 'discogs-xml-converter-linux-x86_64.tar.gz.sha256' \
            --dir "$target_dir" \
            --clobber; then
        return 1
    fi
    (cd "$target_dir" && sha256sum -c discogs-xml-converter-linux-x86_64.tar.gz.sha256) || return 1
    tar -xzf "$target_dir/discogs-xml-converter-linux-x86_64.tar.gz" -C "$target_dir" || return 1
    test -x "$target_dir/discogs-xml-converter" || return 1
    return 0
}

PREBUILT_DIR="$REPO_DIR/.prebuilt-converter"
if download_prebuilt_converter "$PREBUILT_DIR"; then
    echo "    using prebuilt converter from WXYC/discogs-xml-converter latest release"
    export PATH="$PREBUILT_DIR:$PATH"
else
    echo "    prebuilt binary unavailable; falling back to cargo build"
    (cd "$CONVERTER_DIR" && cargo build --release --quiet)
    export PATH="$CONVERTER_DIR/target/release:$PATH"
fi

# ---------------------------------------------------------------------------
# 2b. Apply any pending alembic migrations against the destination DB.
#     Without this, the next monthly tick after a migration merges to main
#     fails at the COPY for the affected table (Railway's schema is one
#     revision behind; the GH Actions cron that used to apply migrations
#     was disabled 2026-05-05). No-op when the DB is already at head, so
#     safe to keep on unconditionally — same posture as --truncate-existing.
#     Skipped under REBUILD_SMOKE=1 because the upgrade is a DB write and
#     smoke mode is read-only by contract (see step 5). See #222.
# ---------------------------------------------------------------------------
if [ "${REBUILD_SMOKE:-}" != "1" ]; then
    echo "[$(date -u +%H:%M:%SZ)] apply pending alembic migrations against \$DATABASE_URL_DISCOGS"
    (cd "$REPO_DIR" && alembic upgrade head)
fi

# ---------------------------------------------------------------------------
# 3. Pull daily-fresh library.db produced by sync-library workflow
# ---------------------------------------------------------------------------
WORK_DIR="$(mktemp -d "$REPO_DIR/data-rebuild.XXXXXX")"
# Snapshot $? before `rm -rf` clobbers it, then thread the captured code
# through to on_error as $2. See #269.
# shellcheck disable=SC2154  # `rc` is assigned in this trap body, not external
trap 'rc=$?; rm -rf "$WORK_DIR"; on_error $LINENO "$rc"' ERR
trap 'rm -rf "$WORK_DIR"' EXIT

# Redirect TMPDIR to the EBS-backed WORK_DIR. Without this, run_pipeline.py's
# `tempfile.TemporaryDirectory(prefix="discogs_pipeline_")` falls back to
# /tmp, which is tmpfs (~50% of RAM = ~2 GB on c6i.large) on Amazon Linux
# 2023. The converter's CSV staging exceeds that partway through the
# release scan and crashes with ENOSPC (run 1 + run 2 of #267). Pinning
# TMPDIR here keeps the temp dir on the EBS root volume; the existing
# WORK_DIR cleanup trap reaps the nested temp dir too. See #271 (and #268
# for the earlier disk-budget hypothesis that didn't address this).
export TMPDIR="$WORK_DIR"

echo "[$(date -u +%H:%M:%SZ)] download library.db from LML release artifact"
gh release download streaming-data-v1 \
    --repo WXYC/library-metadata-lookup \
    --pattern library.db \
    --output "$WORK_DIR/library.db" \
    --clobber
echo "    library.db: $(du -h "$WORK_DIR/library.db" | cut -f1)"

# ---------------------------------------------------------------------------
# 4. Resolve dump URLs — try current month, fall back to previous if 404/403
# ---------------------------------------------------------------------------
# Discogs publishes the releases and artists dumps together each month under
# the same YYYY/discogs_YYYYMM01_*.xml.gz convention, but the two files can
# land on the CDN minutes-to-hours apart. Probe BOTH URLs and only commit
# to a month if both are reachable — otherwise we'd pay the ~10 GB releases
# download then fail at the artists curl, and the ERR/EXIT traps would wipe
# $WORK_DIR forcing a full re-download next tick. LML#497.
echo "[$(date -u +%H:%M:%SZ)] resolve Discogs dump URLs"
year=$(date -u +%Y)
month=$(date -u +%m)

dump_url() {
    # dump_url <year> <yyyymm> <kind>
    printf 'https://data.discogs.com/?download=data%%2F%s%%2Fdiscogs_%s01_%s.xml.gz' "$1" "$2" "$3"
}

both_reachable() {
    curl -sIfL --max-time 15 -o /dev/null "$1" && curl -sIfL --max-time 15 -o /dev/null "$2"
}

url="$(dump_url "$year" "${year}${month}" releases)"
artists_url="$(dump_url "$year" "${year}${month}" artists)"
if ! both_reachable "$url" "$artists_url"; then
    prev=$(date -u -d "1 month ago" +%Y%m 2>/dev/null \
        || date -u -v-1m +%Y%m)
    prev_year=${prev:0:4}
    url="$(dump_url "$prev_year" "$prev" releases)"
    artists_url="$(dump_url "$prev_year" "$prev" artists)"
    if ! both_reachable "$url" "$artists_url"; then
        fail "neither current nor previous month has both releases.xml.gz and artists.xml.gz reachable"
    fi
    echo "    current-month dump not yet fully published; falling back to ${prev}"
fi
echo "    releases URL: $url"
echo "    artists URL:  $artists_url"

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
    # Smoke mode validates both URLs are reachable. A 64 KiB Range request
    # confirms DNS, TLS, Cloudflare reachability, and the gzip magic in
    # the first chunk -- without paying the full transfer.
    echo "[$(date -u +%H:%M:%SZ)] REBUILD_SMOKE=1 — validating dump URL reachability"
    smoke_file="$WORK_DIR/releases-smoke.xml.gz"
    curl -fL --max-time 30 -r 0-65535 -o "$smoke_file" "$url"
    smoke_bytes=$(wc -c < "$smoke_file" | tr -d ' ')
    if [ "$smoke_bytes" -lt 1024 ]; then
        fail "smoke mode read only ${smoke_bytes} bytes from releases URL"
    fi
    echo "    releases smoke OK: read ${smoke_bytes} bytes"
    artists_smoke_file="$WORK_DIR/artists-smoke.xml.gz"
    curl -fL --max-time 30 -r 0-65535 -o "$artists_smoke_file" "$artists_url"
    artists_smoke_bytes=$(wc -c < "$artists_smoke_file" | tr -d ' ')
    if [ "$artists_smoke_bytes" -lt 1024 ]; then
        fail "smoke mode read only ${artists_smoke_bytes} bytes from artists URL"
    fi
    echo "    artists smoke OK: read ${artists_smoke_bytes} bytes"
    notify_slack ":mag:" "smoke test passed (no DB write performed)"
    exit 0
fi

# --continue-at - resumes from the size already on disk if a prior attempt
# left a partial. --retry-all-errors widens curl's retry-on matrix to any
# non-zero exit, including the mid-stream HTTP/2 INTERNAL_ERROR (exit 92)
# that plain --retry refuses to retry. Five attempts at 30s spacing covers
# a transient CDN incident without unbounded re-cost.
#
# After each fetch, assert min on-disk size. curl returning exit 0 isn't
# enough — a 0-byte or truncated file on disk produces no XML records and
# the pipeline silently exits 0 with empty artist/release tables, exactly
# the LML#497 regression this script is meant to detect. Min sizes are well
# below the real dump sizes (~10 GB releases, ~2 GB artists) but high
# enough that a corrupt/partial download fails loudly here.
assert_min_size() {
    # assert_min_size <path> <min_bytes> <label>. fail() so the ERR-trap
    # observability path (Slack notify) still fires for size-floor failures.
    local path="$1" min="$2" label="$3"
    local actual
    actual=$(wc -c < "$path" | tr -d ' ')
    if [ "$actual" -lt "$min" ]; then
        fail "$label download too small: ${actual} bytes (expected at least ${min})"
    fi
}

echo "[$(date -u +%H:%M:%SZ)] download releases dump → $WORK_DIR/releases.xml.gz"
curl -fL --continue-at - --retry 5 --retry-delay 30 --retry-all-errors \
    -o "$WORK_DIR/releases.xml.gz" \
    "$url"
assert_min_size "$WORK_DIR/releases.xml.gz" $((1024 * 1024 * 1024)) "releases.xml.gz"
echo "    releases download complete ($(du -h "$WORK_DIR/releases.xml.gz" | cut -f1))"

# Artists dump is ~2 GB compressed. Sibling fetch under the same resilience
# flags. Once both files are in $WORK_DIR, run_pipeline.py is invoked in
# directory mode (--xml "$WORK_DIR") so the converter's run_directory path
# triggers process_artists alongside the release scanner. Without this fetch
# the artist-side CSVs are never written and artist.profile stays 97.9%
# NULL. LML#497.
echo "[$(date -u +%H:%M:%SZ)] download artists dump → $WORK_DIR/artists.xml.gz"
curl -fL --continue-at - --retry 5 --retry-delay 30 --retry-all-errors \
    -o "$WORK_DIR/artists.xml.gz" \
    "$artists_url"
assert_min_size "$WORK_DIR/artists.xml.gz" $((100 * 1024 * 1024)) "artists.xml.gz"
echo "    artists download complete ($(du -h "$WORK_DIR/artists.xml.gz" | cut -f1))"

echo "[$(date -u +%H:%M:%SZ)] start pipeline"
# Default mode (no --truncate-existing): the import path is idempotent via
# `import_release_via_upsert` (staging-table COPY + UPSERT excluding the
# artwork columns from the SET list), so a rerun against a populated DB
# preserves LML's runtime artwork back-patches. Adding --truncate-existing
# here would force the legacy destructive path and wipe those back-patches —
# the failure mode WXYC/discogs-etl#252 documents from the 2026-05-30 run.
# The duplicate-key failure mode from #188 that originally motivated the
# flag is no longer reachable on the default path (ON CONFLICT skips it).
#
# Directory mode (--xml "$WORK_DIR"): the converter scans the directory for
# .xml / .xml.gz files and dispatches each by root-element auto-detection
# (run_directory in main.rs). With both releases.xml.gz and artists.xml.gz
# present, process_artists runs alongside the release scanner, writing the
# artist-side CSVs (artist.csv, artist_alias.csv, artist_name_variation.csv,
# artist_member.csv) into the CSV staging dir; import_csv.py --base-only's
# inline import_artist_details call then loads them. LML#497.
# --xml-type is intentionally absent — it forces single-file mode and would
# skip artist processing.
python "$REPO_DIR/scripts/run_pipeline.py" \
    --xml "$WORK_DIR" \
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
