#!/bin/bash
set -eo pipefail

if [[ -d "$HOME/Library/Logs" ]]; then
    LOG_FILE="$HOME/Library/Logs/library-metadata-lookup-etl.log"
else
    LOG_FILE="$(mktemp)"
fi
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SLACK_WEBHOOK_URL="${SLACK_MONITORING_WEBHOOK:-}"
NOTIFY_ENABLED=false
EXIT_CODE=0

# Python interpreter: allow override via PYTHON_BIN, prefer .venv, fall back to python3
PYTHON="${PYTHON_BIN:-.venv/bin/python}"
if ! command -v "$PYTHON" &>/dev/null; then
    PYTHON="python3"
fi

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --notify)
            NOTIFY_ENABLED=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--notify]"
            exit 1
            ;;
    esac
done

log() {
    local msg="$(date '+%Y-%m-%d %H:%M:%S') - $1"
    echo "$msg" >> "$LOG_FILE"
    echo "$msg"
}

notify_error() {
    local message="$1"
    log "ERROR: $message"

    if [[ "$NOTIFY_ENABLED" == "true" && -n "$SLACK_WEBHOOK_URL" ]]; then
        curl -s -X POST "$SLACK_WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{\"text\":\":warning: *Library ETL Failed*\n$message\"}" \
            >> "$LOG_FILE" 2>&1 || true
    fi
}

upload_library_db() {
    local url="$1"
    local label="$2"
    local db_path="$3"

    log "Uploading library.db to $label ($url)..."

    UPLOAD_OUTPUT=$(mktemp)
    HTTP_CODE=$(curl -s -o "$UPLOAD_OUTPUT" -w "%{http_code}" \
        -X POST "$url/admin/upload-library-db" \
        -H "Authorization: Bearer $ADMIN_TOKEN" \
        -F "file=@$db_path" \
        2>> "$LOG_FILE")

    if [[ "$HTTP_CODE" -eq 200 ]]; then
        ROW_COUNT=$($PYTHON -c "import json,sys; print(json.load(sys.stdin).get('row_count','?'))" < "$UPLOAD_OUTPUT" 2>/dev/null || echo "?")
        log "Uploaded to $label successfully ($ROW_COUNT rows)"
        rm -f "$UPLOAD_OUTPUT"
        return 0
    else
        ERROR_BODY=$(cat "$UPLOAD_OUTPUT")
        rm -f "$UPLOAD_OUTPUT"
        notify_error "Upload to $label failed (HTTP $HTTP_CODE): $ERROR_BODY"
        return 1
    fi
}

cd "$REPO_DIR"

# Load environment variables from .env if it exists
if [[ -f .env ]]; then
    set -a
    source .env
    set +a
fi

# Validate required environment variables
if [[ -z "$ADMIN_TOKEN" ]]; then
    log "ERROR: ADMIN_TOKEN is required"
    exit 1
fi

log "Starting library sync"

# Catalog source: Backend-Service's HTTP export, not tubafrenzy's MySQL
# (WXYC/discogs-etl#346). This block used to tunnel into Kattare and run two
# `mysql -B -N` SELECTs against `wxycmusic`; Kattare hosting ends 2026-09-22
# and the catalog's write authority moved to Backend-Service ahead of it, so
# the MySQL read path had nothing left to read. The SELECTs themselves are not
# gone -- `scripts/catalog_parity_diff.py` keeps its own MySQL producer, which
# is what certifies the two sides agree -- they are just no longer on the
# daily path, and they retire with the host rather than with this script.
#
# Everything from the streaming-links enrichment down is deliberately
# untouched by that swap: those steps consume the built SQLite, never the
# source, so the cutover is invisible to them.
#
# BACKEND_CATALOG_URL is a knob rather than a constant so that a one-off can
# aim at staging, exactly as catalog-parity.yml allows; unset means prod.
BACKEND_CATALOG_URL="${BACKEND_CATALOG_URL:-https://api.wxyc.org}"

# Credentials reach the builder through the environment and never through
# argv, which is readable by any `ps` and is echoed by `set -x` and by GitHub
# Actions command traces. Two accepted shapes: a pre-minted service-account
# JWT for an operator running this by hand, or the service account's
# email/password for an unattended run -- a better-auth JWT lives 15 minutes,
# far short of a daily schedule, so CI signs in per run instead of storing a
# token. Checked here rather than left to the builder because an
# unauthenticated run otherwise surfaces several HTTP round-trips later, as a
# producer failure whose real cause is one missing secret.
if [[ -z "$BACKEND_CATALOG_TOKEN" && ( -z "$BACKEND_CATALOG_EMAIL" || -z "$BACKEND_CATALOG_PASSWORD" ) ]]; then
    notify_error "Missing Backend catalog credentials: set BACKEND_CATALOG_TOKEN, or both BACKEND_CATALOG_EMAIL and BACKEND_CATALOG_PASSWORD"
    exit 1
fi

# `mktemp -d` creates only the directory, so library.db does not exist yet --
# which is what build_library_db.py requires. It refuses a pre-existing
# --output, builds beside the target and renames on success, so a failed run
# leaves nothing half-written behind for the next one to trip over.
DB_PATH=$(mktemp -d)/library.db

# Captured as well as logged, so the builder's own `error: ...` line reaches
# notify_error's message rather than only the run log. "the build failed" on
# its own sends whoever reads it hunting for the one fact that mattered. The
# retired MySQL block did the same with its $ERROR_DETAILS tail.
#
# Note the Slack half of notify_error is inert today: this script only posts
# when invoked with --notify, and sync-library.yml passes no flags and carries
# no SLACK_MONITORING_WEBHOOK (docs/automation.md claims otherwise and is
# wrong about it -- pre-existing, not introduced here). The detail still lands
# in the ETL log and the run output, and is already correct for the day that
# wiring gets fixed.
BUILD_OUTPUT=$(mktemp)
log "Building library.db from $BACKEND_CATALOG_URL..."
if ! $PYTHON scripts/build_library_db.py \
    --source "$BACKEND_CATALOG_URL" \
    --output "$DB_PATH" 2>&1 | tee -a "$LOG_FILE" "$BUILD_OUTPUT"; then
    # Prefer the builder's own `error: ` line over the last line of output:
    # the producer prints its diagnosis there, and a trailing warning (an
    # unfetchable compilation-track export, say) would otherwise displace it.
    ERROR_DETAILS=$(sed -n 's/^error: //p' "$BUILD_OUTPUT" | tail -1 || true)
    if [[ -z "$ERROR_DETAILS" ]]; then
        ERROR_DETAILS=$(tail -1 "$BUILD_OUTPUT" || true)
    fi
    ERROR_DETAILS=$(printf '%s' "$ERROR_DETAILS" | sed 's/"/\\"/g')
    rm -f "$DB_PATH" "$BUILD_OUTPUT"
    rmdir "$(dirname "$DB_PATH")" 2>/dev/null || true
    notify_error "library.db build from $BACKEND_CATALOG_URL failed: $ERROR_DETAILS"
    exit 1
fi
rm -f "$BUILD_OUTPUT"

# The per-day row count, which is both a log record and the input to the floor
# below. The retired MySQL block logged the same thing off its TSV line count.
ROW_COUNT=$($PYTHON -c "import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute('SELECT COUNT(*) FROM library').fetchone()[0])" "$DB_PATH" 2>>"$LOG_FILE") || ROW_COUNT=""
log "Built library.db with ${ROW_COUNT:-<error>} rows (floor ${LIBRARY_ROW_FLOOR:-60000})"

# Absolute row floor, checked before enrichment and before either upload.
#
# The two guards that already existed leave a wide gap between them. The
# producer refuses a catalog export of *exactly* zero rows, and
# STREAMING_APPLE_FLOOR asks for 100 apple_music_url links against a ~64,000-row
# catalog. A partial export -- an over-narrow token scope, a server-side query
# regression returning a slice, a truncated cached buffer -- lands squarely
# between the two and publishes a gutted library.db, which the upload then
# swaps in for production's wholesale. Nothing downstream would object, and
# after the Kattare host goes away there is no second catalog left to notice.
#
# 60,000 is chosen against measured counts, not rounded down from a guess. The
# last MySQL-sourced sync uploaded 64,766 rows (2026-09-16); the Backend side
# of the parity run taken the same day holds 64,359 (matched 64,156 +
# extra_in_backend 203). The floor sits ~6.8% -- about 4,350 rows -- below
# that, which is orders of magnitude more headroom than the catalog has ever
# moved in a day: normal movement is net growth plus the occasional librarian
# delete, and the largest one-off shrink anyone has proposed is the 119-row
# unpropagated-delete cohort (0.18%).
#
# Deliberately a catastrophe guard and not a drift detector. It catches losing
# thousands of rows; it will not notice losing fifty, and tightening it until
# it would is how a floor starts failing honest days -- and a failed sync is
# its own outage, since production then keeps serving a staler catalog. Small
# drift is what the parity harness measures while tubafrenzy still answers;
# after that, nothing does, which is an argument for a trend check on the
# uploaded row count rather than for a brittle floor here.
#
# Set LIBRARY_ROW_FLOOR=0 to opt out -- the same escape hatch
# STREAMING_APPLE_FLOOR offers, for a local run against a fixture.
LIBRARY_ROW_FLOOR="${LIBRARY_ROW_FLOOR:-60000}"
if [[ "$LIBRARY_ROW_FLOOR" -gt 0 ]] && [[ -z "$ROW_COUNT" || "$ROW_COUNT" -lt "$LIBRARY_ROW_FLOOR" ]]; then
    rm -f "$DB_PATH"
    rmdir "$(dirname "$DB_PATH")" 2>/dev/null || true
    notify_error "library.db build produced ${ROW_COUNT:-0} rows (< floor $LIBRARY_ROW_FLOOR); aborting before upload rather than replacing production with a truncated catalog"
    exit 1
fi

# Enrich with streaming links (optional — skipped if streaming_availability.db unavailable)
LML_DIR="${LML_REPO_DIR:-$(dirname "$REPO_DIR")/library-metadata-lookup}"
STREAMING_DB="$LML_DIR/streaming_availability.db"

if [[ -f "$STREAMING_DB" && -f "$LML_DIR/scripts/export_streaming_links.py" ]]; then
    log "Enriching with streaming links..."
    if $PYTHON "$LML_DIR/scripts/export_streaming_links.py" \
        --library-db "$DB_PATH" \
        --streaming-db "$STREAMING_DB" 2>&1 | tee -a "$LOG_FILE"; then
        log "Streaming links enrichment complete"
    else
        log "WARNING: Streaming links enrichment failed (continuing without)"
    fi
else
    log "Skipping streaming links (streaming_availability.db not found)"
fi

# Post-enrichment floor assertion (LML#672, belt-and-suspenders). A zero/low
# apple_music_url count means enrichment silently produced a thin library.db
# (download flake, missing streaming db, export bug); fail BEFORE upload rather
# than strip prod's streaming links. STREAMING_APPLE_FLOOR is an absolute floor
# at the consumption layer, complementary to LML's relative upload-coverage guard.
# Set STREAMING_APPLE_FLOOR=0 to opt out (e.g. a local run with no streaming db).
STREAMING_APPLE_FLOOR="${STREAMING_APPLE_FLOOR:-100}"
APPLE_COUNT=$($PYTHON -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); t=c.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name='streaming_links'\").fetchone(); print(c.execute('SELECT COUNT(apple_music_url) FROM streaming_links').fetchone()[0] if t else 0)" "$DB_PATH" 2>>"$LOG_FILE") || APPLE_COUNT=""
log "Streaming links apple_music_url count: ${APPLE_COUNT:-<error>} (floor $STREAMING_APPLE_FLOOR)"
if [[ -z "$APPLE_COUNT" || "$APPLE_COUNT" -lt "$STREAMING_APPLE_FLOOR" ]]; then
    rm -f "$DB_PATH"
    notify_error "Streaming enrichment produced only ${APPLE_COUNT:-0} apple_music_url links (< floor $STREAMING_APPLE_FLOOR); aborting before upload to avoid stripping prod streaming links"
    exit 1
fi

# Upload to staging (if URL configured)
if [[ -n "$STAGING_URL" ]]; then
    upload_library_db "$STAGING_URL" "staging" "$DB_PATH" || EXIT_CODE=1
fi

# Upload to production (if URL configured)
if [[ -n "$PRODUCTION_URL" ]]; then
    upload_library_db "$PRODUCTION_URL" "production" "$DB_PATH" || EXIT_CODE=1
fi

# Re-derive the va_release VA-compilation lookup table in the discogs-cache
# (#344), ahead of the recall-index build below that reads it via LML's
# comp-title matcher. Unconditional on purpose: this doubles as the one-off
# prod backfill and the ongoing freshness mechanism (LML inserts API-fetched
# VA releases into the cache at runtime, and the monthly rebuild currently
# only runs on manual dispatch). The script's own floor guard rolls a
# suspiciously thin derivation back to the previous table, so a bad day here
# can't clobber a good table. Soft-fail without touching EXIT_CODE -- that
# variable gates the recall-index build and the library.db release upload,
# and a derivation failure must block neither (the recall build just reads
# the previous derivation, or degrades on its own).
log "Deriving va_release in the discogs-cache..."
if ! "$PYTHON" scripts/derive_va_release.py 2>&1 | tee -a "$LOG_FILE"; then
    log "WARNING: va_release derivation failed (continuing; recall-index build reads the previous derivation, if any)"
fi

# Build the V/A compilation-track recall index (lml_cache.compilation_track_location,
# LML#1019 / WXYC/discogs-etl#339). Best-effort, mirroring the streaming-links
# enrichment block above: a failure here must never strip or block the library.db
# upload that already succeeded, so it only runs once uploads are known-good and any
# failure just warns and continues.
#
# Unlike export_streaming_links.py, this script imports LML's own application
# package (entity/lookup/config/discogs/scripts) and third-party dependencies
# (aiosqlite, asyncpg, wxyc_etl, ...) that $PYTHON (this repo's own venv) does not
# have installed. Prefer $LML_DIR/.venv/bin/python -- the interpreter LML's own
# checkout is normally developed/provisioned with -- and fall back to $PYTHON so an
# unprovisioned LML checkout (e.g. a bare CI clone with no venv) fails the same
# soft-fail WARN-and-continue path below instead of aborting the sync.
#
# Runs as a module (`-m scripts...`), not a bare script path, because the script
# itself does `from scripts.match_compilations import ...` -- a package-relative
# import that only resolves when $LML_DIR is on sys.path, which `-m` does via cwd.
if [[ $EXIT_CODE -ne 0 ]]; then
    log "Skipping compilation-track recall index build (library.db upload did not fully succeed)"
elif [[ ! -f "$LML_DIR/scripts/build_compilation_track_location.py" ]]; then
    log "Skipping compilation-track recall index build (build script not found in LML checkout)"
else
    log "Building compilation-track recall index (incremental)..."
    LML_PYTHON="$LML_DIR/.venv/bin/python"
    if ! command -v "$LML_PYTHON" &>/dev/null; then
        LML_PYTHON="$PYTHON"
    fi
    if (cd "$LML_DIR" && "$LML_PYTHON" -m scripts.build_compilation_track_location \
        --incremental --library-db "$DB_PATH" 2>&1 | tee -a "$LOG_FILE"); then
        log "Compilation-track recall index build complete"
    else
        log "WARNING: Compilation-track recall index build failed (continuing without)"
    fi
fi

# Copy library.db to LIBRARY_DB_OUTPUT if set (for CI to upload as artifact)
if [[ -n "$LIBRARY_DB_OUTPUT" && -f "$DB_PATH" ]]; then
    cp "$DB_PATH" "$LIBRARY_DB_OUTPUT"
    log "Copied library.db to $LIBRARY_DB_OUTPUT"
fi

# Clean up
rm -f "$DB_PATH"
rmdir "$(dirname "$DB_PATH")" 2>/dev/null || true

if [[ $EXIT_CODE -eq 0 ]]; then
    log "Library sync completed successfully"
else
    log "Library sync completed with errors (see above)"
fi

exit $EXIT_CODE
