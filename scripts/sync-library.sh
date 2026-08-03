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

# Build MySQL connection URL from individual env vars
if [[ -z "$LIBRARY_DB_HOST" || -z "$LIBRARY_DB_USER" || -z "$LIBRARY_DB_PASSWORD" || -z "$LIBRARY_DB_NAME" ]]; then
    notify_error "Missing required LIBRARY_DB_* environment variables"
    exit 1
fi
# Set up SSH tunnel to Kattare if LIBRARY_SSH_HOST is configured
if [[ -n "$LIBRARY_SSH_HOST" && -n "$LIBRARY_SSH_USER" ]]; then
    LOCAL_DB_PORT=13306
    log "Opening SSH tunnel to $LIBRARY_SSH_HOST..."
    ssh -f -N -L "${LOCAL_DB_PORT}:${LIBRARY_DB_HOST}:3306" \
        "${LIBRARY_SSH_USER}@${LIBRARY_SSH_HOST}" \
        -o StrictHostKeyChecking=no -o ConnectTimeout=10
    DB_HOST="127.0.0.1"
    DB_PORT="$LOCAL_DB_PORT"
    log "SSH tunnel established on port $LOCAL_DB_PORT"
else
    DB_HOST="$LIBRARY_DB_HOST"
    DB_PORT="3306"
fi

# Run ETL: query MySQL via CLI (bypasses Python driver auth issues with MySQL 4.1)
DB_PATH=$(mktemp -d)/library.db
MYSQL_HOST="${DB_HOST:-$LIBRARY_DB_HOST}"
MYSQL_PORT="${DB_PORT:-3306}"

# The 11th SELECT column is CROSS_REFERENCE_NAMES: a correlated subquery over
# LIBRARY_CODE_CROSS_REFERENCE that pipe-joins (" | ") the PRESENTATION_NAMEs
# of any LIBRARY_CODEs cataloger-cross-referenced to this row's own code, in
# either FK direction (CROSS_REFERENCING_ARTIST_ID / CROSS_REFERENCED_LIBRARY_
# CODE_ID both -> LIBRARY_CODE.ID), excluding the row's own name. This is the
# same alias link wxyc-catalog's TubafrenzySource.fetch_cross_referenced_artists
# reads for the XML-converter artist filter -- here it rides along on every
# library.db row instead of only feeding that allowlist. See
# WXYC/discogs-etl#334.
#
# ALTERNATE_ARTIST_NAME, ALBUM_ARTIST, and the CROSS_REFERENCE_NAMES subquery
# are wrapped in IFNULL(<expr>, '') because `mysql -B -N` on this server
# prints a genuine SQL NULL as the literal 4-character text "NULL", not the
# "\N" sentinel tsv_to_sqlite.py's parser expects -- and since ALBUM_ARTIST
# feeds the library_fts index, an unwrapped NULL became a literal 'NULL'
# string that a typed search for "null" then matched against the whole
# catalog (verified in prod: 64,780 album_artist / 63,904
# cross_reference_names rows). IFNULL only ever substitutes for a *real* SQL
# NULL, so an artist or cross-reference genuinely named the text "NULL"
# still passes through untouched -- this is deliberately a SQL-layer fix, not
# Python string-sniffing in tsv_to_sqlite.py, which would risk corrupting
# that row instead. The other columns in this SELECT (ID, TITLE,
# PRESENTATION_NAME, CALL_LETTERS, both CALL_NUMBERS, and both REFERENCE_NAME
# columns) are always populated and are left unwrapped.
ETL_OUTPUT=$(mktemp)
CSV_FILE=$(mktemp)
if ! MYSQL_PWD="$LIBRARY_DB_PASSWORD" mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$LIBRARY_DB_USER" \
    --default-character-set=utf8 -B -N "$LIBRARY_DB_NAME" \
    -e "SELECT r.ID, r.TITLE, lc.PRESENTATION_NAME, lc.CALL_LETTERS, lc.CALL_NUMBERS, r.CALL_NUMBERS, g.REFERENCE_NAME, f.REFERENCE_NAME, IFNULL(r.ALTERNATE_ARTIST_NAME, ''), IFNULL(r.ALBUM_ARTIST, ''), IFNULL((SELECT GROUP_CONCAT(DISTINCT xlc.PRESENTATION_NAME SEPARATOR ' | ') FROM LIBRARY_CODE_CROSS_REFERENCE xcr, LIBRARY_CODE xlc WHERE xlc.ID = CASE WHEN xcr.CROSS_REFERENCING_ARTIST_ID = lc.ID THEN xcr.CROSS_REFERENCED_LIBRARY_CODE_ID WHEN xcr.CROSS_REFERENCED_LIBRARY_CODE_ID = lc.ID THEN xcr.CROSS_REFERENCING_ARTIST_ID ELSE NULL END AND (xcr.CROSS_REFERENCING_ARTIST_ID = lc.ID OR xcr.CROSS_REFERENCED_LIBRARY_CODE_ID = lc.ID) AND xlc.ID != lc.ID), '') FROM LIBRARY_RELEASE r JOIN LIBRARY_CODE lc ON r.LIBRARY_CODE_ID = lc.ID JOIN FORMAT f ON r.FORMAT_ID = f.ID JOIN GENRE g ON lc.GENRE_ID = g.ID" \
    > "$CSV_FILE" 2> "$ETL_OUTPUT"; then
    ERROR_DETAILS=$(cat "$ETL_OUTPUT" | tail -1 | sed 's/"/\\"/g')
    cat "$ETL_OUTPUT" >> "$LOG_FILE"
    rm -f "$ETL_OUTPUT" "$CSV_FILE" "$DB_PATH"
    notify_error "MySQL query failed: $ERROR_DETAILS"
    exit 1
fi
cat "$ETL_OUTPUT" >> "$LOG_FILE"
rm -f "$ETL_OUTPUT"

ROW_COUNT=$(wc -l < "$CSV_FILE" | tr -d ' ')
log "Fetched $ROW_COUNT rows from MySQL"

# Fetch compilation track artists (supplementary to LIBRARY_RELEASE; restores the
# export dropped in the #65 slim-down -- WXYC/discogs-etl#332). Reuses the same
# MySQL auth as the query above. artist_name/track_title are free text but mysql
# -B -N already escapes embedded tab/newline/backslash bytes in field values, so
# tsv_to_sqlite.py's tab/newline-based TSV parser (shared with the LIBRARY_RELEASE
# export above) is safe to reuse unchanged.
# Degrades gracefully: pre-V008 fixtures / the Backend-Service catalog source have
# no COMPILATION_TRACK_ARTIST table, so a "doesn't exist" failure here is expected
# and must not fail the overall library sync (or any other CTA-fetch error, since
# CTA is supplementary -- LIBRARY_RELEASE must never be blocked by it).
#
# TRACK_TITLE is wrapped in IFNULL(TRACK_TITLE, '') for the same reason as
# ALBUM_ARTIST/ALTERNATE_ARTIST_NAME above: this is the same `mysql -B -N`
# invocation style, so a genuine SQL NULL here renders as the literal text
# "NULL" too. LIBRARY_RELEASE_ID and ARTIST_NAME are documented NOT NULL
# (see _import_compilation_track_artists in tsv_to_sqlite.py) and stay
# unwrapped.
CTA_CSV_FILE=$(mktemp)
CTA_ETL_OUTPUT=$(mktemp)
CTA_TSV_ARGS=()
if MYSQL_PWD="$LIBRARY_DB_PASSWORD" mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$LIBRARY_DB_USER" \
    --default-character-set=utf8 -B -N "$LIBRARY_DB_NAME" \
    -e "SELECT LIBRARY_RELEASE_ID, ARTIST_NAME, IFNULL(TRACK_TITLE, '') FROM COMPILATION_TRACK_ARTIST ORDER BY LIBRARY_RELEASE_ID" \
    > "$CTA_CSV_FILE" 2> "$CTA_ETL_OUTPUT"; then
    CTA_ROW_COUNT=$(wc -l < "$CTA_CSV_FILE" | tr -d ' ')
    log "Fetched $CTA_ROW_COUNT compilation track artist rows from MySQL"
    if [[ "$CTA_ROW_COUNT" -gt 0 ]]; then
        CTA_TSV_ARGS=(--cta-tsv "$CTA_CSV_FILE")
    fi
else
    CTA_STDERR=$(cat "$CTA_ETL_OUTPUT")
    if echo "$CTA_STDERR" | grep -qi "doesn't exist"; then
        log "COMPILATION_TRACK_ARTIST table not found, skipping compilation track artist export"
    else
        log "WARNING: compilation track artist query failed, continuing without it: $(echo "$CTA_STDERR" | tail -1)"
    fi
fi
cat "$CTA_ETL_OUTPUT" >> "$LOG_FILE"
rm -f "$CTA_ETL_OUTPUT"

# Build SQLite database from TSV output (plus compilation_track_artist, if fetched)
if ! $PYTHON scripts/tsv_to_sqlite.py "$CSV_FILE" "$DB_PATH" "${CTA_TSV_ARGS[@]}" 2>&1 | tee -a "$LOG_FILE"; then
    rm -f "$CSV_FILE" "$CTA_CSV_FILE" "$DB_PATH"
    notify_error "SQLite export failed"
    exit 1
fi
rm -f "$CSV_FILE" "$CTA_CSV_FILE"

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
