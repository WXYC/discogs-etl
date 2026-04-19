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

ETL_OUTPUT=$(mktemp)
CSV_FILE=$(mktemp)
if ! MYSQL_PWD="$LIBRARY_DB_PASSWORD" mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$LIBRARY_DB_USER" \
    --default-character-set=utf8 -B -N "$LIBRARY_DB_NAME" \
    -e "SELECT r.ID, r.TITLE, lc.PRESENTATION_NAME, lc.CALL_LETTERS, lc.CALL_NUMBERS, r.CALL_NUMBERS, g.REFERENCE_NAME, f.REFERENCE_NAME, r.ALTERNATE_ARTIST_NAME FROM LIBRARY_RELEASE r JOIN LIBRARY_CODE lc ON r.LIBRARY_CODE_ID = lc.ID JOIN FORMAT f ON r.FORMAT_ID = f.ID JOIN GENRE g ON lc.GENRE_ID = g.ID" \
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

# Build SQLite database from TSV output
if ! $PYTHON - "$CSV_FILE" "$DB_PATH" <<'PYEOF'
import sqlite3, sys

tsv_path, db_path = sys.argv[1], sys.argv[2]

conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute("""CREATE TABLE library (
    id INTEGER PRIMARY KEY, title TEXT, artist TEXT, call_letters TEXT,
    artist_call_number INTEGER, release_call_number INTEGER,
    genre TEXT, format TEXT, alternate_artist_name TEXT
)""")
cur.execute("""CREATE VIRTUAL TABLE library_fts USING fts5(
    title, artist, alternate_artist_name, content='library', content_rowid='id'
)""")

count = 0
with open(tsv_path, encoding="utf-8") as f:
    for line in f:
        fields = line.rstrip("\n").split("\t")
        if len(fields) != 9:
            print(f"WARNING: skipping malformed row with {len(fields)} fields", file=sys.stderr)
            continue
        # MySQL -B outputs \N for NULL
        row = [None if v == "\\N" else v for v in fields]
        cur.execute("INSERT INTO library VALUES (?,?,?,?,?,?,?,?,?)", row)
        count += 1

cur.execute("""INSERT INTO library_fts(rowid, title, artist, alternate_artist_name)
    SELECT id, title, artist, alternate_artist_name FROM library""")
cur.execute("CREATE INDEX idx_artist ON library(artist)")
cur.execute("CREATE INDEX idx_title ON library(title)")
cur.execute("CREATE INDEX idx_alternate_artist ON library(alternate_artist_name)")
conn.commit()
conn.close()
print(f"Exported {count} rows to {db_path}")
PYEOF
then
    rm -f "$CSV_FILE" "$DB_PATH"
    notify_error "SQLite export failed"
    exit 1
fi
rm -f "$CSV_FILE"

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

# Upload to staging (if URL configured)
if [[ -n "$STAGING_URL" ]]; then
    upload_library_db "$STAGING_URL" "staging" "$DB_PATH" || EXIT_CODE=1
fi

# Upload to production (if URL configured)
if [[ -n "$PRODUCTION_URL" ]]; then
    upload_library_db "$PRODUCTION_URL" "production" "$DB_PATH" || EXIT_CODE=1
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
