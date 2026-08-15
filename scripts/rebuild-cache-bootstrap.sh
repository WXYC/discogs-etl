#!/usr/bin/env bash
# rebuild-cache-bootstrap.sh — runs as user-data on a freshly-spawned
# ephemeral EC2 instance owned by the `wxyc-discogs-rebuild` CloudFormation
# stack. The launcher Lambda boots the instance, clones discogs-etl onto
# the root volume, and execs this script.
#
# Responsibilities:
#   1. Install runtime deps (Python 3.11, Rust, git, postgres client, gh, AWS CLI v2)
#   2. Clone discogs-xml-converter and build its release binary
#   3. Pull DATABASE_URL_DISCOGS / GH_TOKEN / SLACK_MONITORING_WEBHOOK / SENTRY_DSN
#      (and the optional PRUNE_AUDIT_ENABLED flag) from SSM
#   4. Invoke scripts/rebuild-cache.sh (the same script the legacy host runs)
#   5. Upload the rebuild log to the S3 bucket named in $REBUILD_LOG_BUCKET
#   6. shutdown -h now — the launch template sets
#      InstanceInitiatedShutdownBehavior=terminate so the AWS-side stop is
#      what releases the EC2 + EBS billing.
#
# Environment (passed via instance tags / launch template env, NOT user-data):
#   REBUILD_SSM_PREFIX     SSM parameter path prefix (e.g. /wxyc/discogs-rebuild)
#   REBUILD_LOG_BUCKET     S3 bucket for archived per-run logs
#   AWS_REGION             AWS region (e.g. us-east-1) — IMDSv2 also reachable
#
# Failure semantics:
#   - Any unhandled exit triggers `trap EXIT`, which uploads whatever log we
#     have and runs `shutdown -h now`. The launch template's
#     InstanceInitiatedShutdownBehavior=terminate makes that release the
#     instance even when the bootstrap crashed mid-stream.
#   - The sweeper Lambda is the second line of defence: any rebuild-tagged
#     instance still running >3h after launch is force-terminated.
#
# Runbook: docs/ec2-rebuild-runbook.md (Ephemeral instance section).

set -euo pipefail

# cloud-init runs user-data with a stripped environment — HOME, USER, and
# LOGNAME are not set, and `set -u` trips the moment the script references
# any of them (e.g. the Rust-install `"$HOME/.cargo/bin/cargo"` line, or
# `sudo chown "$USER:$USER" "$CONVERTER_DIR"`). Default to root-appropriate
# values up front; an interactive shell inherits the existing values and
# these no-op. See #176 (caught live on the 2026-05-10 run #2 attempt).
export HOME="${HOME:-/root}"
export USER="${USER:-root}"
export LOGNAME="${LOGNAME:-root}"

REPO_DIR="${REPO_DIR:-/opt/discogs-etl}"
CONVERTER_DIR="${CONVERTER_DIR:-/opt/discogs-xml-converter}"
LOG_DIR="${LOG_DIR:-/var/log/discogs-rebuild}"
LAUNCH_ID="bootstrap-$(date -u +%Y-%m-%dT%H%MZ)-pid$$"
BOOTSTRAP_LOG="${LOG_DIR}/bootstrap-$(date -u +%Y-%m-%dT%H%MZ).log"
# INSTANCE_ID is replaced by the IMDS-derived value once IMDSv2 is reachable.
# Until then the trap and breadcrumb use $LAUNCH_ID as the S3 prefix, so a
# crash before IMDS still leaves a per-launch trace findable by timestamp +
# CloudTrail RunInstances correlation.
INSTANCE_ID="$LAUNCH_ID"
# AWS_REGION needs a default before the breadcrumb's `aws s3 cp` runs —
# aws CLI on EC2 does not auto-resolve region from IMDS without it. The
# launch template lives in us-east-1 so this default is correct in
# practice; IMDS overrides it below for posterity.
AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_REGION

mkdir -p "$LOG_DIR"
exec > >(tee -a "$BOOTSTRAP_LOG") 2>&1

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

# Drop a "bootstrap started" breadcrumb into S3 before any set-e-fatal
# call. Even if everything below this line dies before the trap can run,
# the operator at least sees a marker proving the script began executing.
# Best-effort — `|| log WARN` keeps a credentials/network failure on the
# breadcrumb itself from killing the script. See #174.
{
    echo "launch_id=${LAUNCH_ID}"
    echo "pid=$$"
    echo "utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$LOG_DIR/00-started.txt"
if [ -n "${REBUILD_LOG_BUCKET:-}" ]; then
    aws s3 cp --only-show-errors \
        "$LOG_DIR/00-started.txt" \
        "s3://${REBUILD_LOG_BUCKET}/${LAUNCH_ID}/00-started.txt" \
        || true
else
    log "WARN: REBUILD_LOG_BUCKET unset; skipping S3 breadcrumb"
fi

# Slack helper. Reads SLACK_MONITORING_WEBHOOK from env once it is sourced.
notify_slack() {
    local emoji="$1" message="$2"
    if [ -z "${SLACK_MONITORING_WEBHOOK:-}" ]; then
        return 0
    fi
    curl -sS -X POST "$SLACK_MONITORING_WEBHOOK" \
        -H 'Content-Type: application/json' \
        -d "{\"text\":\"${emoji} Discogs cache rebuild (ephemeral): ${message}\"}" \
        --max-time 10 || true
}

# Concurrent-rebuild guard (#311). The launcher's #304 precheck only covers
# the EventBridge-dispatched path; a rebuild started by calling RunInstances /
# the launch template directly (as the 2026-07-06 #298 recovery did) slips
# past it. The bootstrap is the authoritative choke point for *all* launch
# paths — the last step before the pipeline touches the shared Railway cache.
#
# Peer query mirrors the launcher/sweeper `list_active_rebuild_instances`
# filter (tag:Project=discogs-rebuild + pending/running). Keep it in sync.
# The bootstrap is bash, so it shells out to `aws ec2 describe-instances`
# rather than importing the Python helper (needs ec2:DescribeInstances on
# InstanceRole; Describe has no resource-level scoping).
#
# Tie-break — a *total* order so exactly one of N concurrently-booted
# instances proceeds: earliest LaunchTime wins, ties broken by the
# lexicographically smaller InstanceId. LaunchTime is ISO-8601 UTC, so a
# plain `sort` over "<LaunchTime>\t<InstanceId>" lines yields that order and
# `head -n1` is the winner. The earliest-launched instance sees itself win
# and proceeds; every later instance sees an earlier peer and bows out. No
# mutual-suicide path.
#
# Fail-open only covers a *successful* query that legitimately finds zero
# peers, or one that hasn't yet caught up to self (DescribeInstances is
# eventually consistent) — the launcher precheck already ran and the >3h
# sweeper is the backstop. TOCTOU note: DescribeInstances isn't atomic, but
# this is the *late* authoritative check, a much narrower window than the
# launcher's.
#
# A *failed* query (non-zero `aws` exit — AccessDenied, throttling, network)
# is a different condition and does NOT fail open: it aborts. On 2026-08-04
# an org-account instance whose InstanceRole lacked ec2:DescribeInstances hit
# this path; `2>/dev/null || instances=""` discarded the AccessDenied error
# and collapsed it into the same empty string a legitimately-empty result
# produces, so the guard logged a benign-looking WARN and proceeded to
# destroy ~119,432 rows (#352). A guard that cannot run is not a guard — see
# #355.
abort_if_not_winning_rebuild() {
    local instances winner stderr_file query_exit=0 query_stderr
    stderr_file="$(mktemp)"
    instances="$(aws ec2 describe-instances \
        --filters 'Name=tag:Project,Values=discogs-rebuild' \
                  'Name=instance-state-name,Values=pending,running' \
        --query 'Reservations[].Instances[].[LaunchTime,InstanceId]' \
        --output text 2>"$stderr_file")" || query_exit=$?
    query_stderr="$(tr '\n' ' ' < "$stderr_file")"
    rm -f "$stderr_file"

    if [ "$query_exit" -ne 0 ]; then
        log "ERROR: BootstrapPeerQueryFailed: aws ec2 describe-instances exited ${query_exit}; cannot verify no peer rebuild is running -- stderr: ${query_stderr}"
        notify_slack ":rotating_light:" "peer query failed (exit ${query_exit}) on ${INSTANCE_ID} -- aborting rather than risk writing the shared cache alongside an unranked peer. stderr: ${query_stderr}"
        # A failed guard is not a guard. This aborts (nonzero exit) rather
        # than falling through to the fail-open branches below — trap
        # on_exit EXIT still uploads the log and runs shutdown -h now.
        exit 1
    fi

    if [ -z "$instances" ]; then
        log "WARN: peer check found no active rebuild instances (self not yet visible?); proceeding"
        return 0
    fi
    if ! printf '%s\n' "$instances" | grep -qF "$INSTANCE_ID"; then
        log "WARN: peer check does not yet list self ${INSTANCE_ID}; cannot rank, proceeding"
        return 0
    fi

    winner="$(printf '%s\n' "$instances" | sort | head -n1 | awk '{print $NF}')"
    if [ "$winner" != "$INSTANCE_ID" ]; then
        log "BootstrapCollisionAborted: peer ${winner} outranks self ${INSTANCE_ID}; bowing out before any cache write"
        notify_slack ":no_entry:" "concurrent rebuild detected — ${INSTANCE_ID} bowing out (winner ${winner}); no cache write, self-terminating"
        # exit 0 → trap on_exit EXIT uploads the log and runs shutdown -h now
        # (InstanceInitiatedShutdownBehavior=terminate releases the instance).
        exit 0
    fi
    log "peer check: ${INSTANCE_ID} is the winning rebuild instance; proceeding"
}

# trap EXIT runs on every exit path — clean or panic. It uploads the log
# and calls shutdown unconditionally so a crashed bootstrap can't leak the
# instance past the InstanceInitiatedShutdownBehavior=terminate window.
# Registered before any IMDS / SSM / dnf / git / curl call so an early
# failure still triggers the upload-and-shutdown chain. See #173.
on_exit() {
    local exit_code=$?
    set +e
    log "on_exit (exit_code=${exit_code})"
    if [ -n "${REBUILD_LOG_BUCKET:-}" ] && [ -d "$LOG_DIR" ]; then
        aws s3 cp --recursive --only-show-errors \
            "$LOG_DIR/" "s3://${REBUILD_LOG_BUCKET}/${INSTANCE_ID}/" \
            || log "WARN: log upload to s3://${REBUILD_LOG_BUCKET}/${INSTANCE_ID}/ failed"
    fi
    if [ "$exit_code" -ne 0 ]; then
        notify_slack ":warning:" "bootstrap exited ${exit_code} on ${INSTANCE_ID}; log uploaded to s3://${REBUILD_LOG_BUCKET}/${INSTANCE_ID}/"
    fi
    log "shutdown -h now"
    /usr/sbin/shutdown -h now || true
}
trap on_exit EXIT

# IMDSv2: get a session token, then read instance id + region. Required
# regardless of whether the script runs interactively or as user-data.
imds_token() {
    curl -fsS -X PUT 'http://169.254.169.254/latest/api/token' \
        -H 'X-aws-ec2-metadata-token-ttl-seconds: 300' --max-time 5
}
imds_get() {
    local token="$1" path="$2"
    curl -fsS "http://169.254.169.254/latest/${path}" \
        -H "X-aws-ec2-metadata-token: ${token}" --max-time 5
}

TOKEN="$(imds_token)"
INSTANCE_ID="$(imds_get "$TOKEN" meta-data/instance-id)"
AWS_REGION="$(imds_get "$TOKEN" meta-data/placement/region)"
export AWS_REGION
log "instance ${INSTANCE_ID} region ${AWS_REGION}"

# ---------------------------------------------------------------------------
# 1. System packages (Amazon Linux 2023). Idempotent — re-running this
#    script after a partial install is safe.
# ---------------------------------------------------------------------------
log "dnf install build deps + postgres client + gh"
sudo dnf install -y --quiet \
    gcc gcc-c++ make git pkgconfig openssl-devel \
    python3.11 python3.11-pip python3.11-devel \
    postgresql15

# gh is not in the AL2023 default repo.
if ! command -v gh >/dev/null 2>&1; then
    sudo dnf install -y --quiet 'dnf-command(config-manager)'
    sudo dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
    sudo dnf install -y --quiet gh
fi

# AWS CLI v2 ships in the AL2023 base AMI; if missing, install it.
if ! command -v aws >/dev/null 2>&1; then
    log "aws cli v2 missing — installing"
    curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscliv2.zip
    sudo dnf install -y --quiet unzip
    (cd /tmp && unzip -q awscliv2.zip && sudo ./aws/install)
fi

# ---------------------------------------------------------------------------
# 2. Rust toolchain (stable). Cached on the instance store across re-runs
#    only matters for hot-EC2 reuse, which is not our model — we eat the
#    ~90s install on every monthly tick.
# ---------------------------------------------------------------------------
log "install Rust toolchain"
if [ ! -x "$HOME/.cargo/bin/cargo" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain stable --profile minimal
fi
# shellcheck source=/dev/null
source "$HOME/.cargo/env"

# ---------------------------------------------------------------------------
# 3. Clone discogs-xml-converter; discogs-etl is already in $REPO_DIR
#    (cloned by the user-data stub before this script ran).
# ---------------------------------------------------------------------------
log "clone + build discogs-xml-converter"
if [ ! -d "$CONVERTER_DIR/.git" ]; then
    sudo mkdir -p "$CONVERTER_DIR"
    sudo chown "$USER:$USER" "$CONVERTER_DIR"
    git clone --depth 1 https://github.com/WXYC/discogs-xml-converter.git "$CONVERTER_DIR"
fi
(cd "$CONVERTER_DIR" && cargo build --release --quiet)
export PATH="$CONVERTER_DIR/target/release:$PATH"

# ---------------------------------------------------------------------------
# 4. Python venv for discogs-etl
# ---------------------------------------------------------------------------
log "set up Python venv + pip install discogs-etl"
if [ ! -d "$REPO_DIR/.venv" ]; then
    python3.11 -m venv "$REPO_DIR/.venv"
fi
# shellcheck source=/dev/null
source "$REPO_DIR/.venv/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -e "${REPO_DIR}[dev]"

# ---------------------------------------------------------------------------
# 5. Pull secrets from SSM Parameter Store. The instance role grants
#    ssm:GetParameters on $REBUILD_SSM_PREFIX/* and kms:Decrypt on the
#    associated KMS key (default aws/ssm CMK).
# ---------------------------------------------------------------------------
SSM_PREFIX="${REBUILD_SSM_PREFIX:-/wxyc/discogs-rebuild}"
log "fetch secrets from SSM at ${SSM_PREFIX}/"

ssm_param() {
    aws ssm get-parameter --with-decryption \
        --name "$1" --query 'Parameter.Value' --output text 2>/dev/null || true
}

DATABASE_URL_DISCOGS="$(ssm_param "${SSM_PREFIX}/DATABASE_URL_DISCOGS")"
GH_TOKEN="$(ssm_param "${SSM_PREFIX}/GH_TOKEN")"
SLACK_MONITORING_WEBHOOK="$(ssm_param "${SSM_PREFIX}/SLACK_MONITORING_WEBHOOK")"
SENTRY_DSN="$(ssm_param "${SSM_PREFIX}/SENTRY_DSN")"
# Optional, default-off (discogs-etl#217 Phase 1). A missing param is the
# steady state — ssm_param's `2>/dev/null || true` returns empty, exactly
# like SENTRY_DSN above, so this never aborts a normal rebuild.
PRUNE_AUDIT_ENABLED="$(ssm_param "${SSM_PREFIX}/PRUNE_AUDIT_ENABLED")"

if [ -z "$DATABASE_URL_DISCOGS" ]; then
    echo "::error:: DATABASE_URL_DISCOGS missing from SSM at ${SSM_PREFIX}/" >&2
    exit 2
fi
if [ -z "$GH_TOKEN" ]; then
    echo "::error:: GH_TOKEN missing from SSM at ${SSM_PREFIX}/" >&2
    exit 2
fi

export DATABASE_URL_DISCOGS GH_TOKEN SLACK_MONITORING_WEBHOOK SENTRY_DSN
export REPO_DIR CONVERTER_DIR LOG_DIR

notify_slack ":hourglass:" "starting on ${INSTANCE_ID}"

# ---------------------------------------------------------------------------
# 6. Concurrent-rebuild guard (#311). Run as late as possible — right before
#    the first write to the shared cache — so a peer about to finish and
#    terminate is reflected. If we're not the winning instance, this exits 0
#    and never reaches the handoff below.
# ---------------------------------------------------------------------------
abort_if_not_winning_rebuild

# ---------------------------------------------------------------------------
# 7. Optional prune-audit dump wiring (discogs-etl#217 Phase 1). Off by
#    default; only takes effect when the operator has deliberately set
#    ${SSM_PREFIX}/PRUNE_AUDIT_ENABLED for a one-off audit rebuild (see
#    infra/ephemeral-rebuild/README.md). When absent, PRUNE_AUDIT_ENABLED is
#    empty and this whole block no-ops — PRUNE_AUDIT_DUMP_DIR stays unset and
#    run_pipeline.py runs exactly as it does today. $LOG_DIR is the only
#    directory the trap-EXIT S3 sync picks up, so nesting the dump under it is
#    what lets the artifact survive instance termination; run_pipeline.py
#    appends its own prune-audit-<UTC-date>/ subdirectory underneath.
#
#    The value is lowercased before the truthy test so a hand-typed True/TRUE
#    matches the documented `true`, and a set-but-non-truthy value (a typo,
#    `false`, `0`, `yes`) is logged rather than silently dropped — the operator
#    writes this SSM value by hand and would otherwise only discover a case slip
#    after the multi-hour rebuild finishes leaving no dump in S3.
# ---------------------------------------------------------------------------
if [ "${PRUNE_AUDIT_ENABLED,,}" = "true" ] || [ "$PRUNE_AUDIT_ENABLED" = "1" ]; then
    export PRUNE_AUDIT_DUMP_DIR="$LOG_DIR/prune-audit"
    log "prune-audit dump ENABLED -> $PRUNE_AUDIT_DUMP_DIR (will upload to S3 with logs)"
elif [ -n "$PRUNE_AUDIT_ENABLED" ]; then
    log "WARN: PRUNE_AUDIT_ENABLED='${PRUNE_AUDIT_ENABLED}' is not truthy (expected 'true' or '1'); prune-audit dump DISABLED"
fi

# ---------------------------------------------------------------------------
# 8. Hand off to the existing rebuild-cache.sh. It already handles the
#    streaming download, pipeline run, and drift watchdog. We share its log
#    directory so the s3 sync at exit picks both up.
# ---------------------------------------------------------------------------
log "exec rebuild-cache.sh"
"$REPO_DIR/scripts/rebuild-cache.sh"

log "rebuild-cache.sh exited 0; trap EXIT will upload + shutdown"
