#!/usr/bin/env bash
# rebuild-cache-bootstrap.sh — runs as user-data on a freshly-spawned
# ephemeral EC2 instance owned by the `wxyc-discogs-rebuild` CloudFormation
# stack. The launcher Lambda boots the instance, clones discogs-etl onto
# the root volume, and execs this script.
#
# Responsibilities:
#   1. Install runtime deps (Python 3.11, Rust, git, postgres client, gh, AWS CLI v2)
#   2. Clone discogs-xml-converter and build its release binary
#   3. Pull DATABASE_URL_DISCOGS / GH_TOKEN / SLACK_MONITORING_WEBHOOK from SSM
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

REPO_DIR="${REPO_DIR:-/opt/discogs-etl}"
CONVERTER_DIR="${CONVERTER_DIR:-/opt/discogs-xml-converter}"
LOG_DIR="${LOG_DIR:-/var/log/discogs-rebuild}"
BOOTSTRAP_LOG="${LOG_DIR}/bootstrap-$(date -u +%Y-%m-%dT%H%MZ).log"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$BOOTSTRAP_LOG") 2>&1

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

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
AWS_REGION="${AWS_REGION:-$(imds_get "$TOKEN" meta-data/placement/region)}"
export AWS_REGION
log "instance ${INSTANCE_ID} region ${AWS_REGION}"

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

# trap EXIT runs on every exit path — clean or panic. It uploads the log
# and calls shutdown unconditionally so a crashed bootstrap can't leak the
# instance past the InstanceInitiatedShutdownBehavior=terminate window.
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
# 6. Hand off to the existing rebuild-cache.sh. It already handles the
#    streaming download, pipeline run, and drift watchdog. We share its log
#    directory so the s3 sync at exit picks both up.
# ---------------------------------------------------------------------------
log "exec rebuild-cache.sh"
"$REPO_DIR/scripts/rebuild-cache.sh"

log "rebuild-cache.sh exited 0; trap EXIT will upload + shutdown"
