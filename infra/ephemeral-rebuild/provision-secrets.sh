#!/usr/bin/env bash
# provision-secrets.sh — one-time operator script that writes the SecureString
# parameters the bootstrap reads from SSM. Idempotent (`--overwrite` everywhere)
# and re-runnable for value rotations.
#
# Usage:
#     ./provision-secrets.sh              # interactive, prompts for each value
#     SSM_PREFIX=/some/other/path \
#         ./provision-secrets.sh          # override the prefix (matches the
#                                         # stack's SsmPrefix parameter)
#     AWS_REGION=us-west-2 \
#         ./provision-secrets.sh          # override the region (default
#                                         # us-east-1, matches the stack)
#     EXPECTED_ACCOUNT=123456789012 \
#         ./provision-secrets.sh          # override the account guard (default
#                                         # 503977661500, the rebuild account)
#
# Prereqs:
#     - AWS credentials configured for the rebuild account (503977661500).
#     - aws CLI on PATH (v1 or v2; both support --overwrite + SecureString).
#
# What it writes:
#     <prefix>/DATABASE_URL_DISCOGS         (required) — Railway public-proxy URL
#     <prefix>/GH_TOKEN                     (required) — PAT with repo:read on
#                                                        WXYC/library-metadata-lookup
#     <prefix>/SLACK_MONITORING_WEBHOOK     (optional) — Slack incoming webhook
#     <prefix>/SENTRY_DSN                   (optional) — Sentry DSN
#
# Optional values are skipped on empty input. The script never deletes; if you
# need to remove an optional parameter later, run `aws ssm delete-parameter`
# directly.

set -euo pipefail

SSM_PREFIX="${SSM_PREFIX:-/wxyc/discogs-rebuild}"
AWS_REGION="${AWS_REGION:-us-east-1}"
EXPECTED_ACCOUNT="${EXPECTED_ACCOUNT:-503977661500}"
export AWS_REGION

# Account sanity check — writing the wrong DATABASE_URL into the wrong account
# is silent until the next monthly tick and then very loud, so confirm before
# anything moves.
if ! identity="$(aws sts get-caller-identity --output text \
    --query '[Account,Arn]' 2>/dev/null)"; then
    echo "ERROR: aws sts get-caller-identity failed. Configure AWS credentials first." >&2
    exit 1
fi
account="${identity%%[[:space:]]*}"
arn="${identity##*[[:space:]]}"

# Hard-fail before the y/N prompt if the caller is in the wrong account. The
# y/N display below is the second line of defence; this is the typo-proof
# first one. To deliberately target a non-default account (sandbox, test
# stack, etc.), set EXPECTED_ACCOUNT=<id> to that account's id.
if [ "$account" != "$EXPECTED_ACCOUNT" ]; then
    cat <<EOF >&2
ERROR: caller account ${account} does not match EXPECTED_ACCOUNT ${EXPECTED_ACCOUNT}.
       Configure AWS_PROFILE / credentials for the rebuild account, or pass
       EXPECTED_ACCOUNT=${account} to override (sandbox / test stack only).
EOF
    exit 1
fi

cat <<EOF
About to write SecureString parameters to:
    Account: ${account}
    Region:  ${AWS_REGION}
    Prefix:  ${SSM_PREFIX}/
    Caller:  ${arn}
EOF
read -rp "Proceed? [y/N] " ack
case "$ack" in
    y|Y|yes|YES) ;;
    *) echo "aborted"; exit 1 ;;
esac

put_secret() {
    local name="$1" prompt="$2" required="$3"
    local value
    read -rsp "$prompt: " value
    echo
    if [ -z "$value" ]; then
        if [ "$required" = "required" ]; then
            echo "ERROR: $name is required." >&2
            return 1
        fi
        echo "  (skipped)"
        return 0
    fi
    aws ssm put-parameter --type SecureString --overwrite \
        --name "${SSM_PREFIX}/${name}" --value "$value" >/dev/null
    echo "  wrote ${SSM_PREFIX}/${name}"
}

put_secret DATABASE_URL_DISCOGS \
    "DATABASE_URL_DISCOGS (Railway public-proxy URL)" required
put_secret GH_TOKEN \
    "GH_TOKEN (PAT with repo:read on WXYC/library-metadata-lookup)" required
put_secret SLACK_MONITORING_WEBHOOK \
    "SLACK_MONITORING_WEBHOOK (or blank to skip)" optional
put_secret SENTRY_DSN \
    "SENTRY_DSN (or blank to skip)" optional

echo
echo "Parameters now under ${SSM_PREFIX}/:"
# --recursive so a future sub-prefix layout (e.g. /optional/SLACK_…) doesn't
# silently truncate the summary.
aws ssm get-parameters-by-path --path "$SSM_PREFIX" --recursive \
    --query 'Parameters[].[Name,Type,LastModifiedDate]' --output table
