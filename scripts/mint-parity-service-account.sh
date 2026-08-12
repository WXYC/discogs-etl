#!/usr/bin/env bash
#
# Mint the catalog:read service account the parity harness signs in as (#365).
#
# Run this once, by hand, from your own terminal. It holds an admin credential
# against production, so it is deliberately not wired into any workflow.
#
#   ./scripts/mint-parity-service-account.sh --admin-email you@example.org
#
# On success it writes two files into --out-dir (default: a fresh mktemp -d):
#
#   svc.password   the generated service-account password
#   svc.user_id    the new user's id, needed to rotate or revoke later
#
# and prints where they landed. Feed them to `gh secret set` (the command is
# printed at the end) and delete the directory.
#
# Design notes, each of which has a guard in
# tests/unit/test_mint_parity_service_account.py:
#
#   * No secret ever reaches a command line. argv is readable by any `ps` and
#     is echoed by `set -x`; every request body goes to curl on stdin. This is
#     the same argument docs/architecture.md makes for LIBRARY_DB_PASSWORD and
#     MYSQL_PWD.
#   * admin/create-user, never the org's admin/provision-user wrapper. The
#     latter emails an account-setup link whose token lives 7 days -- the
#     length of the parity soak -- and setting the password afterwards does
#     not revoke it.
#   * The admin session is revoked on the way out, from a trap, because
#     better-auth pins session.expiresIn to a year. An admin session left
#     behind is a year-long admin credential.
#   * The credential is exercised before it is reported: signing in as the new
#     account and exchanging for a JWT proves the org `member` row landed,
#     which is where catalog:read actually comes from.
#
# See docs/architecture.md, "Minting and rotating the parity service account".

set -euo pipefail

AUTH_URL="${BACKEND_AUTH_URL:-https://api.wxyc.org/auth}"
# Non-routing by construction: `.invalid` is reserved by RFC 2606 and can
# never be delegated, so no future catch-all on wxyc.org can turn this into a
# mailbox that could run forget-password against the account.
SERVICE_EMAIL="catalog-parity@wxyc.invalid"
SERVICE_NAME="Catalog Parity"
# better-auth's CSRF guard 400s an origin-less sign-in with
# MISSING_OR_NULL_ORIGIN. This must be one of BETTER_AUTH_TRUSTED_ORIGINS.
ORIGIN="${BACKEND_AUTH_ORIGIN:-https://dj.wxyc.org}"
ADMIN_EMAIL=""
OUT_DIR=""

usage() {
    cat <<'USAGE'
Usage: mint-parity-service-account.sh [options]

  --admin-email EMAIL   Admin to sign in as (prompted if omitted)
  --auth-url URL        Auth service base (default: https://api.wxyc.org/auth)
  --service-email EMAIL Service account to create (default: catalog-parity@wxyc.invalid)
  --origin ORIGIN       Origin header (default: https://dj.wxyc.org)
  --out-dir DIR         Where to write svc.password / svc.user_id (default: mktemp -d)
  -h, --help            This message

The admin password is read from the terminal without echo. It is never passed
as an argument, written to disk, or logged.
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --admin-email) ADMIN_EMAIL="$2"; shift 2 ;;
        --auth-url) AUTH_URL="$2"; shift 2 ;;
        --service-email) SERVICE_EMAIL="$2"; shift 2 ;;
        --origin) ORIGIN="$2"; shift 2 ;;
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }
die() { log "ERROR: $*"; exit 1; }

for tool in curl jq python3; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool is required but not on PATH"
done

# Refuse to put an admin password on a plaintext wire. Loopback is exempt so
# this can be rehearsed against a local auth service.
case "$AUTH_URL" in
    https://*) ;;
    http://127.0.0.1[:/]*|http://localhost[:/]*|http://\[::1\][:/]*) log "WARNING: plaintext loopback auth URL" ;;
    *) die "auth URL must be https (or loopback): $AUTH_URL" ;;
esac
AUTH_URL="${AUTH_URL%/}"

if [[ -z "$ADMIN_EMAIL" ]]; then
    read -r -p 'Admin email: ' ADMIN_EMAIL
fi
[[ -n "$ADMIN_EMAIL" ]] || die "an admin email is required"

# Secrets land here; keep them off other users' terminals from the start
# rather than chmod-ing after the write.
umask 077
if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="$(mktemp -d)"
fi
mkdir -p "$OUT_DIR"

SESSION=""
cleanup() {
    local status=$?
    # Revoke on every exit path, including a failure between sign-in and
    # create-user: the session is good for a year either way.
    if [[ -n "$SESSION" ]]; then
        if printf '{}' | curl -sS -o /dev/null -X POST "${AUTH_URL}/sign-out" \
            -H "Authorization: Bearer ${SESSION}" \
            -H 'Content-Type: application/json' -H "Origin: ${ORIGIN}" \
            --data-binary @- 2>/dev/null; then
            log "revoked the admin session"
        else
            log "WARNING: could not revoke the admin session; it stays valid for a year."
            log "         Revoke it by hand: POST ${AUTH_URL}/admin/revoke-user-sessions"
        fi
        SESSION=""
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

# One auth call. Body on stdin, status separated from the payload so a 4xx is
# a failure rather than a payload that happens to lack the field we wanted.
HTTP_STATUS=""
RESPONSE=""
call_auth() {
    local path="$1" bearer="${2:-}"
    local -a args=(-sS -X POST "${AUTH_URL}${path}"
        -H 'Content-Type: application/json' -H 'Accept: application/json'
        -H "Origin: ${ORIGIN}" --data-binary @- -w $'\n%{http_code}')
    if [[ -n "$bearer" ]]; then
        args+=(-H "Authorization: Bearer ${bearer}")
    fi
    local raw
    raw="$(curl "${args[@]}")" || die "${path} could not be reached"
    HTTP_STATUS="${raw##*$'\n'}"
    RESPONSE="${raw%$'\n'*}"
}

# 1. Admin sign-in. The password is read without echo and piped straight into
#    the JSON body -- it exists only in this process's memory.
read -r -s -p "Password for ${ADMIN_EMAIL}: " ADMIN_PASSWORD
echo >&2
[[ -n "$ADMIN_PASSWORD" ]] || die "an admin password is required"

call_auth "/sign-in/email" "" \
    < <(jq -n --arg e "$ADMIN_EMAIL" --arg p "$ADMIN_PASSWORD" '{email:$e,password:$p}')
unset ADMIN_PASSWORD
[[ "$HTTP_STATUS" == "200" ]] || die "admin sign-in failed with HTTP ${HTTP_STATUS}: ${RESPONSE}"
SESSION="$(jq -r '.token // empty' <<<"$RESPONSE")"
[[ -n "$SESSION" ]] || die "admin sign-in returned no session token"
log "signed in as ${ADMIN_EMAIL}"

# 2. Create the principal. No `role` field: that one is better-auth's global
#    admin role, not the org role. The org `member` row -- which is where
#    catalog:read comes from -- is inserted by a user.create.after hook, and a
#    hooks.after branch auto-verifies the address (BS#1118).
SERVICE_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))')"
call_auth "/admin/create-user" "$SESSION" \
    < <(jq -n --arg e "$SERVICE_EMAIL" --arg n "$SERVICE_NAME" --arg p "$SERVICE_PASSWORD" \
        '{email:$e,name:$n,password:$p,data:{emailVerified:true}}')
if [[ "$HTTP_STATUS" != "200" ]]; then
    die "create-user failed with HTTP ${HTTP_STATUS}: ${RESPONSE}"
fi
USER_ID="$(jq -r '.user.id // empty' <<<"$RESPONSE")"
[[ -n "$USER_ID" ]] || die "create-user returned no user id: ${RESPONSE}"
log "created ${SERVICE_EMAIL} (${USER_ID})"

# 3. Store the credential before verifying it. A verification failure still
#    leaves an account that exists, and an account whose password was thrown
#    away is worse than one that needs a second look.
printf '%s' "$SERVICE_PASSWORD" >"${OUT_DIR}/svc.password"
printf '%s\n' "$USER_ID" >"${OUT_DIR}/svc.user_id"

# 4. Exercise it. Signing in as the new account and exchanging for a JWT is
#    the cheapest proof that the member row landed and the password is what we
#    think it is -- the two failures that would otherwise surface as a broken
#    soak days later.
call_auth "/sign-in/email" "" \
    < <(jq -n --arg e "$SERVICE_EMAIL" --arg p "$SERVICE_PASSWORD" '{email:$e,password:$p}')
[[ "$HTTP_STATUS" == "200" ]] \
    || die "the new account could not sign in (HTTP ${HTTP_STATUS}); its password is in ${OUT_DIR}/svc.password"
SERVICE_SESSION="$(jq -r '.token // empty' <<<"$RESPONSE")"
[[ -n "$SERVICE_SESSION" ]] || die "the new account signed in but returned no token"

JWT_STATUS="$(curl -sS -o /dev/null -w '%{http_code}' "${AUTH_URL}/token" \
    -H "Authorization: Bearer ${SERVICE_SESSION}" -H "Origin: ${ORIGIN}" \
    -H 'Accept: application/json')"
[[ "$JWT_STATUS" == "200" ]] \
    || die "the new account could not exchange for a JWT (HTTP ${JWT_STATUS}); check that the org member row was created"

# Don't leave the verification's own year-long session behind either.
printf '{}' | curl -sS -o /dev/null -X POST "${AUTH_URL}/sign-out" \
    -H "Authorization: Bearer ${SERVICE_SESSION}" \
    -H 'Content-Type: application/json' -H "Origin: ${ORIGIN}" --data-binary @- \
    || log "WARNING: could not sign out the verification session"
unset SERVICE_PASSWORD SERVICE_SESSION

log "verified: the account can sign in and mint a catalog:read JWT"
cat >&2 <<EOF

Done. Credentials are in ${OUT_DIR}:

  gh secret set BACKEND_CATALOG_EMAIL --repo WXYC/discogs-etl --body ${SERVICE_EMAIL}
  gh secret set BACKEND_CATALOG_PASSWORD --repo WXYC/discogs-etl --body-file ${OUT_DIR}/svc.password

Keep svc.user_id somewhere durable -- rotation (admin/set-user-password) and
revocation (admin/revoke-user-sessions) both need it. Then:

  rm -rf ${OUT_DIR}
EOF
