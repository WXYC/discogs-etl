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
ADMIN_USERNAME=""
USE_SUPPLIED_SESSION=false
OUT_DIR=""

usage() {
    cat <<'USAGE'
Usage: mint-parity-service-account.sh [options]

  --admin-email EMAIL   Admin to sign in as, by email
  --admin-username NAME Admin to sign in as, by username (WXYC accounts may
                        have either; dj-site picks by whether the identifier
                        looks like an email). One of the two is required.
  --auth-url URL        Auth service base (default: https://api.wxyc.org/auth)
  --service-email EMAIL Service account to create (default: catalog-parity@wxyc.invalid)
  --admin-session       Use a session token you already hold instead of a
                        password (prompted for, never in argv). For accounts
                        that sign in by one-time code and have no password.
                        Get one from a signed-in browser: DevTools > Storage >
                        Cookies > dj.wxyc.org, the cookie whose name ends in
                        `better-auth.session_token`. Copy the whole value,
                        including the `.` and everything after it -- that is
                        the signature, and the bearer plugin verifies it. The
                        cookie is httpOnly, so `document.cookie` will not show
                        it. Do NOT copy `set-auth-jwt` or the body of an
                        /auth/token response: those are JWTs, and the admin
                        API authenticates by session.
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
        --admin-username) ADMIN_USERNAME="$2"; shift 2 ;;
        --admin-session) USE_SUPPLIED_SESSION=true; shift ;;
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

if [[ -n "$ADMIN_EMAIL" && -n "$ADMIN_USERNAME" ]]; then
    die "pass --admin-email or --admin-username, not both"
fi
# Catch the confusable pair locally. better-auth 422s an "@" as
# INVALID_USERNAME, and learning that from production is a slow way to find
# out which flag you wanted.
if [[ -n "$ADMIN_USERNAME" && "$ADMIN_USERNAME" == *@* ]]; then
    die "--admin-username takes a username; '${ADMIN_USERNAME}' is an address, so use --admin-email"
fi
if [[ -n "$ADMIN_EMAIL" && "$ADMIN_EMAIL" != *@* ]]; then
    die "--admin-email takes an address; '${ADMIN_EMAIL}' has no '@', so use --admin-username"
fi
if [[ "$USE_SUPPLIED_SESSION" == true && ( -n "$ADMIN_EMAIL" || -n "$ADMIN_USERNAME" ) ]]; then
    die "--admin-session signs in for you; drop --admin-email/--admin-username"
fi
if [[ "$USE_SUPPLIED_SESSION" == false && -z "$ADMIN_EMAIL" && -z "$ADMIN_USERNAME" ]]; then
    IFS= read -r -p 'Admin email or username: ' ADMIN_IDENTIFIER
    if [[ "$ADMIN_IDENTIFIER" == *@* ]]; then
        ADMIN_EMAIL="$ADMIN_IDENTIFIER"
    else
        ADMIN_USERNAME="$ADMIN_IDENTIFIER"
    fi
fi

# better-auth exposes the two as separate endpoints with separate body
# fields; the username plugin is enabled on this deployment, and dj-site
# routes to /sign-in/username whenever the identifier has no "@". An
# account that signs in by username has no working email path, and the
# refusal is the same INVALID_EMAIL_OR_PASSWORD either way.
ADMIN_IDENTIFIER=""
SIGN_IN_PATH=""
SIGN_IN_FIELD=""
if [[ "$USE_SUPPLIED_SESSION" == true ]]; then
    :
elif [[ -n "$ADMIN_USERNAME" ]]; then
    ADMIN_IDENTIFIER="$ADMIN_USERNAME"
    SIGN_IN_PATH="/sign-in/username"
    SIGN_IN_FIELD="username"
else
    ADMIN_IDENTIFIER="$ADMIN_EMAIL"
    SIGN_IN_PATH="/sign-in/email"
    SIGN_IN_FIELD="email"
fi

# Secrets land here; keep them off other users' terminals from the start
# rather than chmod-ing after the write.
umask 077
if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="$(mktemp -d)"
fi
mkdir -p "$OUT_DIR"

SESSION=""
# Only ever revoke what this script minted. A session the operator handed us
# is their dj-site login; signing it out would log them off mid-task. Same
# rule the harness applies to $BACKEND_CATALOG_TOKEN.
SESSION_IS_OURS=false
cleanup() {
    local status=$?
    if [[ -n "$SESSION" && "$SESSION_IS_OURS" == false ]]; then
        log "leaving the session you supplied alone; this script revokes only what it mints"
        SESSION=""
    fi
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

# 1. Get an admin session, either by signing in or by taking one the operator
#    already holds. The password is read without echo and goes straight into
#    the JSON body -- it exists only in this process's memory.
if [[ "$USE_SUPPLIED_SESSION" == true ]]; then
    IFS= read -r -s -p 'Admin session token (the better-auth.session_token cookie value): ' SESSION
    echo >&2
    [[ -n "$SESSION" ]] || die "no session token given"
    # A JWT pasted here would authenticate as nobody: the admin endpoints
    # resolve their caller through getSessionFromCtx, which the bearer plugin
    # feeds from a *session* token. Browsers see both -- as `set-auth-jwt` and
    # `set-auth-token` -- and confusing them produces another opaque 401, so
    # tell them apart locally instead.
    if python3 -c '
import base64, json, sys
parts = sys.argv[1].split(".")
if len(parts) != 3:
    sys.exit(1)
seg = parts[0]
try:
    header = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
except Exception:
    sys.exit(1)
sys.exit(0 if isinstance(header, dict) and "alg" in header else 1)
' "$SESSION"; then
        die "that is a JWT (set-auth-jwt), not a session token. The admin API needs the session: copy set-auth-token, or the token field from the /auth/sign-in response body."
    fi
    SESSION_IS_OURS=false
    log "using the session token you supplied (it will not be revoked)"
else
    IFS= read -r -s -p "Password for ${ADMIN_IDENTIFIER}: " ADMIN_PASSWORD
    echo >&2
    [[ -n "$ADMIN_PASSWORD" ]] || die "an admin password is required"

    call_auth "$SIGN_IN_PATH" "" \
        < <(jq -n --arg f "$SIGN_IN_FIELD" --arg i "$ADMIN_IDENTIFIER" --arg p "$ADMIN_PASSWORD" \
            '{($f): $i, password: $p}')
    unset ADMIN_PASSWORD
    if [[ "$HTTP_STATUS" == "401" ]]; then
        # better-auth answers "no such user" and "wrong password" identically,
        # by design. Say which things to check, because the operator staring at
        # a password they know is correct cannot tell those apart.
        log "ERROR: ${SIGN_IN_PATH} rejected ${ADMIN_IDENTIFIER} (HTTP 401): ${RESPONSE}"
        log "       better-auth returns this for an unknown ${SIGN_IN_FIELD} AND for a wrong password."
        if [[ "$SIGN_IN_FIELD" == "email" ]]; then
            log "       If you sign in to dj.wxyc.org with a username rather than that address,"
            log "       re-run with --admin-username <name>: they are separate endpoints."
        else
            log "       If you sign in to dj.wxyc.org with an email address, re-run with"
            log "       --admin-email <address>: they are separate endpoints."
        fi
        log "       If you sign in with a one-time code and have no password, re-run with"
        log "       --admin-session and paste a session token from a signed-in browser."
        exit 1
    fi
    [[ "$HTTP_STATUS" == "200" ]] || die "admin sign-in failed with HTTP ${HTTP_STATUS}: ${RESPONSE}"
    SESSION="$(jq -r '.token // empty' <<<"$RESPONSE")"
    [[ -n "$SESSION" ]] || die "admin sign-in returned no session token"
    SESSION_IS_OURS=true
    log "signed in as ${ADMIN_IDENTIFIER}"
fi

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
