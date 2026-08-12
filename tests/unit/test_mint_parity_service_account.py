"""Guards on the parity service-account mint script (#365).

The script runs once, by hand, holding the highest-privilege credential in
the org, against production. That combination means the usual safety net --
"run it and see" -- does not exist: there is no staging rehearsal that would
catch a password leaked into ``ps`` output, and no second chance to un-email
an account-setup token. So the properties that make the procedure safe are
asserted here against the script's text rather than its behavior.

This mirrors the drift guard on ``sync-library.sh`` in
``test_catalog_parity_diff.py``: the shell is the artifact an operator runs,
so the shell is what gets checked.
"""

from __future__ import annotations

import os
import re
import stat
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "mint-parity-service-account.sh"
ARCHITECTURE_DOC = REPO_ROOT / "docs" / "architecture.md"

# The address the harness signs in as. Non-routing by construction: `.invalid`
# is reserved by RFC 2606 and can never be delegated, so no future catch-all
# on wxyc.org can turn it into a mailbox that could run `forget-password`
# against an account holding the whole catalog.
SERVICE_ACCOUNT_EMAIL = "catalog-parity@wxyc.invalid"


@pytest.fixture(scope="module")
def script() -> str:
    return SCRIPT_PATH.read_text(encoding="utf-8")


class TestMintScript:
    """Properties an operator cannot verify by reading the output."""

    def test_the_script_exists_and_is_executable(self) -> None:
        assert SCRIPT_PATH.exists(), f"missing {SCRIPT_PATH}"
        mode = SCRIPT_PATH.stat().st_mode
        assert mode & stat.S_IXUSR, "an operator runbook step must be runnable"

    def test_it_fails_loudly(self, script: str) -> None:
        """No unset variable, no ignored error, no swallowed pipe failure.

        A mint that half-succeeds is worse than one that fails: it can leave a
        live account whose password was never stored.
        """
        assert "set -euo pipefail" in script

    def test_no_secret_ever_reaches_a_command_line(self, script: str) -> None:
        """argv is world-readable through `ps`, and `set -x` echoes it.

        This is the same argument `docs/architecture.md` makes for routing
        LIBRARY_DB_PASSWORD through MYSQL_PWD. Bodies go to curl on stdin.
        """
        secrets = ("ADMIN_PASSWORD", "SERVICE_PASSWORD", "SESSION")
        for line in script.splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or "curl" not in stripped:
                continue
            for name in secrets:
                assert f'-d "${name}' not in stripped
                assert f'--data "${name}' not in stripped
        # Every request body is piped in, never passed as an argument.
        assert "--data-binary @-" in script
        assert not re.search(r"--data(-binary|-raw)?\s+['\"]?\{", script), (
            "a literal JSON body on the command line is one edit away from "
            "carrying a password there too"
        )

    def test_the_auth_helper_is_never_run_in_a_pipeline(self, script: str) -> None:
        """`jq ... | call_auth` runs the function in a subshell.

        Its assignments to HTTP_STATUS/RESPONSE are then lost to the parent,
        which reads them as empty and reports "sign-in failed with HTTP :"
        for a request that in fact succeeded -- after the server has already
        minted a year-long session the script no longer knows how to revoke.
        Bodies go in through process substitution instead.
        """
        assert "| call_auth" not in script
        assert "< <(jq" in script

    def test_the_admin_password_is_never_echoed(self, script: str) -> None:
        """Read silently, so it stays out of the terminal and out of history."""
        assert re.search(r"read\b[^\n]*-[a-z]*s[a-z]*\b[^\n]*ADMIN_PASSWORD", script)

    def test_the_password_read_does_not_strip_whitespace(self, script: str) -> None:
        """A bare `read` splits on IFS and eats leading/trailing spaces.

        A password that legitimately starts or ends with a space would be
        silently mangled into one that fails -- and the operator, who can see
        that their password is correct, has no way to tell that apart from a
        genuinely rejected credential.
        """
        assert re.search(r"IFS=\s+read\b[^\n]*ADMIN_PASSWORD", script)

    def test_it_can_sign_in_by_username(self, script: str) -> None:
        """WXYC accounts authenticate by username or by email.

        dj-site branches on whether the identifier looks like an email and
        calls `signIn.username` when it does not, so an operator whose account
        is username-based cannot mint at all through an email-only path. What
        they see is an indistinguishable INVALID_EMAIL_OR_PASSWORD.
        """
        assert "/sign-in/username" in script
        assert "--admin-username" in script

    def test_the_sign_in_failure_is_actionable(self, script: str) -> None:
        """better-auth cannot say which half was wrong, so the script must.

        A 401 here means the identifier did not match a user OR the password
        did not match that user -- and for an admin staring at a password they
        know is right, "which one?" is the whole question.
        """
        code = "\n".join(line for line in script.splitlines() if not line.strip().startswith("#"))
        assert "INVALID_EMAIL_OR_PASSWORD" in code or "--admin-username" in code
        assert re.search(r"401", code), "the 401 branch must explain itself"

    def test_it_mints_the_documented_principal(self, script: str) -> None:
        """Drift guard: the script and the runbook must name one account.

        Two spellings of the address means the secret CI holds signs in as an
        account nobody documented -- and the documented one lingers unused
        with a password nobody rotates.
        """
        assert SERVICE_ACCOUNT_EMAIL in script
        assert SERVICE_ACCOUNT_EMAIL in ARCHITECTURE_DOC.read_text(encoding="utf-8")

    def test_it_uses_create_user_not_provision_user(self, script: str) -> None:
        """provision-user emails a 7-day reset token -- the length of the soak.

        Setting the password afterwards does not revoke it, so that path
        leaves a live takeover link for anyone who can read the mailbox.
        """
        assert "admin/create-user" in script
        # Checked over executable lines only: the script is expected to
        # *explain* why it avoids provision-user, just never to call it.
        code = "\n".join(line for line in script.splitlines() if not line.strip().startswith("#"))
        assert "provision-user" not in code

    def test_it_does_not_send_a_global_admin_role(self, script: str) -> None:
        """`role` in this body is better-auth's admin plugin, not the org role.

        The org `member` row -- where catalog:read actually comes from -- is
        inserted by a database hook. Setting `role` here would instead enroll
        the service account in the admin plugin.
        """
        assert '"role"' not in script
        assert "role:" not in script

    def test_it_revokes_the_admin_session_it_mints(self, script: str) -> None:
        """better-auth pins session.expiresIn to a year.

        An admin session left behind is a year-long admin credential -- the
        same defect the harness's own `_TokenSource.close` exists to avoid,
        one privilege level up.
        """
        assert "/sign-out" in script
        assert "trap " in script, "revocation must survive a mid-script failure"

    def test_the_stored_password_is_not_world_readable(self, script: str) -> None:
        assert "umask 077" in script or "chmod 600" in script

    def test_the_password_is_generated_not_chosen(self, script: str) -> None:
        """48 bytes from `secrets`, so no human ever picks or retypes it."""
        assert "token_urlsafe(48)" in script

    def test_it_refuses_to_put_the_password_on_a_plaintext_wire(self, script: str) -> None:
        """The same guard the harness applies to its own auth URL."""
        assert "https://" in script
        assert re.search(r"https\b", script)
        assert "must be https" in script or "refusing" in script.lower()

    def test_it_verifies_the_credential_before_reporting_success(self, script: str) -> None:
        """A password stored but never exercised is a soak that fails on day 1.

        Signing in as the new account and exchanging for a JWT also proves the
        org `member` row landed -- the hook that grants catalog:read.
        """
        assert "/sign-in/email" in script
        assert "/token" in script


class TestRunbookAgreement:
    """The script and the prose must not describe different procedures."""

    def test_the_runbook_points_at_the_script(self) -> None:
        doc = ARCHITECTURE_DOC.read_text(encoding="utf-8")
        assert "mint-parity-service-account.sh" in doc

    def test_the_secret_names_agree(self, script: str) -> None:
        doc = ARCHITECTURE_DOC.read_text(encoding="utf-8")
        for name in ("BACKEND_CATALOG_EMAIL", "BACKEND_CATALOG_PASSWORD"):
            assert name in script
            assert name in doc


def test_the_scratchpad_default_is_not_a_repo_path(script: str) -> None:
    """A generated password must never land where `git add -A` can reach it."""
    assert not re.search(r"OUT_DIR=\$\{?\w*:?-?\.?/?(scripts|docs|tests)/", script)
    assert os.sep in script  # sanity: the script does write files somewhere
