"""Pin the daily sync's catalog source: Backend-Service, not tubafrenzy MySQL (#346).

``scripts/sync-library.sh`` builds the ``library.db`` that LML search, the
song-request line, the dj-site catalog, and iOS metadata all read. Until this
cutover it built that file by tunnelling into Kattare and running two
``mysql -B -N`` SELECTs against tubafrenzy's ``wxycmusic``. Kattare hosting
ends 2026-09-22; the MySQL half had to go, and what replaces it is
``scripts/build_library_db.py`` (#417) against the Backend catalog export.

What is pinned here is the *seam*, in both directions:

1. **The MySQL read path is gone and stays gone.** Not a style preference --
   after 2026-09-22 the host it reaches simply will not answer, and a
   re-introduced tunnel or SELECT would fail the daily sync outright rather
   than degrade. The failure mode is silent for a day and then loud: LML keeps
   serving yesterday's snapshot, so nothing pages until someone notices the
   catalog has stopped moving.
2. **Everything downstream of the build is untouched.** The streaming-links
   enrichment, its floor guard, the two uploads, the ``va_release`` derive and
   the recall-index build are all source-agnostic -- they consume the built
   SQLite, not MySQL -- so the cutover is supposed to be invisible to them.
   Order assertions below say so in a way a future edit cannot quietly break.
3. **Both Backend-driven workflows authenticate as the same service account.**
   ``catalog-parity.yml`` wired ``BACKEND_CATALOG_*`` first (#365); this sync
   is the second consumer of the same credential, and the two must not drift
   into naming it differently. That guard is deliberately the shape of the one
   this cutover retired (``test_select_statements_match_sync_library_sh``,
   which held the daily SELECTs and the harness's copies in lockstep): when
   two files have to agree about one thing, assert it rather than hope.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNC_SCRIPT = REPO_ROOT / "scripts" / "sync-library.sh"
SYNC_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "sync-library.yml"
PARITY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "catalog-parity.yml"

# The env names that belonged to the MySQL read path and to nothing else.
#
# ``LIBRARY_DB_OUTPUT`` is deliberately NOT in this list despite the shared
# prefix: it names where the finished library.db is copied for the LML release
# upload, is set by the workflow, and survives the cutover. A prefix match
# instead of these exact names would report it as a regression.
_RETIRED_ENV_NAMES = (
    "LIBRARY_DB_HOST",
    "LIBRARY_DB_USER",
    "LIBRARY_DB_PASSWORD",
    "LIBRARY_DB_NAME",
    "LIBRARY_SSH_HOST",
    "LIBRARY_SSH_USER",
)


@pytest.fixture(scope="module")
def script() -> str:
    return SYNC_SCRIPT.read_text(encoding="utf-8")


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _steps(path: Path) -> list[dict[str, Any]]:
    jobs = _load(path)["jobs"]
    assert len(jobs) == 1, f"expected exactly one job in {path.name}"
    return next(iter(jobs.values()))["steps"]


def _step(path: Path, needle: str) -> dict[str, Any]:
    for step in _steps(path):
        if needle.lower() in str(step.get("name", "")).lower():
            return step
    raise AssertionError(
        f"no step named like {needle!r} in {path.name}; "
        f"have {[s.get('name') for s in _steps(path)]}"
    )


def _backend_env(step: dict[str, Any]) -> dict[str, str]:
    """Every ``BACKEND_`` name a step passes the producer.

    The prefix is ``BACKEND_`` and not ``BACKEND_CATALOG_`` deliberately: the
    producer also reads ``BACKEND_AUTH_URL`` and ``BACKEND_AUTH_ORIGIN``
    (overrides for the better-auth base URL and the CSRF Origin header), and a
    guard that filtered them out would let exactly the drift it exists to
    catch -- one workflow pointed at a different auth origin than the other --
    pass unnoticed."""
    return {k: v for k, v in (step.get("env") or {}).items() if k.startswith("BACKEND_")}


# The live build invocation, matched rather than string-searched: the script's
# own comments name the same path in prose (as they should -- they explain why
# it is there), and a plain ``.index`` would anchor every assertion below on
# whichever comment happens to come first.
_BUILD_INVOCATION_RE = re.compile(
    r'"?\$\{?PYTHON\}?"?[ \t]+scripts/build_library_db\.py\s*(?:\\\s*)?'
    r'--source[ \t]+"\$\{?BACKEND_CATALOG_URL\}?"\s*(?:\\\s*)?'
    r'--output[ \t]+"\$\{?DB_PATH\}?"'
)


def _build_invocation(script: str) -> re.Match[str]:
    match = _BUILD_INVOCATION_RE.search(script)
    assert match, (
        "sync-library.sh must build $DB_PATH by running scripts/build_library_db.py "
        "with --source $BACKEND_CATALOG_URL"
    )
    return match


class TestSyncScriptBuildsFromBackend:
    def test_invokes_the_build_cli_against_the_backend_source(self, script: str) -> None:
        """One invocation, producing the same ``$DB_PATH`` the rest of the
        script already consumes -- the whole point of #417's single-database
        entry point is that the flip is a substitution, not a rewiring."""
        _build_invocation(script)

    def test_the_build_is_guarded_and_aborts_the_run(self, script: str) -> None:
        """An unguarded build would leave an absent-or-partial ``$DB_PATH``
        for the enrichment and upload steps to read. ``library.db`` is
        replaced wholesale on upload, so publishing a thin one is a catalog
        outage, not a degraded day."""
        start = _build_invocation(script).start()
        line_start = script.rindex("\n", 0, start)
        assert re.search(r"if\s+!\s", script[line_start:start]), (
            "the build must be `if ! ...`-guarded so a failure aborts before upload"
        )

    def test_credentials_never_reach_the_command_line(self, script: str) -> None:
        """``build_library_db.py`` reads them from the environment by design:
        argv is readable by any ``ps`` and is echoed by ``set -x`` and by
        GitHub Actions command traces."""
        invocation = _build_invocation(script).group(0)
        assert "BACKEND_CATALOG_PASSWORD" not in invocation
        assert "BACKEND_CATALOG_TOKEN" not in invocation

    def test_missing_credentials_fail_before_the_build(self, script: str) -> None:
        """Fail fast and notify, matching how the retired ``LIBRARY_DB_*``
        check behaved: an unauthenticated run would otherwise surface several
        HTTP round-trips later as an opaque producer error."""
        build = _build_invocation(script).start()
        guard = script.index("BACKEND_CATALOG_TOKEN")
        assert guard < build
        preflight = script[guard:build]
        assert "BACKEND_CATALOG_EMAIL" in preflight and "BACKEND_CATALOG_PASSWORD" in preflight
        assert "notify_error" in preflight

    def test_no_mysql_client_invocation_remains(self, script: str) -> None:
        for fragment in ('-e "SELECT', "MYSQL_PWD", "mysql -h", "--default-character-set"):
            assert fragment not in script, f"{fragment!r} is part of the retired MySQL read path"

    def test_no_ssh_tunnel_remains(self, script: str) -> None:
        assert "ssh -f -N -L" not in script
        assert "StrictHostKeyChecking" not in script

    def test_retired_mysql_env_names_are_gone(self, script: str) -> None:
        for name in _RETIRED_ENV_NAMES:
            assert name not in script, f"{name} belonged to the MySQL read path"

    def test_library_db_output_survives(self, script: str) -> None:
        """The similarly-prefixed variable that is NOT part of the read path:
        it is how the built file reaches the LML release upload."""
        assert "LIBRARY_DB_OUTPUT" in script


class TestDownstreamStepsAreUnchanged:
    """The cutover replaces the producer and nothing else."""

    def test_every_post_build_step_still_runs_in_order(self, script: str) -> None:
        # Anchored on each step's own executable line, not on its name: the
        # script explains these steps in prose as well as running them (the
        # row floor's comment names STREAMING_APPLE_FLOOR to contrast the two
        # guards), and a bare name search would order the comments instead.
        order = [
            'LIBRARY_ROW_FLOOR="${LIBRARY_ROW_FLOOR:-',
            '"$LML_DIR/scripts/export_streaming_links.py"',
            'STREAMING_APPLE_FLOOR="${STREAMING_APPLE_FLOOR:-',
            'upload_library_db "$STAGING_URL"',
            'upload_library_db "$PRODUCTION_URL"',
            "scripts/derive_va_release.py",
            "build_compilation_track_location",
        ]
        positions = [_build_invocation(script).start()]
        for needle in order:
            assert needle in script, f"{needle!r} must survive the catalog-source cutover"
            positions.append(script.index(needle))
        assert positions == sorted(positions), (
            f"post-build steps ran out of order: {list(zip(['build', *order], positions))}"
        )


class TestSyncWorkflowBackendCredentials:
    def test_backend_credentials_reach_the_sync_step(self) -> None:
        env = _backend_env(_step(SYNC_WORKFLOW, "Run library sync"))
        assert set(env) == {
            "BACKEND_CATALOG_URL",
            "BACKEND_CATALOG_EMAIL",
            "BACKEND_CATALOG_PASSWORD",
        }, (
            "the auth-override names are absent on both workflows; adding one here needs the other too"
        )

    def test_the_service_account_secrets_match_the_parity_soak(self) -> None:
        """Both workflows sign in as ``catalog-parity@wxyc.invalid`` (#365), so
        a rotation must not be able to land on only one of them."""
        sync = _backend_env(_step(SYNC_WORKFLOW, "Run library sync"))
        parity = _backend_env(_step(PARITY_WORKFLOW, "Run catalog parity diff"))
        credential_names = {"BACKEND_CATALOG_EMAIL", "BACKEND_CATALOG_PASSWORD"}
        assert {k: v for k, v in sync.items() if k in credential_names} == {
            k: v for k, v in parity.items() if k in credential_names
        }

    def test_the_catalog_url_deliberately_does_not_match_the_parity_soak(self) -> None:
        """The URL is the one name that must NOT be shared, and this asserts the
        divergence rather than tolerating it.

        The soak reads a repo-level ``vars.BACKEND_CATALOG_URL`` so an operator
        can aim a dispatch at staging. Sharing that variable here would mean a
        forgotten staging override silently redirects the next *scheduled* run:
        production's library.db would be built from staging's catalog and
        uploaded to production LML, with nothing failing and only a log line to
        say so. On the soak a stale override costs a wasted run; here it costs
        the catalog."""
        sync = _backend_env(_step(SYNC_WORKFLOW, "Run library sync"))
        parity = _backend_env(_step(PARITY_WORKFLOW, "Run catalog parity diff"))
        assert sync["BACKEND_CATALOG_URL"] != parity["BACKEND_CATALOG_URL"]

    def test_the_catalog_url_is_not_read_from_the_shared_repo_variable(self) -> None:
        """Asserted against the resolved env expression, not the file text: the
        comment beside it names ``vars.BACKEND_CATALOG_URL`` precisely because
        that is the wiring it is warning against, and a text search cannot tell
        the warning apart from the thing warned about."""
        url = _backend_env(_step(SYNC_WORKFLOW, "Run library sync"))["BACKEND_CATALOG_URL"]
        assert "vars.BACKEND_CATALOG_URL" not in url, (
            "the daily sync must not read the repo-level variable the soak shares; "
            "a stale staging override would redirect a scheduled production build"
        )

    def test_a_scheduled_run_resolves_the_catalog_url_to_production(self) -> None:
        """``inputs.*`` is empty on a scheduled run, so the fallback is what a
        cron tick actually builds from -- it has to be the production URL
        literally, not another indirection that can be edited out of band."""
        url = _backend_env(_step(SYNC_WORKFLOW, "Run library sync"))["BACKEND_CATALOG_URL"]
        assert "inputs.backend_catalog_url" in url
        assert "'https://api.wxyc.org'" in url

    def test_the_override_is_a_dispatch_input(self) -> None:
        triggers = _load(SYNC_WORKFLOW).get("on", _load(SYNC_WORKFLOW).get(True))
        inputs = (triggers.get("workflow_dispatch") or {}).get("inputs") or {}
        assert "backend_catalog_url" in inputs, (
            "the staging override has to exist somewhere an operator can reach; "
            "a per-dispatch input cannot outlive the run that set it"
        )
        assert inputs["backend_catalog_url"].get("default") == "https://api.wxyc.org"


class TestSyncWorkflowFailureNotifier:
    """A daily job that writes production's catalog must alert when it fails.

    It had no failure path at all: no ``if: failure()`` step and no webhook in
    env, so the two stale-catalog days this month were found by a human rather
    than by an alert. The script's own ``--notify`` is not the fix -- the
    workflow never passes it, and a step also catches what the script cannot
    see (the streaming-db download, the checkout, a runner death).
    """

    def _notifier(self) -> dict[str, Any]:
        return _step(SYNC_WORKFLOW, "Notify Slack on failure")

    def test_the_notifier_runs_only_on_failure(self) -> None:
        assert self._notifier().get("if") == "failure()"

    def test_the_notifier_reads_the_monitoring_webhook(self) -> None:
        env = self._notifier().get("env") or {}
        assert "secrets.SLACK_MONITORING_WEBHOOK" in env.get("SLACK_WEBHOOK_URL", "")

    def test_a_missing_secret_is_itself_loud(self) -> None:
        """Copied from rebuild-cache.yml deliberately: when the secret is unset
        the notifier fails rather than skipping quietly, so the
        nobody-is-listening state cannot be the thing that falls silent. That
        is the #219 lesson, and this job is the one where it costs more."""
        run = self._notifier().get("run", "")
        assert "::error::" in run and "exit 1" in run

    def test_the_alert_carries_a_link_to_the_run(self) -> None:
        env = self._notifier().get("env") or {}
        assert "github.run_id" in env.get("RUN_URL", "")

    def test_mysql_toolchain_steps_are_gone(self) -> None:
        """The client, the ssh-agent and the host-key scan existed only to
        reach Kattare. The parity soak still installs all three -- it is the
        one remaining MySQL reader -- so this is a per-workflow assertion, not
        a repo-wide one.

        Asserted against the parsed steps rather than the file text on
        purpose: the step that removed them left a comment saying what went
        and why, and a text search cannot tell that explanation apart from a
        re-introduction."""
        for step in _steps(SYNC_WORKFLOW):
            executed = f"{step.get('uses', '')}\n{step.get('run', '')}"
            for fragment in ("mariadb-client", "webfactory/ssh-agent", "ssh-keyscan"):
                assert fragment not in executed, (
                    f"step {step.get('name') or step.get('uses')!r} still runs {fragment}, "
                    "which was only ever needed to reach Kattare"
                )

    def test_retired_secrets_are_gone(self) -> None:
        """Both halves matter: an env key with no secret behind it would be an
        empty string the shell reads as unset, and a ``secrets.`` reference
        with no env key still keeps a dead secret alive in the rotation."""
        source = SYNC_WORKFLOW.read_text(encoding="utf-8")
        env_keys = {key for step in _steps(SYNC_WORKFLOW) for key in (step.get("env") or {})}
        for name in (*_RETIRED_ENV_NAMES, "SSH_PRIVATE_KEY"):
            assert name not in env_keys, f"{name} is no longer read by the daily sync"
            assert f"secrets.{name}" not in source, f"{name} is no longer read by the daily sync"
