# Automation

## Pipeline Lifecycle

This pipeline runs monthly (or when Discogs publishes new data dumps). It has a completely different lifecycle from the request-handling services that consume its output.

## Monthly Cache Rebuild (`rebuild-cache.yml`)

**Status (2026-05-07)**: the GH Actions cron is disabled. The rebuild now runs on a one-shot ephemeral EC2 spawned monthly by the `wxyc-discogs-rebuild` SAM stack (`infra/ephemeral-rebuild/`). Setup + recurring-ops instructions in [`infra/ephemeral-rebuild/README.md`](../infra/ephemeral-rebuild/README.md). The Backend-Service EC2 cron is the legacy fallback; once two ephemeral runs land successfully, it is removed per the procedure in [`docs/ec2-rebuild-runbook.md`](ec2-rebuild-runbook.md). The GH workflow file stays in the repo as a `workflow_dispatch`-only manual escape hatch (no scheduled trigger).

**Why EC2**: GH-hosted runner egress IPs are 403'd by Discogs's Cloudflare front at `data.discogs.com/?download=`; residential and EC2 IPs aren't. Plus the job's compute envelope (multi-tens-of-GB stream + 60-90 min wall) is wrong for free Actions minutes. EC2 already hosts Backend-Service, so the marginal cost of adding this cron is effectively $0.

The job fetches the current month's `releases.xml.gz`, `artists.xml.gz`, and — as of #317 — `masters.xml.gz` from `data.discogs.com` (the public download endpoint — direct S3 access via `discogs-data-dumps.s3.us-west-2.amazonaws.com` returns 403), downloads the daily-fresh `library.db` from `WXYC/library-metadata-lookup`'s `streaming-data-v1` release (produced by `sync-library.yml`), and runs the full XML-mode pipeline (steps 2-10) against `DATABASE_URL_DISCOGS`. `masters.xml.gz` is fetched **best-effort**: it tracks whichever month the releases/artists reachability gate resolves to, but a not-yet-published masters dump only skips the masters phase (a warning) rather than failing the time-sensitive releases/artists rebuild, and `import_csv.py`'s `import_masters` no-ops when `master.csv` is absent. Masters are loaded scoped to `release.master_id` (the same library scope the rest of the cache enforces) and carry `master.main_release_id`, which LML#858 uses for its master→release conversion.

The current `scripts/rebuild-cache.sh` (the ephemeral-EC2 path in the status note above) disk-spools each dump to the EBS-backed work dir with `curl --continue-at - --retry-all-errors`, so a mid-stream HTTP/2 reset resumes from the byte already on disk, then hands the converter the directory (`--xml "$WORK_DIR"`) so its directory-mode auto-dispatch runs `process_artists` and `process_masters` alongside the release scan (#181, LML#497, #317). It prefers the prebuilt converter binary published by `discogs-xml-converter`'s release workflow, falling back to a source `cargo build` on any failure. (The earlier FIFO streaming design — `mkfifo data/releases.xml.gz` piped into the converter — was the Backend-Service-EC2-era workaround for a ~14 GB disk budget; it was unrecoverable on a mid-stream reset and was removed in #181.)

The workflow forwards `--library-db` to discogs-xml-converter, which applies its built-in pair-wise (artist, title) filter inside the streaming scanner. The import payload to `DATABASE_URL_DISCOGS` is ~50K release rows instead of the converter's ~4M, which is what makes a Railway-sized destination DB feasible (the unfiltered import overflows the volume at `COPY release_artist`; see #128 and WXYC/discogs-xml-converter#45).

After the pipeline succeeds, the workflow runs `scripts/check_cache_drift.py` against the just-rebuilt cache. It compares `COUNT(DISTINCT artist) FROM library` (sqlite) to `COUNT(DISTINCT artist_name) FROM release_artist` (cache); if the ratio falls below `0.7`, the step exits non-zero, the workflow's `failure()` Slack notifier fires, and the watchdog itself posts a more specific drift message via `SLACK_MONITORING_WEBHOOK`. This is the third acceptance criterion of [#125](https://github.com/WXYC/discogs-etl/issues/125): drift between rebuilds must be visible without a human looking. A pipeline-level failure (the rebuild itself crashing) also fires the same Slack notifier through the workflow's final `if: failure()` step, mirroring the `--notify` pattern in `scripts/sync-library.sh`.

**Library catalog source**: the workflow used to call `--generate-library-db --catalog-source tubafrenzy --catalog-db-url ...` to build `library.db` inline. That path required direct MySQL connectivity to Kattare, which is impossible from a GitHub-hosted runner (Kattare's MySQL only resolves from inside Kattare's network — the daily `sync-library.yml` workflow tunnels in over SSH). Reusing sync-library's pre-built artifact keeps the SSH credentials in one place. By 06:00 UTC on the 4th, the sync upload from 12:00 UTC on the 3rd is the freshest available snapshot. The watchdog reuses the same `data/library.db` for its drift comparison so the rebuild and the comparison are looking at the same library snapshot.

**Caveat — capacity**: the production path disk-spools each dump to the EBS-backed work dir rather than streaming it (see the `scripts/rebuild-cache.sh` paragraph above), so the capacity constraints to watch are the EBS volume size and the EC2 wall-clock — not a GitHub free-runner's ~14 GB disk or 6-hour limits, which now bind only the `workflow_dispatch` escape-hatch workflow. The work dir has to hold every compressed dump that spools to it — `releases.xml.gz`, `artists.xml.gz`, and, since #317, `masters.xml.gz` — so size the EBS volume for the combined on-disk footprint of all three and re-check it whenever a dump is added to the fetch list. The wall-clock envelope is the ~60-90 min the "Why EC2" note cites; if a future month pushes materially past that, the lever is a larger EC2 instance or volume, not a hosted-runner tier.

**Required GitHub secrets:**

| Secret | Description |
|--------|-------------|
| `DATABASE_URL_DISCOGS` | PostgreSQL URL for the destination cache database |
| `DISCOGS_TOKEN` | Discogs API token (optional; only matters if rate limits are hit) |
| `SENTRY_DSN` | Sentry DSN for error reporting (optional; JSON logging still works without it) |
| `SLACK_MONITORING_WEBHOOK` | Slack incoming webhook for failure + drift alerts. **Required for production-grade alerting**: when this secret is unset the `Notify Slack on failure` step *itself* fails (with an `::error::` annotation explaining the missing-secret state) instead of silently skipping, so a workflow-level failure can't fall silent the way the 2026-05-05 runs did (#219). The dispatcher's GitHub failure-notification email is the sole signal in that degraded state. |

**Upstream dependency**: a successful `sync-library.yml` run must have uploaded `library.db` to the `streaming-data-v1` release on `WXYC/library-metadata-lookup` before this workflow fires, or the `Download library.db from LML release artifact` step fails with `release asset not found`. The default `${{ github.token }}` has read scope on the public LML repo; no extra PAT required.

**Interpreting a failed run** (#219):

1. **`Verify dump URL is reachable` returns HTTP 403**: this is the expected outcome on a GitHub-hosted runner — Discogs's Cloudflare front blocks runner egress IPs (see "Why EC2" above). The preflight fails inside ~1 second so the operator doesn't burn 5+ minutes of pipeline work on a dump-host they can't reach. *Fix*: don't dispatch this workflow against the default `data.discogs.com` URL from a GH runner; either kick the rebuild on the `wxyc-discogs-rebuild` SAM stack (see `infra/ephemeral-rebuild/README.md`) or dispatch with an explicit `dump_url` input pointing at an asset mirrored to a runner-reachable host.
2. **`Verify alembic baseline is stamped` fails with "alembic_version table missing or empty"**: the destination DB was rebuilt without the one-time `alembic stamp head` per `docs/migrations-runbook.md`. *Fix*: run the stamp procedure, then redispatch.
3. **`Run pipeline (with streamed dump)` exits with "curl exited N while streaming"**: a network-side failure mid-stream (transient HTTP/2 reset, etc.). Bash now surfaces the curl exit code with priority over any downstream symptom (the converter complaining about an empty FIFO). *Fix*: redispatch; if it keeps recurring, escalate to a self-hosted runner.
4. **`Notify Slack on failure` is the only red step**: this means a prior step failed *and* `SLACK_MONITORING_WEBHOOK` is unset. Configure the secret, then redispatch.

**Manual fallback** when the workflow is unavailable or refuses to reach `data.discogs.com`: ssh into the Backend-Service EC2 box and run `scripts/rebuild-cache.sh` directly. The script's failure-path Slack `--notify` is the same surface as the workflow's. The detailed legacy-EC2-cron runbook is in [`docs/ec2-rebuild-runbook.md`](ec2-rebuild-runbook.md).

## Library Sync (`sync-library.yml`)

A GitHub Actions cron workflow runs `scripts/sync-library.sh` daily at noon UTC (7 AM EST / 8 AM EDT) to export the WXYC library catalog to SQLite (via `wxyc-export-to-sqlite` from wxyc-catalog) and upload it to library-metadata-lookup staging and production environments.

The workflow can also be triggered manually: `gh workflow run sync-library.yml`

The `--notify` flag is always passed, so Slack notifications are sent on failure when `SLACK_MONITORING_WEBHOOK` is configured.

**Streaming-links enrichment (WXYC/library-metadata-lookup#672):** before upload, the exported `library.db` is enriched with streaming URLs from `streaming_availability.db`. That file is read from the LML Railway **volume** via `GET /admin/download-streaming-db` (the canonical copy), not the GitHub Release — the "Set up streaming links enrichment" step authenticates with `ADMIN_TOKEN` against `PRODUCTION_URL`. The download **hard-fails** on a non-200 response, an empty file, or a non-SQLite/zero-albums body (a naive `curl -o` exits 0 on HTTP 404/500 and writes the error body), so a Railway outage at sync time **aborts the run** and production keeps **yesterday's** `library.db` (which still has its streaming links) rather than publishing a zero-link database. After enrichment, `sync-library.sh` asserts the `streaming_links.apple_music_url` count exceeds `STREAMING_APPLE_FLOOR` (default `100`) and aborts before upload if it doesn't; set `STREAMING_APPLE_FLOOR=0` to opt out (e.g. a local run with no streaming db).

**Required GitHub secrets:**

| Secret | Description |
|--------|-------------|
| `SSH_PRIVATE_KEY` | Private key authorized on Kattare |
| `LIBRARY_SSH_HOST` | Kattare SSH hostname |
| `LIBRARY_SSH_USER` | SSH username |
| `LIBRARY_DB_HOST` | MySQL host (as seen from SSH host) |
| `LIBRARY_DB_USER` | MySQL username |
| `LIBRARY_DB_PASSWORD` | MySQL password |
| `LIBRARY_DB_NAME` | MySQL database name |
| `ADMIN_TOKEN` | Bearer token for library-metadata-lookup admin endpoints |
| `STAGING_URL` | Staging base URL for library-metadata-lookup |
| `PRODUCTION_URL` | Production base URL for library-metadata-lookup |
| `SLACK_MONITORING_WEBHOOK` | Slack webhook for error notifications (optional) |

After a successful run, verify the library-metadata-lookup health endpoint returns healthy with the expected row count.
