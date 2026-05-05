# EC2 monthly cache rebuild — operator runbook

The Discogs cache rebuild runs monthly via cron on the WXYC EC2 host. This
runbook covers the one-time setup, the recurring operator concerns, and the
troubleshooting playbook.

## Why this lives on EC2

We tried running the rebuild as a GitHub Actions cron and it doesn't fit
([discogs-etl#138](https://github.com/WXYC/discogs-etl/issues/138) and
the disable-cron PR follow-up). Specifically:

- Discogs's Cloudflare front (`data.discogs.com`) returns 403 from
  GitHub-hosted runner egress IPs. The same URL serves 200 from any
  residential or AWS-EC2-style IP.
- The job's compute envelope (~30+ min wall, multi-tens-of-GB stream)
  burns Actions minutes for what should be a short job hosted close to
  the destination DB.

EC2 fixes both: residential-class IP + colocation with cheaper egress to
Railway. Cost is effectively $0/month — runs against the existing
Backend-Service EC2 (the same `ssh wxyc-ec2` host the API uses).

## One-time setup

All commands run on the EC2 host as `ec2-user` unless noted.

### 1. Install runtimes

The Backend-Service EC2 runs Amazon Linux 2023. Package names below match that. If the host is a different distro, adapt accordingly.

```bash
# Build toolchain (cargo needs `cc` to link) + git + Postgres client
sudo dnf install -y gcc gcc-c++ make git postgresql15

# Python 3.11 — the highest the AL2023 default repos ship; pyproject.toml
# requires >=3.9 so this is well within range. (3.12 is not in the default
# repo on AL2023 as of 2026-05; use this instead.)
sudo dnf install -y python3.11 python3.11-pip python3.11-devel

# Rust toolchain (stable) — needed to build discogs-xml-converter
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable
source "$HOME/.cargo/env"

# GitHub CLI for `gh release download`
sudo dnf install -y gh
```

### 2. Clone the repos

```bash
sudo mkdir -p /opt/discogs-etl /opt/discogs-xml-converter
sudo chown ec2-user:ec2-user /opt/discogs-etl /opt/discogs-xml-converter

git clone https://github.com/WXYC/discogs-etl.git /opt/discogs-etl
git clone https://github.com/WXYC/discogs-xml-converter.git /opt/discogs-xml-converter

# Bootstrap the Python venv used by the cron script.
python3.11 -m venv /opt/discogs-etl/.venv
source /opt/discogs-etl/.venv/bin/activate
pip install -e "/opt/discogs-etl[dev]"

# Pre-build the Rust binary so the first cron tick doesn't pay the cold-build cost.
(cd /opt/discogs-xml-converter && cargo build --release)
```

### 3. Authenticate `gh` for the LML release-asset download

`gh release download` against the public `WXYC/library-metadata-lookup` repo
needs a token with at least `repo:read` (any classic PAT or fine-grained
token scoped to the repo works).

```bash
gh auth login --with-token <<<"$YOUR_TOKEN"
gh auth status
```

Or set `GH_TOKEN` in the env file (next step).

### 4. Provision secrets

Create `/etc/discogs-rebuild.env` (root-readable only) with:

```ini
DATABASE_URL_DISCOGS=postgresql://postgres:<pw>@<word>.proxy.rlwy.net:<port>/railway
SLACK_MONITORING_WEBHOOK=https://hooks.slack.com/services/...   # optional
SENTRY_DSN=https://<key>@<org>.ingest.sentry.io/<project>      # optional
GH_TOKEN=...                                                   # if not using `gh auth login`
```

Lock it down so the file isn't world-readable, but leave it readable to the cron user (`ec2-user` by default — root mode 600 means cron can't `source` it):

```bash
sudo chown root:ec2-user /etc/discogs-rebuild.env
sudo chmod 640 /etc/discogs-rebuild.env
```

If you'd rather run the cron as root, use `sudo crontab -e` in step 7 instead and tighten the file to `chown root:root` + `chmod 600`.

### 5. Set up logging directory

```bash
sudo mkdir -p /var/log/discogs-rebuild
sudo chown ec2-user:ec2-user /var/log/discogs-rebuild
```

### 6. Validate the host setup with smoke mode (no DB write)

`REBUILD_SMOKE=1` runs everything that can fail at host setup time —
git pulls, venv refresh, cargo build, gh release download, dump URL
resolution, FIFO + curl handshake — and exits 0 *before* writing anything
to `$DATABASE_URL_DISCOGS`. Run this first so you can fix any host-side
issue without touching prod:

```bash
sudo -u ec2-user bash -c '
    set -a; . /etc/discogs-rebuild.env; set +a
    REBUILD_SMOKE=1 /opt/discogs-etl/scripts/rebuild-cache.sh
'
```

Expected: ~3-5 min, ends with `smoke OK: read NNNNN bytes from the
streamed dump` and (if configured) a `🔍 smoke test passed (no DB
write performed)` Slack message.

### 7. Test the full rebuild manually before scheduling

Once smoke is green, run the real thing once before adding the cron
entry — `~60-90 min`, writes to Railway prod:

```bash
sudo -u ec2-user bash -c '
    set -a; . /etc/discogs-rebuild.env; set +a
    /opt/discogs-etl/scripts/rebuild-cache.sh
'
```

Expected: ends with `rebuild complete` in
`/var/log/discogs-rebuild/<timestamp>.log` and (if configured) a
`✅ Discogs cache rebuild: rebuilt successfully` Slack message.

### 8. Add the cron entry

Edit `ec2-user`'s crontab (`crontab -e`):

```cron
# Monthly Discogs cache rebuild — 06:00 UTC on the 4th of each month
# (a few days after Discogs publishes the monthly dump).
0 6 4 * * set -a; . /etc/discogs-rebuild.env; set +a; /opt/discogs-etl/scripts/rebuild-cache.sh
```

The `set -a; . /etc/discogs-rebuild.env; set +a` pattern sources the env
file with auto-export so the script sees the secrets. (Cron does not by
default source any shell rc, so `source` alone wouldn't propagate.)

## Recurring operations

### Watching a run

```bash
# follow the in-flight log
ssh wxyc-ec2 'tail -f /var/log/discogs-rebuild/$(ls -1 /var/log/discogs-rebuild | tail -1)'
```

### Triggering a manual run

```bash
ssh wxyc-ec2
sudo -u ec2-user bash -c '
    set -a; . /etc/discogs-rebuild.env; set +a
    /opt/discogs-etl/scripts/rebuild-cache.sh
'
```

The script's `flock` will refuse to start if another rebuild is already
running (zero-exit no-op).

### Smoke mode

Same recipe with `REBUILD_SMOKE=1` exits before any DB write — useful to
re-validate after upgrading the EC2 host (Python, Rust, system packages)
or after rotating the Railway PG password without risking a partial
rebuild against a broken connection:

```bash
sudo -u ec2-user bash -c '
    set -a; . /etc/discogs-rebuild.env; set +a
    REBUILD_SMOKE=1 /opt/discogs-etl/scripts/rebuild-cache.sh
'
```

## Troubleshooting

### "another rebuild is already running"

Lock file at `/var/run/discogs-rebuild.lock`. Either a previous run is
genuinely still in flight (check `ps -ef | grep run_pipeline`) or it
crashed without releasing the lock (which `flock` cleans up on next reboot
or by `rm`-ing the file).

### Slack alerts but no log file in `/var/log/discogs-rebuild/`

The trap fires before the `tee` redirect could write anything. Run
manually to see stderr.

### `gh release download` fails with "release asset not found"

Means `sync-library.yml` hasn't run successfully today/recently. Check
[its workflow runs](https://github.com/WXYC/discogs-etl/actions/workflows/sync-library.yml)
and trigger a fresh sync (`gh workflow run sync-library.yml`) before
rerunning the rebuild.

### Pipeline crashes with `psycopg.errors.DiskFull`

The cache rebuild presumes `--pair-filter` is doing its job (~50K release
rows, well inside Railway's volume). If a future change removes
`--pair-filter` or the library expands meaningfully, the volume fills.
Either restore the filter, expand the Railway volume, or move the cache
off Railway.

### Pipeline crashes with `relation "release" does not exist` early

The destination DB hasn't been alembic-stamped. Apply the procedure in
[`docs/migrations-runbook.md`](migrations-runbook.md) once.
