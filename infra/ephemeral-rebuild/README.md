# Ephemeral rebuild stack — operator runbook

CloudFormation/SAM stack that runs the WXYC monthly Discogs cache rebuild on a one-shot EC2 instance instead of permanent infrastructure. Deployed to AWS account `503977661500` in `us-east-1`, alongside [`wxyc-canary`](https://github.com/WXYC/wxyc-canary).

The stack itself does no work — it provisions the infra (Launch Template, two Lambdas, IAM, S3 log bucket, alarms, SNS) and steps out of the way. EventBridge fires the launcher once a month; the launcher boots an EC2; the EC2 self-terminates when it's done. Everything billable lasts ~90 minutes per month.

## What's in the stack

| Resource | Purpose |
|---|---|
| `LauncherFunction` (Lambda) | Fired by EventBridge `cron(0 6 4 * ? *)`. Calls `RunInstances` on the launch template with a tiny user-data stub. |
| `SweeperFunction` (Lambda) | Fired hourly. Force-terminates any rebuild-tagged EC2 older than `MAX_INSTANCE_AGE_HOURS` (default 3) and emits the `StaleInstanceTerminated` metric. |
| `LaunchTemplate` (EC2) | Pins instance type, AMI (latest AL2023 via SSM public parameter), 100 GB gp3 root, IMDSv2-only, `InstanceInitiatedShutdownBehavior=terminate`. |
| `InstanceRole` / `InstanceProfile` (IAM) | Attached to the spawned EC2. Grants `ssm:GetParameters` on `${SsmPrefix}/*`, `kms:Decrypt` (scoped via `kms:ViaService`), and `s3:PutObject` on the log bucket. No EC2 mutation grants — shutdown is what releases the instance. |
| `LogBucket` (S3) | Per-run log archive. Bootstrap's `trap EXIT` `aws s3 cp`s `/var/log/discogs-rebuild/` to `s3://wxyc-discogs-rebuild-logs-<account>/<instance-id>/`. 180-day lifecycle. |
| `AlertTopic` (SNS) | Alarm fan-out. Optional email subscription via the `AlertEmail` parameter; subscribe Slack webhook lambdas externally. |
| `LauncherErrorAlarm` / `StaleInstanceAlarm` (CloudWatch) | Pages operators on (a) the launcher Lambda crashing, (b) the sweeper firing (= bootstrap panicked before `shutdown`). |

The bootstrap script lives in this repo at [`scripts/rebuild-cache-bootstrap.sh`](../../scripts/rebuild-cache-bootstrap.sh) — *not* in the stack. The launcher's user-data clones discogs-etl and execs that script, so changing the bootstrap requires no Lambda redeploy.

## One-time setup

### 1. Provision SSM parameters

The stack does **not** create the SecureString parameters — bootstrap reads them, but they're operator-managed so the secret values aren't in CloudFormation drift history. Run `./provision-secrets.sh` from this directory; it prompts (with hidden input) for each value and writes them under `/wxyc/discogs-rebuild/`:

```bash
cd infra/ephemeral-rebuild
./provision-secrets.sh
```

The script hard-fails before any write if the caller's AWS account isn't `503977661500` (the rebuild account), then displays account/region/prefix/caller-arn and asks for an explicit `y` confirmation as the second line of defence. `--overwrite` makes it safe to re-run for rotations. The final summary table lists parameter names + types only — never the decrypted values. Three env-var overrides are honored: `SSM_PREFIX=/some/other/path` if you deployed the stack with a non-default `SsmPrefix`, `AWS_REGION=…` if you deployed it outside the default `us-east-1`, and `EXPECTED_ACCOUNT=<id>` to deliberately target a sandbox/test account.

### 2. Deploy the stack

CI deploys on push to `main` via [`.github/workflows/deploy-ephemeral-rebuild.yml`](../../.github/workflows/deploy-ephemeral-rebuild.yml) when this directory or `scripts/rebuild-cache-bootstrap.sh` changes. For the first deploy or any out-of-band change, do it locally:

```bash
cd infra/ephemeral-rebuild
sam build
sam deploy --guided \
  --parameter-overrides \
    AlertEmail=ops@wxyc.org
```

The first guided deploy writes its choices to `samconfig.toml`; subsequent deploys can use `sam deploy` with no flags.

### 3. Confirm the schedule

```bash
aws events list-rule-names-by-target \
  --target-arn $(aws cloudformation describe-stacks \
      --stack-name wxyc-discogs-rebuild \
      --query 'Stacks[0].Outputs[?OutputKey==`LauncherFunctionArn`].OutputValue' \
      --output text)
```

The schedule rule should be named like `wxyc-discogs-rebuild-LauncherFunctionMonthly-*`.

### 4. Run a manual rebuild before the first cron tick

The `cron(0 6 4 * ? *)` schedule means month #1 of the new path doesn't fire until the 4th. Trigger one manually first:

```bash
aws lambda invoke \
  --function-name discogs-rebuild-launcher \
  --invocation-type RequestResponse \
  /tmp/launcher-out.json && cat /tmp/launcher-out.json
```

Watch the spawned instance's bootstrap log via SSM Session Manager (or wait for the S3 archive after termination):

```bash
INSTANCE_ID=$(jq -r .instance_id /tmp/launcher-out.json)
aws ssm start-session --target "$INSTANCE_ID"
# inside the session:
sudo tail -F /var/log/cloud-init-bootstrap.log /var/log/discogs-rebuild/*.log
```

Once the instance terminates and Slack reports `:white_check_mark: rebuilt successfully`, the new path is proven.

## Routine operations

### Manual run

```bash
aws lambda invoke --function-name discogs-rebuild-launcher \
  --invocation-type RequestResponse /tmp/launcher-out.json
```

### Tail an in-flight rebuild

The bootstrap mirrors stdout to `/var/log/cloud-init-bootstrap.log` and the rebuild proper to `/var/log/discogs-rebuild/*.log`. Both upload to S3 on shutdown via the `trap EXIT` hook, but mid-flight you'll need SSM Session Manager:

```bash
INSTANCE_ID=$(aws ec2 describe-instances \
    --filters Name=tag:Project,Values=discogs-rebuild \
              Name=instance-state-name,Values=running \
    --query 'Reservations[].Instances[].InstanceId' --output text)
aws ssm start-session --target "$INSTANCE_ID"
```

### Inspect a past rebuild's log

```bash
aws s3 ls s3://wxyc-discogs-rebuild-logs-<account>/
aws s3 cp --recursive s3://wxyc-discogs-rebuild-logs-<account>/i-0xxxxxxx/ ./logs/
```

Logs older than 180 days are auto-deleted by the bucket lifecycle.

### Force-terminate a stuck rebuild

If the bootstrap crashed before reaching `shutdown -h now` and the sweeper hasn't fired yet (it runs hourly):

```bash
aws ec2 describe-instances \
  --filters Name=tag:Project,Values=discogs-rebuild \
            Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].[InstanceId,LaunchTime]' --output table

aws ec2 terminate-instances --instance-ids i-0xxxxxxx
```

## Alarms

| Alarm | Fires when | First step |
|---|---|---|
| `discogs-rebuild-launcher-errors` | The launcher Lambda errored before completing `RunInstances`. | `aws logs tail /aws/lambda/discogs-rebuild-launcher --since 1h`. Usually IAM scope drift on `iam:PassRole` or a hand-edited launch template. |
| `discogs-rebuild-stale-instance` | The sweeper terminated a rebuild EC2 that was past its 3h budget. | Pull the log archive from S3 (the sweeper terminates *after* shutdown would have, so the bootstrap's `trap EXIT` upload should have run). Check what step the bootstrap was on when it stalled. |

Slack drift / pipeline-failure messages from the bootstrap itself flow through the `SLACK_MONITORING_WEBHOOK` SSM parameter — they're a different channel than CloudWatch alarms.

## Costs (for budgeting)

- EC2: t3.medium, ~$0.04/hr × ~1.5 hr × 12 months ≈ $0.70/year.
- EBS: 100 GB gp3 attached for ~1.5 hr/month ≈ $0.10/year.
- Lambda + EventBridge + CloudWatch metrics + SNS: under $0.10/year.
- S3 (log archive): negligible at the 180-day TTL.

Total: well under $2/year.

## Caveats

- **Default VPC** is required. The launch template doesn't set `SubnetId`, so RunInstances picks the default subnet of the default VPC. If the operator deletes the default VPC for compliance reasons, parameterize `SubnetId` and `SecurityGroupId`.
- **AMI drift.** The default for `AmiId` resolves to the latest AL2023 image at deploy time. If a future AL2023 base AMI breaks the bootstrap (e.g., dnf rename, default partition layout change), pin `AmiId` to a known-good ID via `--parameter-overrides`.
- **No spot.** A spot reclaim mid-rebuild would discard the partial dump (the spool file lives on the instance's gp3 volume, which is destroyed on terminate) and force a full re-download on the replacement instance. Stick with on-demand.

## Dump-download retry behavior

`scripts/rebuild-cache.sh` spools `releases.xml.gz` to `$WORK_DIR/releases.xml.gz` via a single `curl` invocation:

```
curl -fL --continue-at - --retry 5 --retry-delay 30 --retry-all-errors -o "$WORK_DIR/releases.xml.gz" "$url"
```

- `--continue-at -` resumes from the on-disk size on each retry, so a 9-minute partial isn't re-paid from byte 0.
- `--retry-all-errors` retries on any non-zero curl exit, including the mid-stream HTTP/2 `INTERNAL_ERROR` (curl exit 92) that plain `--retry` ignores.
- 5 attempts × 30 s delay covers a ~few-minute CDN incident. If all 5 attempts fail, curl exits non-zero, the script's `ERR` trap fires `notify_slack ":warning:"`, and the trap-EXIT chain in `rebuild-cache-bootstrap.sh` archives the log to S3 before terminate.

When triaging an alarm: check the per-instance log in `s3://wxyc-discogs-rebuild-logs-503977661500/<instance-id>/` for the curl line. If it shows multiple "Trying again" / "Resuming with..." messages before failing, the CDN was unhealthy through the entire window. If it shows a clean exit-0 followed by a converter failure, the issue is downstream of curl. (#181)

## Related

- `WXYC/discogs-etl#163` — the issue this stack closes.
- `WXYC/wxyc-canary` — pattern reference (CloudFormation flavor, account, region).
- `docs/ec2-rebuild-runbook.md` — legacy Backend-Service-EC2 cron path; kept until two successful runs land via the new path.
