# Ephemeral rebuild stack — operator runbook

CloudFormation/SAM stack that runs the WXYC monthly Discogs cache rebuild on a one-shot EC2 instance instead of permanent infrastructure. Deployed to the WXYC organization AWS account `203767826763` in `us-east-1`, alongside [`wxyc-canary`](https://github.com/WXYC/wxyc-canary), by the OIDC role in [`infra/bootstrap/`](../bootstrap/README.md).

> **This stack runs in the org account, and only there.** It ran in a personal account between 2026-05-30 and the [#353](https://github.com/WXYC/discogs-etl/issues/353) cutover — not instead of the org copy but *alongside* it, so two EventBridge rules fired the same `cron(0 6 4 * ? *)` at the same shared database and the two rebuilds clobbered each other's scratch tables, destroying 27,163 releases on 2026-08-04 ([#352](https://github.com/WXYC/discogs-etl/issues/352)). If you find this stack absent from an account, that absence is deliberate: it means the migration happened. Do not read it as "never deployed" and re-create it — that is precisely the inference [#248](https://github.com/WXYC/discogs-etl/issues/248) made in good faith, and it undid a correct migration.

The stack itself does no work — it provisions the infra (Launch Template, two Lambdas, IAM, S3 log bucket, alarms, SNS) and steps out of the way. EventBridge fires the launcher once a month; the launcher boots an EC2; the EC2 self-terminates when it's done. Everything billable lasts ~90 minutes per month.

## What's in the stack

| Resource | Purpose |
|---|---|
| `LauncherFunction` (Lambda) | Fired by EventBridge `cron(0 6 4 * ? *)`. Prechecks for an already-running rebuild (`Project=discogs-rebuild`, pending/running) and aborts cleanly with a `LaunchCollisionAborted` metric if one exists (#304); otherwise calls `RunInstances` on the launch template with a tiny user-data stub. |
| `SweeperFunction` (Lambda) | Fired hourly. Force-terminates any rebuild-tagged EC2 older than `MAX_INSTANCE_AGE_HOURS` (default 3) and emits the `StaleInstanceTerminated` metric. |
| `LaunchTemplate` (EC2) | Pins instance type, AMI (latest AL2023 via SSM public parameter), 100 GB gp3 root, IMDSv2-only, `InstanceInitiatedShutdownBehavior=terminate`. |
| `InstanceRole` / `InstanceProfile` (IAM) | Attached to the spawned EC2. Grants `ssm:GetParameters` on `${SsmPrefix}/*`, `kms:Decrypt` (scoped via `kms:ViaService`), `s3:PutObject` on the log bucket, and `ec2:DescribeInstances` (`Resource: '*'`, since Describe has no resource scoping) for the bootstrap's concurrent-rebuild guard (#311). Still no EC2 *mutation* grants — self-termination is `shutdown -h now` (release via `InstanceInitiatedShutdownBehavior=terminate`), not `ec2:TerminateInstances`. Since #355, `ec2:DescribeInstances` is no longer merely a guard input: the bootstrap aborts rather than fail-opens when the query itself errors, so losing this grant (drift, a policy edit) doesn't just weaken the guard — it aborts every monthly rebuild until the grant is restored. |
| `LogBucket` (S3) | Per-run log archive. Bootstrap's `trap EXIT` `aws s3 cp`s `/var/log/discogs-rebuild/` to `s3://wxyc-discogs-rebuild-logs-<account>/<instance-id>/`. 180-day lifecycle. |
| `AlertTopic` (SNS) | Alarm fan-out. Optional email subscription via the `AlertEmail` parameter; subscribe Slack webhook lambdas externally. |
| `LauncherErrorAlarm` / `StaleInstanceAlarm` / `ReleaseCountAlarm` (CloudWatch) | Pages operators on (a) the launcher Lambda crashing, (b) the sweeper firing (= bootstrap panicked before `shutdown`), (c) `release_count` falling through its floor (= the cache lost a large share of its rows). |

The bootstrap script lives in this repo at [`scripts/rebuild-cache-bootstrap.sh`](../../scripts/rebuild-cache-bootstrap.sh) — *not* in the stack. The launcher's user-data clones discogs-etl and execs that script, so changing the bootstrap requires no Lambda redeploy.

## One-time setup

### 1. Provision SSM parameters

The stack does **not** create the SecureString parameters — bootstrap reads them, but they're operator-managed so the secret values aren't in CloudFormation drift history. Run `./provision-secrets.sh` from this directory; it prompts (with hidden input) for each value and writes them under `/wxyc/discogs-rebuild/`:

```bash
cd infra/ephemeral-rebuild
./provision-secrets.sh
```

The script hard-fails before any write if the caller's AWS account isn't `203767826763` (the WXYC org account), then displays account/region/prefix/caller-arn and asks for an explicit `y` confirmation as the second line of defence. `--overwrite` makes it safe to re-run for rotations. The final summary table lists parameter names + types only — never the decrypted values. Three env-var overrides are honored: `SSM_PREFIX=/some/other/path` if you deployed the stack with a non-default `SsmPrefix`, `AWS_REGION=…` if you deployed it outside the default `us-east-1`, and `EXPECTED_ACCOUNT=<id>` to deliberately target a sandbox/test account.

### 1a. Optional: enable the prune-coverage audit dump for one run

`scripts/rebuild-cache-bootstrap.sh` reads an optional `${SSM_PREFIX}/PRUNE_AUDIT_ENABLED` parameter (discogs-etl#217 Phase 1). It is unset by default and a missing param is the steady state — the bootstrap treats that exactly like the other optional params (`SENTRY_DSN`) and proceeds normally. Set it only on the deliberate audit rebuild that #217 Phase 1 calls for:

```bash
aws ssm put-parameter --name /wxyc/discogs-rebuild/PRUNE_AUDIT_ENABLED --value true --type String
```

The `/wxyc/discogs-rebuild` prefix above is the default `SsmPrefix`. If you deployed the stack with a non-default `SsmPrefix`, substitute it here (and in the `delete-parameter` below) — it must match the `${SSM_PREFIX}` the bootstrap reads, or the flag lands at a path the bootstrap never looks at and silently has no effect.

When present and truthy (`true` or `1`, case-insensitive), the bootstrap exports `PRUNE_AUDIT_DUMP_DIR=$LOG_DIR/prune-audit` before handing off to `rebuild-cache.sh`, so `run_pipeline.py` writes the prune classification artifacts under that path. A value that is set but not truthy (a typo, `false`, `0`) logs a `WARN: PRUNE_AUDIT_ENABLED=… is not truthy … DISABLED` line and leaves the dump off, so a mistake is visible in the bootstrap log rather than silently dropped. `$LOG_DIR` is what the `trap EXIT` handler syncs to S3 on shutdown, so the dump survives instance termination and lands at `s3://<REBUILD_LOG_BUCKET>/<instance-id>/prune-audit/prune-audit-<UTC-date>/`.

**Delete the param after the run** so subsequent (normal) rebuilds go back to default-off:

```bash
aws ssm delete-parameter --name /wxyc/discogs-rebuild/PRUNE_AUDIT_ENABLED
```

Leaving it set would make every future monthly rebuild dump the audit artifacts, which is harmless to cache data but wastes the extra classification pass and S3 storage.

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

`samconfig.toml` names the SAM artifact bucket explicitly (`s3_bucket = …`) rather than setting `resolve_s3`. `resolve_s3` reconciles the `aws-sam-cli-managed-default` CloudFormation stack, which the least-privilege CI deploy role is not authorized to touch. Note that the two are mutually exclusive — SAM rejects `--s3-bucket` and `--resolve-s3` together, and a `samconfig.toml` default counts as "provided", so re-adding `resolve_s3` there breaks the CI deploy even though nothing on the command line changed.

#### Reading a CI deploy run ([#396](https://github.com/WXYC/discogs-etl/issues/396))

**A green run of this workflow does not by itself mean the stack was deployed.** It runs `sam deploy --no-fail-on-empty-changeset`, so SAM exits 0 both when it applies a changeset and when it has nothing to apply. Between the [#353](https://github.com/WXYC/discogs-etl/issues/353) account cutover and 2026-08-16 the deploy role lacked `ec2:DescribeImages` and could not have applied *anything*; the workflow was green throughout, and the condition surfaced only when [#358](https://github.com/WXYC/discogs-etl/issues/358)'s `ReleaseCountAlarm` happened to be the first real changeset in that window and failed loudly.

The flag stays — three of the four triggers legitimately produce no template diff, and reddening those would put a permanent failing check on `main`. Instead every run now ends with a **Report deploy outcome** step (`scripts/sam_deploy_summary.py`) that renders which of these it was into the run summary and one annotation:

| Outcome | Step exit | Annotation | Meaning |
|---|---|---|---|
| Changeset applied | `0` | `notice` | The stack now matches `template.yaml` at this commit |
| **Nothing was deployed** | `0` | **`warning`** | SAM had nothing to apply. Nothing reached AWS |
| Failed | SAM's own code | `error` | CloudFormation rolled back; the stack is behind `main` |
| Outcome could not be determined | `65` | `error` | Exit 0, output matched neither marker — fails closed |
| No exit status was recorded | `65` | `error` | Almost always: an earlier step failed and `sam deploy` was not reached |

A **nothing was deployed** warning is expected when the trigger was a change to `scripts/rebuild-cache-bootstrap.sh` (the instance fetches that from the repo at run time; it is not part of the rendered template), a change to the workflow file, or a bare `workflow_dispatch` on an already-deployed `main`. It is **suspicious** when the push changed `template.yaml` or a Lambda handler in this directory — that means a template change did not reach the stack.

**Outcome could not be determined** means a SAM CLI upgrade reworded one of the two lines the renderer matches (`Successfully created/updated stack` / `No changes to deploy`). Read the deploy step's log, confirm what actually happened, then update `scripts/sam_deploy_summary.py` and the verbatim log fixtures in `tests/unit/test_sam_deploy_summary.py` — replacing those fixtures is how the new wording gets pinned. Failing closed is deliberate: an exit-0 deploy whose output cannot be read might have deployed nothing, and treating that as success is exactly what #396 was.

On an `AccessDenied` failure the summary names the denied IAM actions and points at [`infra/bootstrap/`](../bootstrap/README.md) — the grant belongs on the **deploy role**, not the `InstanceRole` this stack's rebuild assumes at run time.

### 3. Confirm the schedule

```bash
aws events list-rule-names-by-target \
  --target-arn $(aws cloudformation describe-stacks \
      --stack-name wxyc-discogs-rebuild \
      --query 'Stacks[0].Outputs[?OutputKey==`LauncherFunctionArn`].OutputValue' \
      --output text)
```

The schedule rule should be named like `wxyc-discogs-rebuild-LauncherFunctionMonthly-*`.

#### Arming and disarming it

`ScheduleState` (`ENABLED` / `DISABLED`, default `ENABLED`) controls whether the monthly rule fires. **Change it through the stack, never with `aws events disable-rule`** — the rule is CloudFormation-managed, so a console/CLI disable is drift that the next `sam deploy` reverts, and that deploy fires automatically on any push touching this directory. See [`docs/ec2-rebuild-runbook.md`](../../docs/ec2-rebuild-runbook.md) for the full procedure and the caveat about manually launched rebuilds.

CI does not pass `ScheduleState`, and SAM sends `UsePreviousValue=true` for parameters it does not override, so a deliberate `DISABLED` survives redeploys until someone sets it back.

The sweeper's hourly rule is not gated by this parameter. It force-terminates rebuild instances past their wall-clock budget, including manually launched ones — disarming the monthly rebuild must not disarm the cleanup.

Do not reach for the SAM `Enabled:` property as an alternative spelling. SAM resolves `Enabled` at transform time in Python, so an intrinsic like `!Ref` is always truthy and the rule comes out **armed** regardless of the parameter's value — silently, and `sam validate --lint` does not catch it. `tests/unit/test_ephemeral_rebuild_template.py` asserts against the transformed `AWS::Events::Rule` to keep that from creeping back in.

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
| `discogs-rebuild-release-count-floor` | The daily `release_count` reading fell below `ReleaseCountAlarmThreshold` — the cache likely lost a large share of its rows. | Compare against the prior successful run (`aws cloudwatch get-metric-statistics --namespace WXYC/DiscogsCache --metric-name release_count --statistics Minimum --period 86400 --start-time <t0> --end-time <t1>`), then read the most recent rebuild log in `s3://wxyc-discogs-rebuild-logs-<account>/`. The incident class is #352: a colliding rebuild truncating `release` and only partially reloading it. |

`ReleaseCountAlarmThreshold` (default `75000`) is the floor. It is a template parameter so it can be revised as the cache grows — `sam deploy --parameter-overrides ReleaseCountAlarmThreshold=<n>`, never by hand-editing the alarm resource. The default is provisional and the parameter's own `Description` in `template.yaml` carries the full rationale, including the trap in recomputing it: a trailing-90-day minimum taken over a window that contains a collapse yields a floor *below* the collapses the alarm exists to catch, so exclude known-incident days.

The floor alarm sets `TreatMissingData: missing`, unlike the two above it. Absence of a `release_count` datapoint means the count could not be taken — the publisher runs `COUNT(*) FROM release` and the whole publish chain in `sync-library.yml` is `if: success()` — so absence must not score as healthy. Practical consequence for operators: before the first daily publish lands, this alarm reads `INSUFFICIENT_DATA`, which is the expected post-deploy state, not a misconfiguration. It does **not** alert on the publisher going silent; if the series stops, the alarm holds its last state. That's a separate liveness concern and isn't covered here.

The launcher also emits a `LaunchCollisionAborted` metric (namespace `WXYC/DiscogsRebuild`) when its #304 precheck suppresses a launch because a rebuild is already in flight. That's the guard working as intended, not a fault, so there's no alarm on it — but a non-zero count is worth a look (usually a manual launch racing the monthly cron). Query it with `aws cloudwatch get-metric-statistics --namespace WXYC/DiscogsRebuild --metric-name LaunchCollisionAborted --statistics Sum --period 86400 --start-time <t0> --end-time <t1>`.

The launcher precheck only covers the EventBridge-dispatched path; a rebuild started by calling `RunInstances` / the launch template directly (as the 2026-07-06 #298 recovery did, deadlocking two instances on the shared cache) slips past it. The bootstrap adds the authoritative late guard (#311): `scripts/rebuild-cache-bootstrap.sh` queries the same peer set (`ec2:DescribeInstances`, `tag:Project=discogs-rebuild` + pending/running) right before the `rebuild-cache.sh` handoff. The tie-break is a total order — earliest `LaunchTime` wins, ties broken by smaller `InstanceId` — so exactly one of N concurrently-booted instances proceeds and the rest bow out (no mutual suicide). The bootstrap's own guard resolves to one of three outcomes:

- **Winner.** The query succeeds and this instance is earliest `LaunchTime` (or the query legitimately found no peers, including the eventually-consistent case where `DescribeInstances` hasn't caught up to self yet) — proceeds to the `rebuild-cache.sh` handoff.
- **Loser.** The query succeeds and finds an earlier peer — posts a `:no_entry:` Slack notice ("concurrent rebuild detected — … bowing out") via `SLACK_MONITORING_WEBHOOK` and self-terminates via `exit 0` before any cache write.
- **Query failure.** `ec2:DescribeInstances` itself errors (`AccessDenied`, throttling, a network failure) — since #355 this no longer fails open. It posts a `:rotating_light:` Slack alert and aborts via a non-zero exit rather than proceeding as though no peer existed. See [`BootstrapPeerQueryFailed`](../../docs/ec2-rebuild-runbook.md#bootstrappeerqueryfailed--the-run-terminated-before-touching-the-cache) in the runbook for the stderr-shape triage.

All three paths let `trap on_exit EXIT` upload the log and run `shutdown -h now`.

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

- `--continue-at -` resumes from the on-disk size across curl's own `--retry` attempts within a single invocation. It does *not* survive across script invocations: `WORK_DIR` is `mktemp -d` per run and the EXIT trap deletes it, so a manual re-run of `rebuild-cache.sh` always starts from byte 0. The resume is what saves us when a single curl invocation hits a mid-stream reset; whole-script retries are still full re-downloads.
- `--retry-all-errors` retries on any non-zero curl exit, including the mid-stream HTTP/2 `INTERNAL_ERROR` (curl exit 92) that plain `--retry` ignores. The widened matrix also retries on 4xx/5xx HTTP statuses, so a Discogs URL flip mid-window would burn 5 × 30 s before failing — the earlier `curl -sIfL` HEAD probe in the URL-resolution step is what actually catches "URL doesn't exist" up front.
- 5 attempts × 30 s delay covers a ~few-minute CDN incident. If all 5 attempts fail, curl exits non-zero, the script's `ERR` trap fires `notify_slack ":warning:"`, and the trap-EXIT chain in `rebuild-cache-bootstrap.sh` archives the log to S3 before terminate.

The download is sequential with the converter — curl finishes, *then* `run_pipeline.py` starts. The earlier FIFO design overlapped the two; sequential adds roughly +14 min worst-case to a 60–90 min run, which is the cost of resumability.

When triaging an alarm: check the per-instance log in `s3://wxyc-discogs-rebuild-logs-203767826763/<instance-id>/` for the curl line. (Runs from 2026-05-30 to the #353 cutover archived to the non-org account's bucket instead; that bucket is retained read-only for the #352 / #188 / #298 / #217 forensics.) Without `--verbose`, curl's retry messages look like `Warning: Transient problem: ... Will retry in 30 seconds. N retries left.` Multiple of those before a final non-zero exit means the CDN was unhealthy through the whole window. A clean curl exit-0 followed by a converter failure means the issue is downstream of curl. (#181)

## Related

- `WXYC/discogs-etl#163` — the issue this stack closes.
- `WXYC/wxyc-canary` — pattern reference (CloudFormation flavor, account, region).
- `docs/ec2-rebuild-runbook.md` — legacy Backend-Service-EC2 cron path; kept until two successful runs land via the new path.
