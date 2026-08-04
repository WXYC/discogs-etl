# `infra/bootstrap` — CI deploy role

One CloudFormation stack, `discogs-etl-deploy-role`, holding the IAM role GitHub Actions assumes via OIDC. It is the chicken-and-egg layer: CI cannot deploy this, because this is what lets CI deploy. Apply it by hand, with admin credentials, once per account.

Everything else in `infra/` is deployed *by* CI using the role defined here.

## Which account

The WXYC organization account (`203767826763`, SSO profile `wxyc-api`), `us-east-1` — the same account [`wxyc-canary`](https://github.com/WXYC/wxyc-canary) runs in.

WXYC infrastructure does not run in personal AWS accounts. This stack exists because that rule was violated by accident: a correct migration to the org account in May 2026 was undone by [#248](https://github.com/WXYC/discogs-etl/issues/248), which observed the org-account stack's *absence* from the personal account and read it as "never deployed" rather than "already moved". The result was two armed copies of the monthly rebuild schedule writing the same database, and the data loss in [#352](https://github.com/WXYC/discogs-etl/issues/352). See [#353](https://github.com/WXYC/discogs-etl/issues/353).

## Deploy

```bash
aws sso login --profile wxyc-api

aws cloudformation deploy \
  --profile wxyc-api \
  --region us-east-1 \
  --template-file infra/bootstrap/deploy-role.yaml \
  --stack-name discogs-etl-deploy-role \
  --capabilities CAPABILITY_NAMED_IAM
```

`CAPABILITY_NAMED_IAM` (not `CAPABILITY_IAM`) is required because the role has an explicit `RoleName` — the workflows reference it by ARN, so it cannot be auto-named.

Then set the ARN from the stack output as the repository variable `AWS_ROLE_TO_ASSUME`:

```bash
ARN=$(aws cloudformation describe-stacks --profile wxyc-api --region us-east-1 \
        --stack-name discogs-etl-deploy-role \
        --query 'Stacks[0].Outputs[?OutputKey==`DeployRoleArn`].OutputValue' --output text)
gh variable set AWS_ROLE_TO_ASSUME --repo WXYC/discogs-etl --body "$ARN"
```

## What the role is for

Two workflows assume it, which is why it is named for the repo rather than for the stack it deploys:

| Workflow | Uses it to |
|---|---|
| `deploy-ephemeral-rebuild.yml` | `sam deploy` the `wxyc-discogs-rebuild` stack |
| `sync-library.yml` | publish daily cache-health metrics to the `WXYC/DiscogsCache` CloudWatch namespace |

The second is easy to overlook when tightening the policy. Dropping the `CacheHealthMetrics` statement does not break any deploy — it silently stops the daily `release_count` series, and the floor alarm built on it ([#358](https://github.com/WXYC/discogs-etl/issues/358)) decays to `INSUFFICIENT_DATA` instead of firing. An alarm watching a metric nobody publishes is the failure mode this whole line of work exists to remove.

## Trust

`sts:AssumeRoleWithWebIdentity`, restricted to `repo:WXYC/discogs-etl:ref:refs/heads/main` by exact string match. Not `StringLike`, not `repo:WXYC/discogs-etl:*` — a workflow running from a feature branch, a tag, or a fork PR cannot assume it.

The GitHub OIDC provider itself is **not** declared here. It is account-global, unique per URL, and already owned by the `wxyc-canary-deploy` stack; a second declaration fails with `EntityAlreadyExists`. This template references it, deriving `arn:aws:iam::<account>:oidc-provider/token.actions.githubusercontent.com` by default. If a third WXYC repo adopts OIDC, extract the provider into a shared stack rather than adding another reference.

## Verifying the policy

The policy is deliberately least-privilege and was written by enumerating the target stack's resources, so the failure mode is a missing grant, not an excessive one. Verify with the simulator rather than by deploying:

```bash
ROLE=arn:aws:iam::203767826763:role/discogs-etl-deploy

aws iam simulate-principal-policy --profile wxyc-api \
  --policy-source-arn "$ROLE" \
  --action-names cloudformation:CreateChangeSet ssm:GetParameters \
                 ec2:CreateLaunchTemplate iam:CreateInstanceProfile \
                 s3:PutObject lambda:UpdateFunctionCode \
  --query 'EvaluationResults[].[EvalActionName,EvalDecision]' --output table
```

`cloudwatch:PutMetricData` must be simulated separately, **with its condition key supplied** — the statement is scoped by namespace condition, and the simulator returns `implicitDeny` without it:

```bash
aws iam simulate-principal-policy --profile wxyc-api \
  --policy-source-arn "$ROLE" \
  --action-names cloudwatch:PutMetricData \
  --context-entries ContextKeyName=cloudwatch:namespace,ContextKeyType=string,ContextKeyValues=WXYC/DiscogsCache \
  --query 'EvaluationResults[].[EvalActionName,EvalDecision]' --output table
```

An `implicitDeny` there is a simulator artifact if the context is missing and a real bug if it is present. Do not "fix" a denial by widening to `AdministratorAccess` — read the action name out of the `AccessDenied` and add that one grant.

## Note on resource naming

The policy scopes to two prefixes on purpose. Resources `infra/ephemeral-rebuild/template.yaml` names explicitly are `discogs-rebuild-*` (Lambdas, SNS topic, alarms) or bare `discogs-rebuild` (launch template); resources CloudFormation auto-names inherit the stack name and land under `wxyc-discogs-rebuild-*` (IAM roles, instance profile, EventBridge rules, log bucket). A policy written from the stack name alone matches only half of them.
