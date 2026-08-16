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

Two layers, because neither catches what the other does.

**Statically**, `tests/unit/test_deploy_role_policy.py` pins the actions this deploy path is known to require, so a tightening that drops one fails at merge rather than at the next changeset. It asserts specific experimentally-established requirements, not minimality — that judgment stays with a human reading the template.

**Operationally**, against the real deployed role:

```bash
./infra/bootstrap/simulate-deploy-role.sh
```

25 expectations, exit non-zero if any fails. Run it after every deploy of this stack and after any policy edit. It covers the deploy actions, the `sync-library.yml` metrics grant, four negative controls (so a policy that got too permissive fails too, not just one that got too narrow), and two expected simulator artifacts.

**Do not simulate without `--resource-arns`.** Every statement here is resource-scoped, so a bare `simulate-principal-policy --action-names ...` evaluates against `*`, matches nothing, and returns `implicitDeny` for all of them — which reads as a completely broken role. It is the wrong question, not a finding.

Two results are `implicitDeny` even with the correct resource, and the script asserts they stay that way:

| action | resource |
|---|---|
| `cloudformation:ExecuteChangeSet` | `…:changeSet/<name>/<uuid>` |
| `cloudformation:CreateChangeSet` | `…:aws:transform/Serverless-2016-10-31` |

The simulator resolves `--resource-arns` against the resource types IAM's service-authorization reference registers per action, and both of those register `stack` only. A changeSet or transform ARN therefore matches no statement regardless of what the policy grants — the tell is that `ExecuteChangeSet` against the *stack* ARN comes back `allowed`. Both grants are nonetheless load-bearing at deploy time: `wxyc-canary` added them in commits `8cc483a` and `be022a7` after real `AccessDenied` failures. Their only real proof is a successful `sam deploy`.

`cloudwatch:PutMetricData` is scoped by condition rather than by resource, so it needs `--context-entries ContextKeyName=cloudwatch:namespace,…`. Without it you get `implicitDeny` and an invitation to widen a statement that was never wrong.

Do not respond to a denial by widening to `AdministratorAccess` — read the action name out of the `AccessDenied` and add that one grant.

## Note on resource naming

The policy scopes to two prefixes on purpose. Resources `infra/ephemeral-rebuild/template.yaml` names explicitly are `discogs-rebuild-*` (Lambdas, SNS topic, alarms) or bare `discogs-rebuild` (launch template); resources CloudFormation auto-names inherit the stack name and land under `wxyc-discogs-rebuild-*` (IAM roles, instance profile, EventBridge rules, log bucket). A policy written from the stack name alone matches only half of them.
