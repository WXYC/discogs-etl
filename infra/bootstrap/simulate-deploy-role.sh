#!/usr/bin/env bash
# simulate-deploy-role.sh — verification for infra/bootstrap/deploy-role.yaml.
#
# A bootstrap IAM template has no behaviour a unit test can assert before it is
# deployed, so this is its test: run it after every `cloudformation deploy` of
# the deploy-role stack, and after any edit to the policy.
#
# Usage:
#     ./simulate-deploy-role.sh                    # defaults below
#     AWS_PROFILE_NAME=other ./simulate-deploy-role.sh
#     ACCOUNT_ID=123456789012 ./simulate-deploy-role.sh
#
# Exit code is 0 only if every expectation holds.
#
# ---------------------------------------------------------------------------
# Why every action is simulated against an explicit --resource-arns
#
# `simulate-principal-policy` with no --resource-arns evaluates against `*`.
# Every statement in this policy is resource-scoped, so `*` matches none of them
# and the sweep comes back implicitDeny across the board — which reads as a
# completely broken role. It isn't; it is the wrong question.
#
# Two actions return implicitDeny even WITH the right resource, and are expected
# to. They are checked here anyway, as expected-implicitDeny, so that nobody
# rediscovers them and "fixes" the policy by widening it:
#
#   cloudformation:ExecuteChangeSet  vs  .../changeSet/<name>/<uuid>
#   cloudformation:CreateChangeSet   vs  .../aws:transform/Serverless-...
#
# The simulator resolves --resource-arns against the resource types IAM's
# service-authorization reference registers for each action. Both of those
# actions register `stack` only, so a changeSet or transform ARN matches no
# statement and yields implicitDeny regardless of what the policy grants.
# (ExecuteChangeSet against the *stack* ARN is allowed, below, which is the
# tell.) Both grants are nonetheless load-bearing at deploy time — wxyc-canary
# added them in commits 8cc483a and be022a7 after real AccessDenied failures.
# Their only real proof is a successful `sam deploy`.
# ---------------------------------------------------------------------------

set -uo pipefail

PROFILE="${AWS_PROFILE_NAME:-wxyc-api}"
ACCOUNT="${ACCOUNT_ID:-203767826763}"
REGION="${AWS_REGION:-us-east-1}"
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/discogs-etl-deploy"

failures=0

decide() { # decide <action> <resource-arn> [extra args...]
    aws iam simulate-principal-policy --profile "$PROFILE" \
        --policy-source-arn "$ROLE_ARN" \
        --action-names "$1" --resource-arns "$2" "${@:3}" \
        --query 'EvaluationResults[0].EvalDecision' --output text 2>/dev/null
}

check() { # check <expected> <action> <resource-arn> [extra args...]
    local expected="$1" action="$2" resource="$3"
    local got
    got="$(decide "$action" "$resource" "${@:4}")"
    if [ "$got" = "$expected" ]; then
        printf '  ok       %-34s %s\n' "$action" "$got"
    else
        printf '  FAIL     %-34s got %s, want %s\n      %s\n' \
            "$action" "$got" "$expected" "$resource" >&2
        failures=$((failures + 1))
    fi
}

echo "Simulating $ROLE_ARN"
echo
echo "Deploy actions (expect allowed):"
check allowed cloudformation:CreateChangeSet \
    "arn:aws:cloudformation:${REGION}:${ACCOUNT}:stack/wxyc-discogs-rebuild/1111-2222"
check allowed cloudformation:DescribeStacks \
    "arn:aws:cloudformation:${REGION}:${ACCOUNT}:stack/wxyc-discogs-rebuild/1111-2222"
check allowed cloudformation:ExecuteChangeSet \
    "arn:aws:cloudformation:${REGION}:${ACCOUNT}:stack/wxyc-discogs-rebuild/1111-2222"
check allowed ssm:GetParameters \
    "arn:aws:ssm:${REGION}::parameter/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
check allowed s3:PutObject \
    "arn:aws:s3:::aws-sam-cli-managed-default-samclisourcebucket-v0kc5deo10yi/wxyc-discogs-rebuild/artifact.zip"
check allowed s3:PutObject \
    "arn:aws:s3:::wxyc-discogs-rebuild-logs-${ACCOUNT}/i-0123456789abcdef0/rebuild.log"
check allowed ec2:CreateLaunchTemplate \
    "arn:aws:ec2:${REGION}:${ACCOUNT}:launch-template/lt-0123456789abcdef0"
check allowed iam:CreateRole \
    "arn:aws:iam::${ACCOUNT}:role/wxyc-discogs-rebuild-InstanceRole-ABC123"
check allowed iam:CreateInstanceProfile \
    "arn:aws:iam::${ACCOUNT}:instance-profile/wxyc-discogs-rebuild-InstanceProfile-ABC123"
check allowed iam:PassRole \
    "arn:aws:iam::${ACCOUNT}:role/wxyc-discogs-rebuild-InstanceRole-ABC123"
check allowed lambda:UpdateFunctionCode \
    "arn:aws:lambda:${REGION}:${ACCOUNT}:function:discogs-rebuild-launcher"
check allowed lambda:AddPermission \
    "arn:aws:lambda:${REGION}:${ACCOUNT}:function:discogs-rebuild-sweeper"
check allowed events:PutRule \
    "arn:aws:events:${REGION}:${ACCOUNT}:rule/wxyc-discogs-rebuild-LauncherFunctionMonthly-ABC123"
check allowed sns:CreateTopic \
    "arn:aws:sns:${REGION}:${ACCOUNT}:discogs-rebuild-alerts"
check allowed cloudwatch:PutMetricAlarm \
    "arn:aws:cloudwatch:${REGION}:${ACCOUNT}:alarm:discogs-rebuild-launcher-errors"
check allowed logs:PutRetentionPolicy \
    "arn:aws:logs:${REGION}:${ACCOUNT}:log-group:/aws/lambda/discogs-rebuild-launcher:*"

echo
echo "sync-library.yml's daily cache-health metrics (not a deploy grant):"
# Scoped by condition, not by resource. Without --context-entries this returns
# implicitDeny and invites widening the statement to fix a non-problem.
check allowed cloudwatch:PutMetricData '*' \
    --context-entries ContextKeyName=cloudwatch:namespace,ContextKeyType=string,ContextKeyValues=WXYC/DiscogsCache

echo
echo "Negative controls (expect implicitDeny — a pass here is a real finding):"
check implicitDeny cloudwatch:PutMetricData '*' \
    --context-entries ContextKeyName=cloudwatch:namespace,ContextKeyType=string,ContextKeyValues=SomeoneElses/Namespace
check implicitDeny cloudformation:DeleteStack \
    "arn:aws:cloudformation:${REGION}:${ACCOUNT}:stack/wxyc-canary/1111-2222"
check implicitDeny iam:CreateRole \
    "arn:aws:iam::${ACCOUNT}:role/some-unrelated-role"
check implicitDeny s3:DeleteObject \
    "arn:aws:s3:::some-other-bucket/key"

echo
echo "Expected simulator artifacts (see the header comment — NOT policy gaps):"
check implicitDeny cloudformation:ExecuteChangeSet \
    "arn:aws:cloudformation:${REGION}:${ACCOUNT}:changeSet/samcli-deploy1/1111-2222"
check implicitDeny cloudformation:CreateChangeSet \
    "arn:aws:cloudformation:${REGION}:aws:transform/Serverless-2016-10-31"

echo
if [ "$failures" -eq 0 ]; then
    echo "All expectations held."
else
    echo "${failures} expectation(s) did not hold." >&2
fi
exit "$((failures > 0))"
