# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Clumio Bulk Restore is a serverless AWS Lambda + Step Functions application that orchestrates bulk restore operations across multiple AWS resource types (EBS, EC2, RDS, DynamoDB, S3 Protection Groups) using the Clumio API. Python 3.12+.

## Commands

```bash
make install-dev    # Install all dev dependencies
make build          # Bundle Lambda functions + deps into build/clumio_bulk_restore.zip
make test           # Run unit tests with coverage (Green test runner, generates HTML report)
make lint           # Lint with Ruff
make format         # Format with Ruff
make format-check   # Check formatting without modifying
make mypy           # Type check with mypy
make clean          # Remove build artifacts
```

**Run a single test file:**
```bash
PYTHONPATH=code python3 -m green -v code.test.test_common
```

**Run a single test class/method:**
```bash
PYTHONPATH=code python3 -m green -v code.test.test_common.TestUtilFunctions
PYTHONPATH=code python3 -m green -v code.test.test_common.TestUtilFunctions.test_method_name
```

**CI pipeline** (GitHub Actions) runs: lint -> mypy -> format-check -> test.

## Architecture

### Pipeline Flow (Step Functions state machine)
1. **Validate** (`clumio_bulk_validate_input`) - Merges `DefaultInput` with per-group overrides in `RestoreGroups`
2. **Discover regions** (`clumio_bulk_list_regions`) - Resolves AWS environments/regions via Clumio API
3. **List assets** (`clumio_bulk_list_assets`) - Finds assets matching filters (tags, resource type)
4. **List backups** (`clumio_bulk_{ebs,ec2,rds,dynamodb,s3}_list_backups`) - Finds backups within time window
5. **Sort/filter** (`clumio_bulk_sort_list_backups`) - Removes empty results
6. **Restore** (`clumio_bulk_{ebs,ec2,rds,dynamodb,s3}_restore`) - Initiates restores via Clumio API
7. **Monitor** (`clumio_bulk_retrieve_restore_task`) - Polls Clumio task status. The Lambda polls internally for up to 10 min (20s intervals) and either returns `{status: 200/403, ...}` on terminal state or **raises `RestoreInProgress`** when the task is still running. The state machine's `Retry` block on this Lambda re-invokes it (default: every `PollingIntervalSeconds=60` for up to `PollingMaxAttempts=200` attempts ≈ 48h wall-time, sized for 64TB-class restores). Once attempts are exhausted, a matching `Catch` routes to the resource-type fail state.

### Key Modules
- **`code/common.py`** - Shared utilities: API client init (with exponential backoff retry), Clumio token management (env var or AWS Secrets Manager), pagination, tag filtering, timestamp filtering, environment lookups
- **`code/utils/dates.py`** - Date/time helpers (ISO formats, UTC, day-offset calculations)
- Each Lambda handler follows the pattern: `lambda_handler(events: dict, context: LambdaContext) -> dict` returning `{status, msg, ...}`

### API Integration
- Clumio Python SDK (`clumioapi`) for all API calls
- Bearer token auth from input JSON or AWS Secrets Manager (`clumio/token/bulk_restore`)
- Retry: 8 retries with exponential backoff on 429/5xx
- Filters use MongoDB-style syntax: `{'field': {'$eq': value}}`

### Infrastructure
- Single CloudFormation template in `code/`: `clumio_bulk_deploy_cft.yaml`. Defines `BulkRestoreStateMachine` and `BulkListStateMachine` over a shared Lambda set (the five `List*` backup Lambdas are defined once and referenced by both state machines), a shared `BulkLogGroup`. Stack outputs: `Version`, `BulkRestoreStateMachineArn/Name`, `BulkListStateMachineArn/Name`, `LogGroupName`.
- Rendered into `build/clumio_bulk_deploy_cft.yaml` by `make build` (with the `__BULK_RESTORE_VERSION__` placeholder substituted at build time).
- Lambda runtime: Python 3.12, timeouts 120-800s
- Example inputs and IAM policies in `examples/`

### Step Functions scale architecture
Both state machines fan out via nested Map states. The **inner per-record / per-asset Maps** (5 in each state machine, one per resource type) run as **Distributed Maps** (`ProcessorConfig.Mode: DISTRIBUTED`, `ExecutionType: STANDARD`) — each iteration runs as a child execution, so its events come out of the child's 25,000-event budget rather than the parent's. This lifts the practical ceiling from ~150-250 records/execution (with INLINE Maps) to ~10,000+. Parameters baked into each Distributed Map: `MaxConcurrency: 100`, `ToleratedFailurePercentage: 100` (don't abort the whole batch on a few failed items).
The outer Maps (`Split Runs by Input Groups`, `Split Run per Individual Region`, `Split Run per Resource Type`) stay INLINE — their iteration counts are inherently small (groups, regions, fixed-5 resource types). The IAM role on the state machines (`!Ref LambdaIAMRole`) needs `states:StartExecution` for child executions; the existing `examples/iam_policy_permissions_example.json` already grants `states:*`.

### Polling-loop design
The polling-loop pattern that detects long-running Clumio restores **does not** sit in the state machine as an explicit Wait→re-invoke loop. Instead:
- `clumio_bulk_retrieve_restore_task.py` raises `RestoreInProgress` when the task is still running (status would have been 205 in the old design)
- The Task-Lambda invocation in each resource type's polling section has a `Retry` matching `RestoreInProgress` (interval `${PollingIntervalSeconds}`, max attempts `${PollingMaxAttempts}`) and a `Catch` that routes to the resource's existing fail state when attempts are exhausted
- Each retry contributes ~3 events (`TaskFailed` + retry overhead) instead of ~12 events for the old `Wait` + `Pass` + `Task Lambda` + `Choice` cycle
- Tunable via the `PollingIntervalSeconds` (default 60) and `PollingMaxAttempts` (default 200, ≈48h cap) CFT parameters; bump `PollingMaxAttempts` for very large or slow restores

### Tagging deployed resources
Customers tag the deployed resources via **stack-level tags** at deploy time — CloudFormation auto-propagates them to every Lambda, the state machines, and the LogGroup. No template parameters, no per-resource plumbing, unlimited pairs.

```bash
# create-stack syntax (space-separated Key=...,Value=... pairs)
aws cloudformation create-stack \
  --stack-name clumio-bulk-restore \
  --template-body file://build/clumio_bulk_deploy_cft.yaml \
  --capabilities CAPABILITY_IAM \
  --tags Key=Environment,Value=prod Key=Owner,Value=platform-team Key=CostCenter,Value=12345

# deploy syntax (space-separated Key=Value pairs — no commas, no Key=/Value= prefixes)
aws cloudformation deploy \
  --stack-name clumio-bulk-restore \
  --template-file build/clumio_bulk_deploy_cft.yaml \
  --capabilities CAPABILITY_IAM \
  --tags Environment=prod Owner=platform-team CostCenter=12345
```

To update tags on an existing stack, run `aws cloudformation update-stack` with the new `--tags` set (CFN will re-propagate). Note: if a customer deploys via the AWS Console, the **Tags** section of the stack-creation wizard provides the same propagation behavior.

### Versioning
- `VERSION` file at repo root is the single source of truth for the build version (e.g. `1.0.0`)
- `make build` stamps the version into the artifacts:
  - Writes `version.txt` inside the Lambda zip
  - Names the zip itself `clumio_bulk_restore-${VERSION}.zip` (versioned filename)
  - Substitutes the `__BULK_RESTORE_VERSION__` placeholder in all three CFTs (in `code/`) and writes the rendered templates to `build/`. Each Lambda's `Code.S3Key` resolves to `<LambdaZipObjectPrefix>-${VERSION}.zip` — so a new release means a new S3 key, which is what forces CloudFormation to re-pull the Lambda code on stack update (CFN does **not** re-pull when only a Lambda's `Description` changes).
- Stack outputs surface the deployed version (`Version`) and each Lambda's description still includes `(v${CodeVersion})` for in-AWS-console auditing.
- Release flow: bump `VERSION`, run `make build`, upload `build/clumio_bulk_restore-${VERSION}.zip` to the S3 bucket pointed at by `LambdaCodeLocationBucket`, then deploy the rendered CFT from `build/`. Customers do not need to override `CodeVersion` or any other parameter to trigger a code update — the new S3 key handles it.
- Never edit the CFTs in `code/` to set a version directly; the `__BULK_RESTORE_VERSION__` placeholder must be preserved so the build stamps it.

> [!IMPORTANT]
> The CFT parameter for the Lambda zip key was renamed from `LambdaZipObject` (full filename, e.g. `clumio_bulk_restore.zip`) to `LambdaZipObjectPrefix` (prefix only, e.g. `clumio_bulk_restore`). Existing stacks updating to a new template will drop the old parameter; the new default works for the standard release flow. Customers who customized the old `LambdaZipObject` value need to set `LambdaZipObjectPrefix` explicitly on their next stack update.

## Code Style
- **Ruff**: line-length 100, single quotes, Google-style docstrings
- **mypy**: strict mode (`disallow_untyped_defs = true`)
- **Docstring exemption**: test files (`*/test/test_*.py`) are excluded from docstring rules
- **Pre-commit hooks**: install with `pre-commit install -t pre-commit -t pre-push`. Temporarily skip a hook with `SKIP=<hook-id> git commit`
- **Branch naming**: pushes only allowed to `user/*`, `team/*`, or `revert-*-user/*` branches
