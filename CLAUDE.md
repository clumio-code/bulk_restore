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
7. **Monitor** (`clumio_bulk_retrieve_restore_task`) - Polls task status (up to 10 min, 20s intervals)

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
- Two CloudFormation templates in `code/`: `clumio_bulk_restore_deploy_cft.yaml` (restore) and `clumio_bulk_list_deploy_cft.yaml` (list/discovery)
- Lambda runtime: Python 3.12, timeouts 120-600s
- Example inputs and IAM policies in `examples/`

### Versioning
- `VERSION` file at repo root is the single source of truth for the build version (e.g. `1.0.0`)
- `make build` stamps it into the artifacts:
  - Copies `VERSION` into the Lambda zip as `version.txt`
  - Substitutes the `__BULK_RESTORE_VERSION__` placeholder in both CFTs (in `code/`) and writes the rendered templates to `build/`
- Both CFTs expose the version to deployers via:
  - A `Version` stack Output
  - The `CodeVersion` parameter (default stamped from `VERSION`); each Lambda's description includes `(v${CodeVersion})`. Override at deploy time only to force a Lambda code update under an unchanged S3 key.
- Release flow: bump `VERSION`, run `make build`, upload `build/clumio_bulk_restore.zip` and the rendered CFTs from `build/`. Never edit the CFTs in `code/` to set a version directly — the placeholder must be preserved so the build stamps it.

## Code Style
- **Ruff**: line-length 100, single quotes, Google-style docstrings
- **mypy**: strict mode (`disallow_untyped_defs = true`)
- **Docstring exemption**: test files (`*/test/test_*.py`) are excluded from docstring rules
- **Pre-commit hooks**: install with `pre-commit install -t pre-commit -t pre-push`. Temporarily skip a hook with `SKIP=<hook-id> git commit`
- **Branch naming**: pushes only allowed to `user/*`, `team/*`, or `revert-*-user/*` branches
