# Copyright 2024, Clumio, a Commvault Company.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Lambda function to bulk restore DynamoDB."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi.exceptions import clumio_exception
from clumioapi.models import (
    dynamo_db_restore_source_backup_options,
    dynamo_db_table_restore_source,
    dynamo_db_table_restore_target,
    global_secondary_index,
    key_schema_element,
    local_secondary_index,
    projection,
    provisioned_throughput,
    restore_aws_dynamodb_table_v1_request,
    sse_specification,
    stream_specification,
)

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


# --- Helpers: convert snake_case source_* dicts (emitted by the list_backups
# Lambda) back into the v1.0.x SDK model instances the restore API expects.
# Each helper validates input shape and raises ValueError with the offending
# field path so misconfigured user overrides surface as a 422 below rather
# than crashing the Lambda with AttributeError. ---


def _maybe_decode_json(value: Any) -> Any:
    r"""If value is a string that parses as JSON, return the parsed value.

    Step Functions input passed through certain Console / Pass-state flows can
    end up string-wrapped (e.g. ``"{\"k\": 1}"`` instead of ``{"k": 1}``).
    Auto-decode so the override paths don't reject these as type-mismatches.
    Strings that don't parse as JSON are returned unchanged so the downstream
    isinstance check still surfaces a clean 422.
    """
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


def _ensure_dict(value: Any, field: str) -> dict:
    """Return value if it is a dict, else raise ValueError naming the field."""
    value = _maybe_decode_json(value)
    if not isinstance(value, dict):
        raise ValueError(f'{field} must be a JSON object (dict); got {type(value).__name__}')
    return value


def _ensure_bool(value: Any, field: str) -> bool | None:
    """Coerce value to bool (decoding a JSON-string wrapper) or None; else raise ValueError."""
    if value is None:
        return None
    value = _maybe_decode_json(value)
    if not isinstance(value, bool):
        raise ValueError(f'{field} must be a JSON boolean (true/false); got {type(value).__name__}')
    return value


def _ensure_int(value: Any, field: str) -> int | None:
    """Coerce value to int (decoding a JSON-string wrapper) or None; else raise ValueError."""
    if value is None:
        return None
    value = _maybe_decode_json(value)
    # bool subclasses int in Python; reject it explicitly so True doesn't sneak in as 1.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f'{field} must be a JSON integer; got {type(value).__name__}')
    return value


def _ensure_list_of_dict(value: Any, field: str) -> list[dict]:
    """Return value if it is a list of dicts, else raise ValueError."""
    value = _maybe_decode_json(value)
    if not isinstance(value, list):
        raise ValueError(f'{field} must be a JSON list; got {type(value).__name__}')
    decoded: list[dict] = []
    for idx, raw_item in enumerate(value):
        item = _maybe_decode_json(raw_item)
        if not isinstance(item, dict):
            raise ValueError(
                f'{field}[{idx}] must be a JSON object (dict); got {type(item).__name__}'
            )
        decoded.append(item)
    return decoded


def _build_provisioned_throughput(
    value: Any, field: str = 'target_provisioned_throughput'
) -> provisioned_throughput.ProvisionedThroughput | None:
    if value is None:
        return None
    d = _ensure_dict(value, field)
    if not d:
        return None
    return provisioned_throughput.ProvisionedThroughput(
        ReadCapacityUnits=_ensure_int(d.get('read_capacity_units'), f'{field}.read_capacity_units'),
        WriteCapacityUnits=_ensure_int(
            d.get('write_capacity_units'), f'{field}.write_capacity_units'
        ),
    )


def _build_sse_specification(
    value: Any, field: str = 'target_sse_specification'
) -> sse_specification.SSESpecification | None:
    if value is None:
        return None
    d = _ensure_dict(value, field)
    if not d:
        return None
    return sse_specification.SSESpecification(
        KmsKeyType=d.get('kms_key_type'),
        KmsMasterKeyId=d.get('kms_master_key_id'),
    )


def _build_stream_specification(
    value: Any, field: str = 'target_stream_specification'
) -> stream_specification.StreamSpecification | None:
    if value is None:
        return None
    d = _ensure_dict(value, field)
    if not d:
        return None
    # list_backups serializes via SDK .dict() which yields snake_case for StreamSpecification.
    return stream_specification.StreamSpecification(
        Enabled=_ensure_bool(d.get('enabled'), f'{field}.enabled'),
        ViewType=d.get('view_type'),
    )


def _build_key_schema(
    value: Any, field: str = 'key_schema'
) -> list[key_schema_element.KeySchemaElement] | None:
    if value is None:
        return None
    items = _ensure_list_of_dict(value, field)
    if not items:
        return None
    return [
        key_schema_element.KeySchemaElement(
            AttributeName=item.get('attribute_name'),
            KeyType=item.get('key_type'),
        )
        for item in items
    ]


def _build_projection(value: Any, field: str = 'projection') -> projection.Projection | None:
    if value is None:
        return None
    d = _ensure_dict(value, field)
    if not d:
        return None
    return projection.Projection(
        NonKeyAttributes=d.get('non_key_attributes'),
        ProjectionType=d.get('projection_type'),
    )


def _build_gsi_list(
    value: Any, field: str = 'target_global_secondary_indexes'
) -> list[global_secondary_index.GlobalSecondaryIndex] | None:
    if value is None:
        return None
    items = _ensure_list_of_dict(value, field)
    if not items:
        return None
    return [
        global_secondary_index.GlobalSecondaryIndex(
            IndexName=i.get('index_name'),
            KeySchema=_build_key_schema(i.get('key_schema'), f'{field}[{idx}].key_schema'),
            Projection=_build_projection(i.get('projection'), f'{field}[{idx}].projection'),
            ProvisionedThroughput=_build_provisioned_throughput(
                i.get('provisioned_throughput'),
                f'{field}[{idx}].provisioned_throughput',
            ),
        )
        for idx, i in enumerate(items)
    ]


def _build_lsi_list(
    value: Any, field: str = 'target_local_secondary_indexes'
) -> list[local_secondary_index.LocalSecondaryIndex] | None:
    if value is None:
        return None
    items = _ensure_list_of_dict(value, field)
    if not items:
        return None
    return [
        local_secondary_index.LocalSecondaryIndex(
            IndexName=i.get('index_name'),
            KeySchema=_build_key_schema(i.get('key_schema'), f'{field}[{idx}].key_schema'),
            Projection=_build_projection(i.get('projection'), f'{field}[{idx}].projection'),
        )
        for idx, i in enumerate(items)
    ]


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911, PLR0915
    """Handle the lambda function to bulk restore DynamoDB."""
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    raw_record = events.get('record', {})
    raw_target = events.get('target', {})

    inputs: dict[str, Any] = {'resource_type': 'DynamoDB'}

    # Validate top-level container types up front. The accessors below assume
    # dict semantics; if the caller (state machine or direct invoker) passes a
    # malformed shape, surface a clean 422 instead of crashing on .get().
    if not isinstance(raw_target, dict):
        return {
            'status': 422,
            'msg': f'target must be a JSON object; got {type(raw_target).__name__}',
            'inputs': inputs,
        }
    if not isinstance(raw_record, dict):
        return {
            'status': 422,
            'msg': f'record must be a JSON object; got {type(raw_record).__name__}',
            'inputs': inputs,
        }
    target: dict = raw_target
    record: dict = raw_record

    target_region: str | None = target.get('target_region', None)
    target_account: str | None = target.get('target_account', None)
    change_set_name: str | None = target.get('change_set_name', None)

    if not record:
        return {'status': 402, 'msg': f'failed invalid backup record {record}', 'inputs': inputs}

    raw_backup_record = record.get('backup_record', {})
    if not isinstance(raw_backup_record, dict):
        return {
            'status': 422,
            'msg': (
                f'record.backup_record must be a JSON object; '
                f'got {type(raw_backup_record).__name__}'
            ),
            'inputs': inputs,
        }
    backup_record: dict = raw_backup_record
    source_backup_id: str = backup_record.get('source_backup_id', '')
    source_table_name: str = record.get('table_name', '')
    tags: list[dict[str, Any]] | None = target.get('source_ddn_tags', None)

    # Phase 2 per-field user overrides. For each field, prefer the target_*
    # value if explicitly set in the user spec; otherwise fall back to the
    # source_* value captured at list-backups time (Phase 1 mirror-source).
    def _override_or_source(key: str, default: Any = None) -> Any:
        """Use target['target_<key>'] if set, else backup_record['source_<key>']."""
        override = target.get(f'target_{key}')
        if override is not None:
            return override
        return backup_record.get(f'source_{key}', default)

    billing_mode = _override_or_source('billing_mode')
    table_class = _override_or_source('table_class')
    global_table_version = _override_or_source('global_table_version')
    provisioned_throughput_input = _override_or_source('provisioned_throughput')
    sse_specification_input = _override_or_source('sse_specification')
    stream_specification_input = _override_or_source('stream_specification')
    pitr_status = _override_or_source('pitr_status')
    contributor_insights_status = _override_or_source('contributor_insights_status')
    deletion_protection_enabled = _override_or_source('deletion_protection_enabled')
    global_secondary_indexes_input = _override_or_source('global_secondary_indexes')
    local_secondary_indexes_input = _override_or_source('local_secondary_indexes')
    # RestoreWcu has no source equivalent — user-only knob.
    restore_wcu = target.get('target_restore_wcu')

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token)

    # Retrieve the environment ID.
    target_env_id = common.get_environment_id_or_raise(client, target_account, target_region)

    # Build the restore request.
    source = dynamo_db_table_restore_source.DynamoDBTableRestoreSource(
        SecurevaultBackup=dynamo_db_restore_source_backup_options.DynamoDBRestoreSourceBackupOptions(
            BackupId=source_backup_id,
        )
    )
    # Mirror-source defaults with per-field overrides. Each target field uses
    # the user-specified target_<field> if present, else falls back to the
    # source_<field> captured by list-backups (Phase 1 behavior). Builders
    # raise ValueError on malformed override payloads; surface as 422.
    try:
        restore_target = dynamo_db_table_restore_target.DynamoDBTableRestoreTarget(
            EnvironmentId=target_env_id,
            TableName=f'{source_table_name}-{change_set_name}',
            Tags=common.tags_from_dict(tags) if tags else None,
            BillingMode=billing_mode,
            TableClass=table_class,
            GlobalTableVersion=global_table_version,
            ProvisionedThroughput=_build_provisioned_throughput(provisioned_throughput_input),
            SseSpecification=_build_sse_specification(sse_specification_input),
            StreamSpecification=_build_stream_specification(stream_specification_input),
            PitrStatus=_ensure_bool(pitr_status, 'target_pitr_status'),
            ContributorInsightsStatus=_ensure_bool(
                contributor_insights_status, 'target_contributor_insights_status'
            ),
            DeletionProtectionEnabled=_ensure_bool(
                deletion_protection_enabled, 'target_deletion_protection_enabled'
            ),
            RestoreWcu=_ensure_int(restore_wcu, 'target_restore_wcu'),
            GlobalSecondaryIndexes=_build_gsi_list(global_secondary_indexes_input),
            LocalSecondaryIndexes=_build_lsi_list(local_secondary_indexes_input),
        )
    except ValueError as e:
        logger.error('Invalid DynamoDB restore override: %s', e)
        return {'status': 422, 'msg': f'invalid override: {e}', 'inputs': inputs}
    request = restore_aws_dynamodb_table_v1_request.RestoreAwsDynamodbTableV1Request(
        Source=source,
        Target=restore_target,
    )
    logger.info(
        '%s .... %s .... %s .... %s',
        source_backup_id,
        target_env_id,
        f'{source_table_name}-{change_set_name}',
        tags,
    )
    inputs = {
        'resource_type': 'DynamoDB',
        'run_token': common.generate_random_string(),
        'task': None,
        'source_backup_id': source_backup_id,
        'source_table_name': source_table_name,
    }
    try:
        # Run restore.
        logger.info('Restore DynamoDB table from backup ID %s...', source_backup_id)
        result = client.restored_aws_dynamodb_tables_v1.restore_aws_dynamodb_table(body=request)
    except clumio_exception.ClumioException as e:
        logger.error('DynamoDB restore failed with exception: %s', e)
        return {'status': 400, 'msg': f'Failure during restore request: {e}', 'inputs': inputs}
    logger.info('DynamoDB restore task %s started successfully.', result.TaskId)
    inputs['task'] = result.TaskId
    return {'status': 200, 'msg': 'completed', 'inputs': inputs}
