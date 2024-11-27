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

from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration, exceptions, models

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911
    """Handle the lambda function to bulk restore DynamoDB."""
    bear: str | None = events.get('bear', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    record: dict = events.get('record', {})
    target: dict = events.get('target', {})
    target_region: str | None = target.get('target_region', None)
    target_account: str | None = target.get('target_account', None)
    change_set_name: str | None = events.get('target', {}).get('change_set_name', None)

    inputs: dict[str, Any] = {'resource_type': 'DynamoDB'}

    if not record:
        return {'status': 402, 'msg': f'failed invalid backup record {record}', 'inputs': inputs}

    backup_record: dict = record.get('backup_record', {})
    source_backup_id: str = backup_record.get('source_backup_id', '')
    source_table_name: str = record.get('table_name', '')

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not bear:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        bear = msg

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(api_token=bear, hostname=base_url)
    client = clumioapi_client.ClumioAPIClient(config)
    run_token = common.generate_random_string()

    # Retrieve the environment id.
    status_code, result_msg = common.get_environment_id(client, target_account, target_region)
    if status_code != common.STATUS_OK:
        return {'status': status_code, 'msg': result_msg, 'inputs': inputs}
    target_env_id = result_msg

    # Perform the restore.
    source = models.dynamo_db_table_restore_source.DynamoDBTableRestoreSource(
        securevault_backup=models.dynamo_db_restore_source_backup_options.DynamoDBRestoreSourceBackupOptions(
            backup_id=source_backup_id,
        )
    )
    restore_target = models.dynamo_db_table_restore_target.DynamoDBTableRestoreTarget(
        environment_id=target_env_id, table_name=f'{source_table_name}-{change_set_name}'
    )
    request = models.restore_aws_dynamodb_table_v1_request.RestoreAwsDynamodbTableV1Request(
        source=source,
        target=restore_target,
    )
    inputs = {
        'resource_type': 'DynamoDB',
        'run_token': run_token,
        'task': None,
        'source_backup_id': source_backup_id,
        'source_table_name': source_table_name,
    }
    try:
        # Use raw response to catch request error.
        config.raw_response = True
        client = clumioapi_client.ClumioAPIClient(config)
        raw_response, result = client.restored_aws_dynamodb_tables_v1.restore_aws_dynamodb_table(
            body=request
        )
        # Return if non-ok status.
        if not raw_response.ok:
            return {
                'status': raw_response.status_code,
                'msg': raw_response.content,
                'inputs': inputs,
            }
        inputs['task'] = result.task_id
        return {'status': 200, 'msg': 'completed', 'inputs': inputs}
    except exceptions.clumio_exception.ClumioException as e:
        return {'status': '400', 'msg': f'Failure during restore request: {e}', 'inputs': inputs}
