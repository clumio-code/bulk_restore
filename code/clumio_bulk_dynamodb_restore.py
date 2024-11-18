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
import random
import string
from typing import TYPE_CHECKING, Any

import boto3
import botocore.exceptions
from clumio_sdk_v13 import RestoreDDN

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911
    """Handle the lambda function to bulk restore DynamoDB."""
    bear: str | None = events.get('bear', None)
    debug_input: str | int = events.get('debug', 0)
    record: dict = events.get('record', {})
    target_region: str | None = events.get('target', {}).get('target_region', None)
    target_account: str | None = events.get('target', {}).get('target_account', None)
    change_set_name: str | None = events.get('target', {}).get('change_set_name', None)

    inputs = {
        'resource_type': 'DynamoDB',
        'run_token': None,
        'task': None,
        'source_backup_id': None,
        'source_table_name': None,
    }

    if not record:
        return {'status': 402, 'msg': f'failed invalid backup record {record}', 'inputs': inputs}

    source_backup_id: str | None = record.get('backup_record', {}).get('source_backup_id', None)
    source_table_name: str | None = record.get('table_name', None)

    # Validate inputs
    try:
        debug = int(debug_input)
    except (TypeError, ValueError) as error:
        msg = f'failed invalid debug: {error}'
        return {'status': 401, 'task': None, 'msg': msg, 'inputs': inputs}

    if not record:
        return {'status': 205, 'msg': 'no records', 'inputs': inputs}

    # If clumio bearer token is not passed as an input read it from the AWS secret
    if not bear:
        bearer_secret = 'clumio/token/bulk_restore'  # noqa: S105
        secretsmanager = boto3.client('secretsmanager')
        try:
            secret_value = secretsmanager.get_secret_value(SecretId=bearer_secret)
            secret_dict = json.loads(secret_value['SecretString'])
            # username = secret_dict.get('username', None)
            bear = secret_dict.get('token', None)
        except botocore.exceptions.ClientError as client_error:
            code = client_error.response['Error']['Code']
            return {'status': 411, 'msg': f'Describe Volume failed - {code}'}

    ddn_restore_api = RestoreDDN()
    base_url = events.get('base_url', None)
    if base_url:
        ddn_restore_api.set_url_prefix(base_url)
    ddn_restore_api.set_token(bear)
    ddn_restore_api.set_debug(99)
    run_token = ''.join(random.choices(string.ascii_letters, k=13))  # noqa: S311
    target = {
        'account': target_account,
        'region': target_region,
        'table_name': f'-{change_set_name}',
    }

    result_target = ddn_restore_api.set_target_for_ddn_restore(target)
    if not result_target:
        error_msgs = ddn_restore_api.get_error_msg()
        return {'status': 404, 'msg': ':'.join(error_msgs), 'inputs': inputs}

    print(f'target set status {result_target}')
    # Run restore
    ddn_restore_api.save_restore_task()
    results, msg = ddn_restore_api.ddn_restore_from_record([record])

    if not results:
        return {'status': 403, 'msg': msg, 'inputs': inputs}

    # Get a list of tasks for all of the restores.
    task_list = ddn_restore_api.get_restore_task_list()
    if debug > 5:  # noqa: PLR2004
        print(task_list)
    task = task_list[0].get('task', None)
    inputs = {
        'resource_type': 'DynamoDB',
        'run_token': run_token,
        'task': task,
        'source_backup_id': source_backup_id,
        'source_table_name': source_table_name,
    }
    if task_list:
        return {'status': 200, 'msg': 'completed', 'inputs': inputs}
    return {'status': 207, 'msg': 'no restores', 'inputs': inputs}
