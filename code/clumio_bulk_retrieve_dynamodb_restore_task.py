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

"""Lambda function to retrieve the DynamoDB restore task."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import boto3
import botocore.exceptions
from clumio_sdk_v13 import RetrieveTask

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext


def lambda_handler(events, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to retrieve the DynamoDB restore task."""
    bear = events.get('bear', None)
    task = events.get('inputs', {}).get('task', None)

    source_backup_id = events.get('inputs', {}).get('source_backup_id', None)
    source_table_name = events.get('inputs', {}).get('source_table_name', None)
    debug_input = events.get('debug', None)
    run_token = events.get('inputs', {}).get('run_token', None)
    inputs = {
        'resource_type': 'DynamoDB',
        'run_token': run_token,
        'task': task,
        'source_backup_id': source_backup_id,
        'source_table_name': source_table_name,
    }

    try:
        debug = int(debug_input)
    except ValueError as e:
        error = f'invalid debug: {e}'
        return {'status': 401, 'msg': error, 'inputs': inputs}

    if not task:
        return {'status': 402, 'msg': 'no task id', 'inputs': inputs}

    task_id = task

    # If clumio bearer token is not passed as an input read it from the AWS secret
    if not bear:
        bearer_secret = 'clumio/token/bulk_restore'  # noqa: S105
        secretsmanager = boto3.client('secretsmanager')
        try:
            secret_value = secretsmanager.get_secret_value(SecretId=bearer_secret)
            secret_dict = json.loads(secret_value['SecretString'])
            # username = secret_dict.get('username', None)
            bear = secret_dict.get('token', None)
        except botocore.exceptions.ClientError as error:
            code = error.response['Error']['Code']
            return {'status': 411, 'msg': f'Describe Volume failed - {code}'}

    # Initiate API and configure
    retrieve_task_api = RetrieveTask()
    base_url = events.get('base_url', None)
    if base_url:
        retrieve_task_api.set_url_prefix(base_url)
    retrieve_task_api.set_token(bear)
    retrieve_task_api.set_debug(debug)

    # Run API in one time mode
    complete_flag, status, _ = retrieve_task_api.retrieve_task_id(task_id, 'one')
    inputs = {
        'resource_type': 'DynamoDB',
        'run_token': run_token,
        'task': task,
        'source_backup_id': source_backup_id,
        'source_table_name': source_table_name,
    }
    if complete_flag and not status == 'completed':
        return {'status': 403, 'msg': f'task failed {status}', 'inputs': inputs}
    if complete_flag and status == 'completed':
        return {'status': 200, 'msg': 'task completed', 'inputs': inputs}
    return {'status': 205, 'msg': f'task not done - {status}', 'inputs': inputs}
