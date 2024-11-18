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

"""Lambda function to bulk restore RDS."""

from __future__ import annotations

import json
import random
import string
from typing import TYPE_CHECKING, Any

import boto3
from clumio_sdk_v13 import RestoreRDS

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext


def lambda_handler(events, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0915, PLR0911
    """Handle the lambda function to bulk restore RDS."""
    print(f'clumio_rds_restore events: {events}')
    record = events.get('record', {})
    bear = events.get('bear', None)
    target_account = events.get('target', {}).get('target_account', None)
    target_region = events.get('target', {}).get('target_region', None)
    debug_input = events.get('debug', None)
    target_security_group_native_ids = events.get('target', {}).get(
        'target_security_group_native_ids', None
    )
    target_kms_key_native_id = events.get('target', {}).get('target_kms_key_native_id', None)
    target_subnet_group_name = events.get('target', {}).get('target_subnet_group_name', None)
    target_rds_name = events.get('target', {}).get('target_rds_name', None)

    inputs = {
        'resource_type': 'RDS',
        'run_token': None,
        'task': None,
        'source_backup_id': None,
        'source_resource_id': None,
    }

    # Validate inputs
    try:
        debug = int(debug_input)
    except ValueError as e:
        error = f'invalid debug: {e}'
        return {'status': 401, 'task': None, 'msg': f'failed {error}', 'inputs': inputs}

    # If clumio bearer token is not passed as an input read it from the AWS secret
    if not bear:
        bearer_secret = 'clumio/token/bulk_restore'  # noqa: S105
        secretsmanager = boto3.client('secretsmanager')
        try:
            secret_value = secretsmanager.get_secret_value(SecretId=bearer_secret)
            secret_dict = json.loads(secret_value['SecretString'])
            # username = secret_dict.get('username', None)
            bear = secret_dict.get('token', None)
        except botocore.exceptions.ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f'Describe Volume failed - {error}'
            payload = error_msg
            return {'status': 411, 'msg': error_msg}

    if not record:
        return {'status': 205, 'msg': 'no records', 'inputs': inputs}

    # Initiate API and configure
    rds_restore_api = RestoreRDS()
    base_url = events.get('base_url', None)
    if base_url:
        rds_restore_api.set_url_prefix(base_url)
    rds_restore_api.set_token(bear)
    rds_restore_api.set_debug(debug)
    run_token = ''.join(random.choices(string.ascii_letters, k=13))  # noqa: S311

    if record:
        source_backup_id = record.get('backup_record', {}).get('source_backup_id', None)
        resource_id = record.get('resource_id', None)
    else:
        error = f'invalid backup record {record}'
        return {'status': 402, 'msg': f'failed {error}', 'inputs': inputs}
    # new_tag_identifier = [
    #    {"key": "InstanceToScanStatus", "value": "enable"},
    #    {"key": "OrginalInstanceId", "value": source_instance_id},
    #    {"key": "OriginalBackupId", "value": source_backup_id},
    #    {"key": "ClumioTaskToken", "vquitalue": run_token}
    # ]
    # rds_restore_api.add_ec2_tag_to_instance(new_tag_identifier)
    # Set restore target information

    source_name = resource_id
    rnd_string = ''.join(random.choices(string.ascii_letters, k=3))  # noqa: S311
    name_composite = f'{source_name}{target_rds_name}{rnd_string}'

    run_token = ''.join(random.choices(string.ascii_letters, k=13))  # noqa: S311
    if debug > 40:  # noqa: PLR2004
        print(
            f'source_name  {source_name} target_rds_name  {target_rds_name} name_composite  {name_composite}'
        )
    target = {
        'account': target_account,
        'region': target_region,
        'name': name_composite,
        'security_group_native_ids': target_security_group_native_ids,
        'kms_key_native_id': target_kms_key_native_id,
        'subnet_group_name': target_subnet_group_name,
    }
    print(f'rds-restore target: {target}')
    result_target = rds_restore_api.set_target_for_rds_restore(target)
    if not result_target:
        error_msgs = rds_restore_api.get_error_msg()
        msgs_string = ':'.join(error_msgs)
        return {'status': 404, 'msg': msgs_string, 'inputs': inputs}
    print(f'target set status {result_target}')
    # Run restore
    rds_restore_api.save_restore_task()
    [result_run, msg] = rds_restore_api.rds_restore_from_record([record])

    if not result_run:
        return {'status': 403, 'msg': msg, 'inputs': inputs}

    # Get a list of tasks for all of the restores.
    task_list = rds_restore_api.get_restore_task_list()
    if debug > 5:  # noqa: PLR2004
        print(task_list)
    task = task_list[0].get('task', None)
    inputs = {
        'resource_type': 'RDS',
        'run_token': run_token,
        'task': task,
        'source_backup_id': source_backup_id,
        'source_resource_id': resource_id,
    }
    if task_list:
        return {'status': 200, 'msg': 'completed', 'inputs': inputs}
    return {'status': 207, 'msg': 'no restores', 'inputs': inputs}
