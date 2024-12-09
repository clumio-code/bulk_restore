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

"""Lambda function to bulk restore EC2."""

from __future__ import annotations

import random
import string
from typing import TYPE_CHECKING, Any

import common
from clumio_sdk_v13 import RestoreEC2

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911, PLR0912, PLR0915
    """Handle the lambda function to bulk restore EC2."""
    record = events.get('record', {})
    bear = events.get('bear', None)
    target_account = events.get('target', {}).get('target_account', None)
    target_region = events.get('target', {}).get('target_region', None)
    target_az = events.get('target_az', None)
    target_iam_instance_profile_name = events.get('target', {}).get(
        'target_iam_instance_profile_name', None
    )
    target_key_pair_name = events.get('target', {}).get('target_key_pair_name', None)
    target_security_group_native_ids = events.get('target', {}).get(
        'target_security_group_native_ids', None
    )
    target_subnet_native_id = events.get('target', {}).get('target_subnet_native_id', None)
    target_vpc_native_id = events.get('target', {}).get('target_vpc_native_id', None)
    target_kms_key_native_id = events.get('target', {}).get('target_kms_key_native_id', None)

    inputs = {
        'resource_type': 'EC2',
        'run_token': None,
        'task': None,
        'source_backup_id': None,
        'source_instance_id': None,
    }

    if not record:
        return {'status': 205, 'msg': 'no records', 'inputs': inputs}

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not bear:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        bear = msg

    # Initiate API and configure
    ec2_restore_api = RestoreEC2()
    base_url = events.get('base_url', None)
    if base_url:
        ec2_restore_api.set_url_prefix(base_url)
    ec2_restore_api.set_token(bear)
    run_token = ''.join(random.choices(string.ascii_letters, k=13))  # noqa: S311

    if record:
        source_backup_id = record.get('backup_record', {}).get('source_backup_id', None)
        source_instance_id = record.get('instance_id')
    else:
        error = f'invalid backup record {record}'
        return {'status': 402, 'msg': f'failed {error}', 'inputs': inputs}

    target = {
        'account': target_account,
        'region': target_region,
        'aws_az': target_az,
        'iam_instance_profile_name': target_iam_instance_profile_name,
        'key_pair_name': target_key_pair_name,
        'security_group_native_ids': target_security_group_native_ids,
        'subnet_native_id': target_subnet_native_id,
        'vpc_native_id': target_vpc_native_id,
        'kms_key_native_id': target_kms_key_native_id,
    }
    result_target = ec2_restore_api.set_target_for_instance_restore(target)
    if not result_target:
        error_msgs = ec2_restore_api.get_error_msg()
        msgs_string = ':'.join(error_msgs)
        return {'status': 404, 'msg': msgs_string, 'inputs': inputs}
    # Run restore
    ec2_restore_api.save_restore_task()
    [result_run, msg] = ec2_restore_api.ec2_restore_from_record([record])

    if result_run:
        # Get a list of tasks for all of the restores.
        task_list = ec2_restore_api.get_restore_task_list()
        task = task_list[0].get('task', None)
        inputs = {
            'resource_type': 'EC2',
            'run_token': run_token,
            'task': task,
            'source_backup_id': source_backup_id,
            'source_instance_id': source_instance_id,
        }
        if len(task_list) > 0:
            return {'status': 200, 'msg': 'completed', 'inputs': inputs}
        else:
            return {'status': 207, 'msg': 'no restores', 'inputs': inputs}
    else:
        return {'status': 403, 'msg': msg, 'inputs': inputs}
