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

"""Lambda function to bulk restore EBS."""

from __future__ import annotations

import json
import random
import string
from typing import TYPE_CHECKING, Any

import boto3
import botocore.exceptions
import common
from clumioapi import clumioapi_client, configuration, exceptions, models

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


# noqa: PLR0911, PLR0912, PLR0915
def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911, PLR0912, PLR0915
    """Handle the lambda function to bulk restore EBS."""
    record: dict = events.get('record', {})
    bear: str | None = events.get('bear', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    target: dict = events.get('target', {})
    target_account: str | None = target.get('target_account', None)
    target_region: str | None = target.get('target_region', None)
    target_az: str | None = target.get('target_az', None)
    target_kms_key_native_id: str | None = target.get('target_kms_key_native_id', None)
    target_iops: str | int | None = target.get('target_iops', None)
    target_volume_type: str | None = target.get('target_volume_type', None)

    inputs = {
        'resource_type': 'EBS',
        'run_token': None,
        'task': None,
        'source_backup_id': None,
        'source_volume_id': None,
    }

    if not record:
        return {'status': 205, 'msg': 'no records', 'inputs': inputs}

    # Validate inputs
    try:
        if target_iops is not None:
            target_iops = int(target_iops)
    except (TypeError, ValueError) as e:
        error = f'invalid target_iops input: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}

    # If clumio bearer token is not passed as an input read it from the AWS secret
    if not bear:
        bearer_secret = 'clumio/token/bulk_restore'  # noqa: S105
        secretsmanager = boto3.client('secretsmanager')
        try:
            secret_value = secretsmanager.get_secret_value(SecretId=bearer_secret)
            secret_dict = json.loads(secret_value['SecretString'])
            bear = secret_dict.get('token', None)
        except botocore.exceptions.ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f'Describe Volume failed - {error}'
            return {'status': 411, 'msg': error_msg}

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(api_token=bear, hostname=base_url)
    client = clumioapi_client.ClumioAPIClient(config)
    run_token = ''.join(random.choices(string.ascii_letters, k=13))  # noqa: S311

    if record:
        source_backup_id = record.get('backup_record', {}).get('source_backup_id', None)
        source_volume_id = record.get('volume_id')
    else:
        error = f'invalid backup record {record}'
        return {'status': 402, 'msg': f'failed {error}', 'inputs': inputs}

    # Retrieve the environment.
    status_code, result_msg = common.get_environment_id(client, target_account, target_region)
    if status_code != common.STATUS_OK:
        return {'status': status_code, 'msg': result_msg, 'inputs': inputs}
    target_env_id = result_msg

    # Perform the restore.
    source = models.ebs_restore_source.EBSRestoreSource(backup_id=source_backup_id)
    target = models.ebs_restore_target.EBSRestoreTarget(
        aws_az=target_az,
        environment_id=target_env_id,
        iops=target_iops,
        kms_key_native_id=target_kms_key_native_id or None,
        p_type=target_volume_type or None,
        tags=[],
    )
    request = models.restore_aws_ebs_volume_v2_request.RestoreAwsEbsVolumeV2Request(
        source=source, target=target
    )
    try:
        response = client.restored_aws_ebs_volumes_v2.restore_aws_ebs_volume(body=request)
        inputs = {
            'resource_type': 'EBS',
            'run_token': run_token,
            'task': response.task_id,
            'source_backup_id': source_backup_id,
            'source_volume_id': source_volume_id,
        }
        return {'status': 200, 'msg': 'completed', 'inputs': inputs}
    except exceptions.clumio_exception.ClumioException as e:
        return {'status': '400', 'msg': f'Failure during restore request: {e}', 'inputs': inputs}
