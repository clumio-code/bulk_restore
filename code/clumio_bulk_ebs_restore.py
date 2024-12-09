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

import random
import string
from typing import TYPE_CHECKING, Any, Final

import common
from clumioapi import clumioapi_client, configuration, exceptions, models

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


IOPS_APPLICABLE_TYPE: Final = ['gp3', 'io1', 'io2']


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

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not bear:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        bear = msg

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(api_token=bear, hostname=base_url, raw_response=True)
    client = clumioapi_client.ClumioAPIClient(config)
    run_token = ''.join(random.choices(string.ascii_letters, k=13))  # noqa: S311

    backup_record = record.get('backup_record', {})
    source_backup_id = backup_record.get('source_backup_id', None)
    source_volume_id = record.get('volume_id')
    source_volume_type = backup_record.get('source_volume_type', None)

    # Retrieve the environment id.
    status_code, result_msg = common.get_environment_id(client, target_account, target_region)
    if status_code != common.STATUS_OK:
        return {'status': status_code, 'msg': result_msg, 'inputs': inputs}
    target_env_id = result_msg

    # Validate inputs.
    try:
        if target_iops is not None:
            target_iops = int(target_iops)
    except (TypeError, ValueError) as e:
        error = f'invalid target_iops input: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}
    p_type = target_volume_type or source_volume_type
    if target_iops is not None and p_type not in IOPS_APPLICABLE_TYPE:
        return {
            'status': 400,
            'msg': 'IOPS field is not applicable for either source or target volume type.',
            'inputs': {
                'target_volume_type': target_volume_type,
                'source_volume_type': source_volume_type,
            },
        }

    # Perform the restore.
    source = models.ebs_restore_source.EBSRestoreSource(backup_id=source_backup_id)
    restore_target = models.ebs_restore_target.EBSRestoreTarget(
        aws_az=target_az,
        environment_id=target_env_id,
        iops=target_iops,
        kms_key_native_id=target_kms_key_native_id or None,
        p_type=p_type,
        tags=common.tags_from_dict(backup_record.get('source_volume_tags', [])),
    )
    request = models.restore_aws_ebs_volume_v2_request.RestoreAwsEbsVolumeV2Request(
        source=source, target=restore_target
    )

    inputs = {
        'resource_type': 'EBS',
        'run_token': run_token,
        'task': None,
        'source_backup_id': source_backup_id,
        'source_volume_id': source_volume_id,
    }

    try:
        client = clumioapi_client.ClumioAPIClient(config)
        raw_response, result = client.restored_aws_ebs_volumes_v2.restore_aws_ebs_volume(
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
