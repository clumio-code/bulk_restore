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

import logging
from typing import TYPE_CHECKING, Any, Final

import common
from clumioapi import api_helper, exceptions, models

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)

IOPS_APPLICABLE_TYPE: Final = ['gp3', 'io1', 'io2']


# noqa: PLR0911, PLR0912, PLR0915
def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911, PLR0912, PLR0915
    """Handle the lambda function to bulk restore EBS."""
    record: dict = events.get('record', {})
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    target: dict = events.get('target', {})
    target_account: str | None = target.get('target_account', None)
    target_region: str | None = target.get('target_region', None)
    target_az: str | None = target.get('target_az', None)
    target_kms_key_native_id: str | None = target.get('target_kms_key_native_id', None)
    target_iops: str | int | None = target.get('target_iops', None)
    target_volume_type: str | None = target.get('target_volume_type', None)
    target_volume_tags: list[dict[str, Any]] | None = target.get('target_volume_tags', None)

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
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token)

    backup_record = record.get('backup_record', {})
    source_backup_id = backup_record.get('source_backup_id', None)
    source_volume_id = record.get('volume_id')
    source_volume_type = backup_record.get('source_volume_type', None)
    target_volume_tags = target_volume_tags or backup_record.get('source_volume_tags', [])

    # Retrieve the environment ID.
    target_env_id = common.get_environment_id_or_raise(client, target_account, target_region)

    # Validate inputs.
    try:
        if target_iops is not None:
            target_iops = int(target_iops)
    except (TypeError, ValueError) as e:
        error = f'invalid target_iops input: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}
    p_type = target_volume_type or source_volume_type
    if target_iops and p_type not in IOPS_APPLICABLE_TYPE:
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
        tags=target_volume_tags,
    )
    request = models.restore_aws_ebs_volume_v2_request.RestoreAwsEbsVolumeV2Request(
        source=source, target=restore_target
    )

    inputs = {
        'resource_type': 'EBS',
        'run_token': common.generate_random_string(),
        'task': None,
        'source_backup_id': source_backup_id,
        'source_volume_id': source_volume_id,
    }

    try:
        request_dict = api_helper.to_dictionary(request)
        logger.info('Restore EBS volume request: %s', request_dict)
        raw_response, result = client.restored_aws_ebs_volumes_v2.restore_aws_ebs_volume(
            body=request
        )
        # Return if non-ok status.
        if not raw_response.ok:
            logger.error('EBS restore failed with message: %s', raw_response.content)
            return {
                'status': raw_response.status_code,
                'msg': raw_response.content,
                'inputs': inputs,
            }
        logger.info('EBS restore task %s started successfully.', result.task_id)
        inputs['task'] = result.task_id
        return {'status': 200, 'msg': 'completed', 'inputs': inputs}
    except exceptions.clumio_exception.ClumioException as e:
        logger.error('EBS restore failed with exception: %s', e)
        return {'status': '400', 'msg': f'Failure during restore request: {e}', 'inputs': inputs}
