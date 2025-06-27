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

import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi import exceptions, models

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger()


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0915, PLR0911
    """Handle the lambda function to bulk restore RDS."""
    record: dict = events.get('record', {})
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    target: dict = events.get('target', {})
    target_account: str | None = target.get('target_account', None)
    target_region: str | None = target.get('target_region', None)
    target_security_group_native_ids: list | None = target.get(
        'target_security_group_native_ids', None
    )
    target_kms_key_native_id: str | None = target.get('target_kms_key_native_id', None)
    target_subnet_group_name: str | None = target.get('target_subnet_group_name', None)
    target_rds_name: str = target.get('target_rds_name', '')
    target_resource_tags: list | None = target.get('target_resource_tags', None)

    inputs: dict[str, Any] = {'resource_type': 'RDS'}

    # Validate input.
    if not record:
        return {'status': 205, 'msg': 'no records', 'inputs': inputs}
    if not target_rds_name:
        return {'status': 205, 'msg': 'target_rds_name is required input', 'inputs': inputs}

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token)

    backup_record = record.get('backup_record', {})
    source_backup_id = backup_record.get('source_backup_id', '')
    source_resource_id = record.get('resource_id', '')

    # Retrieve the environment id.
    status_code, result_msg = common.get_environment_id(client, target_account, target_region)
    if status_code != common.STATUS_OK:
        return {'status': status_code, 'msg': result_msg, 'inputs': inputs}
    target_env_id = result_msg

    # Perform the restore.
    restore_source = models.rds_resource_restore_source.RdsResourceRestoreSource(
        backup=models.rds_resource_restore_source_air_gap_options.RdsResourceRestoreSourceAirGapOptions(
            backup_id=source_backup_id
        )
    )
    restore_target = models.rds_resource_restore_target.RdsResourceRestoreTarget(
        environment_id=target_env_id,
        instance_class=backup_record['source_instance_class'],
        is_publicly_accessible=backup_record['source_is_publicly_accessible'] or None,
        kms_key_native_id=target_kms_key_native_id or None,
        name=target_rds_name,
        security_group_native_ids=target_security_group_native_ids or None,
        subnet_group_name=target_subnet_group_name or None,
        tags=target_resource_tags,
    )
    request = models.restore_aws_rds_resource_v1_request.RestoreAwsRdsResourceV1Request(
        source=restore_source,
        target=restore_target,
    )
    inputs = {
        'resource_type': 'RDS',
        'run_token': common.generate_random_string(),
        'task': None,
        'source_backup_id': source_backup_id,
        'source_resource_id': source_resource_id,
    }
    try:
        logger.info('Restore RDS from backup %s...', restore_source.backup.backup_id)
        raw_response, result = client.restored_aws_rds_resources_v1.restore_aws_rds_resource(
            body=request
        )

        # Return if non-ok status.
        if not raw_response.ok:
            logger.error('RDS restore failed with message: %s', raw_response.content)
            return {
                'status': raw_response.status_code,
                'msg': raw_response.content,
                'inputs': inputs,
            }
        logger.info('RDS restore task %s completed successfully.', result.task_id)
        inputs['task'] = result.task_id
        return {'status': 200, 'msg': 'completed', 'inputs': inputs}
    except exceptions.clumio_exception.ClumioException as e:
        logger.error('RDS restore failed with exception: %s', e)
        return {'status': '400', 'msg': f'Failure during restore request: {e}', 'inputs': inputs}
