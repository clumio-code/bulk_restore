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
import botocore.exceptions
import common
from clumioapi import clumioapi_client, configuration, exceptions, models

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0915, PLR0911
    """Handle the lambda function to bulk restore RDS."""
    record: dict = events.get('record', {})
    bear: str | None = events.get('bear', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    target: dict = events.get('target', {})
    target_account: str = target.get('target_account', None)
    target_region: str = target.get('target_region', None)
    target_security_group_native_ids: list = target.get(
        'target_security_group_native_ids', None
    )
    target_kms_key_native_id: str = target.get('target_kms_key_native_id', None)
    target_subnet_group_name: str = target.get('target_subnet_group_name', None)
    target_rds_name: str = target.get('target_rds_name', None)

    inputs = {'resource_type': 'RDS',}

    # Validate record.
    if not record:
        return {'status': 205, 'msg': 'no records', 'inputs': inputs}

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
            error_msg = f'Describe token failed - {error}'
            return {'status': 411, 'msg': error_msg}

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(api_token=bear, hostname=base_url)
    client = clumioapi_client.ClumioAPIClient(config)
    run_token = ''.join(random.choices(string.ascii_letters, k=13))  # noqa: S311

    backup_record = record.get('backup_record', {})
    source_backup_id = backup_record.get('source_backup_id', None)
    source_resource_id = record.get('resource_id', None)

    # Retrieve the environment.
    status_code, result_msg = common.get_environment_id(client, target_account, target_region)
    if status_code != common.STATUS_OK:
        return {'status': status_code, 'msg': result_msg, 'inputs': inputs}
    target_env_id = result_msg

    # Perform the restore.
    source = models.rds_resource_restore_source.RdsResourceRestoreSource(
        backup=models.rds_resource_restore_source_air_gap_options.RdsResourceRestoreSourceAirGapOptions(
            backup_id=source_backup_id
        )
    )
    target = models.rds_resource_restore_target.RdsResourceRestoreTarget(
        environment_id=target_env_id,
        instance_class=backup_record['source_instance_class'],
        is_publicly_available=backup_record['source_is_publicly_available'],
        kms_key_native_id=target_kms_key_native_id,
        name=f'{source_resource_id}{target_rds_name}',
        security_group_native_ids=target_security_group_native_ids,
        subnet_group_name=target_subnet_group_name,
        tags=common.tags_from_dict(backup_record['source_resource_tags'])
    )
    request = models.restore_aws_rds_resource_v1_request.RestoreAwsRdsResourceV1Request(
        source=source,
        target=target,
    )
    try:
        response = client.restored_aws_rds_resources_v1.restore_aws_rds_resource(body=request)
        inputs = {
            'resource_type': 'EBS',
            'run_token': run_token,
            'task': response.task_id,
            'source_backup_id': source_backup_id,
            'source_resource_id': source_resource_id,
        }
        return {'status': 200, 'msg': 'completed', 'inputs': inputs}
    except exceptions.clumio_exception.ClumioException as e:
        return {'status': '400', 'msg': f'Failure during restore request: {e}', 'inputs': inputs}
