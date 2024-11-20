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

import json
from typing import TYPE_CHECKING, Any

import boto3
import botocore.exceptions
import common
from clumioapi import clumioapi_client, configuration, exceptions, models

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911, PLR0912, PLR0915
    """Handle the lambda function to bulk restore EC2."""
    record: dict = events.get('record', {})
    bear: str | None = events.get('bear', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    target: dict = events.get('target', {})
    target_account: str | None = target.get('target_account', None)
    target_region: str | None = target.get('target_region', None)
    target_az: str | None = target.get('target_az', None)
    target_iam_instance = target.get('target_iam_instance_profile_name', None)
    target_key_pair_name = target.get('target_key_pair_name', None)
    target_sg_native_ids = target.get('target_security_group_native_ids', None)
    target_subnet_native_id = target.get('target_subnet_native_id', None)
    target_vpc_native_id = target.get('target_vpc_native_id', None)
    target_kms_key_native_id = target.get('target_kms_key_native_id', None)

    inputs = {
        'resource_type': 'EC2',
        'run_token': None,
        'task': None,
        'source_backup_id': None,
        'source_instance_id': None,
    }

    if len(record) == 0:
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
            error_msg = f'Describe Volume failed - {error}'
            return {'status': 411, 'msg': error_msg}

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(api_token=bear, hostname=base_url)
    client = clumioapi_client.ClumioAPIClient(config)
    run_token = common.generate_random_string()

    if record:
        backup_record = record.get('backup_record', {})
        source_backup_id = backup_record.get('source_backup_id', None)
        source_instance_id = record.get('instance_id')
    else:
        error = f'invalid backup record {record}'
        return {'status': 402, 'msg': f'failed {error}', 'inputs': inputs}

    # Retrieve the environment id.
    env_filter = {
        'account_native_id': {'$eq': target_account},
        'aws_region': {'$eq': target_region}
    }
    response = client.aws_environments_v1.list_aws_environments(filter=json.dumps(env_filter))
    if not response.current_count:
        return {
            "status": 402,
            "msg": f"The evironment with acc id {target_account} [{target_region}] cannot be found.",
            "inputs": inputs
        }
    target_env_id = response.embedded.items[0].p_id

    network_interface_list = backup_record.get('source_network_interface_list', [])
    ni_object_list = []
    for ni in network_interface_list:
        ni_object_list.append(
            models.ec2_restore_network_interface.EC2RestoreNetworkInterface(
                device_index=ni['device_index'],
                security_group_native_ids=target_sg_native_ids,
                subnet_native_id=target_subnet_native_id,
            )
        )
    ebs_storage_list = backup_record.get('source_ebs_storage_list', [])
    ebs_storage_object_list = []
    for storage in ebs_storage_list:
        ebs_storage_object_list.append(
            models.ec2_restore_ebs_block_device_mapping.EC2RestoreEbsBlockDeviceMapping(
                volume_native_id=storage['volume_native_id'],
                kms_key_native_id=target_kms_key_native_id,
                name=storage['name'],
                tags=common.tags_from_dict(storage['tags'])
            )
        )


    # Prepare the restore request.
    source = models.ec2_restore_source.EC2RestoreSource(backup_id=source_backup_id)
    instance_restore_target = models.ec2_instance_restore_target.EC2InstanceRestoreTarget(
        aws_az=target_az,
        ebs_block_device_mappings=ebs_storage_object_list,
        environment_id=target_env_id,
        iam_instance_profile_name=target_iam_instance,
        key_pair_name=target_key_pair_name,
        network_interfaces=ni_object_list,
        subnet_native_id=target_subnet_native_id,
        tags=common.tags_from_dict(backup_record['source_instance_tags']),
        vpc_native_id=target_vpc_native_id,
    )
    target = models.ec2_restore_target.EC2RestoreTarget(
        instance_restore_target=instance_restore_target
    )

    request = models.restore_aws_ec2_instance_v1_request.RestoreAwsEc2InstanceV1Request(
        source=source,
        target=target
    )

    try:
        response = client.restored_aws_ec2_instances_v1.restore_aws_ec2_instance(body=request)
        inputs = {
            'resource_type': 'EBS',
            'run_token': run_token,
            'task': response.task_id,
            'source_backup_id': source_backup_id,
            'source_instance_id': source_instance_id,
        }
        return {'status': 200, 'msg': 'completed', 'inputs': inputs}
    except exceptions.clumio_exception.ClumioException as e:
        return {'status': '400', 'msg': f'Failure during restore request: {e}', 'inputs': inputs}
