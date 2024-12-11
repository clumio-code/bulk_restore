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

from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration, exceptions, models

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911, PLR0912, PLR0915
    """Handle the lambda function to bulk restore EC2."""
    record = events.get('record', {})
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    bear = events.get('bear', None)
    target = events.get('target', {})
    target_account = target.get('target_account', None)
    target_region = target.get('target_region', None)
    target_az = events.get('target_az', None)
    target_iam_instance_profile_name = target.get(
        'target_iam_instance_profile_name', None
    )
    target_key_pair_name = target.get('target_key_pair_name', None)
    target_security_group_native_ids = target.get(
        'target_security_group_native_ids', None
    )
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
    run_token = common.generate_random_string()

    if not record:
        error = f'invalid backup record {record}'
        return {'status': 402, 'msg': f'failed {error}', 'inputs': inputs}

    backup_record = record.get('backup_record', {})
    source_backup_id = backup_record.get('source_backup_id', None)
    source_instance_id = record.get('instance_id')

    # Retrieve the environment id.
    status_code, result_msg = common.get_environment_id(client, target_account, target_region)
    if status_code != common.STATUS_OK:
        return {'status': status_code, 'msg': result_msg, 'inputs': inputs}
    target_env_id = result_msg

    # Perform the restore.
    restore_source = models.ec2_restore_source.EC2RestoreSource(
        backup_id=source_backup_id
    )
    ebs_mapping = [
        models.ec2_restore_ebs_block_device_mapping.EC2RestoreEbsBlockDeviceMapping(
            kms_key_native_id=ebs_storage['kms_key_native_id'] or target_kms_key_native_id,
            name=ebs_storage['name'],
            volume_native_id=ebs_storage['volume_native_id'],
            tags=common.tags_from_dict(ebs_storage['tags'])
        ) for ebs_storage in backup_record.get('source_ebs_storage_list', [])
    ]
    network_interfaces = []
    subnet_native_id = target_subnet_native_id
    for interface in backup_record.get('source_network_interface_list', []):
        subnet_native_id = subnet_native_id or interface['subnet_native_id']
        network_interfaces.append(
            models.ec2_restore_network_interface.EC2RestoreNetworkInterface(
                device_index=interface['device_index'],
                network_interface_native_id='',
                security_group_native_ids=target_security_group_native_ids or interface[
                    'security_group_native_ids'
                ],
                subnet_native_id=subnet_native_id,
                restore_default=True,
                restore_from_backup=False,
            )
        )
    instance_restore_target = models.ec2_instance_restore_target.EC2InstanceRestoreTarget(
        aws_az=target_az,
        ebs_block_device_mappings=ebs_mapping,
        environment_id=target_env_id,
        iam_instance_profile_name=target_iam_instance_profile_name or None,
        tags=common.tags_from_dict(backup_record['source_instance_tags']),
        key_pair_name=target_key_pair_name or backup_record['source_key_pair_name'],
        network_interfaces=network_interfaces,
        subnet_native_id=subnet_native_id,
        should_power_on=True,
        vpc_native_id=target_vpc_native_id or backup_record['source_vpc_id']
    )
    restore_target = models.ec2_restore_target.EC2RestoreTarget(
        instance_restore_target=instance_restore_target,
    )
    request = models.restore_aws_ec2_instance_v1_request.RestoreAwsEc2InstanceV1Request(
        source=restore_source,
        target=restore_target,
    )

    inputs = {
        'resource_type': 'EC2',
        'run_token': run_token,
        'task': None,
        'source_backup_id': source_backup_id,
        'source_instance_id': source_instance_id,
    }

    try:
        raw_response, result = client.restored_aws_ec2_instances_v1.restore_aws_ec2_instance(
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
