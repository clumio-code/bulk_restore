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

import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi.exceptions import clumio_exception
from clumioapi.models import (
    ec2_instance_restore_target,
    ec2_restore_ebs_block_device_mapping,
    ec2_restore_network_interface,
    ec2_restore_source,
    ec2_restore_target,
    restore_aws_ec2_instance_v1_request,
)

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911, PLR0912, PLR0915
    """Handle the lambda function to bulk restore EC2."""
    record = events.get('record', {})
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    clumio_token = events.get('clumio_token', None)
    target = events.get('target', {})
    target_account = target.get('target_account', None)
    target_region = target.get('target_region', None)
    target_az = events.get('target_az', None)
    target_iam_instance_profile_name = target.get('target_iam_instance_profile_name', None)
    target_key_pair_name = target.get('target_key_pair_name', None)
    target_security_group_native_ids = target.get('target_security_group_native_ids', None)
    target_subnet_native_id = target.get('target_subnet_native_id', None)
    target_vpc_native_id = target.get('target_vpc_native_id', None)
    target_kms_key_native_id = target.get('target_kms_key_native_id', None)
    target_instance_tags: list[dict[str, Any]] | None = target.get('target_instance_tags', None)
    target_volume_append_tags = target.get('target_volume_append_tags', [])
    should_power_on = target.get('should_power_on', False)
    target_ami_native_id = target.get('target_ami_native_id', '')
    target_eni_cfg_from_backup = target.get('target_eni_cfg_from_backup', False)
    source_account = target.get('source_account', None)
    source_region = target.get('source_region', None)

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
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token)

    if not record:
        error = f'invalid backup record {record}'
        return {'status': 402, 'msg': f'failed {error}', 'inputs': inputs}

    backup_record = record.get('backup_record', {})
    source_backup_id = backup_record.get('source_backup_id', None)
    source_instance_id = record.get('instance_id')
    source_target_account_region_same = True
    if target_account != source_account or target_region != source_region:
        source_target_account_region_same = False

    # Retrieve the environment ID.
    target_env_id = common.get_environment_id_or_raise(client, target_account, target_region)

    # Build the restore request.
    restore_source = ec2_restore_source.EC2RestoreSource(BackupId=source_backup_id)
    ebs_mapping = [
        ec2_restore_ebs_block_device_mapping.EC2RestoreEbsBlockDeviceMapping(
            KmsKeyNativeId=target_kms_key_native_id or ebs_storage['kms_key_native_id'],
            Name=ebs_storage['name'],
            VolumeNativeId=ebs_storage['volume_native_id'],
            Tags=(
                common.tags_from_dict(target_volume_append_tags)
                if target_volume_append_tags
                else []
            )
            + (common.tags_from_dict(ebs_storage['tags']) if ebs_storage.get('tags') else []),
        )
        for ebs_storage in backup_record.get('source_ebs_storage_list', [])
    ]
    network_interfaces = []
    subnet_native_id = target_subnet_native_id
    # If target_subnet_native_id is not provided, use the one from backup.
    target_vpc_native_id = target_vpc_native_id or backup_record['source_vpc_id']
    if not source_target_account_region_same:
        if target_eni_cfg_from_backup:
            logger.warning(  # noqa: PLE1205
                'ENI config from backup cannot be used when restoring to a different account or region. ',
                target_vpc_native_id,
                backup_record['source_vpc_id'],
            )
            target_eni_cfg_from_backup = False
    else:
        target_ami_native_id = target_ami_native_id or backup_record['source_ami_id']

    for interface in backup_record.get('source_network_interface_list', []):
        subnet_native_id = subnet_native_id or interface['subnet_native_id']
        network_interfaces.append(
            ec2_restore_network_interface.EC2RestoreNetworkInterface(
                DeviceIndex=interface['device_index'],
                NetworkInterfaceNativeId='',
                SecurityGroupNativeIds=target_security_group_native_ids
                or interface['security_group_native_ids'],
                SubnetNativeId=subnet_native_id,
                RestoreDefault=not target_eni_cfg_from_backup,
                RestoreFromBackup=target_eni_cfg_from_backup,
            )
        )
    # Key pairs are scoped to an (account, region). Only fall back to the
    # backup's source key pair when restoring into the same account+region,
    # since it wouldn't exist in a different target. Otherwise honor the
    # user-specified value (which may be None).
    key_pair_name = target_key_pair_name
    if not key_pair_name and source_target_account_region_same:
        key_pair_name = backup_record.get('source_key_pair_name')
    instance_restore_target = ec2_instance_restore_target.EC2InstanceRestoreTarget(
        AmiNativeId=target_ami_native_id,
        AwsAz=target_az,
        EbsBlockDeviceMappings=ebs_mapping,
        EnvironmentId=target_env_id,
        IamInstanceProfileName=target_iam_instance_profile_name or None,
        Tags=common.tags_from_dict(target_instance_tags) if target_instance_tags else None,
        KeyPairName=key_pair_name,
        NetworkInterfaces=network_interfaces,
        SubnetNativeId=subnet_native_id,
        ShouldPowerOn=should_power_on,
        VpcNativeId=target_vpc_native_id,
    )
    restore_target = ec2_restore_target.EC2RestoreTarget(
        InstanceRestoreTarget=instance_restore_target,
    )
    request = restore_aws_ec2_instance_v1_request.RestoreAwsEc2InstanceV1Request(
        Source=restore_source,
        Target=restore_target,
    )

    inputs = {
        'resource_type': 'EC2',
        'run_token': common.generate_random_string(),
        'task': None,
        'source_backup_id': source_backup_id,
        'source_instance_id': source_instance_id,
    }

    try:
        logger.info('Restore EC2 instance request: %s', request.dict())
        result = client.restored_aws_ec2_instances_v1.restore_aws_ec2_instance(body=request)
    except clumio_exception.ClumioException as e:
        logger.error('EC2 restore failed with exception: %s', e)
        return {'status': 400, 'msg': f'Failure during restore request: {e}', 'inputs': inputs}
    logger.info('EC2 restore task %s started successfully.', result.TaskId)
    inputs['task'] = result.TaskId
    return {'status': 200, 'msg': 'completed', 'inputs': inputs}
