# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to format the output of the listing layer."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext):
    """Handle the lambda function to format the output of the listing layer."""
    # Retrieve and validate the inputs.
    total_backup_lists: list[dict] = events.get('total_backup_lists', [])
    target_specs: dict[str, Any] = events.get('target_specs', [])

    restore_group: list[dict] = []
    for region_backup_list in total_backup_lists:
        source_region = region_backup_list['region']
        asset_backup_lists: list[dict[str, list[dict]]] = region_backup_list['backup_list']
        for asset_backup_list in asset_backup_lists:
            resource_type, backup_list = list(asset_backup_list.items())[0]
            for backup_record in backup_list:
                record = format_record_per_resource_type(
                    backup_record, resource_type, source_region, target_specs
                )
                restore_group.append(record)

    return {'status': 200, 'RestoreGroup': restore_group}


def format_record_per_resource_type(
        backup: dict, resource_type: str, source_region: str, target_specs: dict
) -> dict[str, Any]:
    """Format the backup record based on their resource type."""
    record = {
        'ResourceType': resource_type,
        'source_account': '',
        'source_region': source_region,
        'search_direction': 'after',
        'end_search_day_offset': 0,
        'target_account': target_specs['target_account']
    }
    resource_target_specs = target_specs[resource_type]
    backup_record = backup.get('backup_record', {})
    region = resource_target_specs.get('target_region', '') or source_region
    if resource_type == 'EBS':
        az = resource_target_specs['target_az'] or backup_record.get('source_az', None)
        volume_type = resource_target_specs['target_volume_type'] or backup_record.get('source_volume_type', None)
        iops = resource_target_specs['target_iops'] or backup_record.get('source_iops', 0)
        kms = resource_target_specs['target_kms_key_native_id'] or backup_record.get('source_kms', None)
        # Check the correctness of volume_type and iops
        if iops not in [0, None] and volume_type not in ['gp3', 'io1', 'io2']:
            raise ValueError
        record.update({
            'target_region': region,
            'target_az': az,
            'target_volume_type': volume_type,
            'target_iops': iops,
            'target_kms_key_native_id': kms,
        })
    elif resource_type == 'EC2':
        az = resource_target_specs['target_az'] or backup_record.get('source_az', None)
        vpc_id = resource_target_specs.get(
            'target_vpc_native_id', None
        ) or backup_record.get('SourceVPCID', None)
        source_subnet = backup_record['source_network_interface_list'][0]['network_interface_native_id']
        subnet_id = resource_target_specs.get('target_subnet_native_id', None) or source_subnet
        key_pair = resource_target_specs['target_key_pair_name'] or backup_record['SourceKeyPairName']
        iam_name = resource_target_specs['target_iam_instance_profile_name'] or backup_record[
            'source_iam_instance_profile_name'
        ]
        kms = resource_target_specs['target_kms_key_native_id'] or backup_record['source_kms']
        record.update({
            'target_region': region,
            'target_az': az,
            'target_vpc_native_id': vpc_id,
            'target_subnet_native_id': subnet_id,
            'target_kms_key_native_id': kms,
            'target_iam_instance_profile_name': iam_name,
            'target_key_pair_name': key_pair
        })
    else:
        return {}
    return record


# def format_ec2_record(resource_target_specs, backup_record):
#     az = resource_target_specs['target_az'] or backup_record.get('source_az', None)
#     vpc_native_id = resource_target_specs.get(
#         'target_vpc_native_id', None
#     ) or backup_record.get('SourceVPCID', None)
#     subnet_native_id = resource_target_specs.get('target_subnet_native_id', None)
#     record.update({
#         'target_region': region,
#         'target_az': az,
#         'target_volume_type': volume_type,
#         'target_iops': iops,
#         'target_kms_key_native_id': kms,
#     })

