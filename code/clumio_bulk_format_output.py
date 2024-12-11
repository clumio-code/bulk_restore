# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to format the output of the listing layer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to format the output of the listing layer."""
    # Retrieve and validate the inputs.
    total_backup_lists: list[dict] = events.get('total_backup_lists', [])
    target_specs: dict[str, Any] = events.get('target_specs', [])
    source_account: str = events.get('source_account', '')

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
                record['source_account'] = source_account
                restore_group.append(record)

    return {'status': 200, 'RestoreGroup': restore_group}


def format_record_per_resource_type(
    backup: dict, resource_type: str, source_region: str, target_specs: dict
) -> dict[str, Any]:
    """Format the backup record based on their resource type."""
    record = {
        'ResourceType': resource_type,
        'source_region': source_region,
        'search_direction': 'before',
        'search_tag_key': '',
        'search_tag_value': '',
        'start_search_day_offset': 0,
        'end_search_day_offset': 0,
        'target_account': target_specs['target_account'],
    }
    resource_target_specs = target_specs[resource_type]
    backup_record = backup.get('backup_record', {})
    region = resource_target_specs.get('target_region', '') or source_region
    if resource_type == 'EBS':
        az = resource_target_specs.get('target_az', None) or backup_record.get('source_az', None)
        volume_type = resource_target_specs.get('target_volume_type', None) or backup_record.get(
            'source_volume_type', None
        )
        iops = resource_target_specs.get('target_iops', 0) or backup_record.get('source_iops', 0)
        kms = resource_target_specs.get('target_kms_key_native_id', None) or backup_record.get(
            'source_kms', None
        )
        # Check the correctness of volume_type and iops
        if iops not in [0, None] and volume_type not in ['gp3', 'io1', 'io2']:
            raise ValueError
        record.update(
            {
                'search_volume_id': backup['volume_id'],
                'target_region': region,
                'target_az': az,
                'target_volume_type': volume_type,
                'target_iops': iops,
                'target_kms_key_native_id': kms,
            }
        )
    elif resource_type == 'EC2':
        az = resource_target_specs.get('target_az', None) or backup_record.get('source_az', None)
        vpc_id = resource_target_specs.get('target_vpc_native_id', None) or backup_record.get(
            'SourceVPCID', None
        )
        source_subnet = backup_record['source_network_interface_list'][0][
            'subnet_native_id'
        ]
        subnet_id = resource_target_specs.get('target_subnet_native_id', None) or source_subnet
        key_pair = (
            resource_target_specs.get('target_key_pair_name', None)
            or backup_record['SourceKeyPairName']
        )
        iam_name = (
            resource_target_specs.get('target_iam_instance_profile_name', None)
            or backup_record['source_iam_instance_profile_name']
        )
        kms = (
            resource_target_specs.get('target_kms_key_native_id', None)
            or backup_record['source_kms']
        )
        sg_id = (
            resource_target_specs.get('target_security_group_native_id', None)
            or backup_record['source_security_group_native_ids']
        )
        record.update(
            {
                'search_instance_id': backup['instance_id'],
                'target_region': region,
                'target_az': az,
                'target_vpc_native_id': vpc_id,
                'target_subnet_native_id': subnet_id,
                'target_kms_key_native_id': kms,
                'target_iam_instance_profile_name': iam_name,
                'target_key_pair_name': key_pair,
                'target_security_group_native_ids': sg_id,
            }
        )
    elif resource_type == 'DynamoDB':
        record.update(
            {
                'search_table_id': backup_record['source_table_id'],
                'target_region': region,
                'change_set_name': resource_target_specs['change_set_name'],
            }
        )
    elif resource_type == 'RDS':
        subnet_group = (
            resource_target_specs.get('target_subnet_group_name', None)
            or backup_record['source_subnet_group_name']
        )
        kms = (
            resource_target_specs.get('target_kms_key_native_id', None)
            or backup_record['source_kms']
        )
        sg_id = (
            resource_target_specs.get('target_security_group_native_id', None)
            or backup_record['source_security_group_native_ids']
        )
        record.update(
            {
                'search_resource_id': backup['resource_id'],
                'target_region': region,
                'target_subnet_group_name': subnet_group,
                'target_rds_name': resource_target_specs.get('target_rds_name'),
                'target_kms_key_native_id': kms,
                'target_security_group_native_ids': sg_id,
            }
        )
    elif resource_type == 'ProtectionGroup':
        record.update(resource_target_specs)
        record.update(
            {'search_pg_name': backup['pg_name'], 'search_bucket_names': backup['pg_bucket_names']}
        )
    else:
        return {}
    return record
