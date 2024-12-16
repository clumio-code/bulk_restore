# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to format the output of the listing layer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

MUST_FILLED_INPUT: Final = '[This field must be filled]'


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to format the output of the listing layer."""
    # Retrieve and validate the inputs.
    total_backup_lists: list[dict] = events.get('total_backup_lists', [])
    target_specs: dict[str, Any] = events.get('target_specs', {})
    source_account: str = events.get('source_account', '')
    target_account: str = target_specs.get('target_account', '')
    if target_account and target_account != source_account:
        is_diff_account = True
    else:
        is_diff_account = False
        target_account = source_account

    restore_group: list[dict] = []
    for region_backup_list in total_backup_lists:
        source_region = region_backup_list['region']
        asset_backup_lists: list[dict[str, list[dict]]] = region_backup_list['backup_list']
        for asset_backup_list in asset_backup_lists:
            resource_type, backup_list = list(asset_backup_list.items())[0]
            for backup_record in backup_list:
                record = format_record_per_resource_type(
                    backup_record, resource_type, source_region, target_specs, is_diff_account
                )
                record['source_account'] = source_account
                record['target_account'] = target_account
                restore_group.append(record)

    return {'status': 200, 'RestoreGroup': restore_group}


def format_record_per_resource_type(
    backup: dict, resource_type: str, source_region: str, target_specs: dict, is_diff_account: bool
) -> dict[str, Any]:
    """Format the backup record based on their resource type."""
    output_record = {
        'ResourceType': resource_type,
        'source_region': source_region,
        'search_direction': 'before',
        'search_tag_key': '',
        'search_tag_value': '',
        'start_search_day_offset': 0,
        'end_search_day_offset': 0,
    }
    resource_target_specs = target_specs.get(resource_type, {})
    backup_record = backup.get('backup_record', {})
    region = resource_target_specs.get('target_region', '') or source_region
    if resource_type == 'EBS':
        output_record.update(
            {
                'search_volume_id': backup['volume_id'],
                'target_region': region,
            }
        )
        output_record.update(get_target_specs_ebs(resource_target_specs, backup_record))
    elif resource_type == 'EC2':
        output_record.update(
            {
                'search_instance_id': backup['instance_id'],
                'target_region': region,
            }
        )
        output_record.update(
            get_target_specs_ec2(resource_target_specs, backup_record, is_diff_account)
        )
    elif resource_type == 'DynamoDB':
        changed_name = resource_target_specs.get('change_set_name', None)
        if not is_diff_account:
            changed_name = changed_name or MUST_FILLED_INPUT
        output_record.update(
            {
                'search_table_id': backup_record['source_table_id'],
                'target_region': region,
                'change_set_name': changed_name,
            }
        )
    elif resource_type == 'RDS':
        output_record.update(
            {
                'search_resource_id': backup['resource_id'],
                'target_region': region,
            }
        )
        output_record.update(
            get_target_specs_rds(resource_target_specs, backup_record, is_diff_account)
        )
    elif resource_type == 'ProtectionGroup':
        output_record.update(resource_target_specs)
        if not target_specs.get('target_bucket', None):
            output_record['target_bucket'] = MUST_FILLED_INPUT
        output_record.update(
            {'search_pg_name': backup['pg_name'], 'search_bucket_names': backup['pg_bucket_names']}
        )
    else:
        return {}
    return output_record


def get_target_specs_ebs(specs: dict[str, Any], record: dict[str, Any]) -> dict:
    """Get or inherit the detailed target specs for EBS asset."""
    az = specs.get('target_az', None) or record.get('source_az', None)
    volume_type = specs.get('target_volume_type', None) or record.get('source_volume_type', None)
    iops = specs.get('target_iops', 0) or record.get('source_iops', 0)
    kms = specs.get('target_kms_key_native_id', None) or record.get('source_kms', None)
    # Check the correctness of volume_type and iops
    if iops not in [0, None] and volume_type not in ['gp3', 'io1', 'io2']:
        raise ValueError
    return {
        'target_az': az,
        'target_volume_type': volume_type,
        'target_iops': iops,
        'target_kms_key_native_id': kms,
    }


def get_target_specs_ec2(
    specs: dict[str, Any], record: dict[str, Any], is_diff_account: bool
) -> dict:
    """Get or inherit the detailed target specs for EC2 asset."""
    az = specs.get('target_az', None) or record.get('source_az', None)
    vpc_id = specs.get('target_vpc_native_id', None)
    subnet_id = specs.get('target_subnet_native_id', None)
    sg_id = specs.get('target_security_group_native_id', None)
    if is_diff_account:
        vpc_id = vpc_id or MUST_FILLED_INPUT
        subnet_id = subnet_id or MUST_FILLED_INPUT
        sg_id = sg_id or MUST_FILLED_INPUT
    else:
        vpc_id = vpc_id or record.get('source_vpc_id', None)
        source_subnet = record['source_network_interface_list'][0]['subnet_native_id']
        subnet_id = subnet_id or source_subnet
        sg_id = sg_id or record['source_security_group_native_ids']
    key_pair = specs.get('target_key_pair_name', None) or record['source_key_pair_name']
    iam_name = (
        specs.get('target_iam_instance_profile_name', None)
        or record['source_iam_instance_profile_name']
    )
    kms = specs.get('target_kms_key_native_id', None) or record['source_kms']
    return {
        'target_az': az,
        'target_vpc_native_id': vpc_id,
        'target_subnet_native_id': subnet_id,
        'target_kms_key_native_id': kms,
        'target_iam_instance_profile_name': iam_name,
        'target_key_pair_name': key_pair,
        'target_security_group_native_ids': sg_id,
    }


def get_target_specs_rds(
    specs: dict[str, Any], record: dict[str, Any], is_diff_account: bool
) -> dict:
    """Get or inherit the detailed target specs for RDS asset."""
    subnet_group = specs.get('target_subnet_group_name', None)
    kms = specs.get('target_kms_key_native_id', None)
    sg_id = specs.get('target_security_group_native_id', None)
    target_name = specs.get('target_rds_name')
    if is_diff_account:
        subnet_group = subnet_group or MUST_FILLED_INPUT
        kms = kms or MUST_FILLED_INPUT
        sg_id = sg_id or MUST_FILLED_INPUT
    else:
        subnet_group = subnet_group or record['source_subnet_group_name']
        kms = kms or record['source_kms']
        sg_id = sg_id or record['source_security_group_native_ids']
        target_name = target_name or MUST_FILLED_INPUT
    return {
        'target_subnet_group_name': subnet_group,
        'target_rds_name': target_name,
        'target_kms_key_native_id': kms,
        'target_security_group_native_ids': sg_id,
    }
