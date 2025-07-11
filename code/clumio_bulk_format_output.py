# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to format the output of the listing layer."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import common

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to format the output of the listing layer."""
    logger.info('Format output...')
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
            if resource_type not in common.RESOURCE_TYPES:
                continue
            for backup_record in backup_list:
                logger.info('%s backup: %s', resource_type, backup_record)
                record = format_record_per_resource_type(
                    backup_record, resource_type, source_region, target_specs, is_diff_account
                )
                record['source_account'] = source_account
                record['target_account'] = target_account
                restore_group.append(record)
    logger.info('Format output complete.')

    # Return the JSON used as input for the restore state machine.
    note = (
        'The DefaultInput section is optional. Its values are used to populate '
        'corresponding fields in the RestoreGroup section, but only if those '
        'fields are empty. If a field in RestoreGroup is not empty, its value '
        'will override the one from DefaultInput.'
    )
    return {
        'status': 200,
        'RestoreGroup': restore_group,
        'Note': note,
        'DefaultInput': {
            'RDS': {
                'target_subnet_group_name': '',
                'target_rds_name': '',
                'target_kms_key_native_id': '',
                'target_security_group_native_ids': [],
            },
            'EC2': {
                'target_vpc_native_id': '',
                'target_subnet_native_id': '',
                'target_security_group_native_ids': [],
            },
            'ProtectionGroup': {'target_bucket': ''},
        },
    }


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
        updated_tags = backup_record.get('source_volume_tags', [])
        append_tags = resource_target_specs.get('append_tags', {})
        if append_tags:
            updated_tags = common.append_tags_to_source_tags(updated_tags, append_tags)
        output_record.update(
            {
                'search_volume_id': backup['volume_id'],
                'target_region': region,
                'target_volume_tags': updated_tags,
            }
        )
        output_record.update(get_target_specs_ebs(resource_target_specs, backup_record))
    elif resource_type == 'EC2':
        updated_tags = backup_record.get('source_instance_tags', [])
        append_tags = resource_target_specs.get('append_tags', {})
        append_tags_ebs = common.format_append_tags(append_tags)
        if append_tags:
            updated_tags = common.append_tags_to_source_tags(updated_tags, append_tags)
        output_record.update(
            {
                'search_instance_id': backup['instance_id'],
                'target_region': region,
                'target_instance_tags': updated_tags,
                'target_volume_append_tags': append_tags_ebs,
            }
        )
        output_record.update(
            get_target_specs_ec2(resource_target_specs, backup_record, is_diff_account)
        )
    elif resource_type == 'DynamoDB':
        changed_name = resource_target_specs.get('change_set_name', None)
        changed_name = changed_name or common.generate_random_string(4)
        updated_tags = backup_record.get('source_ddn_tags', [])
        append_tags = resource_target_specs.get('append_tags', {})
        if append_tags:
            updated_tags = common.append_tags_to_source_tags(updated_tags, append_tags)
        output_record.update(
            {
                'search_table_id': backup_record['source_table_id'],
                'target_region': region,
                'change_set_name': changed_name,
                'source_ddn_tags': updated_tags,
            }
        )
    elif resource_type == 'RDS':
        updated_tags = backup_record.get('source_resource_tags', [])
        append_tags = resource_target_specs.get('append_tags', {})
        if append_tags:
            updated_tags = common.append_tags_to_source_tags(updated_tags, append_tags)
        output_record.update(
            {
                'search_resource_id': backup['resource_id'],
                'target_region': region,
                'target_resource_tags': updated_tags,
            }
        )
        output_record.update(
            get_target_specs_rds(resource_target_specs, backup_record, is_diff_account)
        )
    elif resource_type == 'ProtectionGroup':
        output_record.update(resource_target_specs)
        output_record.update(
            {
                'search_pg_name': backup['pg_name'],
                'search_bucket_names': backup['pg_bucket_names'],
                'search_object_filters': backup['object_filters'],
            }
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
    sg_ids = specs.get('target_security_group_native_ids', None)
    should_power_on = specs.get('should_power_on', False)
    target_ami_native_id = specs.get('target_ami_native_id', None)
    if not is_diff_account:
        # For same account, get values from backup record if not provided in input file.
        vpc_id = vpc_id or record.get('source_vpc_id', None)
        source_subnet = record['source_network_interface_list'][0]['subnet_native_id']
        subnet_id = subnet_id or source_subnet
        sg_ids = sg_ids or record['source_security_group_native_ids']
    key_pair = specs.get('target_key_pair_name', None) or record['source_key_pair_name']
    iam_name = specs.get('target_iam_instance_profile_name', None) or record.get(
        'source_iam_instance_profile_name', None
    )
    kms = specs.get('target_kms_key_native_id', None) or record['source_kms']
    return {
        'target_ami_native_id': target_ami_native_id,
        'target_az': az,
        'target_vpc_native_id': vpc_id,
        'target_subnet_native_id': subnet_id,
        'target_kms_key_native_id': kms,
        'target_iam_instance_profile_name': iam_name,
        'target_key_pair_name': key_pair,
        'target_security_group_native_ids': sg_ids,
        'should_power_on': should_power_on,
    }


def get_target_specs_rds(
    specs: dict[str, Any], record: dict[str, Any], is_diff_account: bool
) -> dict:
    """Get or inherit the detailed target specs for RDS asset."""
    subnet_group = specs.get('target_subnet_group_name', None)
    kms = specs.get('target_kms_key_native_id', None)
    sg_ids = specs.get('target_security_group_native_ids', None)
    target_name = specs.get('target_rds_name', None)
    if not is_diff_account:
        # For same account, get values from backup record if not provided in input file.
        subnet_group = subnet_group or record['source_subnet_group_name']
        kms = kms or record['source_kms']
        sg_ids = sg_ids or record['source_security_group_native_ids']
        if not target_name:
            # Append random string to source database name.
            source_name = record['source_resource_id']
            target_name = f'{source_name}-{common.generate_random_string(4).lower()}'
    return {
        'target_subnet_group_name': subnet_group,
        'target_rds_name': target_name,
        'target_kms_key_native_id': kms,
        'target_security_group_native_ids': sg_ids,
    }
