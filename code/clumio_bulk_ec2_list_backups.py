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

"""Lambda function to retrieve the EC2 backup list."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from clumioapi.models.ec2_backup import EC2Backup
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def backup_record_obj_to_dict(backup: EC2Backup) -> dict:
    """Convert backup record object to dictionary."""
    ebs_mappings = []
    kms_key_native_id = ''
    for ebs_vol in backup.attached_backup_ebs_volumes:
        ebs_mapping = ebs_vol.__dict__
        ebs_mapping['id'] = ebs_mapping.pop('p_id')
        ebs_mapping['type'] = ebs_mapping.pop('p_type')
        if ebs_mapping['tags']:
            # Convert tags to a list of dictionaries if they exist.
            ebs_mapping['tags'] = [aws_tag.__dict__ for aws_tag in ebs_mapping['tags']]
        else:
            # If no tags, set to an empty list.
            ebs_mapping['tags'] = []
        ebs_mappings.append(ebs_mapping)
        kms_key_native_id = ebs_vol.kms_key_native_id
    security_group_native_ids = []
    for eni in backup.network_interfaces:
        security_group_native_ids.extend(eni.security_group_native_ids)
    return {
        'instance_id': backup.instance_id,
        'backup_record': {
            'source_backup_id': backup.p_id,
            'source_ami_id': backup.ami.ami_native_id,
            # TODO: Uncomment when Clumio supports instance profile.
            # 'source_iam_instance_profile_name': backup.iam_instance_profile,
            'source_key_pair_name': backup.key_pair_name,
            'source_network_interface_list': [ni.__dict__ for ni in backup.network_interfaces],
            'source_ebs_storage_list': ebs_mappings,
            'source_instance_tags': [tag.__dict__ for tag in backup.tags] if backup.tags else None,
            'source_vpc_id': backup.vpc_native_id,
            'source_az': backup.aws_az,
            'source_expire_time': backup.expiration_timestamp,
            'source_kms': kms_key_native_id,
            'source_security_group_native_ids': security_group_native_ids,
        },
    }


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to retrieve the EC2 backup list."""
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account: str | None = events.get('source_account', None)
    source_region: str | None = events.get('source_region', None)
    search_tag_key: str | None = events.get('search_tag_key', None)
    search_tag_value: str | None = events.get('search_tag_value', None)
    search_instance_id: str | None = events.get('search_instance_id', None)
    target_specs: dict = events.get('target_specs', {})
    target: dict = events.get('target', {})
    if not search_instance_id:  # For restore SM search_instance_id is under target
        search_instance_id = target.get('search_instance_id', None)
    search_direction: str | None = target.get('search_direction', None)
    start_search_day_offset_input: int = target.get('start_search_day_offset', 0)
    end_search_day_offset_input: int = target.get('end_search_day_offset', 0)

    # Get append_tags from list state machine input.
    append_tags = common.get_append_tags(target_specs, 'EC2')

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Validate input.
    try:
        start_search_day_offset = int(start_search_day_offset_input)
        end_search_day_offset = int(end_search_day_offset_input)
    except (TypeError, ValueError) as e:
        error = f'invalid start/end search day offset: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token)

    # Retrieve the list of backup records.
    sort, api_filter = common.get_sort_and_ts_filter(
        search_direction, start_search_day_offset, end_search_day_offset
    )
    if search_instance_id:
        api_filter['instance_id'] = {'$eq': search_instance_id}
    try:
        logger.info('List EC2 backups with filter: %s', api_filter)
        raw_backup_records = common.get_total_list(
            function=client.backup_aws_ec2_instances_v1.list_backup_aws_ec2_instances,
            api_filter=json.dumps(api_filter),
            sort=sort,
        )
    except clumio_exception.ClumioException as e:
        logger.error('List EC2 backups failed with exception: %s', e)
        return {'status': 401, 'msg': f'List backup error - {e}'}

    # Log number of records found before filtering.
    logger.info('Found %s backup records before applying filters.', len(raw_backup_records))

    # Filter the result based on the source_account and source region.
    logger.info('Filter records by account/region...')
    backup_records = []
    for backup in raw_backup_records:
        if backup.account_native_id == source_account and backup.aws_region == source_region:
            backup_record = backup_record_obj_to_dict(backup)
            backup_records.append(backup_record)

    # Filter the result based on the tags.
    logger.info('Filter records by tags...')
    backup_records = common.filter_backup_records_by_tags(
        backup_records, search_tag_key, search_tag_value, 'source_instance_tags'
    )
    logger.info('Found %s backup records after applying filters.', len(backup_records))

    if not backup_records:
        logger.info('No EC2 backup records found.')
        return {'status': 207, 'records': [], 'target': target, 'msg': 'empty set'}

    return {'status': 200, 'records': backup_records[:1], 'target': target, 'msg': 'completed'}
