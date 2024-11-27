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

"""Lambda function to retrieve the RDS backup list."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from clumioapi.models.rds_database_backup import RdsDatabaseBackup
    from common import EventsTypeDef


def backup_record_obj_to_dict(backup: RdsDatabaseBackup) -> dict:
    """Convert backup record object to dictionary."""
    instances_dict = []
    instance_class = ''
    publicly_available = True
    for instance in backup.instances:
        instance_dict = instance.__dict__
        instance_class = instance_dict.pop('p_class')
        instance_dict['class'] = instance_class
        publicly_available = publicly_available and instance_dict['is_publicly_accessible']
        instances_dict.append(instance_dict)
    return {
        'resource_id': backup.database_native_id,
        'backup_record': {
            'source_backup_id': backup.p_id,
            'source_resource_id': backup.database_native_id,
            'source_resource_tags': [tag.__dict__ for tag in backup.tags],
            'source_encrypted_flag': backup.kms_key_native_id == '',
            'source_instances': instances_dict,
            'source_instance_class': instance_class,
            'source_is_publicly_accessible': publicly_available,
            'source_subnet_group_name': backup.subnet_group_name,
            'source_kms': backup.kms_key_native_id,
            'source_expire_time': backup.expiration_timestamp,
        },
    }


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to retrieve the RDS backup list."""
    bear = events.get('bear', None)
    base_url = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account = events.get('source_account', None)
    source_region = events.get('source_region', None)
    search_tag_key = events.get('search_tag_key', None)
    search_tag_value = events.get('search_tag_value', None)
    target = events.get('target', {})
    search_direction = target.get('search_direction', None)
    start_search_day_offset_input = target.get('start_search_day_offset', 0)
    end_search_day_offset_input = target.get('end_search_day_offset', 10)

    # Validate inputs
    try:
        start_search_day_offset = int(start_search_day_offset_input)
        end_search_day_offset = int(end_search_day_offset_input)
    except ValueError as e:
        error = f'invalid start and/or end day offset: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not bear:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        bear = msg

    # Initiate the Clumio API client.
    if 'https' in base_url:
        base_url = base_url.split('/')[2]
    config = configuration.Configuration(api_token=bear, hostname=base_url)
    client = clumioapi_client.ClumioAPIClient(config)

    # Retrieve the list of backup records.
    sort, ts_filter = common.get_sort_and_ts_filter(
        search_direction, start_search_day_offset, end_search_day_offset
    )
    raw_backup_records = common.get_total_list(
        function=client.backup_aws_rds_resources_v1.list_backup_aws_rds_resources,
        api_filter=json.dumps(ts_filter),
        sort=sort,
    )

    # Filter the result based on the source_account and source region.
    backup_records = []
    for backup in raw_backup_records:
        if backup.account_native_id == source_account and backup.aws_region == source_region:
            backup_record = backup_record_obj_to_dict(backup)
            backup_records.append(backup_record)

    # Filter the result based on the tags.
    backup_records = common.filter_backup_records_by_tags(
        backup_records, search_tag_key, search_tag_value, 'source_resource_tags'
    )

    if not backup_records:
        return {'status': 207, 'records': [], 'target': target, 'msg': 'empty set'}
    return {'status': 200, 'records': backup_records[:1], 'target': target, 'msg': 'completed'}
