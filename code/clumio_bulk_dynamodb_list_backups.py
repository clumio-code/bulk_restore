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

"""Lambda function to bulk restore DynamoDB list backups."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from clumioapi.models.dynamo_db_table_backup_with_e_tag import DynamoDBTableBackupWithETag
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def backup_record_obj_to_dict(backup: DynamoDBTableBackupWithETag) -> dict:
    """Convert backup record object to dictionary."""
    gsi_list = []
    if backup.global_secondary_indexes:
        for gsi in backup.global_secondary_indexes:
            gsi_dict = {
                'index_name': gsi.index_name,
                'key_schema': [schema.__dict__ for schema in gsi.key_schema],
                'projection': gsi.projection.__dict__,
                'provisioned_throughput': common.to_dict_or_none(gsi.provisioned_throughput),
            }
            gsi_list.append(gsi_dict)

    lsi_list = []
    if backup.local_secondary_indexes:
        for lsi in backup.local_secondary_indexes:
            lsi_dict = {
                'index_name': lsi.index_name,
                'key_schema': [schema.__dict__ for schema in lsi.key_schema],
                'projection': lsi.projection.__dict__,
            }
            lsi_list.append(lsi_dict)

    return {
        'table_name': backup.table_name,
        'backup_record': {
            'source_backup_id': backup.p_id,
            'source_table_id': backup.table_id,
            'source_table_name': backup.table_name,
            'source_ddn_tags': [tag.__dict__ for tag in backup.tags] if backup.tags else None,
            'source_sse_specification': common.to_dict_or_none(backup.sse_specification),
            'source_provisioned_throughput': common.to_dict_or_none(backup.provisioned_throughput),
            'source_billing_mode': backup.billing_mode,
            'source_table_class': backup.table_class,
            'source_expire_time': backup.expiration_timestamp,
            'source_global_table_version': backup.global_table_version,
            'source_global_secondary_indexes': gsi_list or None,
            'source_local_secondary_indexes': lsi_list or None,
            'source_replicas': None,
        },
    }


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0912, PLR0915
    """Handle the lambda function to list DynamoDB backups."""
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account: str | None = events.get('source_account', None)
    source_region: str | None = events.get('source_region', None)
    search_tag_key: str | None = events.get('search_tag_key', None)
    search_tag_value: str | None = events.get('search_tag_value', None)
    search_table_id: str | None = events.get('search_table_id', None)
    target_specs: dict = events.get('target_specs', {})
    target: dict = events.get('target', {})
    search_direction: str | None = target.get('search_direction', None)
    start_search_day_offset_input: int = target.get('start_search_day_offset', 0)
    end_search_day_offset_input: int = target.get('end_search_day_offset', 0)

    # Get append_tags from list state machine input.
    append_tags: dict[str, Any] | None = None
    if target_specs and 'DynamoDB' in target_specs:
        append_tags = target_specs['DynamoDB'].get('append_tags', None)

    # Get values from target if called from the restore state machine.
    if not search_table_id:
        search_table_id = target.get('search_table_id', None)
    if not search_tag_key:
        search_tag_key = target.get('search_tag_key', None)
    if not search_tag_value:
        search_tag_value = target.get('search_tag_value', None)

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not clumio_token:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        clumio_token = msg

    # Validate input.
    try:
        start_search_day_offset = int(start_search_day_offset_input)
        end_search_day_offset = int(end_search_day_offset_input)
    except ValueError as e:
        error = f'invalid offset value: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(
        api_token=clumio_token, hostname=base_url, raw_response=True
    )
    client = clumioapi_client.ClumioAPIClient(config)

    # Retrieve the list of backup records.
    sort, api_filter = common.get_sort_and_ts_filter(
        search_direction, start_search_day_offset, end_search_day_offset
    )
    if search_table_id:
        api_filter['table_id'] = {'$eq': search_table_id}
    try:
        logger.info('List DynamoDB backups with filter %s...', api_filter)
        raw_backup_records = common.get_total_list(
            function=client.backup_aws_dynamodb_tables_v1.list_backup_aws_dynamodb_tables,
            api_filter=json.dumps(api_filter),
            sort=sort,
        )
    except clumio_exception.ClumioException as e:
        logger.error('List DynamoDB backups failed with exception: %s', e)
        return {'status': 401, 'msg': f'List backup error - {e}'}

    # Log number of records found before filtering.
    logger.info('Found %s backup records before applying filters.', len(raw_backup_records))

    # Filter the result based on the source_account and source region.
    logger.info('Filter records by account/region...')
    backup_records: list[dict] = []
    for backup in raw_backup_records:
        if backup.account_native_id == source_account and backup.aws_region == source_region:
            backup_record = backup_record_obj_to_dict(backup)
            backup_records.append(backup_record)

    # Filter the result based on the tags.
    logger.info('Filter records by tags...')
    backup_records = common.filter_backup_records_by_tags(
        backup_records, search_tag_key, search_tag_value, 'source_ddn_tags'
    )
    logger.info('Found %s backup records after applying filters.', len(backup_records))

    if not backup_records:
        logger.info('No DynamoDB backup records found.')
        return {'status': 207, 'records': [], 'target': target, 'msg': 'empty set'}

    # Modify tags if append_tags was provided in the target_specs input.
    # This only applies to the list state machine path.
    if append_tags:
        for backup in backup_records:
            tags = backup['backup_record']['source_ddn_tags']
            backup['backup_record']['source_ddn_tags'] = common.append_tags_to_source_tags(
                tags, append_tags
            )

    return {'status': 200, 'records': backup_records[:1], 'target': target, 'msg': 'completed'}
