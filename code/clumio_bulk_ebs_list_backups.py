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

"""Lambda function to list EBS backups."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0912 PLR0915
    """Handle the lambda function to list EBS backups."""
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account: str | None = events.get('source_account', None)
    source_region: str | None = events.get('source_region', None)
    search_tag_key: str | None = events.get('search_tag_key', None)
    search_tag_value: str | None = events.get('search_tag_value', None)
    search_volume_id: str | None = events.get('search_volume_id', None)
    target_specs: dict = events.get('target_specs', {})
    target: dict = events.get('target', {})
    search_direction: str | None = target.get('search_direction', None)
    start_search_day_offset_input: int = target.get('start_search_day_offset', 1)
    end_search_day_offset_input: int = target.get('end_search_day_offset', 0)

    # Get append_tags from list state machine input.
    append_tags: dict[str, Any] | None = None
    if target_specs and 'EBS' in target_specs:
        append_tags = target_specs['EBS'].get('append_tags', None)

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Validate inputs
    try:
        start_search_day_offset = int(start_search_day_offset_input)
        end_search_day_offset = int(end_search_day_offset_input)
    except (TypeError, ValueError) as e:
        error = f'invalid input: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token)

    # Get timestamp filters.
    sort, api_filter = common.get_sort_and_ts_filter(
        search_direction, start_search_day_offset, end_search_day_offset
    )
    if search_volume_id:
        api_filter['volume_id'] = {'$eq': search_volume_id}
    try:
        logger.info('List EBS backups...')
        raw_backup_records = common.get_total_list(
            function=client.backup_aws_ebs_volumes_v2.list_backup_aws_ebs_volumes,
            api_filter=json.dumps(api_filter),
            sort=sort,
        )
    except clumio_exception.ClumioException as e:
        logger.error('List EBS backups failed with exception: %s', e)
        return {'status': 401, 'msg': f'List backup error - {e}'}

    # Log number of records found before filtering.
    logger.info('Found %s backup records before applying filters.', len(raw_backup_records))

    # Filter the result based on the source_account and source region.
    logger.info('Filter records by account/region...')
    backup_records = []
    for backup in raw_backup_records:
        if backup.account_native_id == source_account and backup.aws_region == source_region:
            backup_record = {
                'volume_id': backup.volume_native_id,
                'backup_record': {
                    'source_backup_id': backup.p_id,
                    'source_volume_id': backup.volume_native_id,
                    'source_volume_tags': [tag.__dict__ for tag in backup.tags]
                    if backup.tags
                    else None,
                    'source_encrypted_flag': backup.is_encrypted,
                    'source_az': backup.aws_az,
                    'source_kms': backup.kms_key_native_id,
                    'source_expire_time': backup.expiration_timestamp,
                    'source_volume_type': backup.volume_type,
                    'source_iops': backup.iops,
                },
            }
            backup_records.append(backup_record)

    # Filter the result based on the tags.
    logger.info('Filter records by tags...')
    backup_records = common.filter_backup_records_by_tags(
        backup_records, search_tag_key, search_tag_value, 'source_volume_tags'
    )

    # Log number of records found after filtering.
    logger.info('Found %s backup records after applying filters.', len(backup_records))

    if not backup_records:
        logger.info('No EBS backup records found.')
        return {'status': 207, 'records': [], 'target': target, 'msg': 'empty set'}

    # Modify tags if append_tags was provided in the target_specs input.
    # This only applies to the list state machine path.
    if append_tags:
        for backup in backup_records:
            tags = backup['backup_record']['source_volume_tags']
            backup['backup_record']['source_volume_tags'] = common.append_tags_to_source_tags(
                tags, append_tags
            )

    return {'status': 200, 'records': backup_records[:1], 'target': target, 'msg': 'completed'}
