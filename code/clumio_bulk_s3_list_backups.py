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

"""Lambda function to bulk list S3 backups."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911 PLR0912 PLR0915
    """Handle the lambda function to bulk list S3 backups."""
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account: str | None = events.get('source_account', None)
    source_region: str | None = events.get('source_region', None)
    target_specs: dict = events.get('target_specs', {})
    target: dict = events.get('target', {})
    search_direction: str | None = target.get('search_direction', None)
    start_search_day_offset_input: int = target.get('start_search_day_offset', 0)
    end_search_day_offset_input: int = target.get('end_search_day_offset', 0)
    # Filter passed to the list state machine.
    source_asset_types: dict | None = events.get('source_asset_types', None)
    # Filter passed to the restore state machine.
    search_bucket_names: list | None = events.get('search_bucket_names', None)
    # Filter by protection group name.
    search_name: str | None = events.get('search_pg_name', None)
    if not search_name:
        return {'status': 207, 'records': [], 'target': target, 'msg': 'empty pg name'}

    # Get search_object_filters from input.
    if target_specs and 'ProtectionGroup' in target_specs:
        # Get filter from input to the list state machine.
        object_filters = target_specs['ProtectionGroup'].get('search_object_filters', {})
    else:
        # Get filter from input to the restore state machine.
        object_filters = target.get('search_object_filters', {})

    # Ensure required filter latest_version_only is specified.
    if 'latest_version_only' not in object_filters:
        object_filters['latest_version_only'] = True

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not clumio_token:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        clumio_token = msg

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(
        api_token=clumio_token, hostname=base_url, raw_response=True
    )
    client = clumioapi_client.ClumioAPIClient(config)

    # Get bucket names filter.
    s3_bucket_names: list = []
    if source_asset_types and 'protection_groups' in source_asset_types['ProtectionGroup']:
        for protection_group in source_asset_types['ProtectionGroup']['protection_groups']:
            if protection_group['name'] == search_name:
                s3_bucket_names = protection_group['bucket_names']
                break
    elif search_bucket_names:
        s3_bucket_names = search_bucket_names
    logger.info('Filter PG %s by bucket names: %s', search_name, s3_bucket_names)

    try:
        # List protection group based on the name.
        api_filter = {'name': {'$eq': search_name}}
        logger.info('List protection groups with filter %s...', api_filter)
        pg_list = common.get_total_list(
            function=client.protection_groups_v1.list_protection_groups,
            api_filter=json.dumps(api_filter),
        )
        if not pg_list:
            return {'status': 207, 'records': [], 'target': target, 'msg': 'empty pg list'}
        pg_id = pg_list[0].p_id
        logger.info('Found protection group %s.', search_name)

        # List S3 assets based on the bucket names and pg name.
        api_filter = {'protection_group_id': {'$eq': pg_id}}
        env_resp, env_id = common.get_environment_id(client, source_account, source_region)
        if source_region and env_resp == common.STATUS_OK:
            # If region filter is provided, filter per region.
            api_filter['environment_id'] = {'$eq': env_id}
        logger.info('List buckets with filter %s...', api_filter)
        pg_assets = common.get_total_list(
            function=client.protection_groups_s3_assets_v1.list_protection_group_s3_assets,
            api_filter=json.dumps(api_filter),
        )
        logger.info('Found %s buckets in the protection group.', len(pg_assets))
        if not pg_assets:
            return {
                'status': 207,
                'records': [],
                'target': target,
                'msg': 'empty set of pg s3 assets',
            }
        if not s3_bucket_names:
            # All buckets in the protection group will be restored.
            asset_ids = [item.p_id for item in pg_assets]
            s3_bucket_names = [item.bucket_name for item in pg_assets]
        else:
            # Remove any buckets from the filter that do not exist in the protection group.
            all_bucket_names = [item.bucket_name for item in pg_assets]
            bucket_names = s3_bucket_names.copy()
            for bucket_name in bucket_names:
                if bucket_name not in all_bucket_names:
                    logger.warning('Bucket %s does not exist in the protection group.', bucket_name)
                    s3_bucket_names.remove(bucket_name)
            # Only buckets matching filter will be restored.
            asset_ids = [item.p_id for item in pg_assets if item.bucket_name in s3_bucket_names]
            logger.info('Found %s buckets matching the filter.', len(asset_ids))
            if not asset_ids:
                # All buckets filtered out so nothing to restore in this protection group.
                return {
                    'status': 207,
                    'records': [],
                    'target': target,
                    'msg': 'no buckets match filter',
                }

        # List pg backups based on the time filter and pg id filter.
        sort, api_filter = common.get_sort_and_ts_filter(
            search_direction, start_search_day_offset_input, end_search_day_offset_input
        )
        api_filter['protection_group_id'] = {'$eq': pg_id}
        logger.info('List backups for protection group %s...', search_name)
        raw_backup_records = common.get_total_list(
            function=client.backup_protection_groups_v1.list_backup_protection_groups,
            api_filter=json.dumps(api_filter),
            sort=sort,
        )
        logger.info(
            'Found %s backups for protection group %s.', len(raw_backup_records), search_name
        )
        if not raw_backup_records:
            return {'status': 207, 'records': [], 'target': target, 'msg': 'empty set'}

        records = []
        for item in raw_backup_records:
            records.append(
                {
                    'backup_id': item.p_id,
                    'pg_name': search_name,
                    'pg_asset_ids': asset_ids,
                    'pg_bucket_names': s3_bucket_names,
                    'object_filters': object_filters,
                }
            )
        return {'status': 200, 'records': records[:1], 'target': target, 'msg': 'completed'}
    except clumio_exception.ClumioException as e:
        # This exception could come from multiple API calls above.
        logger.error('Hit exception trying to retrieve protection group backups: %s', e)
        return {'status': 401, 'msg': f'List pg assets error - {e}'}
