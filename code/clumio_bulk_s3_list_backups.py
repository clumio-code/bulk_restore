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

logger = logging.getLogger()


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911 PLR0912 PLR0915
    """Handle the lambda function to bulk list S3 backups."""
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account: str | None = events.get('source_account', None)
    source_region: str | None = events.get('source_region', None)
    target: dict = events.get('target', {})
    search_direction: str | None = target.get('search_direction', None)
    start_search_day_offset_input: int = target.get('start_search_day_offset', 0)
    end_search_day_offset_input: int = target.get('end_search_day_offset', 0)
    object_filters: dict = events.get('search_object_filters', {})
    source_asset_types: dict | None = events.get('source_asset_types', None)

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

    # Get protection group name to bucket name mappings
    pg_to_buckets: dict[str, list[str]] = {}
    if source_asset_types and 'ProtectionGroup' in source_asset_types:
        for protection_group in source_asset_types['ProtectionGroup']['protection_groups']:
            pg_to_buckets[protection_group['name']] = protection_group['bucket_names']
        logger.info('Bucket name filters by PG: %s', pg_to_buckets)

    try:
        # List protection group based on the name.
        search_name = events.get('search_pg_name', None)
        if not search_name:
            return {'status': 207, 'records': [], 'target': target, 'msg': 'empty pg name'}
        api_filter = {'name': {'$eq': search_name}}
        logger.info('List protection groups with filter %s...', api_filter)
        pg_list = common.get_total_list(
            function=client.protection_groups_v1.list_protection_groups,
            api_filter=json.dumps(api_filter),
        )
        if not pg_list:
            return {'status': 207, 'records': [], 'target': target, 'msg': 'empty set of pg list'}
        pg_id = pg_list[0].p_id
        logger.info('Found protection group %s.', search_name)

        # List S3 assets based on the bucket names and pg name.
        s3_bucket_names = None
        if pg_to_buckets and search_name in pg_to_buckets:
            s3_bucket_names = pg_to_buckets[str(search_name)]

        api_filter = {'protection_group_id': {'$eq': pg_id}}
        env_resp, env_id = common.get_environment_id(client, source_account, source_region)
        if source_region and env_resp == common.STATUS_OK:
            # If region filter is provided, filter per region.
            api_filter['environment_id'] = {'$eq': env_id}
        logger.info('List S3 assets with filter %s...', api_filter)
        pg_assets = common.get_total_list(
            function=client.protection_groups_s3_assets_v1.list_protection_group_s3_assets,
            api_filter=json.dumps(api_filter),
        )
        logger.info('Found %s S3 assets.', len(pg_assets))
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
            # Only buckets matching filter will be restored.
            asset_ids = [item.p_id for item in pg_assets if item.bucket_name in s3_bucket_names]
            logger.info('Found %s S3 assets matching filter.', len(asset_ids))
            # TODO: Assert all buckets were found? Allow partial match? Allow no match?

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
