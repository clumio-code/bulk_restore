# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to retrieve the assets of the given environment and resource types."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi.exceptions import clumio_exception

MAX_ASSETS_LIMIT = 100  # Maximum number of assets to return in the response.

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911 PLR0912 PLR0915
    """Handle the lambda functions to list of the assets given the env and resource type."""
    # Retrieve and validate the inputs.
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    resource_type: str = events.get('resource_type', '')
    region: dict = events.get('region', {})
    region_name: str = region.get('region', '')
    env_id: str = region.get('environment_id', '')
    source_asset_types: dict | None = events.get('source_asset_types', None)
    asset_meta_status: dict | None = events.get('asset_meta_status', None)

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Verify source_asset_types was provided.
    if not source_asset_types:
        return {'status': 422, 'msg': 'source_asset_types is required input.'}

    # Check if any filters for the source_asset_types were provided.
    asset_tags: dict = {}
    protection_groups: list[dict] = []
    if resource_type in source_asset_types:
        if 'protection_groups' in source_asset_types[resource_type]:
            # Check for filter, else all S3 assets with backups are restored.
            protection_groups = source_asset_types[resource_type]['protection_groups']
        elif 'asset_tags' in source_asset_types[resource_type]:
            # Check for filter, else all assets with backups are restored.
            asset_tags = source_asset_types[resource_type]['asset_tags']

    # Verify asset_meta_status was provided.
    if not asset_meta_status:
        return {'status': 422, 'msg': 'asset_meta_status is required input.'}
    else:
        protection_status = asset_meta_status.get('protection_status_in', None)
        backup_status = asset_meta_status.get('backup_status_in', None)
        is_deleted = asset_meta_status.get('deleted_status_in', None)

    # Verify at least one asset_meta_status filter was provided.
    if any(value is not None for value in (protection_status, backup_status, is_deleted)):
        pass
    else:
        return {
            'status': 422,
            'msg': 'Some asset status filtering is needed.\
                protection_status_in, backup_status_in or deleted_status_in',
        }

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token)

    # Add specified asset_meta_status filters.
    list_filter: dict = {'environment_id': {'$eq': env_id}}
    if protection_status:
        list_filter['protection_status'] = {'$in': protection_status}
    if backup_status:
        list_filter['backup_status'] = {'$in': backup_status}
    if is_deleted:
        list_filter['is_deleted'] = {'$in': is_deleted}
    logger.info('Listing assets with filter: %s', list_filter)

    # Get the asset listing function based on the resource type.
    if resource_type == 'EBS':
        list_function = client.aws_ebs_volumes_v1.list_aws_ebs_volumes
    elif resource_type == 'EC2':
        list_function = client.aws_ec2_instances_v1.list_aws_ec2_instances
    elif resource_type == 'RDS':
        list_function = client.aws_rds_resources_v1.list_aws_rds_resources
    elif resource_type == 'DynamoDB':
        list_function = client.aws_dynamodb_tables_v1.list_aws_dynamodb_tables
    elif resource_type == 'ProtectionGroup':
        try:
            org_unit_response = client.organizational_units_v2.list_organizational_units()
        except clumio_exception.ClumioException as e:
            logger.error('List organizational units failed with exception: %s', e)
            return {'status': 500, 'msg': f'List OUs error - {e}'}
        if org_unit_response and org_unit_response.Embedded and org_unit_response.Embedded.Items:
            ou_id = org_unit_response.Embedded.Items[0].Id
            list_filter = {'organizational_unit_id': {'$in': [ou_id]}}
        list_function = client.protection_groups_v1.list_protection_groups
    else:
        return {'status': 401, 'msg': f'Resource type {resource_type} is not supported.'}

    # Add specified source_asset_types filters.
    if asset_tags:
        # Add tags.id filter.
        logger.info('Search for asset tags %s...', asset_tags)
        tag_ids = []
        for tag_key, tag_value in asset_tags.items():
            # Get the tag from Clumio inventory matching the specified tag value.
            try:
                tags = client.aws_environment_tags_v1.list_aws_environment_tags(
                    env_id,
                    filter=common.make_filter({'value': {'$contains': tag_value}}),
                    limit=100,
                )
            except clumio_exception.ClumioException as e:
                logger.error('List environment tags failed: %s', e)
                return {'status': 500, 'msg': f'List tags error - {e}'}
            # Get the Clumio tag ID.
            clumio_tag_ids = []
            tag_items = tags.Embedded.Items if tags.Embedded else None
            if tag_items:
                for tag in tag_items:
                    # Ensure exact key/value match as filter only allows $contains.
                    if tag.Value == tag_value and tag.Key == tag_key:
                        logger.info('Found tag {%s:%s} with ID: %s', tag_key, tag_value, tag.Id)
                        clumio_tag_ids.append(tag.Id)
            # Bail out if the tag was not found.
            if not clumio_tag_ids:
                # May need to increase the limit in the list_aws_environment_tags request.
                logger.error('Tag not found: {%s:%s}', tag_key, tag_value)
                return {
                    'status': 200,
                    'region': region_name,
                    'msg': f'Tag not found: {tag_key}:{tag_value}',
                    'asset_ids': [],
                }
            tag_ids += clumio_tag_ids
        list_filter['tags.id'] = {'$all': tag_ids}
        list_filters = [list_filter]
    elif protection_groups:
        # Add protection group 'name' filter.
        list_filters = []
        for protection_group in protection_groups:
            # Need to make a separate list call for each protection group.
            pg_filter = list_filter.copy()
            pg_filter['name'] = {'$eq': protection_group['name']}
            list_filters.append(pg_filter)
    else:
        list_filters = [list_filter]

    # Run list function for each list filter.
    total_assets_list = []
    for count, api_filter in enumerate(list_filters, 1):
        try:
            logger.info('Filter %s of %s: %s', count, len(list_filters), api_filter)
            logger.info('List all %s assets...', resource_type)
            # Valid values for lookback_days is 1-60.
            assets_list = common.get_total_list(
                function=list_function, api_filter=api_filter, lookback_days=60
            )
            total_assets_list += assets_list
        except clumio_exception.ClumioException as e:
            logger.error('List %s assets failed with exception: %s', resource_type, e)
            return {'status': 500, 'msg': f'List {resource_type} assets error - {e}'}

    # Log the total number assets found.
    logger.info('Found %s %s assets.', len(total_assets_list), resource_type)
    if len(total_assets_list) == 0:
        return {
            'status': 200,
            'region': region_name,
            'msg': f'No {resource_type} assets found.',
            'asset_ids': [],
        }
    # For any resource type, if the total number of assets exceeds the maximum limit,
    # State/Task fails with error size exceeding the maximum number of bytes service limit.
    if len(total_assets_list) > MAX_ASSETS_LIMIT:
        return {
            'status': 404,
            'msg': f'Greater than {resource_type} assets found. Apply filters. Too many assets.',
        }
    # Return the assets list.
    if resource_type == 'ProtectionGroup':
        asset_ids = [asset.Name for asset in total_assets_list]
    else:
        asset_ids = [asset.Id for asset in total_assets_list]
    return {'status': 200, 'region': region_name, 'asset_ids': asset_ids}
