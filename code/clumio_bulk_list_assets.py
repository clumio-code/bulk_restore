# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to retrieve the assets of the given environment and resource types."""

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


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:  # noqa: PLR0911, PLR0912 PLR0915
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
    if not clumio_token:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        clumio_token = msg

    # Check if source_asset_types was provided.
    asset_tags: dict = {}
    if source_asset_types and resource_type in source_asset_types:
        asset_tags = source_asset_types[resource_type]['asset_tags']

    # Verify asset_meta_status was provided.
    if not asset_meta_status:
        return {'status': 422, 'msg': 'asset_meta_status is a required input.'}
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
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(
        api_token=clumio_token, hostname=base_url, raw_response=True
    )
    client = clumioapi_client.ClumioAPIClient(config)

    # Get the correct asset listing function based on the resource type.
    list_filter: dict = {'environment_id': {'$eq': env_id}}
    if protection_status:
        list_filter['protection_status'] = {'$in': protection_status}
    if backup_status:
        list_filter['backup_status'] = {'$in': backup_status}
    if is_deleted:
        list_filter['is_deleted'] = {'$in': is_deleted}
    logger.info('Listing assets with filter: %s', json.dumps(list_filter, indent=2))
    if resource_type == 'EBS':
        list_function = client.aws_ebs_volumes_v1.list_aws_ebs_volumes
    elif resource_type == 'EC2':
        list_function = client.aws_ec2_instances_v1.list_aws_ec2_instances
    elif resource_type == 'RDS':
        list_function = client.aws_rds_resources_v1.list_aws_rds_resources
    elif resource_type == 'DynamoDB':
        list_function = client.aws_dynamodb_tables_v1.list_aws_dynamodb_tables
    elif resource_type == 'ProtectionGroup':
        _, org_unit_response = client.organizational_units_v2.list_organizational_units()
        if org_unit_response:
            ou_id = org_unit_response.embedded.items[0].p_id
            list_filter = {'organizational_unit_id': {'$in': [ou_id]}}
        list_function = client.protection_groups_v1.list_protection_groups
    else:
        return {'status': 401, 'msg': f'Resource type {resource_type} is not supported.'}

    # Add tags filter.
    if asset_tags:
        logger.info('Search for asset tags %s...', asset_tags)
        tag_ids = []
        for tag_key, tag_value in asset_tags.items():
            # Get the tag from Clumio inventory matching the specified tag value.
            tag_filter = {'value': {'$contains': tag_value}}
            _, tags = client.aws_environment_tags_v1.list_aws_environment_tags(
                env_id, filter=json.dumps(tag_filter)
            )
            # Get the Clumio tag ID.
            clumio_tag_ids = []
            for tag in tags.embedded.items:
                # Ensure exact key/value match as filter only allows $contains.
                if tag.value == tag_value and tag.key == tag_key:
                    logger.info('Found tag {%s:%s} with ID: %s', tag_key, tag_value, tag.p_id)
                    clumio_tag_ids.append(tag.p_id)
            # Bail out if the tag was not found.
            if not clumio_tag_ids:
                logger.error('Tag not found: {%s:%s}', tag_key, tag_value)
                return {'status': 404, 'msg': f'Tag not found: {tag_key}:{tag_value}'}
            tag_ids += clumio_tag_ids
        list_filter['tags.id'] = {'$all': tag_ids}

    try:
        logger.info('Filter: %s', list_filter)
        logger.info('List all %s assets...', resource_type)
        assets_list = common.get_total_list(
            function=list_function, api_filter=json.dumps(list_filter)
        )
    except clumio_exception.ClumioException as e:
        logger.error('List %s assets failed with exception: %s', resource_type, e)
        return {'status': 401, 'msg': f'List {resource_type} assets error - {e}'}

    # Log the total number assets found.
    logger.info('Found %s %s assets.', len(assets_list), resource_type)

    # Return the assets list.
    if resource_type == 'ProtectionGroup':
        asset_ids = [asset.name for asset in assets_list]
    else:
        asset_ids = [asset.p_id for asset in assets_list]
    return {'status': 200, 'region': region_name, 'asset_ids': asset_ids}
