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

logger = logging.getLogger()


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda functions to list of the assets given the env and resource type."""
    # Retrieve and validate the inputs.
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    resource_type: str = events.get('resource_type', '')
    region: dict = events.get('region', {})
    region_name: str = region.get('region', '')
    env_id: str = region.get('environment_id', '')

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

    # Get the correct asset listing function based on the resource type.
    list_filter: dict = {'environment_id': {'$eq': env_id}}
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

    try:
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
