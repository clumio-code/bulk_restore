# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to retrieve the EBS assets of the given environment."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda functions to list the ebs assets (volume) given the account id and region."""
    # Retrieve and validate the inputs.
    bear: str | None = events.get('bear', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    resource_type: str = events.get('resource_type', '')
    region: dict = events.get('region', {})
    region_name: str = region.get('region', '')
    env_id: str = region.get('environment_id', '')

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not bear:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        bear = msg

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(api_token=bear, hostname=base_url, raw_response=True)
    client = clumioapi_client.ClumioAPIClient(config)

    # List all the EBS volumes.
    list_filter = {'environment_id': {'$eq': env_id}}
    if resource_type == 'EBS':
        list_function = client.aws_ebs_volumes_v1.list_aws_ebs_volumes
    elif resource_type == 'EC2':
        list_function = client.aws_ec2_instances_v1.list_aws_ec2_instances
    else:
        return {'status': 401, 'msg': f'Resource type {resource_type} is not supported.'}

    try:
        assets_list = common.get_total_list(function=list_function, api_filter=json.dumps(list_filter))
    except clumio_exception.ClumioException as e:
        return {'status': 401, 'msg': f'List {resource_type} assets error - {e}'}

    # List the latest backup for each ebs volume
    return {
        'status': 200,
        'region': region_name,
        'asset_ids': [asset.p_id for asset in assets_list],
    }
