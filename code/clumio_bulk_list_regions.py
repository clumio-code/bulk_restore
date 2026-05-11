# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to retrieve the regions of a given account."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda functions to retrieve the regions of a given account."""
    # Retrieve and validate the inputs.
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account: str | None = events.get('source_account', None)
    source_regions: list[str] | None = events.get('source_regions', None)
    source_asset_types: dict = events.get('source_asset_types', {}) or {}
    # The inner "Split Run per Resource Type" Map iterates this list; emit only
    # the resource types the caller asked for so we don't fan out 5 empty
    # iterations per region.
    resource_types = list(source_asset_types.keys())
    if source_account is None:
        return {
            'status': 400,
            'msg': 'source_account is required',
            'inputs': events,
        }

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token)

    # Retrieve all regions.
    env_filter = {
        'account_native_id': {'$eq': source_account},
        'connection_status': {'$eq': 'connected'},
    }
    try:
        logger.info('List AWS environments...')
        parsed_response = client.aws_environments_v1.list_aws_environments(
            filter=common.make_filter(env_filter),
            limit=100,
        )
    except clumio_exception.ClumioException as e:
        logger.error('List AWS environments failed with exception: %s', e)
        return {'status': 500, 'msg': f'List region error - {e}'}

    items = parsed_response.Embedded.Items if parsed_response.Embedded else None
    if not items:
        logger.error('No connected environment found for account %s.', source_account)
        return {
            'status': 404,
            'msg': f'No connected environment found for account {source_account}',
        }

    regions = []
    for env in items:
        if source_regions and env.AwsRegion not in source_regions:
            continue
        regions.append({'region': env.AwsRegion, 'environment_id': env.Id})
    logger.info('Found %s AWS environments.', len(regions))
    logger.info('Resource types to fan out per region: %s', resource_types)
    return {'status': 200, 'regions': regions, 'resource_types': resource_types}
