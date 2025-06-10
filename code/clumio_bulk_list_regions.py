# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to retrieve the regions of a given account."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, List  # noqa: UP035

import common
from clumioapi import clumioapi_client, configuration
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
    if source_account is None:
        return {
            'status': 400,
            'msg': 'source_account is required',
            'inputs': events,
        }

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not clumio_token:
        logger.info('Retrieving Clumio bearer token from AWS secret...')
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

    # Retrieve all regions.
    env_filter = {
        'account_native_id': {'$eq': source_account},
        'connection_status': {'$eq': 'connected'},
    }
    try:
        logger.info('List AWS environments...')
        raw_response, parsed_response = client.aws_environments_v1.list_aws_environments(
            filter=json.dumps(env_filter),
            limit=100,
        )

        # Return if response is not ok.
        if not raw_response.ok:
            logger.error('List AWS environments failed with message: %s', raw_response.content)
            return {
                'status': raw_response.status_code,
                'msg': raw_response.content,
                'inputs': events,
            }

        # Convert parsed response to list of regions and environment_id.
        regions = []
        for env in parsed_response.embedded.items:
            if source_regions and env.aws_region not in source_regions:
                continue
            regions.append(
                {
                    'region': env.aws_region,
                    'environment_id': env.p_id,
                }
            )
        logger.info('Found %s AWS environments.', len(regions))
        return {'status': 200, 'regions': regions}
    except clumio_exception.ClumioException as e:
        logger.error('List AWS environments failed with exception: %s', e)
        return {'status': 401, 'msg': f'List region error - {e}'}
