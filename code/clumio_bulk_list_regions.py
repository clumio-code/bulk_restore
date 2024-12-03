# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to retrieve the regions of a given account."""

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
    """Handle the lambda functions to retrieve the regions of a given account."""
    # Retrieve and validate the inputs.
    bear: str | None = events.get('bear', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account: str | None = events.get('source_account', None)

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

    # Retrieve all regions.
    env_filter = {'account_native_id': {'$eq': source_account}}
    try:
        raw_response, parsed_response = client.aws_environments_v1.list_aws_environments(
            filter=json.dumps(env_filter), limit=100
        )
        # Return if response is not ok.
        if not raw_response.ok:
            return {
                'status': raw_response.status_code,
                'msg': raw_response.content,
                'inputs': events,
            }
        # Convert parsed response to list of regions and environment_id.
        regions = [
            {
                'region': env.aws_region,
                'environment_id': env.p_id,
            }
            for env in parsed_response.embedded.items
        ]
        return {'status': 200, 'regions': regions}
    except clumio_exception.ClumioException as e:
        return {'status': 401, 'msg': f'List region error - {e}'}
