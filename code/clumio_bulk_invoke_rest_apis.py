# Copyright 2025, Clumio, a Commvault Company.
#
"""Lambda function to invoke Clumio REST APIs from AWS Lambda functions."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def get_endpoint_mappings(
    client: clumioapi_client.ClumioAPIClient, filters: dict | None, limit: int
) -> dict:
    """Returns mapping of endpoint labels to client methods."""
    filters_str = json.dumps(filters)
    return {
        'list_aws_environments': client.aws_environments_v1.list_aws_environments(
            filter=filters_str, limit=limit
        ),
        'list_aws_connections': client.aws_connections_v1.list_aws_connections(
            filter=filters_str, limit=limit
        ),
        'list_aws_dynamodb_tables': client.aws_dynamodb_tables_v1.list_aws_dynamodb_tables(
            filter=filters_str, limit=limit
        ),
        'list_aws_s3_buckets': client.aws_s3_buckets_v1.list_aws_s3_buckets(
            filter=filters_str, limit=limit
        ),
        'list_protection_groups': client.protection_groups_v1.list_protection_groups(
            filter=filters_str, limit=limit
        ),
        'list_protection_group_s3_assets': client.protection_groups_s3_assets_v1.list_protection_group_s3_assets(
            filter=filters_str, limit=limit
        ),
        'list_backup_aws_dynamodb_tables': client.backup_aws_dynamodb_tables_v1.list_backup_aws_dynamodb_tables(
            filter=filters_str, limit=limit
        ),
        'list_backup_protection_groups': client.backup_protection_groups_v1.list_backup_protection_groups(
            filter=filters_str, limit=limit
        ),
    }


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda functions to invoke Clumio REST APIs.

    Args:
        events: A dictionary containing the input parameters for the function. Expected keys:
            bear: The Clumio bearer token. If not provided, retrieved from AWS Secrets Manager.
            base_url: The base URL for the Clumio API. Defaults to a predefined value.
            endpoint: The Clumio REST API endpoint to invoke. Supported values:
                list_aws_environments
                list_aws_connections
                list_aws_dynamodb_tables
                list_aws_s3_buckets
                list_protection_groups
                list_protection_group_s3_assets
                list_backup_aws_dynamodb_tables
                list_backup_protection_groups
            filters: Filters to apply to the API request.
            limit: The maximum number of results to return. Defaults to 100.
        context: The AWS Lambda context object.

    Returns:
        Dict of status and error message in the case of invalid endpoint.
        Dict of status, error message, and user input in the case of error.
        Dict of status and response contents in the case of success.
    """
    # Retrieve and validate the inputs.
    bear: str | None = events.get('bear', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    endpoint: str = events.get('endpoint', 'list_aws_environments')
    filters: dict | None = events.get('filters', None)
    limit: int = events.get('limit', 100)

    # If Clumio bearer token is not passed as an input read it from the AWS secret.
    if not bear:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        bear = msg

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(api_token=bear, hostname=base_url, raw_response=True)
    client = clumioapi_client.ClumioAPIClient(config)

    # Get the REST API mappings.
    mappings = get_endpoint_mappings(client, filters, limit)

    # Verify the specified endpoint is supported.
    if endpoint not in mappings:
        return {'status': 400, 'msg': f'Invalid endpoint specified - {endpoint}'}

    # Invoke the REST API.
    try:
        raw_response, parsed_response = mappings[endpoint]
        if not raw_response.ok:
            return {
                'status': raw_response.status_code,
                'msg': raw_response.content,
                'inputs': events,
            }
        return {'status': 200, 'response': json.loads(raw_response.content)}
    except clumio_exception.ClumioException as e:
        return {'status': 401, 'msg': f'API error - {e}'}
