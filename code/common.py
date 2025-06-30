# Copyright 2024, Clumio, a Commvault Company.
#

"""Common methods and constants for the bulk restore lambda functions."""

from __future__ import annotations

import json
import logging
import os
import secrets
import string
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Final, Protocol

import boto3
import botocore.exceptions
from clumioapi import clumioapi_client, configuration, exceptions
from clumioapi.models import aws_tag_common_model
from utils import dates

if TYPE_CHECKING:
    EventsTypeDef = dict[str, Any]
    StatusAndMsgTypeDef = tuple[int, str]
    from clumioapi.models.list_aws_environments_response import ListAWSEnvironmentsResponse

    class ListingCallable(Protocol):
        def __call__(self, filter: str | None, sort: str | None, start: int) -> Any: ...


DEFAULT_BASE_URL: Final = 'https://us-west-2.api.clumio.com/'
DEFAULT_SECRET_PATH: Final = 'clumio/token/bulk_restore'  # noqa: S105
ERROR_CODE: Final = 402
MAX_RETRY: Final = 5
START_TIMESTAMP_STR: Final = 'start_timestamp'
STATUS_OK: Final = 200
RESOURCE_TYPES: Final = ['EBS', 'EC2', 'RDS', 'DynamoDB', 'ProtectionGroup']

logger = logging.getLogger(__name__)


def parse_base_url(base_url: str) -> str:
    """Parse the base URL."""
    return base_url.removeprefix('https://')


def get_sort_and_ts_filter(
    direction: str | None, start_day_offset: int, end_day_offset: int
) -> tuple[str, dict[str, Any]]:
    """Get the sort and the timestamp filter."""
    end_timestamp_str = dates.get_max_n_days_ago(end_day_offset).strftime(dates.ISO_8601_FORMAT)
    start_timestamp_str = dates.get_midnight_n_days_ago(start_day_offset).strftime(
        dates.ISO_8601_FORMAT
    )

    sort = START_TIMESTAMP_STR
    if direction == 'after':
        ts_filter = {START_TIMESTAMP_STR: {'$gt': start_timestamp_str, '$lte': end_timestamp_str}}
    elif direction == 'before':
        sort = f'-{sort}'
        ts_filter = {START_TIMESTAMP_STR: {'$lte': end_timestamp_str}}
    else:
        ts_filter = {}
    return sort, ts_filter


def get_total_list(function: Callable, api_filter: str, **kwargs: Any) -> list:
    """Get the list of all items.

    Args:
        function: A list API function call with pagination feature.
        api_filter: The filter applied to the list API as a parsable JSON document.
        kwargs:
         - sort: The sorting applied to the list API.
    """
    start = 1
    total_list = []
    while True:
        raw_response, parsed_response = function(filter=api_filter, start=start, **kwargs)
        # Raise error if raw response is not ok.
        if not raw_response.ok:
            raise exceptions.clumio_exception.ClumioException(
                raw_response.reason, raw_response.content
            )
        if not parsed_response.total_count:
            break
        total_list.extend(parsed_response.embedded.items)
        if parsed_response.total_pages_count <= start:
            break
        start += 1
    return total_list


def get_environment_id_or_raise(
    client: clumioapi_client.ClumioAPIClient, target_account: str | None, target_region: str | None
) -> str:
    """Get the Clumio environment UUID or raise if not found."""
    status, msg = get_environment_id(client, target_account, target_region)
    if status != STATUS_OK:
        raise exceptions.clumio_exception.ClumioException(msg, str(status))
    return msg


def get_environment_id(
    client: clumioapi_client.ClumioAPIClient,
    target_account: str | None,
    target_region: str | None,
) -> StatusAndMsgTypeDef:
    """Retrieve the environment for given target_account and target_region."""
    if not target_account:
        return ERROR_CODE, 'target_account is required'

    if not target_region:
        return ERROR_CODE, 'target_region is required.'

    env_filter = {
        'account_native_id': {'$eq': target_account},
        'aws_region': {'$eq': target_region},
    }
    retry = 0
    response: ListAWSEnvironmentsResponse | None = None
    while retry < MAX_RETRY:
        _, response = client.aws_environments_v1.list_aws_environments(
            filter=json.dumps(env_filter)
        )
        if response:
            break
        time.sleep(1)
        retry += 1
    if not response:
        return ERROR_CODE, 'Error when listing the aws environments.'
    elif not response.current_count:
        return ERROR_CODE, 'No authorized environment found.'
    return 200, response.embedded.items[0].p_id


def get_bearer_token_if_not_exists(clumio_token: str | None) -> str:
    """Get the Clumio token if it was not provided in the JSON input file."""
    if not clumio_token:
        status, msg = get_bearer_token()
        if status != STATUS_OK:
            raise exceptions.clumio_exception.ClumioException(msg, str(status))
        clumio_token = msg
    return clumio_token


def get_bearer_token() -> StatusAndMsgTypeDef:
    """Retrieve the bearer token from secret manager."""
    secret_arn = os.environ.get('CLUMIO_TOKEN_ARN')
    if not secret_arn:
        # Either provide clumio_token in JSON input file or
        # enter the token in the ClumioTokenArn parameter of the stack.
        return 411, 'CLUMIO_TOKEN_ARN environment variable is not set.'
    secretsmanager = boto3.client('secretsmanager')
    try:
        logger.info('Retrieving Clumio bearer token from AWS secret: %s', secret_arn)
        secret_value = secretsmanager.get_secret_value(SecretId=secret_arn)
        secret_dict = json.loads(secret_value['SecretString'])
        # Get the Clumio token from the key/value pair.
        values = list(secret_dict.values())
        clumio_token = values[0]
        return STATUS_OK, clumio_token
    except botocore.exceptions.ClientError as client_error:
        code = client_error.response['Error']['Code']
        return 411, f'Describe secret failed - {code}'


def get_clumio_api_client(
    base_url: str, clumio_token: str, raw_response: bool = True
) -> clumioapi_client.ClumioAPIClient:
    """Get the Clumio REST API client."""
    base_url = parse_base_url(base_url)
    config = configuration.Configuration(
        api_token=clumio_token, hostname=base_url, raw_response=raw_response
    )
    return clumioapi_client.ClumioAPIClient(config)


def filter_backup_records_by_tags(
    backup_records: list[dict],
    search_tag_key: str | None,
    search_tag_value: str | None,
    tag_field: str,
) -> list[dict]:
    """Filter the list of backup records by tags."""
    # Filter the result based on the tags.
    if not (search_tag_key and search_tag_value):
        return backup_records
    tags_filtered_backups = []
    for backup in backup_records:
        tags = {tag['key']: tag['value'] for tag in backup['backup_record'][tag_field]}
        if tags.get(search_tag_key, None) == search_tag_value:
            tags_filtered_backups.append(backup)
    return tags_filtered_backups


def to_dict_or_none(obj: Any) -> dict | None:
    """Return dict version of an object if it exists, or None otherwise."""
    return obj.__dict__ if obj else None


def tags_from_dict(tags: list[dict[str, str]]) -> list[aws_tag_common_model.AwsTagCommonModel]:
    """Convert list of tags from dict to AwsTagCommonModel."""
    tag_list = []
    for tag in tags:
        tag_list.append(aws_tag_common_model.AwsTagCommonModel(key=tag['key'], value=tag['value']))
    return tag_list


def generate_random_string(length: int = 13) -> str:
    """Generate run token for restore."""
    return ''.join(secrets.choice(string.ascii_letters) for _ in range(length))


def get_append_tags(target_specs: dict, resource_type: str) -> dict:
    """Get the append_tags value from the target_specs input.

    Args:
        target_specs: The target_specs field of the user input.
        resource_type: Resource type EBS|EC2|RDS|DynamoDB.
    """
    append_tags: dict = {}
    if target_specs and resource_type in target_specs:
        append_tags = target_specs[resource_type].get('append_tags', {})
    return append_tags


def append_tags_to_source_tags(tags: list[dict], append_tags: dict) -> list[dict]:
    """Append the append_tags from target_specs to the asset source tags for restore."""
    if tags is None:
        tags = []
    for tag_key, tag_value in append_tags.items():
        new_tag = {'key': tag_key, 'value': tag_value}
        if new_tag not in tags:
            tags.append(new_tag)
    return tags
