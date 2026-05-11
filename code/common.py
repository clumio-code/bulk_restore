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
import urllib.parse as urllib_parse
from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Any, Final, Protocol

import boto3
import botocore.exceptions
from clumioapi import clumioapi_client, configuration, exceptions
from clumioapi.models import aws_tag_common_model
from requests import adapters
from urllib3.util import Retry
from utils import dates

if TYPE_CHECKING:
    EventsTypeDef = dict[str, Any]
    StatusAndMsgTypeDef = tuple[int, str]

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


class Error(Exception):
    """Base exception class."""


class TimeoutException(Error):
    """Exception raised when a timeout occurs."""


# Define the retry strategy
retry_strategy = Retry(
    total=8,  # Total number of retries
    # Retry only on transient signals. 500 is excluded because Clumio's restore
    # endpoints sometimes return 500 for request-shape errors; retrying buries
    # the underlying error body inside a RetryError. Let 500s propagate so the
    # restore Lambdas can log the response and return a structured failure.
    status_forcelist=[429, 502, 503, 504],
    allowed_methods=[
        'HEAD',
        'GET',
        'OPTIONS',
        'PUT',
        'POST',
        'DELETE',
    ],  # Retry on these methods
    backoff_factor=2,  # A delay factor for exponential backoff.
    # Sleep for: {backoff factor} * (2 ** ({number of total retries} - 1))
    # e.g., 0s, 2s, 4s, 8s, 16s, 32s, 64s, 128s
)
retry_adapter = adapters.HTTPAdapter(max_retries=retry_strategy)


def parse_base_url(base_url: str) -> str:
    """Parse the base URL."""
    return base_url.removeprefix('https://')


def get_sort_and_ts_filter(
    direction: str | None,
    start_day_offset: int,
    end_day_offset: int,
) -> tuple[str, dict[str, Any]]:
    """Get the sort and the timestamp filter."""
    end_timestamp_str = dates.get_max_n_days_ago(end_day_offset).strftime(dates.ISO_8601_FORMAT)
    start_timestamp_str = dates.get_midnight_n_days_ago(start_day_offset).strftime(
        dates.ISO_8601_FORMAT,
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


class PassthroughFilter:
    """Adapter for the v1.0.x SDK's typed-filter parameter.

    v1.0.x list methods expect a typed filter object exposing a ``.query_str``
    JSON string. This shim lets callers keep passing legacy MongoDB-style
    filter dicts (or pre-serialized JSON strings) without rewriting every
    call site to a typed pydantic filter class.
    """

    def __init__(self, filter_value: dict | str) -> None:
        """Wrap a dict or pre-serialized JSON string for SDK use."""
        if isinstance(filter_value, str):
            self.query_str = filter_value
        else:
            self.query_str = json.dumps(filter_value)


def make_filter(filter_value: dict | str | None) -> Any:
    """Return a PassthroughFilter for a dict/JSON string, or None for empty input.

    Return type is ``Any`` so callers can pass the result to v1.0.x list methods
    whose ``filter`` parameter expects an endpoint-specific typed filter class;
    ``PassthroughFilter`` is structurally compatible (exposes ``.query_str``).
    """
    if filter_value is None:
        return None
    return PassthroughFilter(filter_value)


def _next_page_start(response: Any) -> str | None:
    """Extract the ``start`` page token from a list response's HATEOAS Next link."""
    links = getattr(response, 'Links', None)
    next_link = getattr(links, 'Next', None) if links is not None else None
    href = getattr(next_link, 'Href', None) if next_link is not None else None
    if not href:
        return None
    qs = urllib_parse.parse_qs(urllib_parse.urlparse(href).query)
    return qs.get('start', [None])[0]


def get_total_list(
    function: Callable,
    api_filter: dict | str | None = None,
    lookback_days: int | None = None,
    **kwargs: Any,
) -> list:
    """Get all items from a paginated v1.0.x list endpoint.

    Iterates HATEOAS ``Links.Next`` pages until exhausted. The SDK raises
    ``ClumioException`` on non-2xx responses; callers may catch or let
    propagate.

    Args:
        function: A list API method on the v1.0.x SDK (e.g.
            ``client.backup_aws_ebs_volumes_v2.list_backup_aws_ebs_volumes``).
        api_filter: A MongoDB-style filter dict or pre-serialized JSON string.
        lookback_days: Calculate backup status for the last `lookback_days` days.
        kwargs: Extra keyword arguments forwarded to ``function`` (e.g. ``sort``).
    """
    total_list: list = []
    start: str | None = None
    base_params: dict[str, Any] = dict(kwargs)
    filter_obj = make_filter(api_filter)
    if filter_obj is not None:
        base_params['filter'] = filter_obj
    if lookback_days is not None:
        base_params['lookback_days'] = lookback_days

    while True:
        params = dict(base_params)
        if start is not None:
            params['start'] = start
        response = function(**params)
        embedded = getattr(response, 'Embedded', None)
        items = getattr(embedded, 'Items', None) if embedded is not None else None
        if items:
            total_list.extend(items)
        start = _next_page_start(response)
        if start is None:
            break
    return total_list


def get_environment_id_or_raise(
    client: clumioapi_client.ClumioAPIClient,
    target_account: str | None,
    target_region: str | None,
) -> str:
    """Get the Clumio environment UUID or raise if not found."""
    status, msg = get_environment_id(client, target_account, target_region)
    if status != STATUS_OK:
        raise exceptions.clumio_exception.ClumioException(f'{msg} (status {status})')
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
    try:
        response = client.aws_environments_v1.list_aws_environments(
            filter=make_filter(env_filter),
        )
    except exceptions.clumio_exception.ClumioException as e:
        logger.error('Error listing AWS environments: %s', e)
        return ERROR_CODE, f'Error when listing the aws environments: {e}'
    if not response.CurrentCount or not response.Embedded or not response.Embedded.Items:
        return ERROR_CODE, 'No authorized environment found.'
    return STATUS_OK, str(response.Embedded.Items[0].Id)


def get_bearer_token_if_not_exists(clumio_token: str | None) -> str:
    """Get the Clumio token if it was not provided in the JSON input file."""
    if not clumio_token:
        status, msg = get_bearer_token()
        if status != STATUS_OK:
            raise exceptions.clumio_exception.ClumioException(f'{msg} (status {status})')
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
    base_url: str,
    clumio_token: str,
) -> clumioapi_client.ClumioAPIClient:
    """Get the Clumio REST API client."""
    base_url = parse_base_url(base_url)
    config = configuration.Configuration(
        api_token=clumio_token,
        hostname=base_url,
    )
    client = clumioapi_client.ClumioAPIClient(config)
    # v1.0.x SDK funnels every controller through a single shared RESTclient.
    session = client.base_controller.client.session
    session.mount('https://', retry_adapter)
    return client


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
    """Return snake_case dict version of a v1.0.x SDK model, or None."""
    if not obj:
        return None
    dict_method = getattr(obj, 'dict', None)
    if callable(dict_method):
        return dict_method()
    return obj.__dict__


def tags_from_dict(tags: list[dict[str, str]]) -> list[aws_tag_common_model.AwsTagCommonModel]:
    """Convert list of tags from dict to AwsTagCommonModel."""
    tag_list = []
    for tag in tags:
        tag_list.append(aws_tag_common_model.AwsTagCommonModel(Key=tag['key'], Value=tag['value']))
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


def format_append_tags(append_tags: dict) -> list[dict]:
    """Format user-provided append_tags to AWS format."""
    tags = []
    for tag_key, tag_value in append_tags.items():
        tags.append({'key': tag_key, 'value': tag_value})
    return tags


def append_tags_to_source_tags(tags: list[dict], append_tags: dict) -> list[dict]:
    """Append the append_tags from target_specs to the asset source tags for restore."""
    if tags is None:
        tags = []
    for tag_key, tag_value in append_tags.items():
        new_tag = {'key': tag_key, 'value': tag_value}
        if new_tag not in tags:
            tags.append(new_tag)
    return tags


def simple_timer(timeout: float, interval: float, label: str | None = None) -> Generator[float]:
    """Simple timer iterator.

    Will count up to timeout, then raise an error. Usually used in a for loop
    that may discard the elapsed time yielded.

    Example:
        count = 0
        for _ in clumio_time.simple_timer(3, 1):
            count += 1
            if count == 2:
                return count
        raise RuntimeError('Code should be unreachable.')

    Do NOT use it in a `while` statement as it will become an infinite loop:
        while clumio_time.simple_timer(3, 1):
            print('infinite loop')

    Args:
        timeout: The number of seconds before timing out.
        interval: How long to sleep between yields.
        label: Option to log the object name in the error message.
        raise_on_timeout: Option to return once the loop completes.

    Yields:
        The amount of time elapsed in seconds.

    Raises:
        TimeoutException: timed out.
    """
    now = time.monotonic()
    start_time = now
    end_time = start_time + timeout
    while now < end_time:
        yield now - start_time
        now = time.monotonic()
        target = min(now + interval, end_time)
        while now < target:
            # Non-blocking sleep: Suspend this and yield to other co-routines to run
            time.sleep(target - now)
            now = time.monotonic()
    if label is not None:
        msg = f'[{label}] Timed out after {timeout} seconds.'
    else:
        msg = f'Timed out after {timeout} seconds.'

    logger.warning(msg)
    raise TimeoutException(msg)
