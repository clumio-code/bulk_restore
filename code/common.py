# Copyright 2024, Clumio, a Commvault Company.
#

"""Common methods and constants for the bulk restore lambda functions."""

from __future__ import annotations

import json
import secrets
import string
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Final, Protocol

from clumioapi import clumioapi_client, exceptions
from clumioapi.models import aws_tag_common_model
from utils import dates

if TYPE_CHECKING:
    EventsTypeDef = dict[str, Any]

    class ListingCallable(Protocol):
        def __call__(self, filter: str | None, sort: str | None, start: int) -> Any: ...


DEFAULT_BASE_URL: Final = 'https://us-west-2.api.clumio.com/'
START_TIMESTAMP_STR: Final = 'start_timestamp'
STATUS_OK: Final = 200


def parse_base_url(base_url: str) -> str:
    """Parse the base URL."""
    if not base_url.startswith('https://'):
        return base_url
    return base_url.split('/', maxsplit=3)[2]


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


def get_total_list(function: Callable, api_filter: str, sort: str) -> list:
    """Get the list of all items.

    Args:
        function: A list API function call with pagination feature.
        api_filter: The filter applied to the list API as a parsable JSON document.
        sort: The sort applied to the list API.
    """
    start = 1
    total_list = []
    while True:
        raw_response, parsed_response = function(filter=api_filter, sort=sort, start=start)
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


def get_environment_id(
    client: clumioapi_client.ClumioAPIClient,
    target_account: str | None,
    target_region: str | None,
) -> tuple[int, str]:
    """Retrieve the environment for given target_account and target_region."""
    if not target_account:
        return 402, 'target_account is required'

    if not target_region:
        return 402, 'target_region is required.'

    env_filter = {
        'account_native_id': {'$eq': target_account},
        'aws_region': {'$eq': target_region},
    }
    response = client.aws_environments_v1.list_aws_environments(filter=json.dumps(env_filter))
    if not response.current_count:
        return 402, 'No authorized environment found.'
    return 200, response.embedded.items[0].p_id


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
