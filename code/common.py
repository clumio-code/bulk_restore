# Copyright 2024, Clumio, a Commvault Company.
#

"""Common methods and constants for the bulk restore lambda functions."""

import datetime
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Final, Protocol

if TYPE_CHECKING:
    EventsTypeDef = dict[str, Any]

    class ListingCallable(Protocol):
        def __call__(self, filter: str | None, sort: str | None, start: int) -> Any: ...


DEFAULT_BASE_URL: Final = 'https://us-west-2.api.clumio.com/'
START_TIMESTAMP_STR: Final = 'start_timestamp'


def parse_base_url(base_url: str) -> str:
    """Parse the base URL."""
    if not base_url.startswith('https://'):
        return base_url
    return base_url.split('/', maxsplit=3)[2]


def get_sort_and_ts_filter(
    direction: str | None, start_day_offset: int, end_day_offset: int
) -> tuple[str, dict[str, Any]]:
    """Get the sort and the timestamp filter."""
    current_timestamp = datetime.datetime.now(datetime.UTC)
    end_timestamp = current_timestamp - datetime.timedelta(days=end_day_offset)
    end_timestamp_str = end_timestamp.strftime('%Y-%m-%d') + 'T23:59:59Z'

    start_timestamp = current_timestamp - datetime.timedelta(days=start_day_offset)
    start_timestamp_str = start_timestamp.strftime('%Y-%m-%d') + 'T00:00:00Z'
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
        response = function(filter=api_filter, sort=sort, start=start)
        if not response.total_count:
            break
        total_list.extend(response.embedded.items)
        if response.total_pages_count <= start:
            break
        start += 1
    return total_list


def filter_backup_records_by_tags(
    backup_records: list[dict], search_tag_key: str, search_tag_value: str, tag_field: str
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
