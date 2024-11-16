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
    else:
        sort = f'-{sort}'
        ts_filter = {START_TIMESTAMP_STR: {'$lte': end_timestamp_str}}

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
