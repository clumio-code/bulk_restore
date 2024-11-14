# Copyright 2024, Clumio, a Commvault Company.
#
import datetime


DEFAULT_BASE_URL = 'https://us-west-2.api.clumio.com/'
START_TIMESTAMP_STR = 'start_timestamp'

def get_sort_and_ts_filter(
    direction: str, start_day_offset: int, end_day_offset: int
) -> tuple[str, dict]:
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


def get_total_list(function, api_filter, sort):
    """Get the list of all items

    Args:
        function: must be list API function call with pagination feature.
        api_filter: the filter applied to the list API.
        sort: the sort applied to the list API.
    """
    start = 1
    total_list = []
    while True:
        response = function(filter=api_filter, sort=sort, start=start)
        if response.total_count == 0:
            break
        total_list.extend(response.embedded.items)
        if response.total_pages_count <= start:
            break
        start += 1
    return total_list

