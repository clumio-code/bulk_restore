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
    elif direction == 'before':
        sort = f'-{sort}'
        ts_filter = {START_TIMESTAMP_STR: {'$lte': end_timestamp_str}}
    else:
        ts_filter = {}
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

def filter_backup_records_by_tags(backup_records, search_tag_key, search_tag_value):
    """Filter the list of backup records by tags."""
    # Filter the result based on the tags.
    if not (search_tag_key and search_tag_value):
        return backup_records
    tags_filtered_backups = []
    for backup in backup_records:
        tags = {
            tag['key']: tag['value'] for tag in backup['backup_record']['source_instance_tags']
        }
        if tags.get(search_tag_key, None) == search_tag_value:
            tags_filtered_backups.append(backup)
    return tags_filtered_backups
