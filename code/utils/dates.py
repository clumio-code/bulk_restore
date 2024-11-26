# Copyright 2024, Clumio, a Commvault Company.
#
"""Random dates convenience methods."""

from __future__ import annotations

import datetime
from typing import Final

DAY_IN_SECONDS: Final = 24 * 3600
# For converting to ISO 3339 time format, e.g., '2020-07-21 16:32:24 +0000 UTC'
ISO_3339_FORMAT: Final = '%Y-%m-%d %H:%M:%S %z %Z'

# For converting to ISO 8601 time format, e.g., '2020-08-03T23:48:55Z
ISO_8601_FORMAT: Final = '%Y-%m-%dT%H:%M:%SZ'
# Other common time formats
DATE_TIME_FORMAT: Final = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT: Final = '%Y-%m-%d'


def get_utc_now() -> datetime.datetime:
    """Returns datetime now in UTC timezone."""
    return datetime.datetime.now(datetime.UTC)


def get_midnight_today_utc() -> datetime.datetime:
    """Returns midnight today UTC."""
    return datetime.datetime.combine(get_utc_now(), datetime.datetime.min.time(), datetime.UTC)


def get_max_today_utc() -> datetime.datetime:
    """Get datetime object representing the today 23:59:59 UTC."""
    return datetime.datetime.combine(get_utc_now(), datetime.datetime.max.time(), datetime.UTC)


def get_midnight_n_days_ago(days: int) -> datetime.datetime:
    """Get datetime object representing midnight N days ago UTC."""
    return get_midnight_today_utc() - datetime.timedelta(days=days)


def get_max_n_days_ago(days: int) -> datetime.datetime:
    """Get datetime object representing N days ago 23:59:59 UTC."""
    return get_max_today_utc() - datetime.timedelta(days=days)
