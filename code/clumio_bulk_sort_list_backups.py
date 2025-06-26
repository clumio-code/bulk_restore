# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to sort the retrieved list of backups."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, list]:
    """Handle the lambda function to sort the retrieved list of backups."""
    backup_lists: list[dict] = events.get('backup_list', [])
    resource_type: str = events.get('resource_type', 'EBS')
    logger.info('Filter %s %s backups...', len(backup_lists), resource_type)
    # Filter out the empty responses.
    filtered_backup_lists: list = []
    for backup_response in backup_lists:
        record = backup_response.get('records', [])
        if not record:
            continue
        filtered_backup_lists.append(record[0])
    logger.info('Filtered down to %s %s backups.', len(filtered_backup_lists), resource_type)
    return {resource_type: filtered_backup_lists}
