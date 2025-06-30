# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to validate the input of the bulk restore."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to validate the input of the bulk restore.

    Expected behavior:
        DefaultInput is optional.
        Individual group values get the priority.
        If a value is not specified in the individual spec, get from the default spec.
        If a value is missing from the default spec and individual spec, return error.
    """
    logger.info('Validate input...')
    default_input_all = events.get('DefaultInput', {})
    restore_groups = events.get('RestoreGroups', {})
    # Iterate through each group/asset to be restored.
    for restore_group in restore_groups:
        resource_type = restore_group['ResourceType']
        if resource_type not in default_input_all:
            # No default input for the resource type.
            continue
        # Get default input for the resource type.
        default_input = default_input_all.get(resource_type, {})
        # Validate input and replace any empty values with default values.
        for field, value in restore_group.items():
            if not value and field in default_input and not default_input[field]:
                # Return error if both the group and default field is empty.
                msg = f'Must provide a value for the {resource_type} field {field}.'
                logger.error(msg)
                return {'status': 400, 'msg': msg}
            if not value and field in default_input and default_input[field]:
                # Update the field to the default value.
                restore_group[field] = default_input[field]
    logger.info('Validate input successful.')
    return {'status': 200, 'RestoreGroups': restore_groups}
