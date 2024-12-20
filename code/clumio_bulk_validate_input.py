# Copyright 2024, Clumio, a Commvault Company.
#
"""Lambda function to validate the input of the bulk restore."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

import common

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to validate the input of the bulk restore."""
    default_input = events.get('DefaultInput', {})
    restore_groups = events.get('RestoreGroups', {})

    for restore_specs in restore_groups:
        resource_type = restore_specs['ResourceType']
        if resource_type not in default_input:
            continue

        required_inputs = default_input.get(resource_type, {})
        for field, value in required_inputs.items():
            if not value:
                # If the required field is not filled, then return error.
                return {
                    'status': 400,
                    'msg': f'The required input {field} for resource type {resource_type} should be filled.',
                }
            if restore_specs[field] != common.FOLLOW_DEFAULT_INPUT:
                # If the field is filled with custom value, use that instead.
                continue
            restore_specs[field] = value

    return {'status': 200, 'RestoreGroups': restore_groups}
