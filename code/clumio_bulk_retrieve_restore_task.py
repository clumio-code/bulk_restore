# Copyright 2024, Clumio, a Commvault Company.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Lambda function to retrieve the restore task status."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import common

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to retrieve the EC2 restore task."""
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    inputs: dict = events.get('inputs', {})
    task_id: str | None = inputs.get('task', None)

    # Verify restore task was received.
    if not task_id:
        return {'status': 402, 'msg': 'no task id', 'inputs': inputs}

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token, raw_response=False)
    status = None
    try:
        for _ in common.simple_timer(600, 20):
            try:
                response = client.tasks_v1.read_task(task_id=task_id)
                status = response.status
                logger.info('[%s] Task status %s.', task_id, status)
                if status == 'completed':
                    break
                if status in ('failed', 'aborted'):
                    return {'status': 403, 'msg': f'task failed {status}', 'inputs': inputs}
            except TypeError:
                logger.error('[%s] Failed to read task.', task_id)
                return {
                    'status': 401,
                    'msg': 'user not authorized to access task.',
                    'inputs': inputs,
                }
    except common.TimeoutException:
        logger.warning('[%s] Task timed out after polling. Last known status: %s.', task_id, status)
        return {'status': 205, 'msg': f'task not done - {status}', 'inputs': inputs}

    return {'status': 200, 'msg': 'task completed', 'inputs': inputs}
