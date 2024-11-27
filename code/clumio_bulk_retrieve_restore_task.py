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

from typing import TYPE_CHECKING, Any

import common
from clumioapi import clumioapi_client, configuration

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to retrieve the EC2 restore task."""
    bear: str | None = events.get('bear', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    inputs: dict = events.get('inputs', {})
    task: str | None = inputs.get('task', None)

    if not task:
        return {'status': 402, 'msg': 'no task id', 'inputs': inputs}

    task_id = task

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    if not bear:
        status, msg = common.get_bearer_token()
        if status != common.STATUS_OK:
            return {'status': status, 'msg': msg}
        bear = msg

    # Initiate the Clumio API client.
    base_url = common.parse_base_url(base_url)
    config = configuration.Configuration(api_token=bear, hostname=base_url)
    client = clumioapi_client.ClumioAPIClient(config)

    try:
        response = client.tasks_v1.read_task(task_id=task_id)
        status = response.status
    except TypeError:
        return {'status': 401, 'msg': 'user not authorized to access task.', 'inputs': inputs}

    if status == 'completed':
        return {'status': 200, 'msg': 'task completed', 'inputs': inputs}
    if status in ('failed', 'aborted'):
        return {'status': 403, 'msg': f'task failed {status}', 'inputs': inputs}
    return {'status': 205, 'msg': f'task not done - {status}', 'inputs': inputs}
