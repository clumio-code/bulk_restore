# Copyright 2024, Clumio Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from botocore.exceptions import ClientError
import boto3
import json
import common
from clumioapi import configuration, clumioapi_client


def lambda_handler(events, context):
    bear = events.get('bear', None)
    base_url = events.get('base_url', common.DEFAULT_BASE_URL)
    inputs = events.get("inputs", {})
    task = inputs.get('task', None)

    if task:
        task_id = task
    else:
        return {"status": 402, "msg": "no task id", "inputs": inputs}

    # If clumio bearer token is not passed as an input read it from the AWS secret
    if not bear:
        bearer_secret = "clumio/token/bulk_restore"
        secretsmanager = boto3.client('secretsmanager')
        try:
            secret_value = secretsmanager.get_secret_value(SecretId=bearer_secret)
            secret_dict = json.loads(secret_value['SecretString'])
            bear = secret_dict.get('token', None)
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"Describe Volume failed - {error}"
            return {"status": 411, "msg": error_msg}

    # Initiate the Clumio API client.
    if 'https' in base_url:
        base_url = base_url.split('/')[2]
    config = configuration.Configuration(api_token=bear, hostname=base_url)
    client = clumioapi_client.ClumioAPIClient(config)

    # Retrieve the task id status.
    response = client.tasks_v1.read_task(task_id=task_id)
    status = response.status

    if status == "completed":
        return {"status": 200, "msg": "task completed", "inputs": inputs}
    elif status in ['failed', 'aborted']:
        return {"status": 403, "msg": f"task failed {status}", "inputs": inputs}
    else:
        return {"status": 205, "msg": f"task not done - {status}", "inputs": inputs}
