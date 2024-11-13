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

from botocore.exceptions import ClientError
import boto3
import json
from clumio_sdk_v13 import DynamoDBBackupList, RestoreDDN, ClumioConnectAccount, AWSOrgAccount, ListEC2Instance, \
    EnvironmentId, RestoreEC2, EC2BackupList, EBSBackupList, RestoreEBS, OnDemandBackupEC2, RetrieveTask


def lambda_handler(events, context):
    bear = events.get('bear', None)
    task = events.get("inputs", {}).get('task', None)
    source_backup_id = events.get("inputs", {}).get('source_backup_id', None)
    resource_id = events.get("inputs", {}).get('source_resource_id', None)
    debug_input = events.get('debug', None)
    run_token = events.get('inputs', {}).get('run_token', None)
    inputs = {
        'resource_type': 'RDS',
        "run_token": run_token,
        "task": task,
        'source_backup_id': source_backup_id,
        'source_resource_id': resource_id
    }

    try:
        debug = int(debug_input)
    except ValueError as e:
        error = f"invalid debug: {e}"
        return {"status": 401, "msg": error, "inputs": inputs}
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
            # username = secret_dict.get('username', None)
            bear = secret_dict.get('token', None)
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"Describe Volume failed - {error}"
            payload = error_msg
            return {"status": 411, "msg": error_msg}

    # Initiate API and configure
    retrieve_task_api = RetrieveTask()
    base_url = events.get('base_url', None)
    if base_url:
        retrieve_task_api.set_url_prefix(base_url)
    retrieve_task_api.set_token(bear)
    retrieve_task_api.set_debug(debug)

    # Run API in one time mode
    [complete_flag, status, response] = retrieve_task_api.retrieve_task_id(task_id, "one")
    inputs = {
        'resource_type': 'RDS',
        "run_token": run_token,
        "task": task_id,
        'source_backup_id': source_backup_id,
        'source_resource_id': resource_id
    }
    if complete_flag and not status == "completed":
        return {"status": 403, "msg": f"task failed {status}", "inputs": inputs}
    elif complete_flag and status == "completed":
        return {"status": 200, "msg": "task completed", "inputs": inputs}
    else:
        return {"status": 205, "msg": f"task not done - {status}", "inputs": inputs}