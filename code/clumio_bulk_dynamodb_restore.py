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
import random
import string
import boto3
import json
from clumio_sdk_v13 import DynamoDBBackupList, RestoreDDN, ClumioConnectAccount, AWSOrgAccount, ListEC2Instance, \
    EnvironmentId, RestoreEC2, RestoreRDS, EC2BackupList, EBSBackupList, RestoreEBS, OnDemandBackupEC2, RetrieveTask


def lambda_handler(events, context):
    bear = events.get('bear', None)
    debug_input = events.get('debug', None)
    record = events.get("record", {})
    target_region = events.get('target',{}).get('target_region', None)
    target_account = events.get('target',{}).get('target_account', None)
    change_set_name = events.get('target',{}).get("change_set_name", None)

    inputs = {
        'resource_type': 'DynamoDB',
        'run_token': None,
        'task': None,
        'source_backup_id': None,
        'source_table_name': None
    }

    if record:
        source_backup_id = record.get("backup_record", {}).get('source_backup_id', None)
        source_table_name = record.get('table_name', None)
    else:
        error = f"invalid backup record {record}"
        return {"status": 402, "msg": f"failed {error}",
                "inputs": inputs}

    # Validate inputs
    try:
        debug = int(debug_input)
    except ValueError as e:
        error = f"invalid debug: {e}"
        return {"status": 401, "task": None, "msg": f"failed {error}",
                "inputs": inputs}

    if len(record) == 0:
        return {"status": 205, "msg": "no records",
                "inputs": inputs}

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

    ddn_restore_api = RestoreDDN()
    ddn_restore_api.set_token(bear)
    ddn_restore_api.set_debug(99)
    run_token = ''.join(random.choices(string.ascii_letters, k=13))
    target = {
        "account": target_account,
        "region": target_region,
        "table_name": f"-{change_set_name}",
    }

    result_target = ddn_restore_api.set_target_for_ddn_restore(target)
    if not result_target:
        error_msgs = ddn_restore_api.get_error_msg()
        msgs_string = ":".join(error_msgs)
        return {"status": 404, "msg": msgs_string,
                "inputs": inputs}
    print(f"target set status {result_target}")
    # Run restore
    ddn_restore_api.save_restore_task()
    [results, msg] = ddn_restore_api.ddn_restore_from_record([record])

    if results:
        # Get a list of tasks for all of the restores.
        task_list = ddn_restore_api.get_restore_task_list()
        if debug > 5: print(task_list)
        task = task_list[0].get("task", None)
        inputs = {
            'resource_type': 'DynamoDB',
            'run_token': run_token,
            'task': task,
            'source_backup_id': source_backup_id,
            'source_table_name': source_table_name
        }
        if len(task_list) > 0:
            return {"status": 200, "msg": "completed",
                    "inputs": inputs}
        else:
            return {"status": 207, "msg": "no restores",
                    "inputs": inputs}
    else:
        return {"status": 403, "msg": msg,
                "inputs": inputs}