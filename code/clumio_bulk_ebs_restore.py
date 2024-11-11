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
from clumioapi import configuration, exceptions, clumioapi_client, models


def lambda_handler(events, context):
    record = events.get("record", {})
    bear = events.get('bear', None)
    base_url = events.get('base_url', None)
    target_account = events.get('target', {}).get('target_account', None)
    target_region = events.get('target', {}).get('target_region', None)
    target_az = events.get('target', {}).get("target_az", None)
    target_kms_key_native_id = events.get('target', {}).get("target_kms_key_native_id", None)
    target_iops = events.get('target', {}).get("target_iops", None)
    target_volume_type = events.get('target', {}).get("target_volume_type", None)

    inputs = {
        'resource_type': 'EBS',
        'run_token': None,
        'task': None,
        'source_backup_id': None,
        'source_volume_id': None
    }

    if len(record) == 0:
        return {"status": 205, "msg": "no records", "inputs": inputs}

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
    run_token = ''.join(random.choices(string.ascii_letters, k=13))

    if record:
        source_backup_id = record.get("backup_record", {}).get('source_backup_id', None)
        source_volume_id = record.get("volume_id")
    else:
        error = f"invalid backup record {record}"
        return {"status": 402, "msg": f"failed {error}", "inputs": inputs}

    # Set restore target information
    target = {
        "account": target_account,
        "region": target_region,
        "aws_az": target_az,
        "iops": target_iops,
        "volume_type": target_volume_type,
        "kms_key_native_id": target_kms_key_native_id
    }

    env_filter = (
        '{'
        '"account_native_id": {"$eq": "' + target_account + '"},'
        '"aws_region": {"$eq": "' + target_region + '"}'
        '}'
    )
    response = client.aws_environments_v1.list_aws_environments(filter=env_filter)
    if not response.current_count:
        return {
            "status": 402,
            "msg": f"The evironment with account_id {target_account} and region {target_region} cannot be found.",
            "inputs": inputs
        }
    target_env_id = response.embedded.items[0].p_id

    # Perform the restore.
    source = models.ebs_restore_source.EBSRestoreSource(backup_id=source_backup_id)
    target = models.ebs_restore_target.EBSRestoreTarget(
        aws_az=target_az,
        environment_id=target_env_id,
        iops=target_iops,
        kms_key_native_id=target_kms_key_native_id,
        p_type=target_volume_type,
    )
    request = models.restore_aws_ebs_volume_v2_request(source=source, target=target)
    try:
        response = client.restored_aws_ebs_volumes_v2.restore_aws_ebs(body=request)
        inputs = {
            'resource_type': 'EBS',
            'run_token': run_token,
            'task': response.task_id,
            'source_backup_id': source_backup_id,
            'source_volume_id': source_volume_id
        }
        return {"status": 200, "msg": "completed", "inputs": inputs}
    except exceptions.clumio_exception.ClumioExceptions as e:
        return {
            "status": "400",
            "msg": f"Failure during restore request: {e}",
            "inputs": inputs
        }