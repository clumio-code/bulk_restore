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
import random
import string
import boto3
import json
from clumioapi import configuration, clumioapi_client, models
import common


def lambda_handler(events, context):
    record = events.get("record", {})
    bear = events.get('bear', None)
    base_url = events.get('base_url', common.DEFAULT_BASE_URL)
    target = events.get('target', {})
    target_account = target.get('target_account', None)
    target_region = target.get('target_region', None)
    target_az = target.get("target_az", None)
    target_iam_instance_profile_name = target.get("target_iam_instance_profile_name", None)
    target_key_pair_name = target.get("target_key_pair_name", None)
    target_security_group_native_ids = target.get("target_security_group_native_ids", None)
    target_subnet_native_id = target.get("target_subnet_native_id", None)
    target_vpc_native_id = target.get("target_vpc_native_id", None)
    target_kms_key_native_id = target.get("target_kms_key_native_id", None)

    inputs = {
        'resource_type': 'EC2',
        'run_token': None,
        'task': None,
        'source_backup_id': None,
        'source_instance_id': None
    }

    # Validate inputs
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
        backup_record = record.get("backup_record", {})
        source_backup_id = backup_record.get('source_backup_id', None)
        source_instance_id = record.get("instance_id")
    else:
        error = f"invalid backup record {record}"
        return {"status": 402, "msg": f"failed {error}", "inputs": inputs}

    # Retrieve the environment id.
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

    # Prepare the restore request.
    source = models.ec2_restore_source.EC2RestoreSource(backup_id=source_backup_id)
    ami_target = models.ec2_ami_restore_target.EC2AMIRestoreTarget(
        ebs_block_device_mappings=ebs_block_device_mappings,
        environment_id=target_env_id,
        name=backup_record.get("source_ami_name", None)
    )
    target = models.ec2_restore_target.EC2RestoreTarget(
        ami_restore_target=ami_target,
    )
    request = models.restore_aws_ec2_instance_v1_request.RestoreAwsEc2InstanceV1Request(
        source=source,
        target=target
    )
    response = client.restored_aws_ec2_instances_v1.restore_aws_ec2_instance(body=request)

    result_target = ec2_restore_api.set_target_for_instance_restore(target)
    if not result_target:
        error_msgs = ec2_restore_api.get_error_msg()
        msgs_string = ":".join(error_msgs)
        return {"status": 404, "msg": msgs_string,
                "inputs": inputs}
    print(f"target set status {result_target}")
    # Run restore
    ec2_restore_api.save_restore_task()
    [result_run, msg] = ec2_restore_api.ec2_restore_from_record([record])


    if result_run:
        # Get a list of tasks for all of the restores.
        task_list = ec2_restore_api.get_restore_task_list()
        if debug > 5: print(task_list)
        task = task_list[0].get("task",None)
        inputs = {
            'resource_type': 'EC2',
            'run_token': run_token,
            'task': task,
            'source_backup_id':source_backup_id,
            'source_instance_id': source_instance_id
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