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
from clumioapi import configuration, clumioapi_client, models


def lambda_handler(events, context):
    """Handle the lambda function to bulk restore S3."""
    bear = events.get('bear', None)
    base_url = events.get('base_url', None)
    target = events.get('target', {})
    record = events.get('record', {})
    target_account = target.get('target_account', None)
    target_region = target.get('target_region', None)
    target_bucket = target.get('target_bucket', None)
    target_prefix = target.get('target_prefix', None)

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

    # Retrieve the bucket id.
    api_filter = (
        '{'
        '"account_native_id": {"$eq": "' + target_account + '"},'
        '"aws_region": {"$eq": "' + target_region + '"},'
        '"name": {"$in": ["' + target_bucket + '"]}'
        '}'
    )
    response = client.aws_s3_buckets_v1.list_aws_s3_buckets(
        filter=api_filter
    )
    if response.total_count == 0:
        return {"status": 207, "msg": "no target bucket found.", "inputs": target}
    target_bucket_id = response.embedded.items[0].p_id
    target_env_id = response.embedded.items[0].environment_id

    source_input = models.protection_group_restore_source.ProtectionGroupRestoreSource(
        backup_id=record['backup_id'],
        object_filters=models.source_object_filters.SourceObjectFilters(
            **record['object_filters']
        ),
        protection_group_s3_asset_ids=record['protection_group_s3_asset_ids'],
    )
    target_input = models.protection_group_restore_target.ProtectionGroupRestoreTarget(
        bucket_id=target_bucket_id,
        environment_id=target_env_id,
        overwrite=True,
        restore_original_storage_class=True,
        prefix=target_prefix,
    )
    req_body = models.restore_protection_group_v1_request.RestoreProtectionGroupV1Request(
        source=source_input, target=target_input
    )
    response = client.restored_protection_groups_v1.restore_protection_group(body=req_body)
    if not response.task_id:
        return {"status": 207, "msg": "restore failed", "inputs": target}
    inputs = {
        'resource_type': 'S3',
        'task': response.task_id,
        'source_backup_id': record['backup_id'],
        'target': target,
    }
    return {"status": 200, "inputs": inputs, "msg": "completed"}
