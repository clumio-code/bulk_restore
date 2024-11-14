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
import datetime
import json
from clumioapi import configuration, clumioapi_client
import common


def lambda_handler(events, context):
    bear = events.get('bear', None)
    base_url = events.get('base_url', None)
    start_search_day_offset_input = int(events.get('start_search_day_offset', 0))
    end_search_day_offset_input = int(events.get('end_search_day_offset', 0))
    target = events.get('target', {})
    object_filters = events.get('search_object_filters', {})
    search_direction = events.get('search_direction', None)

    if 'latest_version_only' not in object_filters:
        object_filters['latest_version_only'] = True

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

    # List protection group based on the name.
    search_name = events.get('search_pg_name', None)
    api_filter = '{"name": {"$eq": "' + search_name + '"}}'
    response = client.protection_groups_v1.list_protection_groups(filter=api_filter)
    if response.total_count == 0:
        return {"status": 207, "records": [], "target": target, "msg": "empty set of pg list"}
    pg_id = response.embedded.items[0].p_id

    # List S3 assets based on the bucket names and pg name.
    s3_bucket_names = events.get('search_bucket_names', None)
    api_filter = '{"protection_group_id": {"$eq": "' + pg_id + '"}}'
    response = client.protection_groups_s3_assets_v1.list_protection_group_s3_assets(
        filter=api_filter
    )
    if response.total_count == 0:
        return {"status": 207, "records": [], "target": target, "msg": "empty set of pg s3 assets"}
    if not s3_bucket_names:
        asset_ids = [item.p_id for item in response.embedded.items]
    else:
        asset_ids = [item.p_id for item in response.embedded.items if item.bucket_name in s3_bucket_names]

    # List pg backups based on the time filter and pg id filter.
    sort, ts_filter = common.get_sort_and_ts_filter(
        search_direction, start_search_day_offset_input, end_search_day_offset_input
    )
    ts_filter['protection_group_id'] = {'$eq': pg_id}
    response = client.backup_protection_groups_v1.list_backup_protection_groups(
        filter=json.loads(ts_filter), sort=sort
    )
    if response.total_count == 0:
        return {"status": 207, "records": [], "target": target, "msg": "empty set"}
    else:
        records = []
        for item in response.embedded.items:
            records.append({
                "backup_id": item.p_id,
                "protection_group_s3_asset_ids": asset_ids,
                "object_filters": object_filters,
            })
        return {"status": 200, "records": records[:1], "target": target, "msg": "completed"}
