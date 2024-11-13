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
from clumioapi import configuration, clumioapi_client
import common


def lambda_handler(events, context):
    bear = events.get('bear', None)
    base_url = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account = events.get('source_account', None)
    source_region = events.get('source_region', None)
    search_tag_key = events.get('search_tag_key', None)
    search_tag_value = events.get('search_tag_value', None)
    search_direction = events.get('search_direction', None)
    start_search_day_offset_input = events.get('start_search_day_offset', 1)
    end_search_day_offset_input = events.get('end_search_day_offset', 0)
    target = events.get('target', {})

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
            error_msg = f"Read secret failed - {error}"
            return {"status": 411, "msg": error_msg}

    # Validate inputs
    try:
        start_search_day_offset = int(start_search_day_offset_input)
        end_search_day_offset = int(end_search_day_offset_input)
    except ValueError as e:
        error = f"invalid input: {e}"
        return {"status": 401, "records": [], "msg": f"failed {error}"}

    # Initiate the Clumio API client.
    if 'https' in base_url:
        base_url = base_url.split('/')[2]
    config = configuration.Configuration(api_token=bear, hostname=base_url)
    client = clumioapi_client.ClumioAPIClient(config)

    # Retrieve the list of backup records.
    sort, ts_filter = common.get_sort_and_ts_filter(
        search_direction, start_search_day_offset, end_search_day_offset
    )
    raw_backup_records = common.get_total_list(
        function=client.backup_aws_ebs_volumes_v2.list_backup_aws_ebs_volumes,
        api_filter=json.loads(ts_filter),
        sort=sort,
    )

    # Filter the result based on the source_account and source region.
    backup_records = []
    for backup in raw_backup_records:
        if backup.account_native_id == source_account and backup.aws_region == source_region:
            backup_record = {
                "volume_id": backup.volume_native_id,
                "backup_record": {
                    "source_backup_id": backup.p_id,
                    "source_volume_id": backup.volume_native_id,
                    "source_volume_tags": [tag.__dict__ for tag in backup.tags],
                    "source_encrypted_flag": backup.is_encrypted,
                    "source_az": backup.aws_az,
                    "source_kms": backup.kms_key_native_id,
                    "source_expire_time": backup.expiration_timestamp
                }
            }
            backup_records.append(backup_record)

    # Filter the result based on the tags.
    if search_tag_key and search_tag_value:
        tags_filtered_backups = []
        for backup in backup_records:
            tags = {
                tag['key']:tag['value'] for tag in backup['backup_record']['source_volume_tags']
            }
            if tags.get(search_tag_key, None) == search_tag_value:
                tags_filtered_backups.append(backup)
        backup_records = tags_filtered_backups

    if len(backup_records) == 0:
        return {"status": 207, "records": [], "target": target, "msg": "empty set"}
    else:
        return {"status": 200, "records": backup_records, "target": target, "msg": "completed"}