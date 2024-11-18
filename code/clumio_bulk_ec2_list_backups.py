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

"""Lambda function to retrieve the EC2 backup list."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import boto3
import botocore.exceptions
import common
from clumioapi import clumioapi_client, configuration

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from clumioapi.models.ec2_backup import EC2Backup

def backup_record_obj_to_dict(backup: EC2Backup) -> dict:
    """Convert backup record object to dictionary."""
    ebs_mappings = []
    for ebs_vol in backup.attached_backup_ebs_volumes:
        ebs_mapping = ebs_vol.__dict__
        ebs_mapping['id'] = ebs_mapping.pop('p_id')
        ebs_mapping['type'] = ebs_mapping.pop('p_type')
        ebs_mapping['tags'] = [tag.__dict__ for tag in ebs_mapping['tags']]
        ebs_mappings.append(ebs_mapping)
    return {
        'instance_id': backup.instance_id,
        'backup_record': {
            'source_backup_id': backup.p_id,
            'SourceAmiId': backup.ami.ami_native_id,
            'source_iam_instance_profile_name': backup.iam_instance_profile,
            'SourceKeyPairName': backup.key_pair_name,
            'source_network_interface_list': [ni.__dict__ for ni in backup.network_interfaces],
            'source_ebs_storage_list': ebs_mappings,
            'source_instance_tags': [tag.__dict__ for tag in backup.tags],
            'SourceVPCID': backup.vpc_native_id,
            'source_az': backup.aws_az,
            'source_expire_time': backup.expiration_timestamp
        }
    }

def lambda_handler(events, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to retrieve the EC2 backup list."""
    bear = events.get('bear', None)
    base_url = events.get('base_url', common.DEFAULT_BASE_URL)
    source_account = events.get('source_account', None)
    source_region = events.get('source_region', None)
    search_tag_key = events.get('search_tag_key', None)
    search_tag_value = events.get('search_tag_value', None)
    target = events.get('target',{})
    search_direction = target.get('search_direction', None)
    start_search_day_offset_input = target.get('start_search_day_offset', 0)
    end_search_day_offset_input = target.get('end_search_day_offset', 10)

    # If clumio bearer token is not passed as an input read it from the AWS secret
    if not bear:
        bearer_secret = 'clumio/token/bulk_restore'  # noqa: S105
        secretsmanager = boto3.client('secretsmanager')
        try:
            secret_value = secretsmanager.get_secret_value(SecretId=bearer_secret)
            secret_dict = json.loads(secret_value['SecretString'])
            bear = secret_dict.get('token', None)
        except botocore.exceptions.ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f'Read secret failed - {error}'
            return {'status': 411, 'msg': error_msg}

    # Validate inputs
    try:
        start_search_day_offset = int(start_search_day_offset_input)
        end_search_day_offset = int(end_search_day_offset_input)
    except ValueError as e:
        error = f'invalid start/end search day offset: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}

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
        function=client.backup_aws_ec2_instances_v1.list_backup_aws_ec2_instances,
        api_filter=json.dumps(ts_filter),
        sort=sort,
    )

    # Filter the result based on the source_account and source region.
    backup_records = []
    for backup in raw_backup_records:
        if backup.account_native_id == source_account and backup.aws_region == source_region:
            backup_record = backup_record_obj_to_dict(backup)
            backup_records.append(backup_record)

    # Filter the result based on the tags.
    backup_records = common.filter_backup_records_by_tags(
        backup_records, search_tag_key, search_tag_value, 'source_instance_tags'
    )

    if not backup_records:
        return {'status': 207, 'records': [], 'target': target, 'msg': 'empty set'}
    return {'status': 200, 'records': backup_records, 'target': target, 'msg': 'completed'}
