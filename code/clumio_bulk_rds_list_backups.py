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

"""Lambda function to retrieve the RDS backup list."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import boto3
import botocore.exceptions
from clumio_sdk_v13 import RDSBackupList

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to retrieve the RDS backup list."""
    bear = events.get('bear', None)
    source_account = events.get('source_account', None)
    source_region = events.get('source_region', None)
    search_tag_key = events.get('search_tag_key', None)
    search_tag_value = events.get('search_tag_value', None)
    search_direction = events.get('search_direction', None)
    start_search_day_offset_input = events.get('start_search_day_offset', 0)
    end_search_day_offset_input = events.get('end_search_day_offset', 10)
    target = events.get('target', {})
    debug_input = events.get('debug', 0)

    # Validate inputs
    try:
        start_search_day_offset = int(start_search_day_offset_input)
        end_search_day_offset = int(end_search_day_offset_input)
        debug = int(debug_input)
    except ValueError as e:
        error = f'invalid task id: {e}'
        return {'status': 401, 'records': [], 'msg': f'failed {error}'}

    # If clumio bearer token is not passed as an input read it from the AWS secret
    if not bear:
        bearer_secret = 'clumio/token/bulk_restore'  # noqa: S105
        secretsmanager = boto3.client('secretsmanager')
        try:
            secret_value = secretsmanager.get_secret_value(SecretId=bearer_secret)
            secret_dict = json.loads(secret_value['SecretString'])
            # username = secret_dict.get('username', None)
            bear = secret_dict.get('token', None)
        except botocore.exceptions.ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f'Describe Volume failed - {error}'
            return {'status': 411, 'msg': error_msg}

    # Initiate List RDS Backups
    rds_backup_list_api = RDSBackupList()
    base_url = events.get('base_url', None)
    if base_url:
        rds_backup_list_api.set_url_prefix(base_url)
    rds_backup_list_api.set_token(bear)
    rds_backup_list_api.set_debug(debug)

    rds_backup_list_api.set_aws_account_id(source_account)
    rds_backup_list_api.set_aws_region(source_region)
    # Set search parameters
    if search_tag_key and search_tag_value:
        rds_backup_list_api.rds_search_by_tag(search_tag_key, search_tag_value)
    if search_direction == 'forwards':
        rds_backup_list_api.set_search_forwards_from_offset(end_search_day_offset)
    elif search_direction == 'backwards':
        rds_backup_list_api.set_search_backwards_from_offset(
            start_search_day_offset, end_search_day_offset
        )

    # Run search
    rds_backup_list_api.run_all()

    result_dict = rds_backup_list_api.rds_parse_results('restore')
    rds_backup_records = result_dict.get('records', [])
    if not rds_backup_records:
        return {'status': 207, 'records': [], 'target': target, 'msg': 'empty set'}
    return {'status': 200, 'records': rds_backup_records, 'target': target, 'msg': 'completed'}
