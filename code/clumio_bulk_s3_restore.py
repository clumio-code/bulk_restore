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

"""Lambda function to bulk restore S3."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import common
from clumioapi import models
from clumioapi.exceptions import clumio_exception

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext
    from common import EventsTypeDef

logger = logging.getLogger(__name__)


def lambda_handler(events: EventsTypeDef, context: LambdaContext) -> dict[str, Any]:
    """Handle the lambda function to bulk restore S3."""
    clumio_token: str | None = events.get('clumio_token', None)
    base_url: str = events.get('base_url', common.DEFAULT_BASE_URL)
    target: dict = events.get('target', {})
    record: dict = events.get('record', {})
    target_account: str | None = target.get('target_account', None)
    target_bucket: str | None = target.get('target_bucket', None)
    target_prefix: str | None = target.get('target_prefix', None)

    # If clumio bearer token is not passed as an input read it from the AWS secret.
    clumio_token = common.get_bearer_token_if_not_exists(clumio_token)

    # Initiate the Clumio API client.
    client = common.get_clumio_api_client(base_url, clumio_token)

    # Build filter to retrieve the target bucket ID.
    api_filter = {
        'account_native_id': {'$eq': target_account},
        'name': {'$in': [target_bucket]},
    }
    try:
        logger.info('List S3 buckets with filter %s...', api_filter)
        s3_buckets = common.get_total_list(
            function=client.aws_s3_buckets_v1.list_aws_s3_buckets, api_filter=json.dumps(api_filter)
        )
        if not s3_buckets:
            logger.error('Target bucket %s not found.', target_bucket)
            return {'status': 207, 'msg': 'no target bucket found', 'inputs': target}
        target_bucket_id = s3_buckets[0].p_id
        target_env_id = s3_buckets[0].environment_id
        logger.info('Found target bucket %s with ID %s.', target_bucket, target_bucket_id)

        # Build the restore request.
        source_input = models.protection_group_restore_source.ProtectionGroupRestoreSource(
            backup_id=record['backup_id'],
            object_filters=models.source_object_filters.SourceObjectFilters(
                **record['object_filters']
            ),
            protection_group_s3_asset_ids=record['pg_asset_ids'],
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

        # Send the restore request.
        logger.info('Restore protection group from backup %s...', source_input.backup_id)
        _, response = client.restored_protection_groups_v1.restore_protection_group(body=req_body)
        if not response.task_id:
            logger.error('Failed to start protection group restore task.')
            return {'status': 207, 'msg': 'restore failed', 'inputs': target}
        inputs = {
            'resource_type': 'ProtectionGroup',
            'task': response.task_id,
            'source_backup_id': record['backup_id'],
            'target': target,
        }
        logger.info('Started protection group restore task %s.', response.task_id)
        return {'status': 200, 'inputs': inputs, 'msg': 'completed'}
    except clumio_exception.ClumioException as e:
        logger.error('Protection group restore failed with exception: %s', e)
        return {'status': 401, 'msg': f'Error - {e}'}
