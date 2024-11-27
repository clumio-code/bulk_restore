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
"""Unit test for common module."""

from __future__ import annotations

import datetime
import unittest
from unittest import mock

import common
import requests
from clumioapi.exceptions import clumio_exception
from clumioapi.models import (
    aws_environment,
    aws_environment_list_embedded,
    list_aws_environments_response,
    list_tasks_response,
    task_list_embedded,
    task_with_e_tag,
)


class TestUtilFunctions(unittest.TestCase):
    """Test the common util functions."""

    def setUp(self) -> None:
        api_client_patch = mock.patch('clumioapi.clumioapi_client.ClumioAPIClient')
        self.api_client = api_client_patch.start()

    def test_get_total_list(self) -> None:
        """Verify get_total_list function."""
        # Ok response.
        task_ids = ['1', '2']
        ok_response = requests.Response()
        ok_response.status_code = 200
        return_vals = [
            (
                ok_response,
                list_tasks_response.ListTasksResponse(
                    embedded=task_list_embedded.TaskListEmbedded(
                        items=[task_with_e_tag.TaskWithETag(p_id=task_id)]
                    ),
                    total_count=2,
                    total_pages_count=2,
                ),
            )
            for task_id in task_ids
        ]
        self.api_client().tasks_v1.list_task.side_effect = return_vals
        tasks_list = common.get_total_list(
            self.api_client().tasks_v1.list_task,
            api_filter='api_filter',
            sort='sort',
        )
        retrieved_task_ids = [task.p_id for task in tasks_list]
        self.assertEqual(task_ids, retrieved_task_ids)

        # Non-ok response.
        non_ok_response = requests.Response()
        non_ok_response.status_code = 401
        self.api_client().tasks_v1.list_task.side_effect = [(non_ok_response, None)]
        with self.assertRaises(clumio_exception.ClumioException):
            _ = common.get_total_list(
                self.api_client().tasks_v1.list_task,
                api_filter='api_filter',
                sort='sort',
            )

    def test_get_environment_id(self) -> None:
        """Verify get_environment_id function."""
        # Empty response.
        target_account = 'target_account'
        target_region = 'target_region'
        self.api_client().aws_environments_v1.list_aws_environments.return_value = (
            list_aws_environments_response.ListAWSEnvironmentsResponse(current_count=0)
        )
        status_code, _ = common.get_environment_id(self.api_client(), target_account, target_region)
        self.assertEqual(status_code, 402)

        # Non-empty response.
        self.api_client().aws_environments_v1.list_aws_environments.return_value = (
            list_aws_environments_response.ListAWSEnvironmentsResponse(
                embedded=aws_environment_list_embedded.AWSEnvironmentListEmbedded(
                    items=[aws_environment.AWSEnvironment(p_id='env_id')]
                ),
                current_count=1,
            )
        )
        status_code, env_id = common.get_environment_id(
            self.api_client(), target_account, target_region
        )
        self.assertEqual(status_code, 200)
        self.assertEqual(env_id, 'env_id')

    def test_filter_backup_records_by_tags(self) -> None:
        """Verify the filter_backup_records_by_tags function."""
        tag_field = 'source_asset_tags'
        target_key = 'target-key'
        backup_records = [
            {
                'asset_id': 'asset_id-1',
                'backup_record': {tag_field: [{'key': target_key, 'value': 'target-value'}]},
            },
            {
                'asset_id': 'asset_id-2',
                'backup_record': {tag_field: [{'key': target_key, 'value': 'no-value'}]},
            },
        ]
        # Empty search tag value.
        filtered_backup_records = common.filter_backup_records_by_tags(
            backup_records, target_key, None, tag_field
        )
        self.assertEqual(backup_records, filtered_backup_records)
        # Non-empty search tag value.
        filtered_backup_records = common.filter_backup_records_by_tags(
            backup_records, target_key, 'target-value', tag_field
        )
        self.assertEqual(len(filtered_backup_records), 1)
        self.assertEqual(backup_records[0]['asset_id'], 'asset_id-1')


class TestGetSortAndTSFilter(unittest.TestCase):
    """Test the get_sort_and_ts_filter function."""

    def test_get_sort_and_ts_filter_after(self) -> None:
        """Verify get_sort_and_ts_filter with 'after' direction."""
        sort, ts_filter = common.get_sort_and_ts_filter(
            'after', start_day_offset=2, end_day_offset=1
        )
        self.assertEqual(sort, common.START_TIMESTAMP_STR)
        current_timestamp = datetime.datetime.now(datetime.UTC)
        two_days_ago = current_timestamp - datetime.timedelta(2)
        two_days_ago_str = two_days_ago.strftime('%Y-%m-%d') + 'T00:00:00Z'
        one_days_ago = current_timestamp - datetime.timedelta(1)
        one_days_ago_str = one_days_ago.strftime('%Y-%m-%d') + 'T23:59:59Z'
        self.assertEqual(ts_filter[common.START_TIMESTAMP_STR]['$gt'], two_days_ago_str)
        self.assertEqual(ts_filter[common.START_TIMESTAMP_STR]['$lte'], one_days_ago_str)

    def test_get_sort_and_ts_filter_before(self) -> None:
        """Verify get_sort_and_ts_filter with 'before' direction."""
        sort, ts_filter = common.get_sort_and_ts_filter(
            'before', start_day_offset=2, end_day_offset=1
        )
        self.assertEqual(sort, f'-{common.START_TIMESTAMP_STR}')
        current_timestamp = datetime.datetime.now(datetime.UTC)
        one_days_ago = current_timestamp - datetime.timedelta(1)
        one_days_ago_str = one_days_ago.strftime('%Y-%m-%d') + 'T23:59:59Z'
        self.assertEqual(ts_filter[common.START_TIMESTAMP_STR]['$lte'], one_days_ago_str)
        self.assertIsNone(ts_filter[common.START_TIMESTAMP_STR].get('$gt', None))

    def test_get_sort_and_ts_filter_negative(self) -> None:
        """Verify get_sort_and_ts_filter with negative inputs."""
        _, ts_filter = common.get_sort_and_ts_filter('random', start_day_offset=2, end_day_offset=1)
        self.assertEqual(ts_filter, {})


class TestParseBaseUrl(unittest.TestCase):
    def test_parse_base_url_same(self) -> None:
        self.assertEqual(
            'us-west-2.api.clumio.com', common.parse_base_url('us-west-2.api.clumio.com')
        )

    def test_parse_base_url_with_https(self) -> None:
        self.assertEqual(
            'us-west-2.api.clumio.com', common.parse_base_url('https://us-west-2.api.clumio.com/')
        )
