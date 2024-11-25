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

from __future__ import annotations

import datetime
import unittest
from code import common
from unittest import mock

from clumioapi.models import list_tasks_response, task_list_embedded, task_with_e_tag


class TestUtilFunctions(unittest.TestCase):
    """Test the common util functions."""

    def setUp(self):
        api_client_patch = mock.patch('clumioapi.clumioapi_client.ClumioAPIClient')
        self.api_client = api_client_patch.start()

    def test_get_total_list(self):
        """Verify get_total_list function."""
        task_ids = ['1', '2']
        return_vals = [
            list_tasks_response.ListTasksResponse(
                embedded=task_list_embedded.TaskListEmbedded(
                    items=[task_with_e_tag.TaskWithETag(p_id=task_id)]
                ),
                total_count=2,
                total_pages_count=2,
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
