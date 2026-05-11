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
from clumioapi.exceptions import clumio_exception
from clumioapi.models import (
    aws_environment,
    aws_environment_list_embedded,
    aws_environment_list_links,
    hateoas_next_link,
    list_aws_environments_response,
    list_tasks_response,
    task_list_embedded,
    task_list_links,
    task_with_e_tag,
)


class TestUtilFunctions(unittest.TestCase):
    """Test the common util functions."""

    def setUp(self) -> None:
        api_client_patch = mock.patch('clumioapi.clumioapi_client.ClumioAPIClient')
        self.api_client = api_client_patch.start()

    def test_get_total_list_paginates_via_links_next(self) -> None:
        """get_total_list follows Links.Next.Href across pages and aggregates Items."""
        page1 = list_tasks_response.ListTasksResponse(
            Embedded=task_list_embedded.TaskListEmbedded(
                Items=[task_with_e_tag.TaskWithETag(Id='1')]
            ),
            Links=task_list_links.TaskListLinks(
                Next=hateoas_next_link.HateoasNextLink(Href='/tasks?start=page2'),
            ),
        )
        page2 = list_tasks_response.ListTasksResponse(
            Embedded=task_list_embedded.TaskListEmbedded(
                Items=[task_with_e_tag.TaskWithETag(Id='2')]
            ),
            Links=task_list_links.TaskListLinks(),  # no Next → terminate
        )
        self.api_client().tasks_v1.list_task.side_effect = [page1, page2]
        tasks_list = common.get_total_list(
            self.api_client().tasks_v1.list_task,
            api_filter={'k': {'$eq': 'v'}},
            sort='sort',
        )
        self.assertEqual([t.Id for t in tasks_list], ['1', '2'])

    def test_get_total_list_propagates_clumio_exception(self) -> None:
        """If the list method raises ClumioException, get_total_list lets it propagate."""
        self.api_client().tasks_v1.list_task.side_effect = clumio_exception.ClumioException(
            'list failed'
        )
        with self.assertRaises(clumio_exception.ClumioException):
            common.get_total_list(self.api_client().tasks_v1.list_task, api_filter='{}')

    def test_get_environment_id_empty(self) -> None:
        """Empty environments list returns ERROR_CODE."""
        self.api_client().aws_environments_v1.list_aws_environments.return_value = (
            list_aws_environments_response.ListAWSEnvironmentsResponse(CurrentCount=0)
        )
        status_code, _ = common.get_environment_id(self.api_client(), 'acct', 'us-west-2')
        self.assertEqual(status_code, common.ERROR_CODE)

    def test_get_environment_id_found(self) -> None:
        """A non-empty result returns 200 and the first environment's Id."""
        self.api_client().aws_environments_v1.list_aws_environments.return_value = (
            list_aws_environments_response.ListAWSEnvironmentsResponse(
                Embedded=aws_environment_list_embedded.AWSEnvironmentListEmbedded(
                    Items=[aws_environment.AWSEnvironment(Id='env_id')]
                ),
                CurrentCount=1,
            )
        )
        status_code, env_id = common.get_environment_id(self.api_client(), 'acct', 'us-west-2')
        self.assertEqual(status_code, common.STATUS_OK)
        self.assertEqual(env_id, 'env_id')

    def test_get_environment_id_clumio_exception(self) -> None:
        """A ClumioException is captured and returned as ERROR_CODE + message."""
        self.api_client().aws_environments_v1.list_aws_environments.side_effect = (
            clumio_exception.ClumioException('list failed')
        )
        status_code, msg = common.get_environment_id(self.api_client(), 'acct', 'us-west-2')
        self.assertEqual(status_code, common.ERROR_CODE)
        self.assertIn('Error', msg)

    def test_passthrough_filter_dict(self) -> None:
        """PassthroughFilter serializes a dict to JSON in query_str."""
        f = common.PassthroughFilter({'name': {'$eq': 'x'}})
        self.assertEqual(f.query_str, '{"name": {"$eq": "x"}}')

    def test_passthrough_filter_str(self) -> None:
        """PassthroughFilter passes a pre-serialized JSON string through unchanged."""
        f = common.PassthroughFilter('{"k": 1}')
        self.assertEqual(f.query_str, '{"k": 1}')

    def test_make_filter_none(self) -> None:
        """make_filter returns None for None input (no filter applied)."""
        self.assertIsNone(common.make_filter(None))

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
