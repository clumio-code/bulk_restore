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
"""Unit test for clumio_bulk_retrieve_restore_task."""

from __future__ import annotations

import unittest
from collections.abc import Generator
from typing import Any
from unittest import mock

import clumio_bulk_retrieve_restore_task
import common
from aws_lambda_powertools.utilities.typing import LambdaContext
from clumioapi.models import read_task_response


def _fake_simple_timer(*_args: Any, **_kwargs: Any) -> Generator[int]:
    """Yield once then raise TimeoutException so the lambda's poll loop exits fast in tests."""
    yield 0
    raise common.TimeoutException('test timeout')


class TestLambdaHandler(unittest.TestCase):
    """Test the lambda handler for retrieving restore task."""

    def setUp(self) -> None:
        """Setup method for class."""
        api_client_patch = mock.patch('clumioapi.clumioapi_client.ClumioAPIClient')
        self.api_client = api_client_patch.start()
        timer_patch = mock.patch(
            'clumio_bulk_retrieve_restore_task.common.simple_timer',
            side_effect=_fake_simple_timer,
        )
        timer_patch.start()
        self.context = LambdaContext()
        self.events = {
            'bear': 'bearer_token',
            'base_url': 'base_url',
            'inputs': {'task': 'task_id'},
        }

    def test_read_task_in_progress_raises(self) -> None:
        """In-progress statuses raise RestoreInProgress so SFN retries the Task Lambda."""
        for status in ['queued', 'in_progress']:
            self.api_client().tasks_v1.read_task.return_value = read_task_response.ReadTaskResponse(
                Status=status,
            )
            with self.assertRaises(clumio_bulk_retrieve_restore_task.RestoreInProgress):
                clumio_bulk_retrieve_restore_task.lambda_handler(self.events, self.context)

    def test_read_task_completed(self) -> None:
        """A completed task returns status 200."""
        self.api_client().tasks_v1.read_task.return_value = read_task_response.ReadTaskResponse(
            Status='completed',
        )
        result = clumio_bulk_retrieve_restore_task.lambda_handler(self.events, self.context)
        self.assertEqual(result['status'], 200)
        self.assertIn('completed', result['msg'])

    def test_read_task_failure_states(self) -> None:
        """Failed or aborted tasks return status 403."""
        for status in ['failed', 'aborted']:
            self.api_client().tasks_v1.read_task.return_value = read_task_response.ReadTaskResponse(
                Status=status,
            )
            result = clumio_bulk_retrieve_restore_task.lambda_handler(self.events, self.context)
            self.assertEqual(result['status'], 403)
            self.assertIn('failed', result['msg'])

    def test_lambda_handler_exists(self) -> None:
        """Verify the lambda handler exists."""
        self.assertTrue(hasattr(clumio_bulk_retrieve_restore_task, 'lambda_handler'))
