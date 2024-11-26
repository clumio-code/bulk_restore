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

import unittest
from code import clumio_bulk_retrieve_restore_task
from unittest import mock

from aws_lambda_powertools.utilities.typing import LambdaContext
from clumioapi.models import read_task_response


class TestLambdaHandler(unittest.TestCase):
    """Test the lambda handler for retrieving restore task."""

    def setUp(self) -> None:
        """Setup method for class."""
        api_client_patch = mock.patch('clumioapi.clumioapi_client.ClumioAPIClient')
        self.api_client = api_client_patch.start()
        self.context = LambdaContext()
        self.events = {
            'bear': 'bearer_token',
            'base_url': 'base_url',
            'inputs': {'task': 'task_id'},
        }

    def test_read_task(self) -> None:
        """Verify the return when the environment id is bad."""
        # In-progress states.
        for status in ['queued', 'in_progress']:
            self.api_client().tasks_v1.read_task.return_value = read_task_response.ReadTaskResponse(
                status=status,
            )
            lambda_result = clumio_bulk_retrieve_restore_task.lambda_handler(
                self.events, self.context
            )
            self.assertEqual(lambda_result['status'], 205)
            self.assertIn('not done', lambda_result['msg'])

        # Succeed state.
        self.api_client().tasks_v1.read_task.return_value = read_task_response.ReadTaskResponse(
            status='completed',
        )
        lambda_result = clumio_bulk_retrieve_restore_task.lambda_handler(self.events, self.context)
        self.assertEqual(lambda_result['status'], 200)
        self.assertIn('completed', lambda_result['msg'])

        # Failure states.
        for status in ['failed', 'aborted']:
            self.api_client().tasks_v1.read_task.return_value = read_task_response.ReadTaskResponse(
                status=status,
            )
            lambda_result = clumio_bulk_retrieve_restore_task.lambda_handler(
                self.events, self.context
            )
            self.assertEqual(lambda_result['status'], 403)
            self.assertIn('failed', lambda_result['msg'])

    def test_lambda_handler_exists(self) -> None:
        self.assertTrue(hasattr(clumio_bulk_retrieve_restore_task, 'lambda_handler'))
