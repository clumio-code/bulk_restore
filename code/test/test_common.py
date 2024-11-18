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

import common


class TestImportable(unittest.TestCase):
    def test_get_sort_and_ts_filter(self) -> None:
        self.assertTrue(hasattr(common, 'get_sort_and_ts_filter'))


class TestParseBaseUrl(unittest.TestCase):
    def test_parse_base_url_same(self) -> None:
        self.assertEqual(
            'us-west-2.api.clumio.com', common.parse_base_url('us-west-2.api.clumio.com')
        )

    def parse_base_url_with_https(self) -> None:
        self.assertEqual(
            'us-west-2.api.clumio.com', common.parse_base_url('https://us-west-2.api.clumio.com/')
        )
