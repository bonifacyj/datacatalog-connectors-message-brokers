#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from google.datacatalog_connectors.kafka.scrape.metadata_values_converter \
    import MetadataValuesConverter


class MetadataValuesConverterTest(unittest.TestCase):

    def test_duration_is_converted_to_human_redable_format(self):
        input_in_ms = 31764000000
        expected_output = '1 y 2 d 15 h 20 min'
        output = MetadataValuesConverter.get_human_readable_duration_value(
            input_in_ms)
        self.assertEqual(output, expected_output)

    def test_size_is_converted_to_human_readable_format(self):
        input_in_bytes = 38117834752
        expected_output = '35.5 GB'
        output = MetadataValuesConverter.get_human_readable_size_value(
            input_in_bytes)
        self.assertEqual(output, expected_output)

    def test_size_converter_returns_tb_for_very_big_values(self):
        input_in_bytes = 263880371700000000
        output = MetadataValuesConverter.get_human_readable_size_value(
            input_in_bytes)
        self.assertTrue('TB' in output)
