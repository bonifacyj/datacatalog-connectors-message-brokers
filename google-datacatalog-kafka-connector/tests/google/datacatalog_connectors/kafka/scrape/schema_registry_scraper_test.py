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

from .. import test_utils

from google.datacatalog_connectors.kafka.scrape.\
    schema_registry_scraper import SchemaRegistryScraper
from google.datacatalog_connectors.kafka.config import MetadataConstants


class MetadataScraperTestCase(unittest.TestCase):

    def test_scrape_schema_metadata_returns_metadata_dict(self):
        schema_registry_client = test_utils.FakeKafkaSchemaRegistryClient()
        scraper = SchemaRegistryScraper(schema_registry_client)
        expected_metadata = {
            MetadataConstants.TOPIC_KEY_SCHEMA: {
                MetadataConstants.SCHEMA_TYPE: "AVRO",
                MetadataConstants.SCHEMA_VERSION: 1,
                MetadataConstants.SCHEMA_STRING:
                    '{"type":"record","name":"updates", '
                    '"fields":[{"name":"id","type":"string"},'
                    '{"name":"degrees","type":"double"}]}'
            },
            MetadataConstants.TOPIC_VALUE_SCHEMA: {
                MetadataConstants.SCHEMA_TYPE: "AVRO",
                MetadataConstants.SCHEMA_VERSION: 1,
                MetadataConstants.SCHEMA_STRING:
                    '{"type":"record","name":"updates", '
                    '"fields":[{"name":"id","type":"string"},'
                    '{"name":"degrees","type":"double"}]}'
            }
        }
        metadata = scraper.scrape_schema_metadata(topic_name="temperature")
        self.maxDiff = None
        self.assertEqual(metadata, expected_metadata)

    def test_scrape_schema_metadata_with_no_valid_subjects(self):
        schema_registry_client = test_utils.FakeKafkaSchemaRegistryClient()
        scraper = SchemaRegistryScraper(schema_registry_client)
        expected_metadata = {}
        metadata = scraper.scrape_schema_metadata(topic_name="humidity")
        self.assertEqual(metadata, expected_metadata)
