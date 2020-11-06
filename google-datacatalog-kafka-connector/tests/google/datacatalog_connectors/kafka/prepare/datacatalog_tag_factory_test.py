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

import os
import unittest

from google.datacatalog_connectors.kafka.config import \
    MetadataConstants, TagTemplateConstants
from google.datacatalog_connectors.kafka.prepare import \
    datacatalog_tag_factory
from google.cloud import datacatalog_v1beta1


class DataCatalogTagFactoryTest(unittest.TestCase):

    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    def test_make_tag_for_cluster_should_set_all_available_fields(self):
        tag_factory = datacatalog_tag_factory.DataCatalogTagFactory()
        tag_template = datacatalog_v1beta1.types.TagTemplate()
        tag_template.name = 'template_name'
        cluster_metadata = {
            MetadataConstants.NUM_BROKERS: 2,
            MetadataConstants.BOOTSTRAP_SERVER: 'test_address',
            MetadataConstants.TOPICS: [{}, {}]
        }

        tag = tag_factory. \
            make_tag_for_cluster(tag_template, cluster_metadata)
        self.assertEqual(2, tag.fields['num_brokers'].double_value)
        self.assertEqual('test_address',
                         tag.fields['bootstrap_address'].string_value)
        self.assertEqual(2, tag.fields['num_topics'].double_value)

    def test_make_tag_for_topic_should_set_all_available_fields(self):
        tag_factory = datacatalog_tag_factory.DataCatalogTagFactory()
        tag_template = datacatalog_v1beta1.types.TagTemplate()
        tag_template.name = 'template_name'
        topic_metadata = {
            MetadataConstants.NUM_PARTITIONS: 3,
            MetadataConstants.CLEANUP_POLICY: 'delete, compact',
            MetadataConstants.RETENTION_TIME: 500,
            MetadataConstants.RETENTION_TIME_TEXT: "500 ms",
            MetadataConstants.RETENTION_SPACE: 20,
            MetadataConstants.RETENTION_SPACE_TEXT: "20 bytes",
            MetadataConstants.MIN_COMPACTION_LAG: 100,
            MetadataConstants.MIN_COMPACTION_LAG_TEXT: "100 ms",
            MetadataConstants.MAX_COMPACTION_LAG: 172800000,
            MetadataConstants.MAX_COMPACTION_LAG_TEXT: "2 d",
            MetadataConstants.TOPIC_VALUE_SCHEMA: {
                MetadataConstants.SCHEMA_STRING: "test-schema-topic-values",
                MetadataConstants.SCHEMA_FORMAT: "AVRO",
                MetadataConstants.SCHEMA_VERSION: 9,
                MetadataConstants.SCHEMA_TYPE: "record",
                MetadataConstants.SCHEMA_NAME: "Calls"
            },
            MetadataConstants.TOPIC_KEY_SCHEMA: {
                MetadataConstants.SCHEMA_STRING: "test-schema-topic-keys",
                MetadataConstants.SCHEMA_FORMAT: "AVRO",
                MetadataConstants.SCHEMA_VERSION: 2
            }
        }
        tag = tag_factory.make_tag_for_topic(tag_template, topic_metadata)
        fields = TagTemplateConstants.get_constants_for_topic_tag_template()
        self.assertEqual(3,
                         tag.fields[fields.num_partitions.name].double_value)
        self.assertEqual('delete, compact',
                         tag.fields[fields.cleanup_policy.name].string_value)
        self.assertEqual(500,
                         tag.fields[fields.retention_time.name].double_value)
        self.assertEqual(
            "500 ms",
            tag.fields[fields.retention_time_as_text.name].string_value)
        self.assertEqual(20,
                         tag.fields[fields.retention_space.name].double_value)
        self.assertEqual(
            "20 bytes",
            tag.fields[fields.retention_space_as_text.name].string_value)
        self.assertEqual(
            100, tag.fields[fields.min_compaction_lag.name].double_value)
        self.assertEqual(
            "100 ms",
            tag.fields[fields.min_compaction_lag_as_text.name].string_value)
        self.assertEqual(
            172800000, tag.fields[fields.max_compaction_lag.name].double_value)
        self.assertEqual(
            "2 d",
            tag.fields[fields.max_compaction_lag_as_text.name].string_value)
        self.assertEqual(
            "test-schema-topic-values",
            tag.fields[fields.payload_physical_schema.name].string_value)
        self.assertEqual(
            "AVRO", tag.fields[
                fields.payload_physical_schema_format.name].string_value)
        self.assertEqual(
            9, tag.fields[
                fields.payload_physical_schema_version.name].double_value)
        self.assertEqual(
            "test-schema-topic-keys",
            tag.fields[fields.keys_physical_schema.name].string_value)
        self.assertEqual(
            "AVRO",
            tag.fields[fields.keys_physical_schema_format.name].string_value)
        self.assertEqual(
            2,
            tag.fields[fields.keys_physical_schema_version.name].double_value)
        self.assertEqual(
            "record",
            tag.fields[fields.payload_physical_schema_type.name].string_value)
        self.assertEqual(
            "Calls",
            tag.fields[fields.payload_physical_schema_name.name].string_value)
