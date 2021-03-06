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
import mock

from google.datacatalog_connectors.commons_test import utils
from google.datacatalog_connectors.kafka import prepare
from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants
from .. import test_utils


@mock.patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.entry_path')
class AssembledEntryFactoryTestCase(unittest.TestCase):
    __PROJECT_ID = 'test_project'
    __LOCATION_ID = 'location_id'
    __ENTRY_GROUP_ID = 'kafka'
    __MOCKED_ENTRY_PATH = 'mocked_entry_path'
    __METADATA_SERVER_HOST = 'metadata_host'
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.kafka.prepare'

    def setUp(self):
        entry_factory = test_utils.FakeDataCatalogEntryFactory(
            self.__PROJECT_ID, self.__LOCATION_ID, self.__METADATA_SERVER_HOST,
            self.__ENTRY_GROUP_ID)
        tag_factory = prepare.DataCatalogTagFactory()
        self.__assembled_entry_factory = prepare.assembled_entry_factory. \
            AssembledEntryFactory(
                AssembledEntryFactoryTestCase.__ENTRY_GROUP_ID,
                entry_factory, tag_factory)
        tag_templates = {
            'kafka_cluster_metadata': {},
            'kafka_topic_metadata': {}
        }
        self.__assembled_entry_factory_with_tag_template = prepare.\
            assembled_entry_factory.AssembledEntryFactory(
                AssembledEntryFactoryTestCase.__ENTRY_GROUP_ID,
                entry_factory, tag_factory, tag_templates)

    def test_dc_entries_should_be_created_from_cluster_metadata(
            self, entry_path):
        entry_path.return_value = \
            AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH
        metadata = utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                                      'test_metadata.json')
        assembled_entries = self.__assembled_entry_factory.\
            make_entries_from_cluster_metadata(metadata)
        num_topics = len(metadata[MetadataConstants.TOPICS])
        num_clusters = 1
        self.assertEqual(num_topics + num_clusters, len(assembled_entries))

    @mock.patch('{}.'.format(__PREPARE_PACKAGE) + 'datacatalog_tag_factory.' +
                'DataCatalogTagFactory.make_tag_for_cluster')
    @mock.patch('{}.datacatalog_tag_factory.'.format(__PREPARE_PACKAGE) +
                'DataCatalogTagFactory.make_tag_for_topic')
    def test_with_tag_templates_should_be_converted_to_dc_entries_with_tags(
            self, make_tag_for_topic, make_tag_for_cluster, entry_path):
        entry_path.return_value = \
            AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH

        entry_factory = \
            self.__assembled_entry_factory_with_tag_template

        cluster_metadata = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'test_metadata.json')
        num_topics = len(cluster_metadata[MetadataConstants.TOPICS])

        prepared_entries = \
            entry_factory. \
            make_entries_from_cluster_metadata(
                cluster_metadata)

        for entry in prepared_entries:
            self.assertEqual(1, len(entry.tags))
        self.assertEqual(num_topics, make_tag_for_topic.call_count)
        self.assertEqual(1, make_tag_for_cluster.call_count)
