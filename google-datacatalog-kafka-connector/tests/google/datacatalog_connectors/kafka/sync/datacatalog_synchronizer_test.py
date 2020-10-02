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

from .. import test_utils
from google.datacatalog_connectors.kafka.sync import \
    datacatalog_synchronizer
from google.datacatalog_connectors.kafka.scrape.\
    metadata_scraper import MetadataScraper
import mock


@mock.patch('google.datacatalog_connectors.kafka.scrape.metadata_scraper.'
            'MetadataScraper.'
            '__init__', lambda self, *args: None)
@mock.patch('google.datacatalog_connectors.kafka.prepare.'
            'assembled_entry_factory.AssembledEntryFactory.__init__',
            lambda self, *args: None)
@mock.patch(
    'google.datacatalog_connectors.commons.ingest.'
    'datacatalog_metadata_ingestor.DataCatalogMetadataIngestor.__init__',
    lambda self, *args: None)
@mock.patch('google.datacatalog_connectors.commons.cleanup.'
            'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.__init__',
            lambda self, *args: None)
@mock.patch('google.datacatalog_connectors.commons.monitoring.'
            'metrics_processor.MetricsProcessor.__init__',
            lambda self, *args: None)
class DatacatalogSynchronizerTestCase(unittest.TestCase):
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
    __KAKFA_PACKAGE = 'google.datacatalog_connectors.kafka'
    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'
    __PROJECT_ID = 'test_project'
    __LOCATION_ID = 'location_id'
    __HOST = 'localhost'
    __ENTRY_GROUP_ID = 'kafka'

    @mock.patch('google.datacatalog_connectors.kafka.'
                'scrape.metadata_scraper.MetadataScraper.get_metadata')
    @mock.patch('google.datacatalog_connectors.kafka.'
                'prepare.assembled_entry_factory.'
                'AssembledEntryFactory.make_entries_from_cluster_metadata')
    @mock.patch('google.datacatalog_connectors.commons.ingest.'
                'datacatalog_metadata_ingestor.'
                'DataCatalogMetadataIngestor.ingest_metadata')
    @mock.patch('google.datacatalog_connectors.commons.cleanup.'
                'datacatalog_metadata_cleaner.DataCatalogMetadataCleaner.'
                'delete_obsolete_metadata')
    @mock.patch('google.datacatalog_connectors.commons.monitoring.'
                'metrics_processor.MetricsProcessor.'
                'process_elapsed_time_metric')
    @mock.patch('google.datacatalog_connectors.commons.monitoring.'
                'metrics_processor.MetricsProcessor.'
                'process_metadata_payload_bytes_metric')
    @mock.patch('google.datacatalog_connectors.commons.monitoring.'
                'metrics_processor.MetricsProcessor.'
                'process_entries_length_metric')
    def test_synchronize_metadata_should_not_raise_error(  # noqa: E125
            self, process_entries_length_metric,
            process_metadata_payload_bytes_metric, process_elapsed_time_metric,
            delete_obsolete_metadata, ingest_metadata,
            make_entries_from_cluster_metadata, get_metadata):
        # Test that all synchronizer calls go through and it doesn't
        # break in the middle
        make_entries_from_cluster_metadata.return_value = [{}]

        synchronizer = datacatalog_synchronizer.DataCatalogSynchronizer(
            DatacatalogSynchronizerTestCase.__PROJECT_ID,
            DatacatalogSynchronizerTestCase.__LOCATION_ID,
            DatacatalogSynchronizerTestCase.__ENTRY_GROUP_ID,
            DatacatalogSynchronizerTestCase.__HOST,
            MetadataScraper,
            test_utils.FakeKafkaConsumer,
            enable_monitoring=True)

        synchronizer.run()
        self.assertEqual(get_metadata.call_count, 1)
        self.assertEqual(make_entries_from_cluster_metadata.call_count, 1)
        self.assertEqual(ingest_metadata.call_count, 1)
        self.assertEqual(delete_obsolete_metadata.call_count, 1)
        self.assertEqual(process_entries_length_metric.call_count, 1)
        self.assertEqual(process_metadata_payload_bytes_metric.call_count, 1)
        self.assertEqual(process_elapsed_time_metric.call_count, 1)
