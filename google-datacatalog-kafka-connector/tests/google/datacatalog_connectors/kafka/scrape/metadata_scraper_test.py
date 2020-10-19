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

from confluent_kafka.admin import AdminClient

from .. import test_utils

from google.datacatalog_connectors.commons_test import utils
from google.datacatalog_connectors.kafka.scrape.\
    metadata_scraper import MetadataScraper


class MetadataScraperTestCase(unittest.TestCase):

    __HOST = 'fake_host'
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    def test_scrape_metadata_with_credentials_should_return_objects(self):
        kafka_admin_client = test_utils.FakeKafkaAdminClient()
        scraper = MetadataScraper(kafka_admin_client, self.__HOST)
        metadata = scraper.get_metadata()
        self.assertGreater(len(metadata), 0)

    def test_scrape_metadata_on_connection_exception_should_re_raise(self):
        test_config = {'bootstrap.servers': self.__HOST}
        admin_client = AdminClient(test_config)
        scraper = MetadataScraper(admin_client, self.__HOST)
        self.assertRaises(Exception, scraper.get_metadata)

    def test_scrape_metadata_should_describe_all_fields(self):
        kafka_admin_client = test_utils.FakeKafkaAdminClient()
        scraper = MetadataScraper(kafka_admin_client, self.__HOST)
        metadata = scraper.get_metadata()
        expected_metadata = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'test_metadata_one_topic.json')
        self.maxDiff = None
        self.assertEqual(metadata, expected_metadata)

    def test_scrape_cluster_without_topics_should_return_cluster_metadata(
            self):
        kafka_admin_client = test_utils.FakeKafkaAdminClientEmptyCluster()
        scraper = MetadataScraper(kafka_admin_client, self.__HOST)
        metadata = scraper.get_metadata()
        expected_metadata = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'test_metadata_no_topics.json')
        self.maxDiff = None
        self.assertEqual(metadata, expected_metadata)
