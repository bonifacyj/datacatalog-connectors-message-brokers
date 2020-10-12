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

from confluent_kafka import Consumer

from .. import test_utils
from google.datacatalog_connectors.kafka.scrape.\
    metadata_scraper import MetadataScraper


class MetadataScraperTestCase(unittest.TestCase):

    __HOST = 'fake_host'

    def test_scrape_metadata_with_credentials_should_return_objects(self):
        kafka_consumer = test_utils.FakeKafkaConsumer()
        scraper = MetadataScraper(kafka_consumer, self.__HOST)
        metadata = scraper.get_metadata()
        self.assertGreater(len(metadata), 0)

    def test_scrape_metadata_on_connection_exception_should_re_raise(self):
        test_config = {'bootstrap.servers': self.__HOST, 'group.id': 'test_id'}
        consumer = Consumer(test_config)
        scraper = MetadataScraper(consumer, self.__HOST)
        self.assertRaises(Exception, scraper.get_metadata)
