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
from google.datacatalog_connectors.kafka.scrape.\
    metadata_scraper import MetadataScraper


class MetadataScraperTestCase(unittest.TestCase):
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.kafka.scrape'

    def test_scrape_metadata_with_credentials_should_return_objects(self):
        scraper = test_utils.FakeMetadataScraper()
        metadata = scraper.get_metadata(
            connection_args={'bootstrap.servers': 'fake_host'})
        self.assertEqual(1, len(metadata))

    def test_scrape_metadata_on_connection_exception_should_re_raise(self):
        scraper = MetadataScraper()
        self.assertRaises(Exception,
                          scraper.get_metadata,
                          connection_args={'bootstrap.servers': 'fake_host'})
