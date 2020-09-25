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

import abc
import argparse
import logging
import os
import sys

from google.datacatalog_connectors.kafka.scrape import metadata_scraper
from google.datacatalog_connectors.kafka.sync import \
    datacatalog_synchronizer

ABC = abc.ABCMeta('ABC', (object,), {})  # compatible with Python 2 *and* 3


class DatacatalogCli(ABC):

    def run(self, argv):
        """Runs the command line."""

        args = self._parse_args(argv)
        # Enable logging
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        if args.service_account_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] \
                = args.service_account_path

        self._get_datacatalog_synchronizer()(
            project_id=args.datacatalog_project_id,
            location_id=args.datacatalog_location_id,
            entry_group_id=self._get_entry_group_id(args),
            kafka_host=self._get_host_arg(args),
            metadata_scraper=self._get_metadata_scraper(),
            connection_args=self._get_connection_args(args),
            enable_monitoring=args.enable_monitoring).run()

    def _get_datacatalog_synchronizer(self):
        return datacatalog_synchronizer.DataCatalogSynchronizer

    def _get_metadata_scraper(self):
        return metadata_scraper.MetadataScraper

    def _get_host_arg(self, args):
        return args.kafka_host

    def _get_entry_group_id(self, args):
        return args.datacatalog_entry_group_id or 'kafka'

    def _parse_args(self, argv):
        parser = argparse.ArgumentParser(
            description='Command line to sync kafka '
            'metadata to Datacatalog')

        parser.add_argument('--datacatalog-project-id',
                            help='Your Google Cloud project ID',
                            required=True)
        parser.add_argument(
            '--datacatalog-location-id',
            help='Location ID to be used for your Google Cloud Datacatalog',
            required=True)
        parser.add_argument('--datacatalog-entry-group-id',
                            help='Entry group ID to be used for your Google '
                            'Cloud Datacatalog')
        parser.add_argument('--kafka-host',
                            help='Your kafka server host',
                            required=True)

        parser.add_argument('--service-account-path',
                            help='Local Service Account path '
                            '(Can be suplied as '
                            'GOOGLE_APPLICATION_CREDENTIALS env '
                            'var)')
        parser.add_argument('--enable-monitoring',
                            help='Enables monitoring metrics on the connector')
        return parser.parse_args(argv)

    def _get_connection_args(self, args):
        return {'bootstrap.servers': args.kafka_host}


def main():
    argv = sys.argv
    DatacatalogCli().run(argv[1:] if len(argv) > 0 else argv)
