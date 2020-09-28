import os
import unittest

from google.datacatalog_connectors.commons_test import utils
from google.datacatalog_connectors.kafka.prepare.datacatalog_entry_factory import DataCatalogEntryFactory
import mock


@mock.patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.entry_path')
class DataCatalogEntryFactoryTestCase(unittest.TestCase):
    __PROJECT_ID = 'test_project'
    __LOCATION_ID = 'location_id'
    __ENTRY_GROUP_ID = 'kafka'
    __MOCKED_ENTRY_PATH = 'mocked_entry_path'

    __METADATA_SERVER_HOST = 'metadata_host'
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.kafka.prepare'

    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    def setUp(self):
        self.__entry_factory = DataCatalogEntryFactory(
            self.__PROJECT_ID, self.__LOCATION_ID, self.__METADATA_SERVER_HOST,
            self.__ENTRY_GROUP_ID)

    def test_topic_metadata_should_be_converted_to_datacatalog_entries(
            self, entry_path):
        entry_path.return_value = \
            DataCatalogEntryFactoryTestCase.__MOCKED_ENTRY_PATH

        metadata = utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                                      'test_metadata.json')
        topics = metadata["topics"]

        for topic in topics:
            entry_id, entry = self.__entry_factory.make_entry_for_topic(topic)
            self.assertIsNotNone(entry_id)
            self.assertEqual('kafka_topic', entry.user_specified_type)
            self.assertEqual('kafka', entry.user_specified_system)
            self.assertEqual(
                DataCatalogEntryFactoryTestCase.__MOCKED_ENTRY_PATH,
                entry.name)
            self.assertIn(
                DataCatalogEntryFactoryTestCase.__METADATA_SERVER_HOST,
                entry.linked_resource)
            self.assertIn(topic, entry.display_name)

    def test_cluster_without_topics_should_be_converted_to_dc_entry(  # noqa
            self, entry_path):
        pass  # it is here so that I don't forget to implement it in the next iteration