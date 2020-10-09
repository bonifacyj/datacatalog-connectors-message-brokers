import os
import unittest
import mock

from google.datacatalog_connectors.commons_test import utils
from google.datacatalog_connectors.kafka.prepare.\
    datacatalog_entry_factory import DataCatalogEntryFactory
from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants
from .. import test_utils


@mock.patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.entry_path')
class DataCatalogEntryFactoryTestCase(unittest.TestCase):
    __PROJECT_ID = 'test_project'
    __LOCATION_ID = 'location_id'
    __ENTRY_GROUP_ID = 'kafka'
    __MOCKED_ENTRY_PATH = 'mocked_entry_path'
    __METADATA_SERVER_HOST = 'metadata_host'
    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    def setUp(self):
        self.__entry_factory = DataCatalogEntryFactory(
            self.__PROJECT_ID, self.__LOCATION_ID, self.__METADATA_SERVER_HOST,
            self.__ENTRY_GROUP_ID)

    def test_topic_metadata_should_be_converted_to_datacatalog_entries(
            self, entry_path):
        entry_path.return_value = \
            DataCatalogEntryFactoryTestCase.__MOCKED_ENTRY_PATH

        metadata = utils.Utils.convert_json_to_object(
            self.__MODULE_PATH, 'test_metadata_one_topic.json')
        topics = metadata[MetadataConstants.TOPICS]

        for topic in topics:
            entry_id, entry = self.__entry_factory.make_entry_for_topic(topic)
            expected_entry = test_utils.get_test_topic_entry()
            self.assertIsNotNone(entry_id)
            self.assertEqual(str(entry),
                             str(expected_entry),
                             msg='Entry fields are not the same')

    def test_cluster_metadata_should_be_converted_to_dc_entries(
            self, entry_path):
        entry_path.return_value = \
            DataCatalogEntryFactoryTestCase.__MOCKED_ENTRY_PATH
        metadata = utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                                      'test_metadata.json')
        entry_id, entry = self.__entry_factory.make_entry_for_cluster(metadata)
        expected_entry = test_utils.get_test_cluster_entry()
        self.assertIsNotNone(entry_id)
        self.assertEqual(str(entry),
                         str(expected_entry),
                         msg='Entry fields are not the same')
