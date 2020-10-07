import os
import unittest
import mock

from google.cloud import datacatalog_v1beta1

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
        self.addTypeEqualityFunc(datacatalog_v1beta1.types.Entry,
                                 self.compare_entries)

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
            self.assertEqual(entry, expected_entry)

    def test_cluster_metadata_should_be_converted_to_dc_entries(
            self, entry_path):
        entry_path.return_value = \
            DataCatalogEntryFactoryTestCase.__MOCKED_ENTRY_PATH
        metadata = utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                                      'test_metadata.json')
        entry_id, entry = self.__entry_factory.make_entry_for_cluster(metadata)
        expected_entry = test_utils.get_test_cluster_entry()
        self.assertIsNotNone(entry_id)
        self.assertEqual(entry,
                         expected_entry,
                         msg='Entry fields are not the same')

    def compare_entries(self, entry1, entry2, msg=None):
        if not msg:
            msg = ""
        if str(entry1) != str(entry2):
            raise self.failureException('{}: \n {} != {}'.format(
                msg, str(entry1), str(entry2)))
