import os
import unittest
import mock

from google.datacatalog_connectors.commons_test import utils
from google.datacatalog_connectors.kafka import prepare
from .. import test_utils


@mock.patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.entry_path')
class AssembledEntryFactoryTestCase(unittest.TestCase):
    __PROJECT_ID = 'test_project'
    __LOCATION_ID = 'location_id'
    __ENTRY_GROUP_ID = 'kafka'
    __MOCKED_ENTRY_PATH = 'mocked_entry_path'

    __METADATA_SERVER_HOST = 'metadata_host'
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.kafka.prepare'

    __MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

    def setUp(self):
        entry_factory = test_utils.FakeDataCatalogEntryFactory(
            self.__PROJECT_ID, self.__LOCATION_ID, self.__METADATA_SERVER_HOST,
            self.__ENTRY_GROUP_ID)
        self.__assembled_entry_factory = prepare.assembled_entry_factory. \
            AssembledEntryFactory(
                AssembledEntryFactoryTestCase.__ENTRY_GROUP_ID, entry_factory)

    def test_dc_entries_should_be_created_from_cluster_metadata(
            self, entry_path):
        entry_path.return_value = \
            AssembledEntryFactoryTestCase.__MOCKED_ENTRY_PATH
        metadata = utils.Utils.convert_json_to_object(self.__MODULE_PATH,
                                                      'test_metadata.json')
        assembled_entries = self.__assembled_entry_factory.\
            make_entries_from_cluster_metadata(metadata)
        topics = metadata["topics"]
        self.assertEqual(len(topics), len(assembled_entries))
