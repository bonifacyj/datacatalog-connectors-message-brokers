import mock
from google.datacatalog_connectors.kafka.prepare.datacatalog_entry_factory import DataCatalogEntryFactory
from google.datacatalog_connectors.kafka.scrape.metadata_scraper import MetadataScraper
from google.datacatalog_connectors.kafka.datacatalog_cli import DatacatalogCli


class FakeDataCatalogEntryFactory(DataCatalogEntryFactory):

    def make_entry_for_topic(self, topic):
        entry = mock.MagicMock()
        entry_id = topic
        return entry_id, entry


class FakeMetadataScraper(MetadataScraper):

    def _get_metadata_from_message_broker_connection(self, connection_args):
        return mock.MagicMock()


class FakeDataCatalogCLI(DatacatalogCli):

    def _get_metadata_scraper(self):
        return FakeMetadataScraper

    def _parse_args(self, argv):
        args = MockedObject()
        args.datacatalog_project_id = "project_id"
        args.datacatalog_location_id = "location_id"
        args.datacatalog_entry_group_id = "kafka"
        args.kafka_host = "test_address"
        args.service_account_path = "path_to_service_account"
        args.enable_monitoring = True
        return args


class MockedObject(object):

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]
