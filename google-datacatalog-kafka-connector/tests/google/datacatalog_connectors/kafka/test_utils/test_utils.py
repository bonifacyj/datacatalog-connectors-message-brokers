import mock
from google.datacatalog_connectors.kafka.prepare.datacatalog_entry_factory import DataCatalogEntryFactory
from google.datacatalog_connectors.kafka.scrape.metadata_scraper import MetadataScraper


class FakeDataCatalogEntryFactory(DataCatalogEntryFactory):

    def make_entry_for_topic(self, topic):
        entry = mock.MagicMock()
        entry_id = topic
        return entry_id, entry


class FakeMetadataScraper(MetadataScraper):

    def _get_metadata_from_message_broker_connection(self, connection_args):
        return mock.MagicMock()
