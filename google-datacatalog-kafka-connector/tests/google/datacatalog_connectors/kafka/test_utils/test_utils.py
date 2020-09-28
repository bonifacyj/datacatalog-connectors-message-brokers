import mock
from google.datacatalog_connectors.kafka.prepare.datacatalog_entry_factory import DataCatalogEntryFactory


class FakeDataCatalogEntryFactory(DataCatalogEntryFactory):

    def make_entry_for_topic(self, topic):
        entry = mock.MagicMock()
        entry_id = topic
        return entry_id, entry
