import mock
from google.cloud import datacatalog_v1beta1
from confluent_kafka.admin import ClusterMetadata, \
    TopicMetadata, BrokerMetadata
from google.datacatalog_connectors.kafka.prepare.\
    datacatalog_entry_factory import DataCatalogEntryFactory
from google.datacatalog_connectors.kafka.datacatalog_cli import DatacatalogCli


class FakeDataCatalogEntryFactory(DataCatalogEntryFactory):

    def make_entry_for_topic(self, topic):
        entry = mock.MagicMock()
        entry_id = topic
        return entry_id, entry


class FakeKafkaConsumer(mock.MagicMock):

    def list_topics(self, timeout=-1):
        raw_metadata = ClusterMetadata()
        raw_metadata.topics = {
            "testTopic0": TopicMetadata(),
            "testTopic1": TopicMetadata()
        }
        raw_metadata.cluster_id = "TestClusterID"
        raw_metadata.brokers = {
            "testBroker0": BrokerMetadata(),
            "testBroker1": BrokerMetadata()
        }
        return raw_metadata


class FakeDataCatalogCLI(DatacatalogCli):

    def _get_kafka_consumer(self, args):
        return FakeKafkaConsumer()

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


def get_test_topic_entry():
    """
    This method creates a test Entry based on metadata
    provided in test_data/test_metadata_one_topic.json file
    :return: test DC Entry
    """
    entry = datacatalog_v1beta1.types.Entry()
    entry.user_specified_type = 'kafka_topic'
    entry.user_specified_system = 'kafka'
    entry.display_name = 'temperature'
    entry.name = 'mocked_entry_path'
    entry.linked_resource = '//metadata_host//temperature'
    return entry


def get_test_cluster_entry():
    """
    This method creates a test Entry based on metadata
    provided in test_data/test_metadata.json file
    :return: test DC Entry
    """
    entry = datacatalog_v1beta1.types.Entry()
    entry.user_specified_type = 'kafka_cluster'
    entry.user_specified_system = 'kafka'
    entry.display_name = 'Kafka cluster 1234'
    entry.name = 'mocked_entry_path'
    entry.linked_resource = '//metadata_host//1234'
    return entry
