import confluent_kafka
import mock
from google.cloud import datacatalog_v1beta1
from confluent_kafka.admin import ClusterMetadata, \
    TopicMetadata, BrokerMetadata, PartitionMetadata,\
    ConfigEntry, ConfigResource
from google.datacatalog_connectors.kafka.prepare.\
    datacatalog_entry_factory import DataCatalogEntryFactory


class FakeDataCatalogEntryFactory(DataCatalogEntryFactory):

    def make_entry_for_topic(self, topic):
        entry = mock.MagicMock()
        entry_id = topic
        return entry_id, entry


class FakeKafkaAdminClient(mock.MagicMock):

    def list_topics(self, timeout=-1):
        '''
        Mock Consumer returns a description of
        topic from test_data/test_metadata_one_topic.json
        '''
        test_topic_metadata = TopicMetadata()
        test_topic_metadata.partitions = {
            0: PartitionMetadata(),
            1: PartitionMetadata(),
            2: PartitionMetadata()
        }

        raw_metadata = ClusterMetadata()
        raw_metadata.topics = {"temperature": test_topic_metadata}
        raw_metadata.cluster_id = "1234"
        raw_metadata.brokers = {
            "testBroker0": BrokerMetadata(),
            "testBroker1": BrokerMetadata()
        }
        return raw_metadata

    def describe_configs(self, config_resources, request_timeout=10):
        resource = ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC,
                                  'temperature')
        fake_future = mock.MagicMock()
        fake_future.result.return_value = get_mock_config_description()
        config_futures = {resource: fake_future}
        return config_futures


def mock_parse_args():
    args = MockedObject()
    args.datacatalog_project_id = "project_id"
    args.datacatalog_location_id = "location_id"
    args.datacatalog_entry_group_id = "kafka"
    args.kafka_host = "test_address"
    args.service_account_path = "path_to_service_account"
    args.group_id = 'test_id'
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


def get_mock_config_description():
    config_desc = {
        'cleanup.policy':
            ConfigEntry('cleanup.policy', 'delete, compact'),
        'retention.ms':
            ConfigEntry('retention.ms', '172800000'),
        'retention.bytes':
            ConfigEntry('retention.bytes', '12'),
        'min.compaction.lag.ms':
            ConfigEntry('min.compaction.lag.ms', '200'),
        'max.compaction.lag.ms':
            ConfigEntry('max.compaction.lag.ms', '9223372036854775807')
    }
    return config_desc
