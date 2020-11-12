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

import confluent_kafka
import mock
import json
from google.cloud import datacatalog_v1beta1
from confluent_kafka.admin import ClusterMetadata, \
    TopicMetadata, BrokerMetadata, PartitionMetadata,\
    ConfigEntry, ConfigResource
from confluent_kafka.schema_registry import Schema, RegisteredSchema
from google.datacatalog_connectors.kafka.prepare.\
    datacatalog_entry_factory import DataCatalogEntryFactory


class FakeDataCatalogEntryFactory(DataCatalogEntryFactory):

    def make_entry_for_topic(self, topic):
        entry = mock.MagicMock()
        entry_id = topic
        return entry_id, entry


class FakeKafkaAdminClientEmptyCluster(mock.MagicMock):

    def list_topics(self, timeout=-1):
        raw_metadata = ClusterMetadata()
        raw_metadata.topics = {}
        raw_metadata.cluster_id = "1234"
        raw_metadata.brokers = {
            "testBroker0": BrokerMetadata(),
            "testBroker1": BrokerMetadata()
        }
        return raw_metadata


class FakeKafkaAdminClient(mock.MagicMock):

    def list_topics(self, timeout=-1):
        '''
        Mock AdminClient returns a description of
        topic from test_data/test_metadata_one_topic.json
        '''
        test_topic_metadata = TopicMetadata()
        test_topic_metadata.partitions = {
            0: PartitionMetadata(),
            1: PartitionMetadata(),
            2: PartitionMetadata()
        }

        raw_metadata = ClusterMetadata()
        raw_metadata.topics = {
            "temperature": test_topic_metadata,
            "__consumer_offsets": test_topic_metadata
        }
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


class FakeKafkaSchemaRegistryClient(mock.MagicMock):

    def get_subjects(self):
        return ["temperature-key", "temperature-value", "test-subject"]

    def get_latest_version(self, subject_name):
        if subject_name == "temperature-key":
            test_schema_str = get_test_avro_schema_topic_keys()
        else:
            test_schema_str = get_test_avro_schema_topic_values()
        schema = Schema(test_schema_str, schema_type="AVRO")
        registered_schema = RegisteredSchema(schema_id=1,
                                             schema=schema,
                                             subject=subject_name,
                                             version=1)
        return registered_schema


def mock_parse_args():
    args = MockedObject()
    args.datacatalog_project_id = "project_id"
    args.datacatalog_location_id = "location_id"
    args.datacatalog_entry_group_id = "kafka"
    args.kafka_host = "test_address"
    args.service_account_path = "path_to_service_account"
    args.schema_registry_url = "test_url"
    args.schema_registry_ssl_ca_location = "test_ssl_ca_location"
    args.schema_registry_ssl_cert_location = "test_ssl_cert_location"
    args.schema_registry_ssl_key_location = "test_ssl_key_location"
    args.schema_registry_auth_user_info = None
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
    subcolumns = [
        datacatalog_v1beta1.types.ColumnSchema(column="num_guests", type="int")
    ]
    entry.schema.columns.append(
        datacatalog_v1beta1.types.ColumnSchema(column="id", type="string"))
    entry.schema.columns.append(
        datacatalog_v1beta1.types.ColumnSchema(column="degrees",
                                               type="double"))
    entry.schema.columns.append(
        datacatalog_v1beta1.types.ColumnSchema(column="another_record",
                                               type="record",
                                               subcolumns=subcolumns))

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


def get_test_avro_schema_topic_values():
    orig_schema = {
        "name":
            "updates",
        "type":
            "record",
        "fields": [{
            "name": "id",
            "type": "string"
        }, {
            "name": "degrees",
            "type": "double"
        }, {
            "name": "another_record",
            "type": {
                "name": "nested_record",
                "type": "record",
                "fields": [{
                    "name": "num_guests",
                    "type": "int"
                }]
            }
        }]
    }
    avro_schema = json.dumps(orig_schema)
    return avro_schema


def get_test_avro_schema_topic_keys():
    orig_schema = {"type": "array", "items": "string"}
    return json.dumps(orig_schema)
