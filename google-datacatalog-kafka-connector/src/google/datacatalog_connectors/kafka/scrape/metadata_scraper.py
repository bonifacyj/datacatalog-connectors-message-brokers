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

import logging
import time
import confluent_kafka
from confluent_kafka.admin import ConfigResource
from confluent_kafka.cimpl import KafkaException

from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants
from .metadata_values_converter import MetadataValuesConverter


class MetadataScraper:

    def __init__(self, bootstrap_server, client, schema_registry_client=None):
        self._bootstrap_server = bootstrap_server
        self._admin_client = client
        self._schema_registry_client = schema_registry_client

    def get_metadata(self):
        try:
            raw_metadata = self._admin_client.list_topics(timeout=10)
            cluster_metadata = self._get_cluster_metadata(raw_metadata)
            topics_metadata = self._get_topics_metadata(raw_metadata)
            cluster_metadata.update(topics_metadata)
            return cluster_metadata
        except:  # noqa:E722 silence linter complaint about bare except
            logging.error(
                'Error connecting to the system to extract metadata.')
            raise

    def _get_cluster_metadata(self, metadata_object):
        cluster_id = metadata_object.cluster_id
        num_brokers = len(metadata_object.brokers)
        cluster_metadata = {
            MetadataConstants.CLUSTER_ID: cluster_id,
            MetadataConstants.NUM_BROKERS: num_brokers,
            MetadataConstants.BOOTSTRAP_SERVER: self._bootstrap_server
        }
        return cluster_metadata

    def _get_topics_metadata(self, metadata_object):
        descriptions = []
        topic_names = list(metadata_object.topics.keys())
        user_topics = [
            topic_name for topic_name in topic_names
            if not self._is_system_topic(topic_name)
        ]
        if len(user_topics) == 0:
            logging.warning('There are no topics in the given cluster.')
        else:
            config_resources = [
                ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC,
                               topic_name) for topic_name in user_topics
            ]
            config_futures = self._admin_client.describe_configs(
                config_resources, request_timeout=10)
            start_describing = time.time()
            for topic, future in config_futures.items():
                try:
                    config = future.result()
                    topic_description = self._assemble_topic_metadata(
                        topic.name, metadata_object, config)
                    descriptions.append(topic_description)
                except KafkaException as e:
                    logging.error("Failed to describe topic {}: {}".format(
                        topic, e))
                    raise
            end_describing = time.time()
            logging.info("Described {} topics in {} seconds".format(
                len(descriptions), end_describing - start_describing))
        topic_metadata = {MetadataConstants.TOPICS: descriptions}
        return topic_metadata

    def _is_system_topic(self, topic_name):
        """
        Assumes that topic names follow the convention:
        all topics starting with underscore are system topics
        :param topic_name: str
        :return: bool
        """
        if topic_name.startswith('_'):
            return True
        return False

    def _assemble_topic_metadata(self, topic_name, raw_metadata, config_desc):
        num_partitions = len(raw_metadata.topics[topic_name].partitions)
        cleanup_policy = config_desc['cleanup.policy'].value

        topic_description = {
            MetadataConstants.TOPIC_NAME: topic_name,
            MetadataConstants.NUM_PARTITIONS: num_partitions,
            MetadataConstants.CLEANUP_POLICY: cleanup_policy
        }

        if 'delete' in cleanup_policy:
            topic_description.update(
                self._get_topic_retention_config(config_desc))
        if 'compact' in cleanup_policy:
            topic_description.update(
                self._get_topic_compaction_config(config_desc))
        if self._schema_registry_client is not None:
            topic_description.update(self._get_topic_schemas(topic_name))
        return topic_description

    def _get_topic_retention_config(self, config):
        retention_time = config['retention.ms'].value
        topic_retention_config = {
            MetadataConstants.RETENTION_TIME:
                int(retention_time),
            MetadataConstants.RETENTION_TIME_TEXT:
                MetadataValuesConverter.get_human_readable_duration_value(
                    retention_time)
        }
        retention_space = config['retention.bytes'].value
        if retention_space != '-1':
            # '-1' means space retention parameter is not set in kafka,
            # therefore we ignore it
            topic_retention_config[MetadataConstants.RETENTION_SPACE] = int(
                retention_space)
            topic_retention_config[
                MetadataConstants.
                RETENTION_SPACE_TEXT] = MetadataValuesConverter.\
                get_human_readable_size_value(
                    retention_space)
        return topic_retention_config

    def _get_topic_compaction_config(self, config):
        topic_compaction_config = {}
        min_compaction_lag_entry = config.get('min.compaction.lag.ms')
        if min_compaction_lag_entry is not None:
            min_compaction_lag = config['min.compaction.lag.ms'].value
            topic_compaction_config.update({
                MetadataConstants.MIN_COMPACTION_LAG:
                    int(min_compaction_lag),
                MetadataConstants.MIN_COMPACTION_LAG_TEXT:
                    MetadataValuesConverter.get_human_readable_duration_value(
                        min_compaction_lag)
            })

        max_compaction_lag_entry = config.get('max.compaction.lag.ms')
        if max_compaction_lag_entry is not None:
            max_compaction_lag = config['max.compaction.lag.ms'].value
            topic_compaction_config.update({
                MetadataConstants.MAX_COMPACTION_LAG:
                    int(max_compaction_lag),
                MetadataConstants.MAX_COMPACTION_LAG_TEXT:
                    MetadataValuesConverter.get_human_readable_duration_value(
                        max_compaction_lag)
            })

        return topic_compaction_config

    def _get_topic_schemas(self, topic_name):
        topic_schemas = {}
        subject_value = topic_name + '-value'
        subject_key = topic_name + '-key'
        subjects = self._schema_registry_client.get_subjects()
        if subject_value in subjects:
            value_schema = self._schema_registry_client.get_latest_version(
                subject_value).schema.schema_str
            topic_schemas[MetadataConstants.TOPIC_VALUE_SCHEMA] = value_schema
        if subject_key in subjects:
            key_schema = self._schema_registry_client.get_latest_version(
                subject_key).schema.schema_str
            topic_schemas[MetadataConstants.TOPIC_KEY_SCHEMA] = key_schema
        return topic_schemas
