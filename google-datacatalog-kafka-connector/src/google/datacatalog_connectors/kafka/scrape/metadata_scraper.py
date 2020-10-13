import logging

import confluent_kafka
from confluent_kafka.admin import ConfigResource
from confluent_kafka.cimpl import KafkaException

from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants
from .metadata_values_converter import MetadataValuesConverter


class MetadataScraper:

    def __init__(self, client, bootstrap_server):
        self._adminClient = client
        self._bootstrap_server = bootstrap_server

    def get_metadata(self):
        try:
            raw_metadata = self._adminClient.list_topics(timeout=20)
            topics_metadata = self._get_topics_metadata(raw_metadata)
            cluster_metadata = self._get_cluster_metadata(raw_metadata)
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
        topic_names = metadata_object.topics.keys()
        descriptions = list()
        config_resources = [
            ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, topic_name)
            for topic_name in topic_names
        ]
        config_futures = self._adminClient.describe_configs(config_resources,
                                                            request_timeout=10)
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
        topic_metadata = {MetadataConstants.TOPICS: descriptions}
        return topic_metadata

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
        min_compaction_lag = config['min.compaction.lag.ms'].value
        max_compaction_lag = config['max.compaction.lag.ms'].value
        topic_compaction_config = {
            MetadataConstants.MIN_COMPACTION_LAG:
                int(min_compaction_lag),
            MetadataConstants.MIN_COMPACTION_LAG_TEXT:
                MetadataValuesConverter.get_human_readable_duration_value(
                    min_compaction_lag),
            MetadataConstants.MAX_COMPACTION_LAG:
                int(max_compaction_lag),
            MetadataConstants.MAX_COMPACTION_LAG_TEXT:
                MetadataValuesConverter.get_human_readable_duration_value(
                    max_compaction_lag)
        }
        return topic_compaction_config
