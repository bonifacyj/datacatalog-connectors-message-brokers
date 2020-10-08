import logging

from confluent_kafka.cimpl import KafkaException
from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants

import confluent_kafka
from confluent_kafka.admin import ConfigResource


class MetadataScraper:

    def __init__(self, consumer):
        self._adminClient = consumer

    def get_metadata(self):
        try:
            raw_metadata = self._adminClient.list_topics(timeout=20)
            topic_metadata = self._get_topic_metadata(raw_metadata)
            cluster_metadata = self._get_cluster_metadata(raw_metadata)
            cluster_metadata.update(topic_metadata)
            return cluster_metadata
        except:  # noqa:E722 silence linter complaint about bare except
            logging.error(
                'Error connecting to the system to extract metadata.')
            raise

    def _get_topic_metadata(self, metadata_object):
        topic_names = metadata_object.topics.keys()
        topic_metadata = {MetadataConstants.TOPICS: topic_names}
        return topic_metadata

    def _get_topic_config(self, topic_names, raw_metadata):
        topics_metadata = list()
        config_resources = [
            ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, topic_name)
            for topic_name in topic_names
        ]
        config_futures = self._adminClient.describe_configs(config_resources,
                                                            request_timeout=10)
        for topic, future in config_futures.items():
            try:
                topic_description = dict()
                config = future.result()
                topic_description["name"] = topic.name
                num_partitions = len(
                    raw_metadata.topics[topic.name].partitions)
                topic_description["num_partitions"] = num_partitions
                cleanup_policy = config['cleanup.policy'].value
                if 'delete' in cleanup_policy:
                    topic_description.update(
                        self._set_retention_space_time(config))
                if 'compact' in cleanup_policy:
                    topic_description.update(self._set_compaction_lag(config))
                topics_metadata.append(topic_description)
            except KafkaException as e:
                print("Failed to describe topic {}: {}".format(topic, e))
            except Exception as e:
                raise
        return topics_metadata

    def _get_cluster_metadata(self, metadata_object):
        cluster_id = metadata_object.cluster_id
        num_brokers = len(metadata_object.brokers)
        cluster_metadata = {
            MetadataConstants.CLUSTER_ID: cluster_id,
            MetadataConstants.BROKERS_NUM: num_brokers
        }
        return cluster_metadata

    def _set_retention_space_time(self, config):
        # todo: make time and space info understandable for humans
        retention_time = config['retention.ms'].value
        topic_retention_config = {'retention_time': retention_time}
        retention_space = config['retention.bytes'].value
        if retention_space != '-1':  # '-1' is a default value, if space retention is not set
            topic_retention_config['retention_space'] = retention_space
        return topic_retention_config

    def _set_compaction_lag(self, config):
        #todo: make time info understandable for humans
        min_compaction_lag = config['min.compaction.lag.ms'].value
        max_compaction_lag = config['max.compaction.lag.ms'].value
        topic_compaction_config = {
            "min_compaction_lag": min_compaction_lag,
            "max_compaction_lag": max_compaction_lag
        }
        return topic_compaction_config
