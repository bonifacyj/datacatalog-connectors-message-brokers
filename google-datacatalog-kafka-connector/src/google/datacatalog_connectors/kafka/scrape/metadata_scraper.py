import logging
from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataDictKeys


class MetadataScraper:

    def __init__(self, consumer):
        self._consumer = consumer

    def get_metadata(self):
        try:
            raw_metadata = self._consumer.list_topics(timeout=20)
            topic_metadata = self._get_topic_metadata(raw_metadata)
            cluster_metadata = self._get_cluster_metadata(raw_metadata)
            cluster_metadata.update(topic_metadata)
            return cluster_metadata
        except:  # noqa:E722
            logging.error(
                'Error connecting to the system to extract metadata.')
            raise
        finally:
            self._consumer.close()

    def _get_topic_metadata(self, metadata_object):
        topic_names = metadata_object.topics.keys()
        topic_metadata = {MetadataDictKeys.TOPICS: topic_names}
        return topic_metadata

    def _get_cluster_metadata(self, metadata_object):
        cluster_id = metadata_object.cluster_id
        num_brokers = len(metadata_object.brokers)
        cluster_metadata = {
            MetadataDictKeys.CLUSTER_ID: cluster_id,
            MetadataDictKeys.BROKERS_NUM: num_brokers
        }
        return cluster_metadata
