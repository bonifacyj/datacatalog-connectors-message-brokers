import logging
from confluent_kafka import Consumer


class MetadataScraper:

    def __init__(self):
        pass

    def get_metadata(self, connection_args):
        metadata = self._get_metadata_from_message_broker_connection(
            connection_args)
        metadata = self._create_metadata_dict(metadata)
        return metadata

    def _get_metadata_from_message_broker_connection(self, connection_args):
        consumer = Consumer(connection_args)
        try:
            cluster_metadata = consumer.list_topics()
            topic_names = cluster_metadata.topics.keys()
            return topic_names
        except:  # noqa:E722
            logging.error(
                'Error connecting to the system to extract metadata.')
            raise
        finally:
            consumer.close()

    def _create_metadata_dict(self, metadata):
        dict_metadata = {'topics': metadata}
        return dict_metadata
