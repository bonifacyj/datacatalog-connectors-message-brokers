from google.cloud import datacatalog_v1beta1
from google.datacatalog_connectors.commons.prepare.base_entry_factory import \
    BaseEntryFactory

from google.datacatalog_connectors.kafka.config.metadata_constants import MetadataDictKeys


class DataCatalogEntryFactory(BaseEntryFactory):
    NO_VALUE_SPECIFIED = 'UNDEFINED'
    EMPTY_TOKEN = '?'

    def __init__(self, project_id, location_id, metadata_host_server,
                 entry_group_id):
        self.__project_id = project_id
        self.__location_id = location_id
        self.__metadata_host_server = metadata_host_server
        self.__entry_group_id = entry_group_id

    def make_entry_for_topic(self, topic):
        """Create a Datacatalog entry from topic information.

         :param topic: name of a Kafka topic
         :return: entry_id, entry
        """

        entry_id = self._format_id(topic)
        entry = datacatalog_v1beta1.types.Entry()

        entry.user_specified_type = 'kafka_topic'
        entry.user_specified_system = self.__entry_group_id

        entry.display_name = self._format_display_name(topic)

        entry.name = datacatalog_v1beta1.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)

        entry.linked_resource = '//{}//{}'.format(self.__metadata_host_server,
                                                  entry_id)

        return entry_id, entry

    def make_entry_for_cluster(self, metadata):
        """
        Create a Datacatalog entry from cluster metadata dict
        :param metadata: dict
        :return: entry_id, entry
        """
        cluster_id = metadata[MetadataDictKeys.CLUSTER_ID]
        entry_id = self._format_id(cluster_id)
        entry = datacatalog_v1beta1.types.Entry()

        entry.user_specified_type = 'kafka_cluster'
        entry.user_specified_system = self.__entry_group_id

        entry.display_name = self._format_display_name(
            "Kafka cluster {}".format(cluster_id))

        entry.name = datacatalog_v1beta1.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)

        entry.linked_resource = '//{}//{}'.format(self.__metadata_host_server,
                                                  entry_id)

        return entry_id, entry
