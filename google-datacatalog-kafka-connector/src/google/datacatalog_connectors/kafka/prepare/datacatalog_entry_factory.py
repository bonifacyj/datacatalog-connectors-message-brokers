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

import re

from google.cloud import datacatalog_v1beta1
from google.datacatalog_connectors.commons.prepare.base_entry_factory import \
    BaseEntryFactory

from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants


class DataCatalogEntryFactory(BaseEntryFactory):

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
        topic_name = topic[MetadataConstants.TOPIC_NAME]
        entry_id = self._create_valid_entry_id(topic_name)
        formatted_name = self._format_id(topic_name)
        entry = datacatalog_v1beta1.types.Entry()

        entry.user_specified_type = 'kafka_topic'
        entry.user_specified_system = self.__entry_group_id

        entry.display_name = self._format_display_name(topic_name)

        entry.name = datacatalog_v1beta1.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)

        entry.linked_resource = '//{}//{}'.format(self.__metadata_host_server,
                                                  formatted_name)

        return entry_id, entry

    def make_entry_for_cluster(self, metadata):
        """
        Create a Datacatalog entry from cluster metadata dict
        :param metadata: dict
        :return: entry_id, entry
        """
        cluster_id = metadata[MetadataConstants.CLUSTER_ID]
        entry_id = self._create_valid_entry_id(cluster_id)
        formatted_id = self._format_id(cluster_id)
        entry = datacatalog_v1beta1.types.Entry()

        entry.user_specified_type = 'kafka_cluster'
        entry.user_specified_system = self.__entry_group_id

        entry.display_name = self._format_display_name(
            "Kafka cluster {}".format(cluster_id))

        entry.name = datacatalog_v1beta1.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)

        entry.linked_resource = '//{}//{}'.format(self.__metadata_host_server,
                                                  formatted_id)

        return entry_id, entry

    def _create_valid_entry_id(self, asset_name):
        """
        DC entry ID cannot start from anything other than
        an underscore or a letter. This method makes sure this
        requirement is met
        :return: entry_id
        """
        invalid_id = re.compile('^[^a-zA-Z_]+.*$')
        if invalid_id.search(asset_name):
            asset_name = '_' + asset_name
        entry_id = self._format_id(asset_name)
        return entry_id
