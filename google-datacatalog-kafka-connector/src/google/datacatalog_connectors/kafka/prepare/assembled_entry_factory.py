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

from google.datacatalog_connectors.commons import prepare
from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants


class AssembledEntryFactory:

    def __init__(self,
                 entry_group_id,
                 datacatalog_entry_factory,
                 datacatalog_tag_factory=None,
                 tag_templates_dict=None):
        self.__datacatalog_entry_factory = datacatalog_entry_factory
        self.__datacatalog_tag_factory = datacatalog_tag_factory
        self.__entry_group_id = entry_group_id
        self.__tag_templates_dict = tag_templates_dict

    def make_entries_from_cluster_metadata(self, metadata):
        assembled_entries = []

        assembled_cluster = self.__make_assembled_entry_for_cluster(metadata)
        assembled_entries.append(assembled_cluster)

        topics = metadata[MetadataConstants.TOPICS]
        for topic in topics:
            assembled_topic = self.__make_assembled_entry_for_topic(topic)
            assembled_entries.append(assembled_topic)
        logging.info('\n%s topics ready to be ingested...', len(topics))

        return assembled_entries

    def __make_assembled_entry_for_topic(self, topic):
        entry_id, entry = self.__datacatalog_entry_factory.\
            make_entry_for_topic(topic)
        return prepare.AssembledEntryData(entry_id, entry)

    def __make_assembled_entry_for_cluster(self, metadata):
        entry_id, entry = self.__datacatalog_entry_factory.\
            make_entry_for_cluster(metadata)
        return prepare.AssembledEntryData(entry_id, entry)
