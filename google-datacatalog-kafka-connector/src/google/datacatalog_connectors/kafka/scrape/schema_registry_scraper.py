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
from confluent_kafka.schema_registry.schema_registry_client \
    import SchemaRegistryError
from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants


class SchemaRegistryScraper:

    def __init__(self, schema_registry_client):
        self._schema_registry_client = schema_registry_client
        self._subjects = self._get_subjects()

    def scrape_schema_metadata(self, topic_name):
        schema_metadata = {}
        subject_value = topic_name + '-value'
        subject_key = topic_name + '-key'
        try:
            key_schema_metadata = self._get_subject_details(
                subject_key, MetadataConstants.TOPIC_KEY_SCHEMA)
            schema_metadata.update(key_schema_metadata)
            value_schema_metadata = self._get_subject_details(
                subject_value, MetadataConstants.TOPIC_VALUE_SCHEMA)
            schema_metadata.update(value_schema_metadata)
            return schema_metadata
        except (ValueError, TypeError) as e:
            logging.error("Failed to pull information about topic {} "
                          "from the Schema Registry: {}".format(topic_name, e))
            raise

    def _get_subjects(self):
        try:
            subjects = self._schema_registry_client.get_subjects()
            return subjects
        except SchemaRegistryError as e:
            logging.error("Failed to get list of the subjects "
                          "from the Schema Registry {}".format(e))
            raise

    def _get_subject_details(self, subject_name, metadata_dict_key):
        topic_schema_metadata = {}
        try:
            if subject_name in self._subjects:
                registered_schema = self._schema_registry_client\
                    .get_latest_version(subject_name)
                schema_str = registered_schema.schema.schema_str
                schema_version = registered_schema.version
                schema_type = registered_schema.schema.schema_type
                topic_schema_metadata[metadata_dict_key] = {
                    MetadataConstants.SCHEMA_VERSION: schema_version,
                    MetadataConstants.SCHEMA_TYPE: schema_type,
                    MetadataConstants.SCHEMA_STRING: schema_str
                }
            return topic_schema_metadata
        except (ValueError, TypeError) as e:
            logging.error("Failed to pull information about subject {} "
                          "from the Schema Registry: {}".format(
                              subject_name, e))
            raise
