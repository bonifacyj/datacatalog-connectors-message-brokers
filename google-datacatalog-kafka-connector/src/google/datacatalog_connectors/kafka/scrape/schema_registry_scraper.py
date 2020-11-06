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
from .avro_schema_parser import AvroSchemaParser


class SchemaRegistryScraper:

    def __init__(self, schema_registry_client):
        self._schema_registry_client = schema_registry_client
        self._subjects = self._get_subjects()

    def scrape_schema_metadata(self, topic_name):
        schema_metadata = {}
        subject_key = topic_name + '-key'
        subject_value = topic_name + '-value'

        key_schema_metadata = self._get_subject_details(subject_key)
        if len(key_schema_metadata) > 0:
            schema_metadata.update(
                {MetadataConstants.TOPIC_KEY_SCHEMA: key_schema_metadata})

        value_schema_metadata = self._get_subject_details(subject_value)
        if len(value_schema_metadata) > 0:
            schema_metadata.update(
                {MetadataConstants.TOPIC_VALUE_SCHEMA: value_schema_metadata})

        return schema_metadata

    def _get_subjects(self):
        try:
            subjects = self._schema_registry_client.get_subjects()
            return subjects
        except SchemaRegistryError as e:
            logging.error("Failed to get list of the subjects "
                          "from the Schema Registry {}".format(e))
            raise

    def _get_subject_details(self, subject_name):
        topic_schema_metadata = {}
        try:
            if subject_name in self._subjects:
                registered_schema = self._schema_registry_client\
                    .get_latest_version(subject_name)
                schema_str = registered_schema.schema.schema_str
                schema_version = registered_schema.version
                schema_format = registered_schema.schema.schema_type
                topic_schema_metadata = {
                    MetadataConstants.SCHEMA_VERSION: schema_version,
                    MetadataConstants.SCHEMA_FORMAT: schema_format,
                    MetadataConstants.SCHEMA_STRING: schema_str
                }
                if schema_format == 'AVRO':
                    topic_schema_metadata.update(
                        self._parse_avro_schema(schema_str))
            return topic_schema_metadata
        except (ValueError, TypeError) as e:
            logging.error("Failed to pull information about subject {} "
                          "from the Schema Registry: {}".format(
                              subject_name, e))
            raise

    def _parse_avro_schema(self, schema_str):
        schema_parser = AvroSchemaParser(schema_str)
        schema_details = {
            MetadataConstants.SCHEMA_TYPE: schema_parser.get_schema_type()
        }
        schema_name = schema_parser.get_schema_name()
        if schema_name:
            schema_details.update({MetadataConstants.SCHEMA_NAME: schema_name})
        schema_fields = schema_parser.get_fields_names_and_types()
        if schema_fields:
            schema_details.update(
                {MetadataConstants.SCHEMA_FIELDS: schema_fields})
        return schema_details
