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

from google.cloud import datacatalog_v1beta1
from google.datacatalog_connectors.kafka.config import \
    MetadataConstants, TagTemplateConstants


class DataCatalogTagFactory:

    def make_tag_for_cluster(self, tag_template, cluster_metadata):
        tag = datacatalog_v1beta1.types.Tag()
        tag.template = tag_template.name
        template_fields = TagTemplateConstants.\
            get_constants_for_cluster_tag_template()
        tag.fields[
            template_fields.num_brokers.name].double_value = cluster_metadata[
                MetadataConstants.NUM_BROKERS]
        tag.fields[template_fields.bootstrap_address.
                   name].string_value = cluster_metadata[
                       MetadataConstants.BOOTSTRAP_SERVER]
        num_topics = len(cluster_metadata[MetadataConstants.TOPICS])
        tag.fields[template_fields.num_topics.name].double_value = num_topics
        return tag

    def make_tag_for_topic(self, tag_template, topic_metadata):
        tag = datacatalog_v1beta1.types.Tag()
        tag.template = tag_template.name
        template_fields = TagTemplateConstants.\
            get_constants_for_topic_tag_template()
        tag.fields[
            template_fields.num_partitions.name].double_value = topic_metadata[
                MetadataConstants.NUM_PARTITIONS]
        tag.fields[
            template_fields.cleanup_policy.name].string_value = topic_metadata[
                MetadataConstants.CLEANUP_POLICY]
        retention_time = topic_metadata.get(MetadataConstants.RETENTION_TIME)
        if retention_time:
            tag.fields[template_fields.retention_time.
                       name].double_value = retention_time
            tag.fields[template_fields.retention_time_as_text.
                       name].string_value = topic_metadata.get(
                           MetadataConstants.RETENTION_TIME_TEXT)
        retention_space = topic_metadata.get(MetadataConstants.RETENTION_SPACE)
        if retention_space:
            tag.fields[template_fields.retention_space.
                       name].double_value = retention_space
            tag.fields[template_fields.retention_space_as_text.
                       name].string_value = topic_metadata.get(
                           MetadataConstants.RETENTION_SPACE_TEXT)
        min_compaction_lag = topic_metadata.get(
            MetadataConstants.MIN_COMPACTION_LAG)
        if min_compaction_lag:
            tag.fields[template_fields.min_compaction_lag.
                       name].double_value = min_compaction_lag
            tag.fields[template_fields.min_compaction_lag_as_text.
                       name].string_value = topic_metadata.get(
                           MetadataConstants.MIN_COMPACTION_LAG_TEXT)

        max_compaction_lag = topic_metadata.get(
            MetadataConstants.MAX_COMPACTION_LAG)
        if max_compaction_lag:
            tag.fields[template_fields.max_compaction_lag.
                       name].double_value = max_compaction_lag
            tag.fields[template_fields.max_compaction_lag_as_text.
                       name].string_value = topic_metadata.get(
                           MetadataConstants.MAX_COMPACTION_LAG_TEXT)
        key_schema = topic_metadata.get(MetadataConstants.TOPIC_KEY_SCHEMA)
        if key_schema:
            tag.fields[template_fields.keys_physical_schema.
                       name].string_value = key_schema.get(
                           MetadataConstants.SCHEMA_STRING)
            tag.fields[template_fields.keys_physical_schema_format.
                       name].string_value = key_schema.get(
                           MetadataConstants.SCHEMA_FORMAT)
            tag.fields[template_fields.keys_physical_schema_version.
                       name].double_value = key_schema.get(
                           MetadataConstants.SCHEMA_VERSION)
            schema_name = key_schema.get(MetadataConstants.SCHEMA_NAME)
            if schema_name:
                tag.fields[template_fields.keys_physical_schema_name.
                           name].string_value = schema_name
            schema_type = key_schema.get(MetadataConstants.SCHEMA_TYPE)
            if schema_type:
                tag.fields[template_fields.keys_physical_schema_type.
                           name].string_value = schema_type
        value_schema = topic_metadata.get(MetadataConstants.TOPIC_VALUE_SCHEMA)
        if value_schema:
            tag.fields[template_fields.payload_physical_schema.
                       name].string_value = value_schema.get(
                           MetadataConstants.SCHEMA_STRING)
            tag.fields[template_fields.payload_physical_schema_format.
                       name].string_value = value_schema.get(
                           MetadataConstants.SCHEMA_FORMAT)
            tag.fields[template_fields.payload_physical_schema_version.
                       name].double_value = value_schema.get(
                           MetadataConstants.SCHEMA_VERSION)
            schema_name = value_schema.get(MetadataConstants.SCHEMA_NAME)
            if schema_name:
                tag.fields[template_fields.payload_physical_schema_name.
                           name].string_value = schema_name
            schema_type = value_schema.get(MetadataConstants.SCHEMA_TYPE)
            if schema_type:
                tag.fields[template_fields.payload_physical_schema_type.
                           name].string_value = schema_type
        return tag
