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


class TagTemplateConstants:
    """
    Constants from this class are used both by
    prepare.DataCatalogTagTemplateFactory and by
    respective tests. This class is intended to be the
    source of truth regarding which fields are expected to exist
    in the tag templates.
    """

    @classmethod
    def get_topic_constants_list(cls):
        topic_constants = cls.get_constants_for_topic_tag_template()
        return list(topic_constants.__dict__.values())

    @classmethod
    def get_cluster_constants_list(cls):
        cluster_constants = cls.get_constants_for_cluster_tag_template()
        return list(cluster_constants.__dict__.values())

    @classmethod
    def get_constants_for_cluster_tag_template(cls):
        return cls.ClusterFields()

    @classmethod
    def get_constants_for_topic_tag_template(cls):
        return cls.TopicFields()

    class ClusterFields:

        def __init__(self):
            self.num_brokers = TagTemplateField(
                'num_brokers', 'Number of brokers',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
            self.num_topics = TagTemplateField(
                'num_topics', 'Number of topics',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
            self.bootstrap_address = TagTemplateField(
                'bootstrap_address', 'Bootstrap address',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING, True)

    class TopicFields:

        def __init__(self):
            self.num_partitions = TagTemplateField(
                'num_partitions', 'Number of partitions',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
            self.retention_time = TagTemplateField(
                'retention_ms', 'Retention ms',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
            self.retention_time_as_text = TagTemplateField(
                'retention_duration_as_text', 'Retention time',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
            self.retention_space = TagTemplateField(
                'retention_bytes', 'Retention bytes',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
            self.retention_space_as_text = TagTemplateField(
                'retention_size_as_text', 'Retention size',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
            self.min_compaction_lag = TagTemplateField(
                'min_compaction_lag_ms', 'Min compaction lag ms',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
            self.min_compaction_lag_as_text = TagTemplateField(
                'min_compaction_lag', 'Min compaction lag',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
            self.max_compaction_lag = TagTemplateField(
                'max_compaction_lag_ms', 'Max compaction lag ms',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
            self.max_compaction_lag_as_text = TagTemplateField(
                'max_compaction_lag', 'Max compaction lag',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
            self.cleanup_policy = TagTemplateField(
                'cleanup_policy', 'Cleanup policy',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
            self.consumer_groups = TagTemplateField(
                'consumer_groups', 'Consumer groups',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
            self.key_schema = TagTemplateField(
                'key_schema', 'Key schema',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
            self.value_schema = TagTemplateField(
                'value_schema', 'Value schema',
                datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)


class TagTemplateField:

    def __init__(self, name, display_name, field_type, is_required=False):
        self.name = name
        self.display_name = display_name
        self.type = field_type
        self.is_required = is_required
