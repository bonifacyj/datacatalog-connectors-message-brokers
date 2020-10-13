from google.cloud import datacatalog_v1beta1


class TagTemplateConstants:
    """
    Constants from this class are used both by
    prepare.DataCatalogTagTemplateFactory and by
    respective tests. This class is intended to be the
    source of truth regarding which fields are expected to exist
    in the tag templates.
    """

    def __init__(self):
        self.cluster_fields = self._define_constants_for_cluster_tag_template()
        self.topic_fields = self._define_constants_for_topic_tag_template()
        self.cluster_constants_list = list(
            self.cluster_fields.__dict__.values())
        self.topic_constants_list = list(self.topic_fields.__dict__.values())

    class TagTemplateField:

        def __init__(self, name, display_name, field_type, is_required=False):
            self.name = name
            self.display_name = display_name
            self.type = field_type
            self.is_required = is_required

    class ClusterFields:

        def __init__(self, num_brokers, num_topics, bootstrap_address):
            self.num_brokers = num_brokers
            self.num_topics = num_topics
            self.bootstrap_address = bootstrap_address

    class TopicFields:

        def __init__(self, num_partitions, retention_time,
                     retention_time_as_text, retention_space,
                     retention_space_as_text, min_compaction_lag,
                     min_compaction_lag_as_text, max_compaction_lag,
                     max_compaction_lag_as_text, cleanup_policy,
                     consumer_groups, schema):
            self.num_partitions = num_partitions
            self.retention_time = retention_time
            self.retention_time_as_text = retention_time_as_text
            self.retention_space = retention_space
            self.retention_space_as_text = retention_space_as_text
            self.min_compaction_lag = min_compaction_lag
            self.min_compaction_lag_as_text = min_compaction_lag_as_text
            self.max_compaction_lag = max_compaction_lag
            self.max_compaction_lag_as_text = max_compaction_lag_as_text
            self.cleanup_policy = cleanup_policy
            self.consumer_groups = consumer_groups
            self.schema = schema

    def _define_constants_for_cluster_tag_template(self):
        num_brokers = self.TagTemplateField(
            'num_brokers', 'Number of brokers',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        num_topics = self.TagTemplateField(
            'num_topics', 'Number of topics',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        bootstrap_address = self.TagTemplateField(
            'bootstrap_address', 'Bootstrap address',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING, True)
        cluster_fields = self.ClusterFields(num_brokers, num_topics,
                                            bootstrap_address)
        return cluster_fields

    def _define_constants_for_topic_tag_template(self):
        num_partitions = self.TagTemplateField(
            'num_partitions', 'Number of partitions',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        retention_time = self.TagTemplateField(
            'retention_ms', 'Retention ms',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        retention_time_as_text = self.TagTemplateField(
            'retention_duration_as_text', 'Retention time',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        retention_space = self.TagTemplateField(
            'retention_bytes', 'Retention bytes',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        retention_space_as_text = self.TagTemplateField(
            'retention_size_as_text', 'Retention size',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        min_compaction_lag = self.TagTemplateField(
            'min_compaction_lag_ms', 'Min compaction lag ms',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        min_compaction_lag_as_text = self.TagTemplateField(
            'min_compaction_lag', 'Min compaction lag',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        max_compaction_lag = self.TagTemplateField(
            'max_compaction_lag_ms', 'Max compaction lag ms',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        max_compaction_lag_as_text = self.TagTemplateField(
            'max_compaction_lag', 'Max compaction lag',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        cleanup_policy = self.TagTemplateField(
            'cleanup_policy', 'Cleanup policy',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        consumer_groups = self.TagTemplateField(
            'consumer_groups', 'Consumer groups',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        schema = self.TagTemplateField(
            'schema', 'Schema',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        topic_fields = self.TopicFields(
            num_partitions, retention_time, retention_time_as_text,
            retention_space, retention_space_as_text, min_compaction_lag,
            min_compaction_lag_as_text, max_compaction_lag,
            max_compaction_lag_as_text, cleanup_policy, consumer_groups,
            schema)
        return topic_fields
