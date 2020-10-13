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
        self.cluster_fields = None
        self.topic_fields = None
        self.field_constants_for_cluster = self.\
            _define_constants_for_cluster_tag_template()
        self.field_constants_for_topic = self.\
            _define_constants_for_topic_tag_template()

    class TagTemplateFieldConstants:

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

        def __init__(self, num_partitions, retention_time, retention_space,
                     min_compaction_lag, max_compaction_lag, cleanup_policy,
                     consumer_groups, schema):
            self.num_partitions = num_partitions
            self.retention_time = retention_time
            self.retention_space = retention_space
            self.min_compaction_lag = min_compaction_lag
            self.max_compaction_lag = max_compaction_lag
            self.cleanup_policy = cleanup_policy
            self.consumer_groups = consumer_groups
            self.schema = schema

    def _define_constants_for_cluster_tag_template(self):
        num_brokers = self.TagTemplateFieldConstants(
            'num_brokers', 'Number of brokers',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        num_topics = self.TagTemplateFieldConstants(
            'num_topics', 'Number of topics',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        bootstrap_address = self.TagTemplateFieldConstants(
            'bootstrap_address', 'Bootstrap address',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING, True)
        self.cluster_fields = self.ClusterFields(num_brokers, num_topics,
                                                 bootstrap_address)
        return [num_brokers, num_topics, bootstrap_address]

    def _define_constants_for_topic_tag_template(self):
        num_partitions = self.TagTemplateFieldConstants(
            'num_partitions', 'Number of partitions',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        retention_time = self.TagTemplateFieldConstants(
            'retention_ms', 'Retention.ms',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        retention_space = self.TagTemplateFieldConstants(
            'retention_bytes', 'Retention.bytes',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        min_compaction_lag = self.TagTemplateFieldConstants(
            'min_compaction_lag_ms', 'Min.compaction.lag.ms',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        max_compaction_lag = self.TagTemplateFieldConstants(
            'max_compaction_lag_ms', 'Max.compaction.lag.ms',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        cleanup_policy = self.TagTemplateFieldConstants(
            'cleanup_policy', 'Cleanup.policy',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        consumer_groups = self.TagTemplateFieldConstants(
            'consumer_groups', 'Consumer groups',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        schema = self.TagTemplateFieldConstants(
            'schema', 'Schema',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        self.topic_fields = self.TopicFields(num_partitions, retention_time,
                                             retention_space,
                                             min_compaction_lag,
                                             max_compaction_lag,
                                             cleanup_policy, consumer_groups,
                                             schema)
        topic_tag_template_fields = [
            num_partitions, retention_time, retention_space,
            min_compaction_lag, max_compaction_lag, cleanup_policy,
            consumer_groups, schema
        ]
        return topic_tag_template_fields
