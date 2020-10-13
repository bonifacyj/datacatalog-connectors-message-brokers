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
        self.field_constants_for_cluster = self.\
            _define_constants_for_cluster_tag_template()
        self.field_constants_for_topic = self.\
            _define_constants_for_topic_tag_template()

    class TagTemplateField:

        def __init__(self, name, display_name, field_type, is_required=False):
            self.name = name
            self.display_name = display_name
            self.type = field_type
            self.is_required = is_required

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
        return [num_brokers, num_topics, bootstrap_address]

    def _define_constants_for_topic_tag_template(self):
        num_partitions = self.TagTemplateField(
            'num_partitions', 'Number of partitions',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE)
        retention_time = self.TagTemplateField(
            'retention_ms', 'Retention ms',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        retention_space = self.TagTemplateField(
            'retention_bytes', 'Retention bytes',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        min_compaction_lag = self.TagTemplateField(
            'min_compaction_lag_ms', 'Min compaction lag ms',
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        max_compaction_lag = self.TagTemplateField(
            'max_compaction_lag_ms', 'Max compaction lag ms',
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
        topic_tag_template_fields = [
            num_partitions, retention_time, retention_space,
            min_compaction_lag, max_compaction_lag, cleanup_policy,
            consumer_groups, schema
        ]
        return topic_tag_template_fields
