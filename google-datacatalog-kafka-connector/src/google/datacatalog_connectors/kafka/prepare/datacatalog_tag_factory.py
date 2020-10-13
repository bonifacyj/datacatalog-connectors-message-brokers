from google.cloud import datacatalog_v1beta1
from google.datacatalog_connectors.kafka.config import MetadataConstants, TagTemplateConstants


class DataCatalogTagFactory:

    def __init__(self):
        self._tag_template_constants = TagTemplateConstants()

    def make_tag_for_cluster(self, tag_template, cluster_metadata):
        tag = datacatalog_v1beta1.types.Tag()
        tag.template = tag_template.name
        template_fields = self._tag_template_constants.cluster_fields
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
        template_fields = self._tag_template_constants.topic_fields
        tag.fields[
            template_fields.num_partitions.name].double_value = topic_metadata[
                MetadataConstants.NUM_PARTITIONS]
        tag.fields[
            template_fields.cleanup_policy.name].string_value = topic_metadata[
                MetadataConstants.CLEANUP_POLICY]
        retention_time = topic_metadata.get(MetadataConstants.RETENTION_TIME)
        if retention_time:
            tag.fields[template_fields.retention_time.
                       name].string_value = retention_time
        retention_space = topic_metadata.get(MetadataConstants.RETENTION_SPACE)
        if retention_space:
            tag.fields[template_fields.retention_space.
                       name].double_value = retention_space
        min_compaction_lag = topic_metadata.get(
            MetadataConstants.MIN_COMPACTION_LAG)
        if min_compaction_lag:
            tag.fields[template_fields.min_compaction_lag.
                       name].string_value = min_compaction_lag
        max_compaction_lag = topic_metadata.get(
            MetadataConstants.MAX_COMPACTION_LAG)
        if max_compaction_lag:
            tag.fields[template_fields.max_compaction_lag.
                       name].string_value = max_compaction_lag
        return tag
