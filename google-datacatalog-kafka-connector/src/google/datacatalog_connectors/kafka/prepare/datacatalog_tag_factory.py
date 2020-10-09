from google.cloud import datacatalog_v1beta1
from google.datacatalog_connectors.kafka.config import MetadataConstants


class DataCatalogTagFactory:

    def __init__(self):
        pass

    def make_tag_for_cluster(self, tag_template, cluster_metadata):
        tag = datacatalog_v1beta1.types.Tag()
        tag.template = tag_template.name
        tag.fields['num_brokers'].double_value = cluster_metadata[
            MetadataConstants.NUM_BROKERS]
        tag.fields['bootstrap_address'].string_value = cluster_metadata[
            MetadataConstants.BOOTSTRAP_SERVER]
        num_topics = len(cluster_metadata[MetadataConstants.TOPICS])
        tag.fields['num_topics'].double_value = num_topics
        return tag

    def make_tag_for_topic(self, tag_template, topic_metadata):
        tag = datacatalog_v1beta1.types.Tag()
        tag.template = tag_template.name
        tag.fields['num_partitions'].double_value = topic_metadata[
            MetadataConstants.NUM_PARTITIONS]
        tag.fields['cleanup_policy'].string_value = topic_metadata[
            MetadataConstants.CLEANUP_POLICY]
        retention_time = topic_metadata.get(MetadataConstants.RETENTION_TIME)
        if retention_time:
            tag.fields['retention_time'].string_value = retention_time
        retention_space = topic_metadata.get(MetadataConstants.RETENTION_SPACE)
        if retention_space:
            tag.fields['retention_space'].double_value = retention_space
        min_compaction_lag = topic_metadata.get(
            MetadataConstants.MIN_COMPACTION_LAG)
        if min_compaction_lag:
            tag.fields['min_compaction_lag'].string_value = min_compaction_lag
        max_compaction_lag = topic_metadata.get(
            MetadataConstants.MAX_COMPACTION_LAG)
        if max_compaction_lag:
            tag.fields['max_compaction_lag'].string_value = max_compaction_lag
        return tag
