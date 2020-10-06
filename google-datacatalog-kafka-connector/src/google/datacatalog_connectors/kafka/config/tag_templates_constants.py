from google.cloud import datacatalog_v1beta1


class TagTemplateConstants:
    """
    Constants from this class are used both by
    prepare.DataCatalogTagTemplateFactory and by
    respective tests. This class is intended to be the
    source of truth regarding which fields are expected to exist
    in the tag templates.
    """

    DISPLAY_NAME_IDX = 0
    FIELD_TYPE_IDX = 1
    IS_REQUIRED_IDX = 2

    @staticmethod
    def get_fields_dict_for_cluster_tag_templates():
        fields_dict = {
            'num_brokers':
                ('Number of brokers',
                 datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE,
                 False),
            'num_topics':
                ('Number of topics',
                 datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE,
                 False),
            'bootstrap_address':
                ('Bootstrap address',
                 datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING, True
                )  # noqa E124 ignore closing bracket does not match identation
        }
        return fields_dict

    @staticmethod
    def get_fields_dict_for_topic_tag_templates():
        fields_dict = {
            'num_partitions':
                ('Number of partitions',
                 datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE),
            'retention_policy':
                ('Retention policy',
                 datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING),
            'cleanup_policy':
                ('Cleanup policy',
                 datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING),
            'consumer_groups':
                ('Consumer groups',
                 datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING),
            'schema':
                ('Schema',
                 datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING)
        }
        return fields_dict
