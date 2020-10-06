import unittest

from google.datacatalog_connectors.kafka.config import TagTemplateConstants
from google.datacatalog_connectors.kafka.prepare \
    import DataCatalogTagTemplateFactory


class DataCatalogTagTemplateFactoryTest(unittest.TestCase):

    __PROJECT_ID = 'test_project'
    __LOCATION_ID = 'test_location'
    __ENTRY_GROUP_ID = 'my_entry_group'

    def test_create_tag_template_for_cluster_metadata(self):
        factory = DataCatalogTagTemplateFactory(self.__PROJECT_ID,
                                                self.__LOCATION_ID,
                                                self.__ENTRY_GROUP_ID)
        tag_template_id, tag_template = factory.\
            create_tag_template_for_cluster_metadata()
        self.assertEqual(tag_template_id, "my_entry_group_cluster_metadata")
        self.assertEqual(tag_template.display_name,
                         "My_entry_group Cluster - Metadata")
        self._assert_fields(
            tag_template,
            TagTemplateConstants.get_fields_dict_for_cluster_tag_templates)

    def test_create_tag_template_for_topic_metadata(self):
        factory = DataCatalogTagTemplateFactory(self.__PROJECT_ID,
                                                self.__LOCATION_ID,
                                                self.__ENTRY_GROUP_ID)
        tag_template_id, tag_template = factory.\
            create_tag_template_for_topic_metadata()
        self.assertEqual(tag_template_id, "my_entry_group_topic_metadata")
        self.assertEqual(tag_template.display_name,
                         "My_entry_group Topic - Metadata")
        self._assert_fields(
            tag_template,
            TagTemplateConstants.get_fields_dict_for_topic_tag_templates)

    def _assert_fields(self, tag_template, get_fields_function):
        fields = get_fields_function()
        for field_name, field_attributes in fields.items():
            self.assertEqual(
                tag_template.fields[field_name].display_name,
                field_attributes[TagTemplateConstants.DISPLAY_NAME_IDX])
            self.assertEqual(
                tag_template.fields[field_name].type.primitive_type,
                field_attributes[TagTemplateConstants.FIELD_TYPE_IDX])
            if TagTemplateConstants.IS_REQUIRED_IDX < len(field_attributes):
                self.assertEqual(
                    tag_template.fields[field_name].is_required,
                    field_attributes[TagTemplateConstants.IS_REQUIRED_IDX])
