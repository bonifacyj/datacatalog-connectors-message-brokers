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
        self._assert_fields(tag_template,
                            TagTemplateConstants().cluster_constants_list)

    def test_create_tag_template_for_topic_metadata(self):
        factory = DataCatalogTagTemplateFactory(self.__PROJECT_ID,
                                                self.__LOCATION_ID,
                                                self.__ENTRY_GROUP_ID)
        tag_template_id, tag_template = factory.\
            create_tag_template_for_topic_metadata()
        self.assertEqual(tag_template_id, "my_entry_group_topic_metadata")
        self.assertEqual(tag_template.display_name,
                         "My_entry_group Topic - Metadata")
        self._assert_fields(tag_template,
                            TagTemplateConstants().topic_constants_list)

    def _assert_fields(self, tag_template, fields):
        for field in fields:
            self.assertEqual(tag_template.fields[field.name].display_name,
                             field.display_name)
            self.assertEqual(
                tag_template.fields[field.name].type.primitive_type,
                field.type)
            self.assertEqual(tag_template.fields[field.name].is_required,
                             field.is_required)
