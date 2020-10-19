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
                            TagTemplateConstants.get_cluster_constants_list())

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
                            TagTemplateConstants.get_topic_constants_list())

    def _assert_fields(self, tag_template, fields):
        for field in fields:
            self.assertEqual(tag_template.fields[field.name].display_name,
                             field.display_name)
            self.assertEqual(
                tag_template.fields[field.name].type.primitive_type,
                field.type)
            self.assertEqual(tag_template.fields[field.name].is_required,
                             field.is_required)
