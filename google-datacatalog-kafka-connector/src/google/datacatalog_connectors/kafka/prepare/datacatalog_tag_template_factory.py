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

from google.datacatalog_connectors.kafka.config import TagTemplateConstants


class DataCatalogTagTemplateFactory:

    def __init__(self, project_id, location_id, entry_group_id):
        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id

    def create_tag_template_for_cluster_metadata(self):
        """
        Creates a Tag Template with technical details
        regarding cluster metadata
        :return: tag_template_id, tag_template
        """
        tag_template_id, tag_template = self._initialize_tag_template(
            metadata_type="cluster")

        fields = TagTemplateConstants().cluster_constants_list
        tag_template = self._create_fields(fields, tag_template)

        return tag_template_id, tag_template

    def create_tag_template_for_topic_metadata(self):
        """
        Creates a Tag Template with technical details
        regarding topic metadata
        :return: tag_template_id, tag_template
        """
        tag_template_id, tag_template = self._initialize_tag_template(
            metadata_type="topic")

        fields = TagTemplateConstants().topic_constants_list
        tag_template = self._create_fields(fields, tag_template)

        return tag_template_id, tag_template

    def _initialize_tag_template(self, metadata_type):
        tag_template = datacatalog_v1beta1.types.TagTemplate()
        tag_template_id = '{}_{}_metadata'.format(self.__entry_group_id,
                                                  metadata_type)

        tag_template.name = \
            datacatalog_v1beta1.DataCatalogClient.tag_template_path(
                project=self.__project_id,
                location=self.__location_id,
                tag_template=tag_template_id)

        tag_template.display_name = '{} {} - Metadata'.format(
            self.__entry_group_id.capitalize(), metadata_type.capitalize())
        return tag_template_id, tag_template

    def _create_fields(self, fields, tag_template):
        for field in fields:
            tag_template.fields[field.name].type.primitive_type = field.type
            tag_template.fields[field.name].display_name = field.display_name
            tag_template.fields[field.name].is_required = field.is_required
        return tag_template
