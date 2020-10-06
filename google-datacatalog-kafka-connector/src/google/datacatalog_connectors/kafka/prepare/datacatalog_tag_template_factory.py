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


class DataCatalogTagTemplateFactory:

    __DOUBLE_TYPE = datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING

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
        tag_template = datacatalog_v1beta1.types.TagTemplate()
        cluster_type = "cluster"
        tag_template_id = '{}_{}_metadata'.format(self.__entry_group_id,
                                                  cluster_type)

        tag_template.name = \
            datacatalog_v1beta1.DataCatalogClient.tag_template_path(
                project=self.__project_id,
                location=self.__location_id,
                tag_template=tag_template_id)

        tag_template.display_name = '{} {} - Metadata'.format(
            self.__entry_group_id.capitalize(), cluster_type.capitalize())

        tag_template.fields[
            'num_brokers'].type.primitive_type = self.__DOUBLE_TYPE
        tag_template.fields['num_brokers'].display_name = 'Number of brokers'

        tag_template.fields[
            'num_topics'].type.primitive_type = self.__DOUBLE_TYPE
        tag_template.fields['num_topics'].display_name = 'Number of topics'

        tag_template.fields[
            'bootstrap_address'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields[
            'bootstrap_address'].display_name = 'Bootstrap address'
        tag_template.fields['bootstrap_address'].is_required = True

        return tag_template_id, tag_template

    def create_tag_template_for_topic_metadata(self):
        """
        Creates a Tag Template with technical details
        regarding topic metadata
        :return: tag_template_id, tag_template
        """
        tag_template = datacatalog_v1beta1.types.TagTemplate()
        topic_type = "topic"
        tag_template_id = '{}_{}_metadata'.format(self.__entry_group_id,
                                                  topic_type)

        tag_template.name = \
            datacatalog_v1beta1.DataCatalogClient.tag_template_path(
                project=self.__project_id,
                location=self.__location_id,
                tag_template=tag_template_id)

        tag_template.display_name = '{} {} - Metadata'.format(
            self.__entry_group_id.capitalize(), topic_type.capitalize())

        tag_template.fields[
            'num_partitions'].type.primitive_type = self.__DOUBLE_TYPE
        tag_template.fields[
            'num_partitions'].display_name = 'Number of partitions'

        tag_template.fields[
            'retention_policy'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields[
            'retention_policy'].display_name = 'Retention policy'

        tag_template.fields[
            'cleanup_policy'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['cleanup_policy'].display_name = 'Cleanup policy'

        tag_template.fields[
            'consumer_groups'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['consumer_groups'].display_name = 'Consumer groups'

        tag_template.fields['schema'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['schema'].display_name = 'Schema'

        return tag_template_id, tag_template
