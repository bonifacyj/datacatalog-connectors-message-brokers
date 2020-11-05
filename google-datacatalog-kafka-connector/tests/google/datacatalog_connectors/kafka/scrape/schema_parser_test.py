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
import json

from google.datacatalog_connectors.kafka.scrape.schema_parser \
    import SchemaParser, AvroSchemaField


class SchemaParserTestCase(unittest.TestCase):

    def test_get_fields_from_simple_schema(self):
        schema_dict = {
            "type":
                "record",
            "name":
                "updates",
            "fields": [{
                "name": "id",
                "type": "string"
            }, {
                "name": "degrees",
                "type": "double"
            }]
        }
        schema_str = json.dumps(schema_dict)
        schema_parser = SchemaParser(schema_str)
        fields = schema_parser.get_fields_names_and_types()
        expected_fields = [
            AvroSchemaField("id", "string"),
            AvroSchemaField("degrees", "double")
        ]
        self.assertEqual(str(fields), str(expected_fields))

    def test_get_fields_from_nested_schema(self):
        schema_dict = {
            "name":
                "MasterSchema",
            "namespace":
                "com.namespace.master",
            "type":
                "record",
            "fields": [{
                "name": "field_1",
                "type": {
                    "name":
                        "Dependency",
                    "namespace":
                        "com.namespace.dependencies",
                    "type":
                        "record",
                    "fields": [{
                        "name": "sub_field_1",
                        "type": "string"
                    }, {
                        "name": "sub_field_2",
                        "type": "int"
                    }]
                }
            }, {
                "name": "field_2",
                "type": "com.namespace.dependencies.Dependency"
            }]
        }
        schema_str = json.dumps(schema_dict)
        schema_parser = SchemaParser(schema_str)
        fields = schema_parser.get_fields_names_and_types()
        expected_subfields = [
            AvroSchemaField("sub_field_1", "string"),
            AvroSchemaField("sub_field_2", "int")
        ]
        expected_fields = [
            AvroSchemaField("field_1", "record", expected_subfields),
            AvroSchemaField("field_2", "record", expected_subfields)
        ]
        self.assertEqual(str(fields), str(expected_fields))

    def test_get_fields_schema_non_primitive_types(self):
        schema_dict = {
            "namespace":
                "test.avro",
            "type":
                "record",
            "name":
                "User",
            "fields": [{
                "name": "map_name",
                "type": {
                    "type": "map",
                    "values": "long"
                }
            }, {
                "name": "favorite_number",
                "type": ["int", "null"]
            }, {
                "name": "favorite_color",
                "type": ["string", "null"]
            }]
        }
        schema_str = json.dumps(schema_dict)
        schema_parser = SchemaParser(schema_str)
        fields = schema_parser.get_fields_names_and_types()
        expected_fields = [
            AvroSchemaField("map_name", "map"),
            AvroSchemaField("favorite_number", "union"),
            AvroSchemaField("favorite_color", "union")
        ]
        self.assertEqual(str(fields), str(expected_fields))
