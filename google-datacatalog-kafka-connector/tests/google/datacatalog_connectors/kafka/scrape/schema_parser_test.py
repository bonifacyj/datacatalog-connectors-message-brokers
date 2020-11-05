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
            AvroSchemaField("string", "id"),
            AvroSchemaField("double", "degrees")
        ]
        self.maxDiff = None
        self.assertListEqual(fields, expected_fields)

    def test_get_fields_from_nested_record_schema(self):
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
            AvroSchemaField("string", "sub_field_1"),
            AvroSchemaField("int", "sub_field_2")
        ]
        expected_fields = [
            AvroSchemaField("record", "field_1", expected_subfields),
            AvroSchemaField("record", "field_2", expected_subfields)
        ]
        self.maxDiff = None
        self.assertEqual(fields, expected_fields)

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
            AvroSchemaField("map", "map_name"),
            AvroSchemaField("union", "favorite_number"),
            AvroSchemaField("union", "favorite_color")
        ]
        self.maxDiff = None
        self.assertEqual(fields, expected_fields)
