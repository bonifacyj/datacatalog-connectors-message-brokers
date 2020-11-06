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

from google.datacatalog_connectors.kafka.scrape.avro_schema_parser \
    import AvroSchemaParser
from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants


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
        schema_parser = AvroSchemaParser(schema_str)
        fields = schema_parser.get_fields_names_and_types()
        expected_fields = [{
            MetadataConstants.FIELD_NAME: "id",
            MetadataConstants.FIELD_TYPE: "string"
        }, {
            MetadataConstants.FIELD_NAME: "degrees",
            MetadataConstants.FIELD_TYPE: "double"
        }]
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
        schema_parser = AvroSchemaParser(schema_str)
        fields = schema_parser.get_fields_names_and_types()
        expected_subfields = [{
            MetadataConstants.FIELD_NAME: "sub_field_1",
            MetadataConstants.FIELD_TYPE: "string"
        }, {
            MetadataConstants.FIELD_NAME: "sub_field_2",
            MetadataConstants.FIELD_TYPE: "int"
        }]
        expected_fields = [{
            MetadataConstants.FIELD_NAME: "field_1",
            MetadataConstants.FIELD_TYPE: "record",
            MetadataConstants.SCHEMA_SUBFIELDS: expected_subfields
        }, {
            MetadataConstants.FIELD_NAME: "field_2",
            MetadataConstants.FIELD_TYPE: "record",
            MetadataConstants.SCHEMA_SUBFIELDS: expected_subfields
        }]
        self.maxDiff = None
        self.assertEqual(fields, expected_fields)

    def test_get_fields_schema_mixed_types(self):
        orig_schema = [{
            "namespace":
                "test.avro",
            "type":
                "record",
            "name":
                "User",
            "fields": [{
                "name": "pet_name",
                "type": "string"
            }, {
                "name": "favorite_color",
                "type": ["string", "null"]
            }]
        }, {
            "type": "map",
            "values": "int"
        }, {
            "type": "enum",
            "name": "Weekdays",
            "symbols": ["Mon", "Tue", "Wed", "Thu", "Fri"]
        }]
        schema_str = json.dumps(orig_schema)
        schema_parser = AvroSchemaParser(schema_str)
        fields = schema_parser.get_fields_names_and_types()

        expected_fields = [{
            MetadataConstants.FIELD_NAME: "pet_name",
            MetadataConstants.FIELD_TYPE: "string"
        }, {
            MetadataConstants.FIELD_NAME: "favorite_color",
            MetadataConstants.FIELD_TYPE: "union"
        }, {
            MetadataConstants.FIELD_NAME: "None",
            MetadataConstants.FIELD_TYPE: "map"
        }, {
            MetadataConstants.FIELD_NAME: "Weekdays",
            MetadataConstants.FIELD_TYPE: "enum"
        }]

        self.maxDiff = None
        self.assertEqual(fields, expected_fields)
