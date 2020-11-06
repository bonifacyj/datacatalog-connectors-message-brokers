from avro import schema
from google.datacatalog_connectors.kafka.config.\
    metadata_constants import MetadataConstants


class AvroSchemaParser:

    RECORD = "record"
    UNION = "union"

    def __init__(self, schema_str):
        self._parsed_schema = schema.parse(schema_str)

    def get_schema_type(self):
        return self._parsed_schema.type

    def get_schema_name(self):
        if self._parsed_schema.type in schema.NAMED_TYPES:
            return self._parsed_schema.name
        return None

    def get_fields_names_and_types(self):
        if self._parsed_schema.type == self.RECORD \
                or self._parsed_schema.type == self.UNION:
            return self._get_fields_names_and_types(self._parsed_schema)
        return None

    def _get_fields_names_and_types(self, parsed_schema):
        if parsed_schema.type == self.RECORD:
            fields = self._scrape_fields_from_record_schema(parsed_schema)
            return fields
        if parsed_schema.type == self.UNION:
            nested_schemas = parsed_schema.schemas
            fields = []
            for nested_schema in nested_schemas:
                found_fields = self._get_fields_names_and_types(nested_schema)
                if found_fields:
                    fields.extend(found_fields)
            if len(fields):
                return fields
        if parsed_schema.type in schema.NAMED_TYPES:
            return [{
                MetadataConstants.FIELD_TYPE: parsed_schema.type,
                MetadataConstants.FIELD_NAME: parsed_schema.name
            }]
        else:
            return [{
                MetadataConstants.FIELD_TYPE: parsed_schema.type,
                MetadataConstants.FIELD_NAME: "None"
            }]

    def _scrape_fields_from_record_schema(self, record_schema):
        avro_fields = []
        fields = record_schema.fields
        for field in fields:
            if field.type.type != self.RECORD:
                avro_fields.append({
                    MetadataConstants.FIELD_TYPE: field.type.type,
                    MetadataConstants.FIELD_NAME: field.name
                })
            else:
                subfields = self._scrape_fields_from_record_schema(field.type)
                avro_fields.append({
                    MetadataConstants.FIELD_TYPE: field.type.type,
                    MetadataConstants.FIELD_NAME: field.name,
                    MetadataConstants.SCHEMA_SUBFIELDS: subfields
                })
        return avro_fields
