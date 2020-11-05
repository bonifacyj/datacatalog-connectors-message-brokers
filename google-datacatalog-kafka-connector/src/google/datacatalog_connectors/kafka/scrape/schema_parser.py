from avro import schema


class SchemaParser:

    def get_fields_names_and_types(self, schema_str):
        parsed_schema = schema.parse(schema_str)
        return self._get_fields_names_and_types(parsed_schema)

    def _get_fields_names_and_types(self, parsed_schema):
        if parsed_schema.type == 'record':
            fields = self._scrape_fields_from_record_schema(parsed_schema)
            return fields
        if parsed_schema.type == 'union':
            nested_schemas = parsed_schema.schemas
            fields = []
            for nested_schema in nested_schemas:
                found_fileds = self._get_fields_names_and_types(nested_schema)
                if found_fileds:
                    fields.extend(found_fileds)
            if len(fields):
                return fields
        return None

    def _scrape_fields_from_record_schema(self, record_schema):
        avro_fields = []
        fields = record_schema.fields
        for field in fields:
            if field.type.type != 'record':
                avro_fields.append(AvroSchemaField(field.name,
                                                   field.type.type))
            else:
                subfields = self._scrape_fields_from_record_schema(field.type)
                avro_fields.append(
                    AvroSchemaField(field.name, field.type.type, subfields))
        return avro_fields


class AvroSchemaField:

    def __init__(self, name, field_type, subfields=None):
        self.name = name
        self.field_type = field_type
        self.subfields = subfields

    def __eq__(self, other):
        return type(other) == type(self) \
            and self.name == other.name \
            and self.field_type == other.field_type \
            and self.subfields == other.subfields
