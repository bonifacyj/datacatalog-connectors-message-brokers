from avro import schema


class SchemaParser:

    def __init__(self, schema_str):
        self._parsed_schema = schema.parse(schema_str)

    def get_schema_type(self):
        return self._parsed_schema.type

    def get_schema_name(self):
        if self._parsed_schema.type in schema.NAMED_TYPES:
            return self._parsed_schema.name
        return None

    def get_fields_names_and_types(self):
        if self._parsed_schema.type == schema.RECORD \
                or self._parsed_schema.type == schema.UNION:
            return self._get_fields_names_and_types(self._parsed_schema)
        return None

    def _get_fields_names_and_types(self, parsed_schema):
        if parsed_schema.type == schema.RECORD:
            fields = self._scrape_fields_from_record_schema(parsed_schema)
            return fields
        if parsed_schema.type == schema.UNION:
            nested_schemas = parsed_schema.schemas
            fields = []
            for nested_schema in nested_schemas:
                found_fields = self._get_fields_names_and_types(nested_schema)
                if found_fields:
                    fields.extend(found_fields)
            if len(fields):
                return fields
        if parsed_schema.type in schema.NAMED_TYPES:
            return [AvroSchemaField(parsed_schema.name, parsed_schema.type)]
        return None

    def _scrape_fields_from_record_schema(self, record_schema):
        avro_fields = []
        fields = record_schema.fields
        for field in fields:
            if field.type.type != schema.RECORD:
                avro_fields.append(AvroSchemaField(field.type.type,
                                                   field.name))
            else:
                subfields = self._scrape_fields_from_record_schema(field.type)
                avro_fields.append(
                    AvroSchemaField(field.type.type, field.name, subfields))
        return avro_fields


class AvroSchemaField:

    def __init__(self, field_type, name=None, subfields=None):
        self.name = name
        self.field_type = field_type
        self.subfields = subfields

    def __eq__(self, other):
        return type(other) == type(self) \
            and self.name == other.name \
            and self.field_type == other.field_type \
            and self.subfields == other.subfields
