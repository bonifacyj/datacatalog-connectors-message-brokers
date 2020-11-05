from avro import schema


class SchemaParser:

    def __init__(self, schema_str):
        self._parsed_schema = schema.parse(schema_str)

    def get_fields_names_and_types(self):
        if self._parsed_schema.type == 'record':
            fields = self._scrape_fields_from_record_schema(
                self._parsed_schema)
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
