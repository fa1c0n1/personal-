package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.pojo.SimpleSchema;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import lombok.SneakyThrows;
import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;

public final class CsvUtils {
    private static final CsvMapper CSV_MAPPER = new CsvMapper();
    private static final CsvSchema SIMPLE_AVRO_SCHEMA_0 = CSV_MAPPER.schemaFor(SimpleSchema.class);
    private static final CsvSchema SIMPLE_AVRO_SCHEMA_1 = CSV_MAPPER.schemaFor(SimpleSchema.class).withHeader();

    private CsvUtils() {

    }

    public static CsvMapper csvMapper() {
        return CSV_MAPPER;
    }

    public static ColumnType columnType(final Schema schema) {
        switch (schema.getType()) {
            case STRING:
            case ENUM:
                return ColumnType.STRING;
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return ColumnType.NUMBER;
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case UNION:
                List<Schema> types = schema.getTypes();
                for (Schema type : types) {
                    if (type.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return columnType(type);
                }
            default:
                throw new UnsupportedOperationException(schema.getType() + " schema type not supported for payloads of type XSV");
        }
    }

    public static CsvSchema csvSchema(final Schema schema) {
        Builder builder = CsvSchema.builder();
        for (Schema.Field child : schema.getFields()) {
            String fieldName = child.name();
            Schema fieldSchema = child.schema();
            builder.addColumn(fieldName, columnType(fieldSchema));
        }
        return builder.build();
    }

    public static String readSimpleSchema(final String schemaId, final String csv) {
        return readSimpleSchema(schemaId, csv, false);
    }

    public static String readSimpleSchema(final String schemaId, final String csv, final boolean includesHeader) {
        return parseCSVSchema(schemaId, csv, includesHeader).toString();
    }

    @SneakyThrows
    public static Schema parseCSVSchema(final String schemaId, final String csv, final boolean includesHeader) {
        String[] schemaTokens = schemaId.trim().split("\\.");
        if (schemaTokens.length <= 1) throw new InvalidInputException("Invalid schemaId. SchemaId should include both namespace & name", Map.of(SCHEMA_ID, schemaId));
        MappingIterator<SimpleSchema> iterator = CSV_MAPPER.readerFor(SimpleSchema.class).with(includesHeader ? SIMPLE_AVRO_SCHEMA_1 : SIMPLE_AVRO_SCHEMA_0).readValues(csv);
        List<SimpleSchema> columns = iterator.readAll();
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("type", "record");
        spec.put("name", schemaTokens[schemaTokens.length - 1]);
        spec.put("namespace", Arrays.stream(schemaTokens, 0, schemaTokens.length - 1).collect(Collectors.joining(".")));
        spec.put("fields", columns.stream().map(c -> c.avroSpec()).collect(Collectors.toList()));
        String schemaString = jsonString(spec);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }
}
