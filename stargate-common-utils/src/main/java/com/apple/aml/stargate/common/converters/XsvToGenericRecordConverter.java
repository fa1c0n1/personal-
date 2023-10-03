package com.apple.aml.stargate.common.converters;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER_CHAR;
import static com.apple.aml.stargate.common.utils.CsvUtils.csvSchema;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public final class XsvToGenericRecordConverter implements ObjectToGenericRecordConverter, Serializable {
    private static final long serialVersionUID = 1L;
    private final Schema schema;
    private final ObjectReader reader;

    public XsvToGenericRecordConverter(final Schema schema) {
        this(schema, DEFAULT_DELIMITER_CHAR);
    }

    public XsvToGenericRecordConverter(final Schema schema, final char delimiter) {
        this.schema = schema;
        this.reader = (new CsvMapper()).readerFor(Map.class).with(csvSchema(this.schema).withColumnSeparator(delimiter));
    }

    public static XsvToGenericRecordConverter converter(final Schema schema, final char delimiter) {
        return new XsvToGenericRecordConverter(schema, delimiter);
    }

    public static XsvToGenericRecordConverter converter(final Schema schema) {
        return new XsvToGenericRecordConverter(schema);
    }

    private static Object toAvro(final JsonNode jsonNode, final Schema schema, final String fieldName) throws Exception {
        if (jsonNode == null || jsonNode.isNull()) {
            return null;
        }
        try {
            switch (schema.getType()) {
                case UNION:
                    List<Schema> types = schema.getTypes();
                    boolean isNullAllowed = false;
                    for (Schema type : types) {
                        if (type.getType() == Schema.Type.NULL) {
                            isNullAllowed = true;
                            continue;
                        }
                        Object avroObject = toAvro(jsonNode, type, fieldName);
                        if (avroObject != null) {
                            return avroObject;
                        }
                    }
                    if (isNullAllowed) {
                        return null;
                    }
                case STRING:
                    return jsonNode.asText();
                case INT:
                    String intStrValue = jsonNode.asText();
                    if (intStrValue == null || intStrValue.isBlank()) {
                        return null;
                    } else {
                        return parseInt(intStrValue);
                    }
                case LONG:
                    String longStrValue = jsonNode.asText();
                    if (longStrValue == null || longStrValue.isBlank()) {
                        return null;
                    } else {
                        return parseLong(longStrValue);
                    }
                case FLOAT:
                    String floatStrValue = jsonNode.asText();
                    if (floatStrValue == null || floatStrValue.isBlank()) {
                        return null;
                    } else {
                        return parseFloat(floatStrValue);
                    }
                case DOUBLE:
                    String doubleStrValue = jsonNode.asText();
                    if (doubleStrValue == null || doubleStrValue.isBlank()) {
                        return null;
                    } else {
                        return parseDouble(doubleStrValue);
                    }
                case BOOLEAN:
                    String boolStrValue = jsonNode.asText();
                    if (boolStrValue == null || boolStrValue.isBlank()) {
                        return null;
                    } else {
                        return parseBoolean(boolStrValue);
                    }
                case ENUM:
                    List<String> symbols = schema.getEnumSymbols();
                    String textValue = jsonNode.textValue();
                    if (symbols.contains(textValue)) {
                        return new GenericData.EnumSymbol(schema, textValue);
                    }
                    break;
                case NULL:
                    return null;
                case FIXED:
                    return jsonNode.toString();
                default:
                    throw new InvalidInputException(schema.getType() + " schema type not supported for payloads of type XSV", Map.of("fieldName", fieldName, "schemaType", schema.getType()));
            }
        } catch (NumberFormatException ne) {
            throw new InvalidInputException("Schema mismatch with supplied jsonNode value", Map.of("fieldName", fieldName, "schemaType", schema.getType()), ne);
        }
        throw new InvalidInputException("Schema mismatch with supplied jsonNode value", Map.of("fieldName", fieldName, "schemaType", schema.getType()));
    }

    private static Object toAvroRecord(final JsonNode jsonNode, final Schema schema) throws Exception {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        for (Schema.Field child : schema.getFields()) {
            JsonNode childNode = jsonNode.get(child.name());
            if (childNode == null) {
                childNode = jsonNode.get(child.name().replace('_', '-'));
            }
            record.set(child, toAvro(childNode, child.schema(), child.name()));
        }
        return record.build();
    }

    @Override
    public GenericRecord convert(final Object object) throws Exception {
        if (object == null) {
            throw new InvalidInputException("null object found. Did you send valid xsv buffer ?");
        }
        if (object instanceof ByteBuffer) {
            return (GenericRecord) toAvroRecord(reader.readTree(((ByteBuffer) object).array()), schema);
        }
        return (GenericRecord) toAvroRecord(reader.readTree(object.toString().getBytes(StandardCharsets.UTF_8)), schema);
    }

    @Override
    public GenericRecord convert(final Object object, final String unstructuredFieldName) throws Exception {
        throw new UnsupportedOperationException("Unstructured support is not available in this class !");
    }

    @Override
    public Iterator<GenericRecord> iterator(final Object object) throws Exception {
        throw new UnsupportedOperationException("iterator support is not available in this class !");
    }
}
