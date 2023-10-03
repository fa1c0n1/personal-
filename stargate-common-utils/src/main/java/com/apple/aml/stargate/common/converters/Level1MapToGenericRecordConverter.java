package com.apple.aml.stargate.common.converters;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public final class Level1MapToGenericRecordConverter implements ObjectToGenericRecordConverter, Serializable {
    private static final long serialVersionUID = 1L;
    private final Schema schema;

    public Level1MapToGenericRecordConverter(final Schema schema) {
        this.schema = schema;
    }

    public static Level1MapToGenericRecordConverter converter(final Schema schema) {
        return new Level1MapToGenericRecordConverter(schema);
    }

    private static Object toAvro(final Object obj, final Schema schema, final String fieldName, final String referenceKey, final Object referenceValue) throws Exception {
        try {
            return asAvro(obj, schema, fieldName, referenceKey, referenceValue);
        } catch (NumberFormatException ne) {
            throw new InvalidInputException("Schema mismatch with supplied obj value", Map.of("fieldName", fieldName, "schemaType", schema.getType(), "suppliedValue", String.valueOf(obj), referenceKey, referenceValue), ne);
        }
    }

    private static Object asAvro(final Object obj, final Schema schema, final String fieldName, final String referenceKey, final Object referenceValue) throws Exception {
        Schema.Type schemaType = schema.getType();
        switch (schemaType) {
            case UNION:
                List<Schema> types = schema.getTypes();
                boolean isNullAllowed = false;
                for (Schema type : types) {
                    if (type.getType() == Schema.Type.NULL) {
                        isNullAllowed = true;
                        continue;
                    }
                    Object avroObject = null;
                    try {
                        if (obj != null) {
                            avroObject = asAvro(obj, type, fieldName, referenceKey, referenceValue);
                        }
                    } catch (NumberFormatException ne) {
                        throw new InvalidInputException("Schema mismatch with supplied obj value", Map.of("fieldName", fieldName, "schemaType", type, "suppliedValue", String.valueOf(obj), referenceKey, referenceValue), ne);
                    } catch (InvalidInputException e) {
                        avroObject = null;
                    }
                    if (avroObject != null) {
                        return avroObject;
                    }
                }
                if (isNullAllowed) {
                    return null;
                }
                throw new InvalidInputException("Schema mismatch with supplied obj value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
            case STRING:
                if (obj == null) {
                    throw new InvalidInputException("Schema mismatch with supplied obj value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                }
                return obj.toString();
            case INT:
                if (obj == null) {
                    throw new InvalidInputException("Schema mismatch with supplied obj value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                }
                if (obj instanceof Integer) {
                    return obj;
                } else {
                    String trimmed = obj.toString().trim();
                    if (trimmed.isBlank()) {
                        throw new InvalidInputException("Schema mismatch with supplied obj value - Blank not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                    }
                    return parseInt(trimmed);
                }
            case LONG:
                if (obj == null) {
                    throw new InvalidInputException("Schema mismatch with supplied obj value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                }
                if (obj instanceof Long) {
                    return obj;
                } else {
                    String trimmed = obj.toString().trim();
                    if (trimmed.isBlank()) {
                        throw new InvalidInputException("Schema mismatch with supplied obj value - Blank not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                    }
                    return parseLong(trimmed);
                }
            case FLOAT:
                if (obj == null) {
                    throw new InvalidInputException("Schema mismatch with supplied obj value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                }
                if (obj instanceof Float) {
                    return obj;
                } else {
                    String trimmed = obj.toString().trim();
                    if (trimmed.isBlank()) {
                        throw new InvalidInputException("Schema mismatch with supplied obj value - Blank not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                    }
                    return parseFloat(trimmed);
                }
            case DOUBLE:
                if (obj == null) {
                    throw new InvalidInputException("Schema mismatch with supplied obj value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                }
                if (obj instanceof Double) {
                    return obj;
                } else {
                    String trimmed = obj.toString().trim();
                    if (trimmed.isBlank()) {
                        throw new InvalidInputException("Schema mismatch with supplied obj value - Blank not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                    }
                    return parseDouble(trimmed);
                }
            case BOOLEAN:
                if (obj == null) {
                    throw new InvalidInputException("Schema mismatch with supplied obj value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                }
                if (obj instanceof Boolean) {
                    return obj;
                } else {
                    return parseBoolean(obj.toString());
                }
            case ENUM:
                if (obj == null) {
                    throw new InvalidInputException("Schema mismatch with supplied obj value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                }
                List<String> symbols = schema.getEnumSymbols();
                String textValue = obj.toString().trim();
                if (textValue.isBlank()) {
                    throw new InvalidInputException("Schema mismatch with supplied obj value - Blank not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
                }
                if (symbols.contains(textValue)) {
                    return new GenericData.EnumSymbol(schema, textValue);
                } else {
                    throw new InvalidInputException("Schema mismatch with supplied obj value - EnumSymbol could not be translated", Map.of("fieldName", fieldName, "schemaType", schemaType, "suppliedValue", textValue, referenceKey, referenceValue));
                }
            case NULL:
                return null;
            case FIXED:
                return obj.toString();
            default:
                throw new InvalidInputException(schema.getType() + " schema type not supported for payloads of flat structure", Map.of("fieldName", fieldName, "schemaType", schemaType, referenceKey, referenceValue));
        }
    }

    private static <O> Object toAvroRecord(final Map<String, O> map, final Schema schema, final String referenceKey, final Object referenceValue) throws Exception {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        for (Schema.Field child : schema.getFields()) {
            O object = map.get(child.name());
            if (object == null) {
                object = map.get(child.name().replace('_', '-'));
            }
            record.set(child, toAvro(object, child.schema(), child.name(), referenceKey, referenceValue));
        }
        return record.build();
    }

    public <O> GenericRecord convert(final Map<String, O> map, final String referenceKey, final Object referenceValue) throws Exception {
        if (map == null) {
            throw new InvalidInputException("null map found. Did you send valid map ?", Map.of(referenceKey, referenceValue));
        }
        return (GenericRecord) toAvroRecord(map, schema, referenceKey, referenceValue);
    }

    @SuppressWarnings("unchecked")
    @Override
    public GenericRecord convert(final Object object) throws Exception {
        return convert((Map<String, ?>) object, "referenceId", "null");
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
