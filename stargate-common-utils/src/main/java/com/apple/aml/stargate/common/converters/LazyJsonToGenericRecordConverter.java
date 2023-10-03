package com.apple.aml.stargate.common.converters;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.fasterxml.jackson.databind.JsonNode;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.jvm.commons.util.Strings.isBlank;

public final class LazyJsonToGenericRecordConverter implements ObjectToGenericRecordConverter, Serializable {
    private static final long serialVersionUID = 1L;
    private final Schema schema;

    public LazyJsonToGenericRecordConverter(final Schema schema) {
        this.schema = schema;
    }

    public static LazyJsonToGenericRecordConverter converter(final Schema schema) {
        return new LazyJsonToGenericRecordConverter(schema);
    }

    static Any jsoniterNode(final Object object) {
        if (object instanceof ByteBuffer) {
            return JsonIterator.deserialize(((ByteBuffer) object).array());
        }
        if (object instanceof String) {
            return JsonIterator.deserialize(((String) object).getBytes(StandardCharsets.UTF_8));
        }
        if (object instanceof JsonNode) {
            return JsonIterator.deserialize(object.toString().getBytes(StandardCharsets.UTF_8));
        }
        return JsonIterator.deserialize(JsonStream.serialize(object).getBytes(StandardCharsets.UTF_8));
    }

    static boolean isNullAllowed(final Schema schema, final Schema.Type schemaType) {
        return schemaType == Schema.Type.UNION && schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
    }

    private Object toAvro(final Any node, final Schema schema, final String fieldName) throws InvalidInputException {
        Schema.Type schemaType = schema.getType();
        if (node == null) {
            if (isNullAllowed(schema, schemaType)) return null;
            throw new InvalidInputException("Schema mismatch with supplied value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, "errorType", "Null not allowed"));
        }
        ValueType valueType = node.valueType();
        if (valueType == ValueType.NULL || valueType == ValueType.INVALID || valueType == null) {
            if (isNullAllowed(schema, schemaType)) return null;
            throw new InvalidInputException("Schema mismatch with supplied value - Invalid value type found", Map.of("fieldName", fieldName, "schemaType", schemaType, "valueTypeFound", String.valueOf(valueType), "errorType", "Invalid value type"));
        }
        try {
            switch (schemaType) {
                case STRING:
                    return node.toString();
                case RECORD:
                    return toAvroRecord(node, schema);
                case ARRAY:
                    if (valueType == ValueType.ARRAY) return toAvroArray(node, schema, fieldName);
                    break;
                case MAP:
                    return toAvroMap(node, schema, fieldName);
                case UNION:
                    Exception latestException = null;
                    for (Schema type : schema.getTypes()) {
                        if (type.getType() == Schema.Type.NULL) continue;
                        try {
                            return toAvro(node, type, fieldName);
                        } catch (Exception e) {
                            latestException = e;
                        }
                    }
                    if (latestException != null) {
                        throw latestException;
                    }
                    break;
                case INT:
                    try {
                        return node.toInt();
                    } catch (Exception e) {
                        if (isNullAllowed(schema, schemaType) && isBlank(node.toString())) return null;
                        throw e;
                    }
                case LONG:
                    try {
                        return node.toLong();
                    } catch (Exception e) {
                        if (isNullAllowed(schema, schemaType) && isBlank(node.toString())) return null;
                        throw e;
                    }
                case FLOAT:
                    try {
                        return node.toFloat();
                    } catch (Exception e) {
                        if (isNullAllowed(schema, schemaType) && isBlank(node.toString())) return null;
                        throw e;
                    }
                case DOUBLE:
                    try {
                        return node.toDouble();
                    } catch (Exception e) {
                        if (isNullAllowed(schema, schemaType) && isBlank(node.toString())) return null;
                        throw e;
                    }
                case BOOLEAN:
                    try {
                        return node.toBoolean();
                    } catch (Exception e) {
                        if (isNullAllowed(schema, schemaType) && isBlank(node.toString())) return null;
                        throw e;
                    }
                case ENUM:
                    List<String> symbols = schema.getEnumSymbols();
                    String textValue = node.toString();
                    if (symbols.contains(textValue)) {
                        return new GenericData.EnumSymbol(schema, textValue);
                    }
                    if (isNullAllowed(schema, schemaType)) return null;
                    break;
                case NULL:
                    return null;
                case FIXED:
                default:
                    return node.object();
            }
        } catch (InvalidInputException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidInputException("Schema mismatch with supplied value - Could not convert to required schema type", Map.of("fieldName", fieldName, "schemaType", schemaType, "valueTypeFound", String.valueOf(valueType), "errorType", "Could not convert to required schema type", ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeValue", String.valueOf(node)), e);
        }
        throw new InvalidInputException("Schema mismatch with supplied value - Unknown object schema", Map.of("fieldName", fieldName, "schemaType", schemaType, "valueTypeFound", String.valueOf(valueType), "errorType", "Unknown object schema"));
    }

    private Object toAvroRecord(final Any input, final Schema schema) throws InvalidInputException {
        Any node = input.valueType() == ValueType.STRING ? jsoniterNode(input.toString()) : input;
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        for (Schema.Field child : schema.getFields()) {
            Any childNode = node.get(child.name());
            if (childNode == null) {
                childNode = node.get(child.name().replace('_', '-'));
            }
            record.set(child, toAvro(childNode, child.schema(), child.name()));
        }
        return record.build();
    }

    private Object toAvroArray(final Any input, final Schema schema, final String fieldName) throws InvalidInputException {
        Any node = input.valueType() == ValueType.STRING ? jsoniterNode(input.toString()) : input;
        int arraySize = node.size();
        if (arraySize == 0) {
            return new ArrayList<>(1);
        }
        Schema arraySchema = schema.getElementType();
        List<Object> records = new ArrayList<>(arraySize);
        for (Any entry : node.asList()) {
            records.add(entry == null ? null : toAvro(entry, arraySchema, fieldName));
        }
        return records;
    }

    private Object toAvroMap(final Any input, final Schema schema, final String fieldName) throws InvalidInputException {
        Any node = input.valueType() == ValueType.STRING ? jsoniterNode(input.toString()) : input;
        Map<String, Object> map = new HashMap<>();
        Schema valueSchema = schema.getValueType();
        for (Map.Entry<String, Any> entry : node.asMap().entrySet()) {
            Any mapNode = entry.getValue();
            map.put(entry.getKey(), mapNode == null ? null : toAvro(mapNode, valueSchema, fieldName));
        }
        return map;
    }

    private Iterator<GenericRecord> jsoniterIterator(final Iterator<Any> iterator) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @SneakyThrows
            @Override
            public GenericRecord next() {
                return (GenericRecord) toAvro(iterator.next(), schema, "<ROOT>");
            }
        };
    }

    @Override
    public GenericRecord convert(final Object object) throws Exception {
        if (object == null) {
            throw new InvalidInputException("null object found. Did you send valid json string ?");
        }
        return (GenericRecord) toAvro(jsoniterNode(object), schema, "<ROOT>");
    }

    @Override
    public GenericRecord convert(final Object object, final String unstructuredFieldName) throws Exception {
        return convert(object);
    }

    @Override
    public Iterator<GenericRecord> iterator(final Object object) throws Exception {
        if (object == null) {
            throw new InvalidInputException("null object found. Did you send valid json array ?");
        }
        final Any any = JsonIterator.deserialize(((ByteBuffer) object).array());
        if (any == null || any.valueType() != ValueType.ARRAY) {
            throw new InvalidInputException("Not a valid JSON Array. Did you send valid json array ?");
        }
        return jsoniterIterator(any.iterator());
    }

}
