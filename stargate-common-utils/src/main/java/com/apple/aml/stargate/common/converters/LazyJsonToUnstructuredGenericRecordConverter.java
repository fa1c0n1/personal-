package com.apple.aml.stargate.common.converters;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.converters.LazyJsonToGenericRecordConverter.isNullAllowed;
import static com.apple.aml.stargate.common.converters.LazyJsonToGenericRecordConverter.jsoniterNode;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;

public class LazyJsonToUnstructuredGenericRecordConverter implements ObjectToGenericRecordConverter, Serializable {
    public static final String DEFAULT_UNSTRUCTURED_FIELD_NAME = "unstructured";
    private static final long serialVersionUID = 1L;
    protected final Schema schema;

    public LazyJsonToUnstructuredGenericRecordConverter(final Schema schema) {
        this.schema = schema;
    }

    public static LazyJsonToUnstructuredGenericRecordConverter converter(final Schema schema) {
        return new LazyJsonToUnstructuredGenericRecordConverter(schema);
    }

    protected Pair<?, ?> toAvro(final Any node, final Schema schema, final String fieldName) throws InvalidInputException {
        Schema.Type schemaType = schema.getType();
        if (node == null) {
            if (isNullAllowed(schema, schemaType)) {
                return Pair.of(null, null);
            }
            throw new InvalidInputException("Schema mismatch with supplied value - Null not allowed", Map.of("fieldName", fieldName, "schemaType", schemaType, "errorType", "Null not allowed"));
        }
        ValueType valueType = node.valueType();
        if (valueType == ValueType.NULL || valueType == ValueType.INVALID || valueType == null) {
            if (isNullAllowed(schema, schemaType)) {
                return Pair.of(null, null);
            }
            throw new InvalidInputException("Schema mismatch with supplied value - Invalid value type found", Map.of("fieldName", fieldName, "schemaType", schemaType, "valueTypeFound", String.valueOf(valueType), "errorType", "Invalid value type"));
        }

        // if node represents XML field with attributes, it is of type ValueType.OBJECT,
        // which is a map where each attribute is a k/v pair,
        // and the value of the XML field is under key ""
        if (valueType == ValueType.OBJECT && node.asMap().containsKey("")) {
            return toAvro(node.get(""), schema, fieldName);
        }

        try {
            switch (schemaType) {
                case STRING:
                    return Pair.of(node.toString(), null);
                case RECORD:
                    return toAvroRecord(node, schema);
                case ARRAY:
                    if (valueType == ValueType.ARRAY) {
                        return toAvroArray(node, schema, fieldName);
                    }
                    break;
                case MAP:
                    return toAvroMap(node, schema, fieldName);
                case UNION:
                    Exception latestException = null;
                    boolean nullAllowed = false;
                    for (Schema type : schema.getTypes()) {
                        if (type.getType() == Schema.Type.NULL) {
                            nullAllowed = true;
                            continue;
                        }
                        try {
                            Pair pair = toAvro(node, type, fieldName);
                            if (pair.getLeft() != null) {
                                return pair;
                            }
                        } catch (Exception e) {
                            latestException = e;
                        }
                    }
                    if (nullAllowed) {
                        return Pair.of(null, node.toString());
                    }
                    if (latestException != null) {
                        throw latestException;
                    }
                    break;
                case INT:
                    return Pair.of(node.toInt(), null);
                case LONG:
                    return Pair.of(node.toLong(), null);
                case FLOAT:
                    return Pair.of(node.toFloat(), null);
                case DOUBLE:
                    return Pair.of(node.toDouble(), null);
                case BOOLEAN:
                    return Pair.of(node.toBoolean(), null);
                case ENUM:
                    List<String> symbols = schema.getEnumSymbols();
                    String textValue = node.toString();
                    if (symbols.contains(textValue)) {
                        return Pair.of(new GenericData.EnumSymbol(schema, textValue), null);
                    }
                    break;
                case NULL:
                    return Pair.of(null, null);
                case FIXED:
                default:
                    return Pair.of(node.object(), null);
            }
        } catch (InvalidInputException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidInputException("Schema mismatch with supplied value - Could not convert to required schema type", Map.of("fieldName", fieldName, "schemaType", schemaType, "valueTypeFound", String.valueOf(valueType), "errorType", "Could not convert to required schema type", ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeValue", String.valueOf(node)), e);
        }
        throw new InvalidInputException("Schema mismatch with supplied value - Unknown object schema", Map.of("fieldName", fieldName, "schemaType", schemaType, "valueTypeFound", String.valueOf(valueType), "errorType", "Unknown object schema"));
    }

    @SuppressWarnings("unchecked")
    protected Pair<Object, Object> toAvroRecord(final Any input, final Schema schema) throws InvalidInputException {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        Any node = input.valueType() == ValueType.STRING ? jsoniterNode(input.toString()) : input;
        Map<String, Any> map = node.asMap();
        Map<String, Object> accumulator = null;
        for (Schema.Field child : schema.getFields()) {
            Any childNode = map.remove(child.name());
            if (childNode == null) {
                childNode = map.remove(child.name().replace('_', '-'));
            }
            Pair<Object, Object> pair = (Pair<Object, Object>) toAvro(childNode, child.schema(), child.name());
            record.set(child, pair.getLeft());
            if (pair.getRight() == null) {
                continue;
            }
            if (accumulator == null) {
                accumulator = new HashMap<>();
            }
            accumulator.put(child.name(), pair.getRight());
        }
        if (map.isEmpty()) {
            return Pair.of(record.build(), accumulator);
        }
        if (accumulator == null) {
            accumulator = new HashMap<>();
        }
        for (Map.Entry<String, Any> entry : map.entrySet()) {
            try {
                accumulator.put(entry.getKey(), readJsonMap(entry.getValue().toString()));
            } catch (Exception ignored) {
                accumulator.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return Pair.of(record.build(), accumulator);
    }

    private Pair<Object, Object> toAvroArray(final Any input, final Schema schema, final String fieldName) throws InvalidInputException {
        Any node = input.valueType() == ValueType.STRING ? jsoniterNode(input.toString()) : input;
        int arraySize = node.size();
        if (arraySize == 0) {
            return Pair.of(new ArrayList<>(1), null);
        }
        Schema arraySchema = schema.getElementType();
        List<Object> records = new ArrayList<>(arraySize);
        List<Object> accumulatorRecords = null;
        for (Any entry : node.asList()) {
            if (entry == null) {
                records.add(null);
                continue;
            }
            Pair<?, ?> pair = toAvro(entry, arraySchema, fieldName);
            records.add(pair.getLeft());
            if (pair.getRight() == null) {
                continue;
            }
            if (accumulatorRecords == null) {
                accumulatorRecords = new ArrayList<>();
            }
            accumulatorRecords.add(pair.getRight());
        }
        return Pair.of(records, accumulatorRecords);
    }

    private Pair<Object, Object> toAvroMap(final Any input, final Schema schema, final String fieldName) throws InvalidInputException {
        Any node = input.valueType() == ValueType.STRING ? jsoniterNode(input.toString()) : input;
        Map<String, Object> map = new HashMap<>();
        Schema valueSchema = schema.getValueType();
        Map<String, Object> accumulator = null;
        for (Map.Entry<String, Any> entry : node.asMap().entrySet()) {
            Any mapNode = entry.getValue();
            if (mapNode == null) {
                map.put(entry.getKey(), null);
                continue;
            }
            Pair<?, ?> pair = toAvro(mapNode, valueSchema, fieldName);
            map.put(entry.getKey(), pair.getLeft());
            if (pair.getRight() == null) {
                continue;
            }
            if (accumulator == null) {
                accumulator = new HashMap<>();
            }
            accumulator.put(entry.getKey(), pair.getRight());
        }
        return Pair.of(map, accumulator);
    }

    @SuppressWarnings("unchecked")
    public Pair<GenericRecord, Map<String, Object>> unstructured(final Object object) throws InvalidInputException {
        if (object == null) {
            throw new InvalidInputException("null object found. Did you send valid json string ?");
        }
        return (Pair<GenericRecord, Map<String, Object>>) toAvro(jsoniterNode(object), schema, "<ROOT>");
    }

    @Override
    public GenericRecord convert(final Object object) throws InvalidInputException, IOException {
        return convert(object, DEFAULT_UNSTRUCTURED_FIELD_NAME);
    }

    @Override
    public GenericRecord convert(final Object object, final String unstructuredFieldName) throws InvalidInputException, IOException {
        if (object == null) {
            throw new InvalidInputException("null object found. Did you send valid json string ?");
        }
        Pair<GenericRecord, Map<String, Object>> pair = unstructured(object);
        if (pair.getRight() == null) {
            return pair.getLeft();
        }
        Optional<Schema.Field> field = schema.getFields().stream().filter(f -> f.name().equalsIgnoreCase(unstructuredFieldName)).findFirst();
        if (!field.isPresent()) {
            return pair.getLeft();
        }
        GenericRecord record = pair.getLeft();
        record.put(field.get().pos(), JsonStream.serialize(pair.getRight()));
        return record;
    }

    @Override
    public Iterator<GenericRecord> iterator(final Object object) throws Exception {
        throw new UnsupportedOperationException("iterator support is not available in this class !");
    }
}
