package com.apple.aml.stargate.common.converters;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public class GenericRecordToMapConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Object fromAvro(final Object object, final Schema schema, final String fieldName) throws Exception {
        if (object == null) {
            return null;
        }
        switch (schema.getType()) {
            case RECORD:
                return fromAvroRecord(object, schema);
            case ARRAY:
                return fromAvroArray(object, schema, fieldName);
            case MAP:
                return fromAvroMap(object, schema, fieldName);
            case STRING:
                return object.toString();
            case INT:
                if (object instanceof Integer) {
                    return object;
                }
                return parseInt(object.toString());
            case LONG:
                if (object instanceof Long) {
                    return object;
                }
                return parseLong(object.toString());
            case FLOAT:
                if (object instanceof Float) {
                    return object;
                }
                return parseFloat(object.toString());
            case DOUBLE:
                if (object instanceof Double) {
                    return object;
                }
                return parseDouble(object.toString());
            case BOOLEAN:
                if (object instanceof Boolean) {
                    return object;
                }
                return parseBoolean(object.toString());
            case UNION:
                List<Schema> types = schema.getTypes();
                for (Schema type : types) {
                    if (type.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return fromAvro(object, type, fieldName);
                }
            case ENUM:
                return object.toString();
            default:
                return object;
        }
    }

    private static Map<String, Object> fromAvroRecord(final Object object, final Schema schema) throws Exception {
        GenericRecord record = (GenericRecord) object;
        Map<String, Object> map = new HashMap<>();
        for (Schema.Field child : schema.getFields()) {
            String key = child.name();
            Object value = record.get(key);
            if (value == null) {
                continue;
            }
            map.put(key, fromAvro(value, child.schema(), child.name()));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Object fromAvroMap(final Object object, final Schema schema, final String fieldName) throws Exception {
        Map<Object, Object> record = (Map<Object, Object>) object;
        Map<Object, Object> map = new HashMap<>();
        Schema valueSchema = schema.getValueType();
        for (Map.Entry<Object, Object> entry : record.entrySet()) {
            map.put(entry.getKey(), fromAvro(entry.getValue(), valueSchema, fieldName));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Object fromAvroArray(final Object object, final Schema schema, final String fieldName) throws Exception {
        Collection<Object> collection = (Collection<Object>) object;
        int arraySize = collection.size();
        if (arraySize == 0) {
            return new ArrayList<>(1);
        }
        Schema arraySchema = schema.getElementType();
        List<Object> records = new ArrayList<>(arraySize);
        Iterator<Object> iterator = collection.iterator();
        while (iterator.hasNext()) {
            records.add(fromAvro(iterator.next(), arraySchema, fieldName));
        }
        return records;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> convert(final GenericRecord record) throws Exception {
        return (Map<String, Object>) fromAvro(record, record.getSchema(), "<ROOT>");
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> convertRecord(final GenericRecord record) throws Exception {
        return (Map<String, Object>) ((Map) readJsonMap(record.toString()));
    }
}
