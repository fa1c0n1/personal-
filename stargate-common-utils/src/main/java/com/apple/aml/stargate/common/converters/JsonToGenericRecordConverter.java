package com.apple.aml.stargate.common.converters;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.JsonUtils.jsonNode;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static org.apache.avro.Schema.Field;

@Data
public final class JsonToGenericRecordConverter implements ObjectToGenericRecordConverter, Serializable {
    private static final long serialVersionUID = 1L;
    private final Schema schema;

    public JsonToGenericRecordConverter(final Schema schema) {
        this.schema = schema;
    }

    public static JsonToGenericRecordConverter converter(final Schema schema) {
        return new JsonToGenericRecordConverter(schema);
    }

    private static Object toAvro(final JsonNode jsonNode, final Schema schema, final String fieldName) throws Exception {
        if (jsonNode == null || jsonNode.isNull()) {
            return null;
        }
        switch (schema.getType()) {
            case RECORD:
                return toAvroRecord(jsonNode, schema);
            case ARRAY:
                if (jsonNode.isArray()) {
                    return toAvroArray(jsonNode, schema, fieldName);
                }
                break;
            case MAP:
                return toAvroMap(jsonNode, schema, fieldName);
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
                return jsonNode.isValueNode() ? jsonNode.asText() : jsonNode.toString();
            case BYTES:
                if (jsonNode.isBinary()) {
                    return jsonNode.binaryValue();
                }
                break;
            case INT:
                if (jsonNode.isNumber()) {
                    return jsonNode.asInt();
                }
                break;
            case LONG:
                if (jsonNode.isNumber()) {
                    return jsonNode.asLong();
                }
                break;
            case FLOAT:
                if (jsonNode.isNumber()) {
                    return (float) jsonNode.asDouble();
                }
                break;
            case DOUBLE:
                if (jsonNode.isNumber()) {
                    return jsonNode.asDouble();
                }
                break;
            case BOOLEAN:
                if (jsonNode.isBoolean()) {
                    return jsonNode.asBoolean();
                }
                break;
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
            default:
                return jsonNode.toString();
        }
        throw new InvalidInputException("Schema mismatch with supplied jsonNode value", Map.of("fieldName", fieldName, "schemaType", schema.getType()));
    }

    private static Object toAvroRecord(final JsonNode jsonNode, final Schema schema) throws Exception {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        for (Field child : schema.getFields()) {
            JsonNode childNode = jsonNode.get(child.name());
            if (childNode == null) {
                childNode = jsonNode.get(child.name().replace('_', '-'));
            }
            record.set(child, toAvro(childNode, child.schema(), child.name()));
        }
        return record.build();
    }

    private static Object toAvroMap(final JsonNode jsonNode, final Schema schema, final String fieldName) throws Exception {
        Map<String, Object> map = new HashMap<>();
        Schema valueSchema = schema.getValueType();
        Iterator<Map.Entry<String, JsonNode>> iterator = jsonNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> childNode = iterator.next();
            map.put(childNode.getKey(), toAvro(childNode.getValue(), valueSchema, fieldName));
        }
        return map;
    }

    private static Object toAvroArray(final JsonNode jsonNode, final Schema schema, final String fieldName) throws Exception {
        int arraySize = jsonNode.size();
        if (arraySize == 0) {
            return new ArrayList<>(1);
        }
        Schema arraySchema = schema.getElementType();
        List<Object> records = new ArrayList<>(arraySize);
        Iterator<JsonNode> iterator = jsonNode.elements();
        while (iterator.hasNext()) {
            records.add(toAvro(iterator.next(), arraySchema, fieldName));
        }
        return records;
    }

    @Override
    public GenericRecord convert(final Object object) throws Exception {
        if (object == null) {
            throw new InvalidInputException("null object found. Did you send valid json ?");
        }
        JsonNode node = (object instanceof JsonNode) ? (JsonNode) object : (object instanceof String ? jsonNode((String) object) : jsonNode(jsonString(object)));
        return (GenericRecord) toAvro(node, schema, "<ROOT>");
    }

    @Override
    public GenericRecord convert(final Object object, final String unstructuredFieldName) throws Exception {
        throw new UnsupportedOperationException("Unstructured support is not available in this class ! Please consider using LazyJsonToUnstructuredGenericRecordConverter");
    }

    @Override
    public Iterator<GenericRecord> iterator(final Object object) throws Exception {
        throw new UnsupportedOperationException("iterator support is not available in this class !");
    }
}
