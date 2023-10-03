package com.apple.aml.stargate.common.converters;

import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.avro.Schema.Field;
import static org.apache.avro.Schema.Type;

@Data
public final class GenericSubsetRecordExtractor implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Schema schema;

    public GenericSubsetRecordExtractor(final Schema schema) {
        this.schema = schema;
    }

    public static GenericSubsetRecordExtractor extractor(final Schema schema) {
        return new GenericSubsetRecordExtractor(schema);
    }

    @SuppressWarnings("unchecked")
    private static Object extractArray(final String fieldName, final Object srcObject, final Schema srcSchema, final Schema targetSchema) throws Exception {
        Schema sourceElementType = srcSchema.getElementType();
        Schema targetElementType = targetSchema.getElementType();
        if (targetElementType.getType() != targetElementType.getType()) {
            throw new InvalidInputException("subset/target array schema mismatches with source array schema", Map.of("fieldName", fieldName, "targetType", targetElementType, "sourceType", sourceElementType));
        }
        Type targetType = targetElementType.getType();
        switch (targetType) { // this switch to avoid creating new array/collection & looping collection and giving direct reference to original array/collection
            case STRING:
                return srcObject == null ? null : srcObject instanceof Utf8 ? srcObject.toString() : srcObject;
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case ENUM:
            case FIXED:
            case NULL:
                return srcObject;
            default:
        }
        boolean isArray = false;
        Collection<?> srcRecords;
        Collection<Object> targetRecords;
        if (srcObject.getClass().isArray()) {
            isArray = true;
            srcRecords = Arrays.asList((Object[]) srcObject);
            targetRecords = new ArrayList<>(srcRecords.size());
        } else {
            srcRecords = (Collection<?>) srcObject;
            try {
                targetRecords = (List<Object>) srcObject.getClass().getDeclaredConstructor().newInstance(); // try to match same collection type as source if possible
            } catch (Exception e) {
                targetRecords = new ArrayList<>(srcRecords.size());
            }
        }
        switch (targetType) {
            case RECORD:
                for (Object srcRecord : srcRecords) {
                    Object extractedObject = extractRecord(fieldName, srcRecord, sourceElementType, targetElementType);
                    targetRecords.add(extractedObject);
                }
                break;
            case ARRAY:
                for (Object srcRecord : srcRecords) {
                    Object extractedObject = extractArray(fieldName, srcRecord, sourceElementType, targetElementType);
                    targetRecords.add(extractedObject);
                }
                break;
            case MAP:
                for (Object srcRecord : srcRecords) {
                    Object extractedObject = extractMap(fieldName, srcRecord, sourceElementType, targetElementType);
                    targetRecords.add(extractedObject);
                }
                break;
            default:
                return srcRecords;
        }
        if (isArray) { // if source is of type java Array, return a java Array itself instead of Collection
            GenericRecord[] returnRecords = new GenericRecord[targetRecords.size()];
            targetRecords.toArray(returnRecords);
            return returnRecords;
        }
        return targetRecords;
    }

    @SuppressWarnings("unchecked")
    private static Object extractMap(final String fieldName, final Object srcObject, final Schema srcSchema, final Schema targetSchema) throws Exception {
        Schema srcValueSchema = srcSchema.getValueType();
        Type srcValueType = srcValueSchema.getType();
        if (srcValueType == Type.UNION) {
            for (Schema schema : srcSchema.getValueType().getTypes()) {
                if (schema.getType() == Type.NULL) {
                    continue;
                }
                srcValueSchema = schema; // pick the first non-null schema ( => no support for multiple non-null unions )
                srcValueType = srcValueSchema.getType();
                break;
            }
        }
        Schema targetValueSchema = targetSchema.getValueType();
        Type targetValueType = targetValueSchema.getType();
        if (targetValueType == Type.UNION) {
            for (Schema schema : targetSchema.getValueType().getTypes()) {
                if (schema.getType() == Type.NULL) {
                    continue;
                }
                targetValueSchema = schema; // pick the first non-null schema ( => no support for multiple non-null unions )
                targetValueType = targetValueSchema.getType();
                break;
            }
        }
        if (targetValueType != srcValueType) {
            throw new InvalidInputException("subset/target value schema mismatches with source value schema", Map.of("fieldName", fieldName, "targetType", targetValueType, "sourceType", srcValueType));
        }
        switch (targetValueType) { // this switch to avoid src Map iteration & new Map creation and get direct handle to src Map reference
            case STRING:
                return srcObject == null ? null : srcObject instanceof Utf8 ? srcObject.toString() : srcObject;
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case FIXED:
            case NULL:
                return srcObject;
            default:
        }
        Map<String, ?> srcMap = (Map<String, ?>) srcObject;
        if (srcMap.isEmpty()) {
            return srcMap;
        }
        Map<String, Object> targetMap;
        try {
            targetMap = (Map<String, Object>) srcObject.getClass().getDeclaredConstructor().newInstance(); // try to match same collection type as source if possible
        } catch (Exception e) {
            targetMap = new HashMap<>(srcMap.size());
        }
        switch (targetValueType) {
            case RECORD:
                for (Map.Entry<String, ?> entry : srcMap.entrySet()) {
                    Object extractedObject = extractRecord(fieldName, entry.getValue(), srcValueSchema, targetValueSchema);
                    targetMap.put(String.valueOf(entry.getKey()), extractedObject);
                }
                return targetMap;
            case ARRAY:
                for (Map.Entry<String, ?> entry : srcMap.entrySet()) {
                    Object extractedObject = extractArray(fieldName, entry.getValue(), srcValueSchema, targetValueSchema);
                    targetMap.put(String.valueOf(entry.getKey()), extractedObject);
                }
                return targetMap;
            case MAP:
                for (Map.Entry<String, ?> entry : srcMap.entrySet()) {
                    Object extractedObject = extractMap(fieldName, entry.getValue(), srcValueSchema, targetValueSchema);
                    targetMap.put(String.valueOf(entry.getKey()), extractedObject);
                }
                return targetMap;
            default:
        }
        return srcObject; // will never reach here
    }

    private static Object extractRecord(final String recordName, final Object srcObject, final Schema srcSchema, final Schema targetSchema) throws Exception {
        GenericRecord srcRecord = (GenericRecord) srcObject;
        GenericRecord targetRecord = new GenericData.Record(targetSchema);
        for (Field targetField : targetSchema.getFields()) {
            String fieldName = targetField.name();
            Field srcField = srcSchema.getField(fieldName);
            if (srcField == null) {
                continue; // subset/target schema is having additional/new field which is missing in parent. will ignore & continue
            }
            Object extractedObject = extract(fieldName, srcRecord.get(fieldName), srcField.schema(), targetField.schema());
            if (extractedObject == null) {
                continue;
            }
            targetRecord.put(fieldName, extractedObject); // value is by reference
        }
        return targetRecord;
    }

    private static Object extract(final String fieldName, final Object srcObject, final Schema srcSchema, final Schema targetSchema) throws Exception {
        Type targetType = targetSchema.getType();
        Schema srcNonNullSchema = srcSchema;
        Schema targetNonNullSchema = targetSchema;
        boolean targetNullAllowed = false;
        if (targetType == Type.UNION) {
            targetNullAllowed = targetNonNullSchema.getTypes().stream().anyMatch(t -> t.getType() == Type.NULL);
            for (Schema schema : targetNonNullSchema.getTypes()) {
                if (schema.getType() != Type.NULL) {
                    targetNonNullSchema = schema; // pick the first non-null schema ( => no support for multiple non-null unions )
                    targetType = schema.getType();
                    break;
                }
            }
        }
        if (srcObject == null) {
            if (targetNullAllowed) {
                return null;
            }
            throw new InvalidInputException("subset/target schema has a non-null field for which input record value has a null or missing field value", Map.of("fieldName", fieldName, "targetType", targetType, "sourceType", srcNonNullSchema.getType()));
        }
        Type srcType = srcNonNullSchema.getType();
        if (srcType == Type.UNION) {
            for (Schema schema : srcNonNullSchema.getTypes()) {
                if (schema.getType() == Type.NULL) {
                    continue;
                }
                srcNonNullSchema = schema; // pick the first non-null schema ( => no support for multiple non-null unions )
                srcType = schema.getType();
                break;
            }
        }
        if (srcType != targetType) {
            throw new InvalidInputException("subset/target schema mismatches with source schema", Map.of("fieldName", fieldName, "targetType", targetType, "sourceType", srcType));
        }
        switch (targetType) {
            case RECORD:
                return extractRecord(fieldName, srcObject, srcNonNullSchema, targetNonNullSchema);
            case ARRAY:
                return extractArray(fieldName, srcObject, srcNonNullSchema, targetNonNullSchema);
            case MAP:
                return extractMap(fieldName, srcObject, srcNonNullSchema, targetNonNullSchema);
            case STRING:
                return srcObject == null ? null : srcObject instanceof Utf8 ? srcObject.toString() : srcObject;
            default:
                return srcObject; // value is by reference
        }
    }

    public GenericRecord extract(final GenericRecord record) throws Exception {
        return (GenericRecord) extract("<ROOT>", record, record.getSchema(), schema);
    }
}
