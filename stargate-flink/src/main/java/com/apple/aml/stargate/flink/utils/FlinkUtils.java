package com.apple.aml.stargate.flink.utils;

import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.FlinkSqlOptions;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.common.services.ErrorService;
import com.apple.aml.stargate.common.services.NodeService;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import com.apple.aml.stargate.flink.inject.NoOpFlinkErrorHandler;
import com.apple.aml.stargate.flink.inject.NoOpFlinkNodeServiceHandler;
import com.apple.aml.stargate.flink.serde.AvroSchemaSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.types.Row;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public final class FlinkUtils {

    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private static final NodeService nodeService = new NoOpFlinkNodeServiceHandler();
    private static final ErrorService errorService = new NoOpFlinkErrorHandler();

    private FlinkUtils() {
    }

    public static void registerSerde(StreamExecutionEnvironment executionEnvironment) {
        executionEnvironment.addDefaultKryoSerializer(org.apache.avro.Schema.class, AvroSchemaSerializer.class);
    }

    public static void registerUDF(StreamExecutionEnvironment executionEnvironment) {
        //TODO need to register UDFs
    }


    public static Map<String, String> convertPropertiesToMap(byte[] propertiesContent, Map<String, String> map) {
        Properties properties = new Properties();

        try (ByteArrayInputStream bis = new ByteArrayInputStream(propertiesContent)) {
            properties.load(bis);
        } catch (IOException e) {
            return map;
        }
        for (String key : properties.stringPropertyNames()) {
            map.put(key, properties.getProperty(key));
        }
        return map;
    }

    public static void writeToFileSystem(String basePath, String fileName, byte[] content) {

        if (basePath == null) {
            basePath = "/mnt/app/shared/lib/";
        }
        String fullPath = Paths.get(basePath, fileName).toString();
        try (FileOutputStream fos = new FileOutputStream(fullPath)) {
            fos.write(content);
            System.out.println("Written " + fileName + " to " + fullPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String processSQLWithPlaceholders(String sql, Map<String, String> placeholderMap) {
        for (Map.Entry<String, String> entry : placeholderMap.entrySet()) {
            String placeholder = "${" + entry.getKey() + "}";
            String value = entry.getValue();
            sql = sql.replace(placeholder, value);
        }
        return sql;
    }

    private static String extractSchemaId(String sql) {
        int start = sql.indexOf("'avro-schema' = '") + "'avro-schema' = '".length();
        int end = sql.indexOf("'", start);
        return sql.substring(start, end);
    }


    public static org.apache.flink.table.api.Schema getTableSchemaFromAvro(String schemaString) {
        org.apache.flink.table.api.Schema.Builder schemaBuilder = org.apache.flink.table.api.Schema.newBuilder();
        RowType rowType = (RowType) AvroSchemaConverter.convertToDataType(schemaString).getLogicalType();

        for (RowType.RowField rowField : rowType.getFields()) {
            String fieldName = rowField.getName();
            LogicalType logicalType = rowField.getType();
            schemaBuilder.column(fieldName, LogicalTypeDataTypeConverter.toDataType(logicalType));
        }
        org.apache.flink.table.api.Schema schema = schemaBuilder.build();
        return schema;
    }


    @SuppressWarnings("unchecked")
    public static Object convertToAvro(Object value, Schema schema) throws IOException {
        if (value == null) {
            return null;
        }
        switch (schema.getType()) {
            case RECORD:
                Map<String, Object> map;
                if (value instanceof String) {
                    String json = (String) value;
                    ObjectMapper objectMapper = new ObjectMapper();
                    map = objectMapper.readValue(json, Map.class);
                } else if (value instanceof Map) {
                    map = (Map<String, Object>) value;
                } else if (value instanceof Row) {
                    Row row = (Row) value;
                    map = rowToMap(row, schema);
                } else {
                    throw new IllegalArgumentException("Value must be a JSON string or a Map");
                }

                GenericRecord record = new GenericData.Record(schema);
                for (Schema.Field field : schema.getFields()) {
                    Object fieldValue = map.get(field.name());
                    record.put(field.name(), convertToAvro(fieldValue, field.schema()));
                }
                return record;
            case ARRAY:
                List<Object> list = (List<Object>) value;
                List<Object> avroList = new ArrayList<>(list.size());
                Schema elementSchema = schema.getElementType();
                for (Object element : list) {
                    avroList.add(convertToAvro(element, elementSchema));
                }
                return avroList;
            case INT:
                return ((Number) value).intValue();
            case LONG:
                if (value instanceof Instant) {
                    return ((Instant) value).toEpochMilli();
                }
                return ((Number) value).longValue();
            default:
                return value;
        }
    }

    public static int getFieldIndex(Row row, String fieldName) {
        String[] fieldNames = row.getFieldNames(true).toArray(new String[0]);
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    private static Map<String, Object> rowToMap(Row row, Schema schema) {
        Map<String, Object> map = new HashMap<>();
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            int fieldIndex = schema.getField(fieldName).pos();
            Object fieldValue = row.getField(fieldIndex);
            map.put(fieldName, fieldValue);
        }
        return map;
    }

    public static RowTypeInfo getSGRowTypeInfo() {
        TupleTypeInfo<Tuple2<String, GenericRecord>> tupleType = new TupleTypeInfo<>(
                Types.STRING,
                TypeInformation.of(GenericRecord.class)
        );
        RowTypeInfo rowTypeInfo = new RowTypeInfo(tupleType);
        return rowTypeInfo;
    }

    public static KV<String, GenericRecord> convertToKV(Tuple2<String, GenericRecord> input) {
        if (input != null) {
            return KV.of(input.f0, input.f1);
        }
        return null;
    }

    public static KV<String, ErrorRecord> convertErrorRecordToKV(Tuple2<String, ErrorRecord> input) {
        if (input != null) {
            return KV.of(input.f0, input.f1);
        }
        return null;
    }


    public static Tuple2<String, ErrorRecord> convertErrorRecordToTuple2(KV<String, ErrorRecord> input) {
        if (input != null) {
            return Tuple2.of(input.getKey(), input.getValue());
        }
        return null;
    }


    public static Tuple2<String, GenericRecord> convertGenericRecordToTuple2(KV<String, GenericRecord> input) {
        if (input != null) {
            return Tuple2.of(input.getKey(), input.getValue());
        }
        return null;
    }

    public static Tuple2<String, GenericRecord> convertGenericRecordToTuple2(Map.Entry<String, GenericRecord> input) {
        if (input != null) {
            return Tuple2.of(input.getKey(), input.getValue());
        }
        return null;
    }


    public static String getTableName(String schemaId) {
        String tableName = null;
        if (schemaId != null) {
            int lastDotIndex = schemaId.lastIndexOf(".");
            if (lastDotIndex == -1) {
                return schemaId;
            }
            return schemaId.substring(lastDotIndex + 1);
        }
        return null;
    }

    public static NodeService nodeService() {
        return nodeService;
    }

    public static ErrorService errorService() {
        return errorService;
    }

    public static String getUniqueKey(Row row, String[] ids) {
        StringBuilder key = new StringBuilder();
        if (ids != null && ids.length > 0) {
            for (String id : ids) {
                key.append(row.getField(id));
            }
        } else {
            key.append(UUID.randomUUID());
        }
        return key.toString();
    }

}

