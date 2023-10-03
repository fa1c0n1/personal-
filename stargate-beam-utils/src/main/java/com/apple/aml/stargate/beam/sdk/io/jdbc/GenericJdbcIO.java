package com.apple.aml.stargate.beam.sdk.io.jdbc;

import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.options.JdbiOptions;
import com.google.common.base.CaseFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class GenericJdbcIO {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public static Object[] columnValues(final GenericRecord record) {
        final Schema schema = record.getSchema();
        List<Object> objects = schema.getFields().stream().map(f -> {
            Object value = getFieldValue(record, f.name());
            if (value == null) return null;
            if ((value instanceof GenericRecord) || (value instanceof GenericData.Record)) return value.toString();
            if ((value instanceof Map) || (value instanceof Collection)) return jsonString(value);
            return value;
        }).collect(Collectors.toList());
        return objects.toArray(new Object[objects.size()]);
    }

    public static String sqlType(final Schema schema) {
        switch (schema.getType()) {
            case UNION:
                List<Schema> types = schema.getTypes();
                for (Schema type : types) {
                    if (type.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return sqlType(type);
                }
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case FLOAT:
            case DOUBLE:
                return "DECIMAL";
            case BOOLEAN:
                return "BOOLEAN";
            default:
                return "VARCHAR";
        }
    }

    public static String createTableStatement(final Schema schema, final String tableName, final CaseFormat columnCase, final String primaryKey) {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append("( ");
        builder.append(schema.getFields().stream().map(f -> String.format("%s %s %s", CaseFormat.LOWER_CAMEL.to(columnCase, f.name()), sqlType(f.schema()), f.name().equals(primaryKey) ? EMPTY_STRING : "NULL")).collect(Collectors.joining(",")));
        if (primaryKey != null) builder.append(", PRIMARY KEY (").append(CaseFormat.LOWER_CAMEL.to(columnCase, primaryKey)).append(")");
        builder.append(" )");
        return builder.toString();
    }

    public SCollection<KV<String, GenericRecord>> transform(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        JdbiOptions options = (JdbiOptions) node.getConfig();
        options.enableSchemaLevelDefaults();
        return collection.apply(node.getName(), new JdbcRecordExecutor(node.getName(), node.getType(), options, true));
    }

    @SuppressWarnings("unchecked")
    public SCollection<KV<String, GenericRecord>> write(final Pipeline pipeline, final StargateNode node, final SCollection<KV<String, GenericRecord>> collection) throws Exception {
        JdbiOptions options = (JdbiOptions) node.getConfig();
        options.enableSchemaLevelDefaults();
        return collection.apply(node.getName(), new JdbcRecordExecutor(node.getName(), node.getType(), options, false));
    }
}
