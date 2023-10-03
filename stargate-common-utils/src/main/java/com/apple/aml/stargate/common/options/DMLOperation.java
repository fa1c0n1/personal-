package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.DELETE_ATTRIBUTE_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.PRIMARY_KEY_NAME;
import static com.apple.aml.stargate.common.utils.AvroUtils.getFieldValue;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastReadJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;

@Data
@EqualsAndHashCode(callSuper = true)
public class DMLOperation extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private String tableName;
    private String primaryKey;
    private Object data;
    @Optional
    private String dmlType;

    public static String columnValue(final Schema schema, final String fieldName, final GenericRecord record) throws Exception {
        switch (schema.getType()) {
            case UNION:
                List<Schema> types = schema.getTypes();
                for (Schema type : types) {
                    if (type.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return columnValue(type, fieldName, record);
                }
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                Object value = getFieldValue(record, fieldName);
                return value == null ? "NULL" : String.valueOf(value);
            case STRING:
                String stringValue = (String) getFieldValue(record, fieldName);
                return stringValue == null ? "NULL" : ("'" + stringValue.replaceAll("'", "''") + "'");
            default:
                Object objectValue = getFieldValue(record, fieldName);
                return objectValue == null ? "NULL" : ("'" + jsonString(objectValue).replaceAll("'", "''") + "'");
        }
    }

    public static String dmlStatement(final String key, final GenericRecord record, final String tableName, final Schema schema, final String dmlType, final String deleteAttributeName) throws Exception {
        if (Boolean.valueOf(String.valueOf(getFieldValue(record, deleteAttributeName)))) {
            return String.format("DELETE FROM %s WHERE %s='%s'", tableName, PRIMARY_KEY_NAME, key);
        }
        StringBuilder insertColumnBuilder = new StringBuilder();
        StringBuilder insertValueBuilder = new StringBuilder();
        insertColumnBuilder.append(PRIMARY_KEY_NAME);
        insertValueBuilder.append("'").append(key).append("'");
        for (Schema.Field child : schema.getFields()) {
            insertColumnBuilder.append(",").append(child.name());
            insertValueBuilder.append(",").append(columnValue(child.schema(), child.name(), record));
        }
        return String.format("%s into %s(%s) values(%s)", dmlType, tableName, insertColumnBuilder, insertValueBuilder);
    }

    public String sql(final GenericRecord record) throws Exception {
        return dmlStatement(primaryKey, record, tableName, record.getSchema(), dmlType, DELETE_ATTRIBUTE_NAME);
    }

    public String postgresql(final GenericRecord record) throws Exception {
        if (Boolean.valueOf(String.valueOf(getFieldValue(record, DELETE_ATTRIBUTE_NAME)))) {
            return sql(record);
        }
        if (!"MERGE".equalsIgnoreCase(getDmlType())) {
            return sql(record);
        }
        Schema schema = record.getSchema();
        StringBuilder insertColumnBuilder = new StringBuilder();
        StringBuilder insertValueBuilder = new StringBuilder();
        StringBuilder setBuilder = new StringBuilder();
        insertColumnBuilder.append(PRIMARY_KEY_NAME);
        insertValueBuilder.append("'").append(primaryKey).append("'");
        for (Schema.Field child : schema.getFields()) {
            insertColumnBuilder.append(",").append(child.name());
            String sqlValue = columnValue(child.schema(), child.name(), record);
            insertValueBuilder.append(",").append(sqlValue);
            setBuilder.append(", ").append(child.name()).append(" = ").append(sqlValue);
        }
        return String.format("insert into %s(%s) values(%s) on conflict(%s) do update set %s", tableName, insertColumnBuilder, insertValueBuilder, PRIMARY_KEY_NAME, setBuilder.substring(1));
    }

    @SuppressWarnings("unchecked")
    public String sql() throws Exception {
        Map<String, Object> map = (data instanceof Map) ? (Map) data : fastReadJson(data.toString(), Map.class);
        if (Boolean.valueOf(String.valueOf(map.get(DELETE_ATTRIBUTE_NAME)))) {
            return String.format("DELETE FROM %s WHERE %s='%s'", tableName, PRIMARY_KEY_NAME, primaryKey);
        }
        return _sql(map);
    }

    private String _sql(final Map<String, Object> map) throws Exception {
        StringBuilder insertColumnBuilder = new StringBuilder();
        StringBuilder insertValueBuilder = new StringBuilder();
        insertColumnBuilder.append(PRIMARY_KEY_NAME);
        insertValueBuilder.append("'").append(primaryKey).append("'");
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            insertColumnBuilder.append(",").append(entry.getKey());
            insertValueBuilder.append(",").append(jsonSqlValue(entry.getValue()));
        }
        return String.format("%s into %s(%s) values(%s)", dmlType, tableName, insertColumnBuilder, insertValueBuilder);
    }

    private String jsonSqlValue(final Object value) throws Exception {
        return value == null ? "NULL" : ((value instanceof String) ? ("'" + value.toString().replaceAll("'", "''") + "'") : (value.getClass().isPrimitive() ? String.valueOf(value) : ("'" + jsonString(value).replaceAll("'", "''") + "'")));
    }

    @SuppressWarnings("unchecked")
    public String postgresql() throws Exception {
        Map<String, Object> map = (data instanceof Map) ? (Map) data : fastReadJson(data.toString(), Map.class);
        if (Boolean.valueOf(String.valueOf(map.get(DELETE_ATTRIBUTE_NAME)))) {
            return String.format("DELETE FROM %s WHERE %s='%s'", tableName, PRIMARY_KEY_NAME, primaryKey);
        }
        if (!"MERGE".equalsIgnoreCase(getDmlType())) {
            return _sql(map);
        }
        StringBuilder insertColumnBuilder = new StringBuilder();
        StringBuilder insertValueBuilder = new StringBuilder();
        StringBuilder setBuilder = new StringBuilder();
        insertColumnBuilder.append(PRIMARY_KEY_NAME);
        insertValueBuilder.append("'").append(primaryKey).append("'");
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            insertColumnBuilder.append(",").append(entry.getKey());
            String sqlValue = jsonSqlValue(entry.getValue());
            insertValueBuilder.append(",").append(sqlValue);
            setBuilder.append(", ").append(entry.getKey()).append(" = ").append(sqlValue);
        }
        return String.format("insert into %s(%s) values(%s) on conflict(%s) do update set %s", tableName, insertColumnBuilder, insertValueBuilder, PRIMARY_KEY_NAME, setBuilder.substring(1));
    }
}
