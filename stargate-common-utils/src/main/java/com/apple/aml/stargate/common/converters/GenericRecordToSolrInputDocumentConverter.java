package com.apple.aml.stargate.common.converters;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.solr.common.SolrInputDocument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public class GenericRecordToSolrInputDocumentConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Object fromAvro(final Object object, final Schema schema, final String fieldName) throws Exception {
        if (object == null) {
            return null;
        }
        switch (schema.getType()) {
            case ARRAY:
                return fromAvroArray(object, schema, fieldName);
            case STRING:
            case ENUM:
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
            case MAP:
            case RECORD:
                if (object instanceof Map) {
                    return object;
                }
                return readJsonMap(jsonString(object));
            default:
                throw new UnsupportedOperationException("Unsupported Type in Avro to Solr");
        }
    }

    public static SolrInputDocument convertRecord(final GenericRecord record) throws Exception {
        final Schema schema = record.getSchema();
        final SolrInputDocument doc = new SolrInputDocument();
        for (Schema.Field child : schema.getFields()) {
            String key = child.name();
            Object value = record.get(key);
            if (value == null) {
                continue;
            }
            doc.addField(key, fromAvro(value, child.schema(), child.name()));
        }
        return doc;
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
}
