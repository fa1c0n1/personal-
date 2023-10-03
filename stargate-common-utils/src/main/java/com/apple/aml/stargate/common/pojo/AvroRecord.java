package com.apple.aml.stargate.common.pojo;

import com.apple.aml.stargate.common.utils.proxies.AvroMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_LATEST_VERSION;

public class AvroRecord implements GenericRecord, Serializable {
    private final GenericRecord record;
    private final int version;

    public AvroRecord(final Schema schema) {
        this(new GenericData.Record(schema), SCHEMA_LATEST_VERSION);
    }

    public AvroRecord(final GenericRecord backingRecord, final int version) {
        this.record = backingRecord;
        this.version = version;
    }

    public AvroRecord(final GenericData.Record other, final boolean deepCopy) {
        this(new GenericData.Record(other, deepCopy), SCHEMA_LATEST_VERSION);
    }

    public AvroRecord(final GenericRecord backingRecord) {
        this(backingRecord, SCHEMA_LATEST_VERSION);
    }

    @Override
    public void put(final String key, final Object value) {
        record.put(key, value);
    }

    @Override
    public Object get(final String key) {
        try {
            Object value = record.get(key);
            return wrappedValue(value, version);
        } catch (Exception | Error e) {
            return null;
        }
    }

    public static Object wrappedValue(final Object value) {
        return wrappedValue(value, SCHEMA_LATEST_VERSION);
    }

    public static Object wrappedValue(final Object value, final int version) {
        if (value == null) return null;
        if (value instanceof GenericRecord) {
            return (value instanceof AvroRecord) ? value : new AvroRecord((GenericRecord) value, version);
        } else if (value instanceof Map) {
            return (value instanceof AvroMap) ? value : new AvroMap((Map) value);
        } else if (value instanceof Utf8) {
            return value.toString();
        }
        return value;
    }

    @Override
    public void put(final int i, final Object v) {
        record.put(i, v);
    }

    @Override
    public Object get(final int i) {
        try {
            Object value = record.get(i);
            return wrappedValue(value, version);
        } catch (Exception | Error e) {
            return null;
        }
    }

    @Override
    public boolean hasField(final String key) {
        try {
            return getSchema().getField(key) != null;
        } catch (Exception | Error e) {
            return false;
        }
    }

    @Override
    public Schema getSchema() {
        return record.getSchema();
    }

    public int getSchemaVersion() {
        return version;
    }

    public GenericRecord record() {
        return record;
    }

    @Override
    public String toString() {
        return record.toString();
    }
}
