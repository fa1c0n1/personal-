package com.apple.aml.stargate.connector.iceberg;

import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.StructLike;

public class GenericRecordStructLikeWrapper implements StructLike {
    private final GenericRecord record;

    public GenericRecordStructLikeWrapper(final GenericRecord record) {
        this.record = record;
    }

    public static GenericRecordStructLikeWrapper wrapper(final GenericRecord record) {
        return new GenericRecordStructLikeWrapper(record);
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int i, Class<T> aClass) {
        return (T) record.get(i);
    }

    @Override
    public <T> void set(int i, T t) {
        throw new UnsupportedOperationException();
    }
}
