package com.apple.aml.stargate.common.services;

import java.util.Map;

public interface AttributeService extends TTLAttributeService {
    default <O> boolean write(final String key, final O value) {
        return write(key, value, -1);
    }

    default <O> Map<String, Boolean> bulkWrite(final Map<String, O> map) {
        return bulkWrite(map, -1);
    }

    default <O> boolean writeField(final String key, final String fieldName, final O fieldValue) {
        return writeField(key, fieldName, fieldValue, -1);
    }

    default boolean writeFields(final String key, final Map<String, ?> fields) {
        return writeFields(key, fields, -1);
    }
}
