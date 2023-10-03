package com.apple.aml.stargate.common.services;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface TTLAttributeService extends LookupService {

    <O> boolean write(final String key, final O value, final int ttl);

    <O> Map<String, Boolean> bulkWrite(final Map<String, O> map, final int ttl);

    <O> boolean writeField(final String key, final String fieldName, final O fieldValue, final int ttl);

    boolean writeFields(final String key, final Map<String, ?> fields, final int ttl);

    boolean delete(final String key);

    Map<String, Boolean> bulkDelete(final List<String> keys);

    boolean deleteField(final String key, final String fieldName);

    boolean deleteFields(final String key, final List<String> fieldNames);

    <O> O read(final String key);

    <I, O> Map<String, O> bulkRead(final Collection<String> keys, final Function<I, O> function);

    @SuppressWarnings("unchecked")
    default <O> Map<String, O> bulkRead(final Collection<String> keys) {
        return (Map<String, O>) bulkRead(keys, o -> o);
    }

    <O> O readList(final String key);

    <O> O readField(final String key, final String fieldName);

    Map<String, ?> readFields(final String key, final List<String> fieldNames);

    Map<String, ?> readRangeFields(final String key, final String startKey, final String endKey);

}
