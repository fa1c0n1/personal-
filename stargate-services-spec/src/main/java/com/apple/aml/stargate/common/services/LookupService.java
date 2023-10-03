package com.apple.aml.stargate.common.services;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface LookupService {
    <O> O readLookup(final String key);

    <I, O> Map<String, O> bulkReadLookup(final Collection<String> keys, final Function<I, O> function);

    @SuppressWarnings("unchecked")
    default <O> Map<String, O> bulkReadLookup(final Collection<String> keys) {
        return (Map<String, O>) bulkReadLookup(keys, o -> o);
    }

    default <O> boolean writeLookup(final String key, final O value) {
        return writeLookup(key, value, -1, -1);
    }

    default <O> boolean writeLookup(final String key, final O value, final int ttl) {
        return writeLookup(key, value, ttl, -1);
    }

    <O> boolean writeLookup(final String key, final O value, final int ttl, final long timestamp);

    default <O> Map<String, Boolean> bulkWriteLookup(final Map<String, O> keyValueMap, final int ttl) {
        return bulkWriteLookup(keyValueMap, ttl, -1);
    }

    <O> Map<String, Boolean> bulkWriteLookup(final Map<String, O> keyValueMap, final int ttl, final long timestamp);

    default boolean deleteLookup(final String key) {
        return deleteLookup(key, -1);
    }

    boolean deleteLookup(final String key, final long timestamp);

    default Map<String, Boolean> bulkDeleteLookup(final List<String> keys) {
        return bulkDeleteLookup(keys, -1);
    }

    Map<String, Boolean> bulkDeleteLookup(final List<String> keys, final long timestamp);

}
