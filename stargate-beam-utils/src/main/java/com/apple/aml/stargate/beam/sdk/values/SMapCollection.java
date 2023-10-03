package com.apple.aml.stargate.beam.sdk.values;

import java.util.Map;

public class SMapCollection<T> {
    private Map<String, SCollection<T>> map;

    private SMapCollection(final Map<String, SCollection<T>> map) {
        this.map = map;
    }

    public static <T> SMapCollection<T> of(final Map<String, SCollection<T>> map) {
        return new SMapCollection<>(map);
    }

    public Map<String, SCollection<T>> collections() {
        return this.map;
    }

    public SMapCollection<T> update(final String key, final SCollection<T> collection) {
        this.map.put(key, collection);
        return this;
    }
}
