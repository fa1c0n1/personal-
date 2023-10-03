package com.apple.aml.stargate.common.utils.proxies;

import org.apache.avro.util.Utf8;

import java.util.Map;

@SuppressWarnings("unchecked")
public class AvroMap extends MapProxy {
    public AvroMap(final Map map) {
        super(map);
    }

    @Override
    public Object get(final Object key) {
        Object value = super.get(key);
        if (value != null) return value;
        if (key instanceof String) return super.get(new Utf8((String) key));
        return null;
    }

    @Override
    public boolean containsKey(final Object key) {
        boolean value = super.containsKey(key);
        if (value) return true;
        if (key instanceof String) return super.containsKey(new Utf8((String) key));
        return false;
    }
}
