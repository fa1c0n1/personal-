package com.apple.aml.stargate.cache.ofs;

import java.util.Collection;
import java.util.Map;

public class SolrCacheLoader implements CacheLoader {
    public <O> O getValue(final String key) throws Exception {
        return null;
    }

    public Map<String, Object> getValues(final Collection<String> keys) throws Exception {
        return null;
    }

    public Collection<Map> getRecords(final String query) throws Exception {
        return null;
    }

    public Collection<Map> getKVRecords(final String key) throws Exception {
        return null;
    }
}
