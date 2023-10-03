package com.apple.aml.stargate.cache.ofs;

import java.util.Collection;
import java.util.Map;

public interface CacheLoader {
    <O> O getValue(final String key) throws Exception;

    Map<String, Object> getValues(final Collection<String> keys) throws Exception;

    Collection<Map> getRecords(final String query) throws Exception;

    Collection<Map> getKVRecords(final String key) throws Exception;
}
