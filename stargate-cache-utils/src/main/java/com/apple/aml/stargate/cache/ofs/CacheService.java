package com.apple.aml.stargate.cache.ofs;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface CacheService {
    Mono<List<Map>> currentTopology();

    <O> O getCacheValue(final String cacheName, final String key) throws Exception;

    Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName) throws Exception;

    Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName, Duration ttl) throws Exception;

    Mono<Boolean> updateKVCache(final String cacheName, final String key, final Object inputValue, final String valueClassName);

    Mono<Map<String, Boolean>> updateKVsCache(final String cacheName, final Map<String, Object> updates, final String valueClassName);

    Mono<Object> getKVCacheValue(final String cacheName, final String key);

    Mono<Map<String, Object>> getKVCacheValues(final String cacheName, final Collection<String> keys);

    Mono<Boolean> deleteKVCacheValue(final String cacheName, final String key);

    Mono<Collection<String>> getKVKeys(final String cacheName);

    Mono<Boolean> executeDDL(final String cacheName, final String ddl);

    Mono<Boolean> createTableDDL(final String cacheName, final String tableName, final String ddl);

    Mono<Boolean> createIndexDDL(final String cacheName, final String tableName, final String indexName, final String ddl);

    Mono<Boolean> executeDML(final String cacheName, final String dml);

    Mono<Map<String, Boolean>> executeDMLs(final String cacheName, final List<String> dmls);

    Mono<List<Map>> executeQuery(final String cacheName, final String sql);

    Mono<Map<String, List<Map>>> executeQueries(final String cacheName, final List<String> sqls);

    Mono<Map<String, Boolean>> executePOJOs(final String cacheName, final List<Object> dmls);
}
