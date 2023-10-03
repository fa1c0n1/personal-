package com.apple.aml.stargate.cache.ofs;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "jvm-local")
public class JvmLocalCacheService implements CacheService {
    static final ConcurrentHashMap<String, Object> JVM_LOCAL_MAP = new ConcurrentHashMap<>(); // should replace with Cache2K or simple LRUCache

    public Mono<List<Map>> currentTopology() {
        return Mono.just(new ArrayList<>());
    }

    @SuppressWarnings("unchecked")
    public <O> O getCacheValue(final String cacheName, final String key) throws Exception {
        return (O) JVM_LOCAL_MAP.get(key);
    }

    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName) throws Exception {
        return updateCacheValue(cacheName, key, inputValue, valueClassName, null);
    }

    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName, final Duration ttl) throws Exception {
        Object value = getAs(inputValue, valueClassName);
        JVM_LOCAL_MAP.put(key, value);
        return true;
    }

    @SuppressWarnings("unchecked")
    public Mono<Boolean> updateKVCache(final String cacheName, final String key, final Object inputValue, final String valueClassName) {
        return Mono.create(sink -> {
            try {
                Object value = getAs(inputValue, valueClassName);
                JVM_LOCAL_MAP.put(key, value);
                sink.success(true);
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<Map<String, Boolean>> updateKVsCache(final String cacheName, final Map<String, Object> updates, final String valueClassName) {
        return Mono.create(sink -> {
            Map<String, Boolean> statusMap = new HashMap<>();
            updates.keySet().stream().forEach(key -> {
                Object inputValue = updates.get(key);
                try {
                    Object value = getAs(inputValue, valueClassName);
                    JVM_LOCAL_MAP.put(key, value);
                    statusMap.put(key, true);
                } catch (Exception e) {
                    statusMap.put(key, false);
                }
            });
            sink.success(statusMap);
        });
    }

    public Mono<Object> getKVCacheValue(final String cacheName, final String key) {
        return Mono.create(sink -> {
            Object value = JVM_LOCAL_MAP.get(key);
            if (value == null) {
                sink.success();
            } else {
                sink.success(value);
            }
        });
    }

    public Mono<Map<String, Object>> getKVCacheValues(final String cacheName, final Collection<String> keys) {
        Map<String, Object> map = new HashMap<>();
        keys.forEach(key -> map.put(key, JVM_LOCAL_MAP.get(key)));
        return Mono.just(map);
    }

    public Mono<Boolean> deleteKVCacheValue(final String cacheName, final String key) {
        JVM_LOCAL_MAP.remove(key);
        return Mono.just(true);
    }

    public Mono<Collection<String>> getKVKeys(final String cacheName) {
        return Mono.create(sink -> sink.success(JVM_LOCAL_MAP.keySet()));
    }

    public Mono<Boolean> executeDDL(final String cacheName, final String sql) {
        throw new UnsupportedOperationException();
    }

    public Mono<Boolean> createTableDDL(final String cacheName, final String tableName, final String sql) {
        throw new UnsupportedOperationException();
    }

    public Mono<Boolean> createIndexDDL(final String cacheName, final String tableName, final String indexName, final String ddl) {
        throw new UnsupportedOperationException();
    }

    public Mono<Boolean> executeDML(final String cacheName, final String dml) {
        throw new UnsupportedOperationException();
    }

    public Mono<Map<String, Boolean>> executeDMLs(final String cacheName, final List<String> dmls) {
        throw new UnsupportedOperationException();
    }

    public Mono<List<Map>> executeQuery(final String cacheName, final String sql) {
        throw new UnsupportedOperationException();
    }

    public Mono<Map<String, List<Map>>> executeQueries(final String cacheName, List<String> sqls) {
        throw new UnsupportedOperationException();
    }

    public Mono<Map<String, Boolean>> executePOJOs(final String cacheName, final List<Object> dmls) {
        throw new UnsupportedOperationException();
    }
}
