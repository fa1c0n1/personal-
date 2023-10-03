package com.apple.aml.stargate.cache.ofs;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.redisson.api.RMapCache;
import org.redisson.api.RMapCacheReactive;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.METHOD_NAME;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.bucketBuilder;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "redis")
public class RedisCacheService implements CacheService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Autowired
    private RedissonClient redisClient;

    private Histogram deleteLatencies;
    private Histogram readLatencies;
    private Histogram readKeysLatencies;
    private Histogram writeLatencies;

    private Counter deleteErrorCounter;
    private Counter readErrorCounter;
    private Counter readKeysErrorCounter;
    private Counter writeErrorCounter;

    @PostConstruct
    public void init() {
        deleteLatencies = bucketBuilder("redis_delete_latencies_ms", Histogram.build().help("Redis delete latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readLatencies = bucketBuilder("redis_read_latencies_ms", Histogram.build().help("Redis read latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readKeysLatencies = bucketBuilder("redis_read_keys_latencies_ms", Histogram.build().help("Redis read all keys latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        writeLatencies = bucketBuilder("redis_write_latencies_ms", Histogram.build().help("Redis write latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();

        deleteErrorCounter = Counter.build().name("redis_delete_error_counter").help("Redis delete error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        readErrorCounter = Counter.build().name("redis_read_error_counter").help("Redis read error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        readKeysErrorCounter = Counter.build().name("redis_read_keys_error_counter").help("Redis read keys error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        writeErrorCounter = Counter.build().name("redis_write_error_counter").help("Redis write error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
    }

    public Mono<List<Map>> currentTopology() {
        return null;
    }

    @SuppressWarnings("unchecked")
    public <O> O getCacheValue(final String cacheName, final String key) throws Exception {
        long startTimeNanos = System.nanoTime();
        try {
            RMapCache<String, Object> cache = redisClient.getMapCache(cacheName);
            return (O) cache.get(key);
        } catch (Exception e) {
            readErrorCounter.labels("redis", "getKVCacheValue").inc();
            throw e;
        } finally {
            readLatencies.labels("redis", "getKVCacheValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName) throws Exception {
        return updateCacheValue(cacheName, key, inputValue, valueClassName, null);
    }

    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName, final Duration ttl) throws Exception {
        long startTimeNanos = System.nanoTime();
        try {
            RMapCache<String, Object> cache = redisClient.getMapCache(cacheName);
            if (key == null) {
                return false;
            }
            if (inputValue == null) {
                cache.remove(key);
                return true;
            }
            Object value = getAs(inputValue, valueClassName);
            cache.put(key, value);
            return true;
        } catch (Exception e) {
            writeErrorCounter.labels("redis", "updateKVCache").inc();
            LOGGER.warn("Could not update redis kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
            throw e;
        } finally {
            writeLatencies.labels("redis", "updateKVCache").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    public Mono<Boolean> updateKVCache(final String cacheName, final String key, final Object inputValue, final String valueClassName) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                RMapCache<String, Object> cache = redisClient.getMapCache(cacheName);
                if (key == null) {
                    sink.success(false);
                    return;
                }
                if (inputValue == null) {
                    cache.remove(key);
                    sink.success(true);
                    return;
                }
                Object value = getAs(inputValue, valueClassName);
                cache.put(key, value);
                sink.success(true);
            } catch (Exception e) {
                writeErrorCounter.labels("redis", "updateKVCache").inc();
                LOGGER.warn("Could not update redis kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
                sink.error(e);
            } finally {
                writeLatencies.labels("redis", "updateKVCache").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Map<String, Boolean>> updateKVsCache(final String cacheName, final Map<String, Object> updates, final String valueClassName) {
        return Mono.create(sink -> {
            Map<String, Boolean> statusMap = new HashMap<>();
            updates.keySet().stream().forEach(key -> {
                Object inputValue = updates.get(key);
                RMapCache<String, Object> cache = redisClient.getMapCache(cacheName);
                long startTimeNanos = System.nanoTime();
                try {
                    if (inputValue == null) {
                        cache.remove(key);
                        statusMap.put(key, true);
                        return;
                    }
                    Object value = getAs(inputValue, valueClassName);
                    cache.put(key, value);
                    statusMap.put(key, true);
                } catch (Exception e) {
                    writeErrorCounter.labels("redis", "updateKVsCache").inc();
                    LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
                    statusMap.put(key, false);
                } finally {
                    writeLatencies.labels("redis", "updateKVsCache").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
                }
            });
            sink.success(statusMap);
        });
    }

    public Mono<Object> getKVCacheValue(final String cacheName, final String key) {
        long startTimeNanos = System.nanoTime();
        try {
            RMapCacheReactive<String, Object> cache = redisClient.reactive().getMapCache(cacheName);
            Mono<Object> value = cache.get(key);
            return value;
        } catch (Exception e) {
            readErrorCounter.labels("redis", "getKVCacheValue").inc();
            return Mono.error(e);
        } finally {
            readLatencies.labels("redis", "getKVCacheValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public Mono<Map<String, Object>> getKVCacheValues(final String cacheName, final Collection<String> keys) {
        return null; // TODO : Provide appropriate implementation
    }

    public Mono<Boolean> deleteKVCacheValue(final String cacheName, final String key) {
        long startTimeNanos = System.nanoTime();
        try {
            RMapCacheReactive<String, Object> cache = redisClient.reactive().getMapCache(cacheName);
            Mono<Boolean> result = cache.remove(key).map(value -> value != null);
            return result;
        } catch (Exception e) {
            deleteErrorCounter.labels("redis", "deleteKVCacheValue").inc();
            return Mono.error(e);
        } finally {
            deleteLatencies.labels("redis", "deleteKVCacheValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public Mono<Collection<String>> getKVKeys(final String cacheName) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                RMapCache<String, Object> cache = redisClient.getMapCache(cacheName);
                sink.success(cache.keySet());
            } catch (Exception e) {
                readKeysErrorCounter.labels("redis", "getKVKeys").inc();
                LOGGER.warn("Error retrieving keys", Map.of("cacheName", cacheName), e);
                sink.error(e);
            } finally {
                readKeysLatencies.labels("redis", "getKVKeys").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
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
