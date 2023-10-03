package com.apple.aml.stargate.cache.ofs;

import com.apple.athena.lookups.client.LookupClient;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.METHOD_NAME;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.bucketBuilder;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "attributes-remote")
public class AttributesRemoteCacheService implements CacheService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Autowired
    private LookupClient client;

    private Histogram deleteLatencies;
    private Histogram readLatencies;
    private Histogram readMultiLatencies;
    private Histogram writeLatencies;

    private Counter deleteErrorCounter;
    private Counter readErrorCounter;
    private Counter readMultiErrorCounter;
    private Counter writeErrorCounter;

    @PostConstruct
    public void init() {
        deleteLatencies = bucketBuilder("attributes_delete_latencies_ms", Histogram.build().help("Attributes delete latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readLatencies = bucketBuilder("attributes_read_latencies_ms", Histogram.build().help("Attributes read latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readMultiLatencies = bucketBuilder("attributes_read_multi_latencies_ms", Histogram.build().help("Attributes read multiple keys latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        writeLatencies = bucketBuilder("attributes_write_latencies_ms", Histogram.build().help("Attributes write latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();

        deleteErrorCounter = Counter.build().name("attributes_delete_error_counter").help("Attributes delete error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        readErrorCounter = Counter.build().name("attributes_read_error_counter").help("Attributes read error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        readMultiErrorCounter = Counter.build().name("attributes_read__multi_error_counter").help("Attributes read multiple keys error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        writeErrorCounter = Counter.build().name("redis_write_error_counter").help("Attributes write error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
    }

    public Mono<List<Map>> currentTopology() {
        return null;
    }

    @SuppressWarnings("unchecked")
    public <O> O getCacheValue(final String cacheName, final String key) throws Exception {
        long startTimeNanos = System.nanoTime();
        try {
            return (O) client.read(key).get();
        } catch (Exception e) {
            readErrorCounter.labels("attributes", "getKVCacheValue").inc();
            throw e;
        } finally {
            readLatencies.labels("attributes", "getKVCacheValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName) throws Exception {
        return updateCacheValue(cacheName, key, inputValue, valueClassName, null);
    }

    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName, final Duration ttl) throws Exception {
        long startTimeNanos = System.nanoTime();
        try {
            if (inputValue == null) {
                client.delete(key, Instant.now().toEpochMilli()).get(); // TODO : join future with Mono
                return true;
            }
            Object value = getAs(inputValue, valueClassName);
            return client.insert(key, value, Optional.of(-1), Instant.now().toEpochMilli()).get();
        } catch (Exception e) {
            writeErrorCounter.labels("attributes", "updateKVCache").inc();
            LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
            throw e;
        } finally {
            writeLatencies.labels("attributes", "updateKVCache").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    public Mono<Boolean> updateKVCache(final String cacheName, final String key, final Object inputValue, final String valueClassName) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                if (inputValue == null) {
                    client.delete(key, Instant.now().toEpochMilli()).get(); // TODO : join future with Mono
                    sink.success(true);
                    return;
                }
                Object value = getAs(inputValue, valueClassName);
                sink.success(client.insert(key, value, Optional.of(-1), Instant.now().toEpochMilli()).get()); // TODO : join future with Mono
            } catch (Exception e) {
                writeErrorCounter.labels("attributes", "updateKVCache").inc();
                LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
                sink.error(e);
            } finally {
                writeLatencies.labels("attributes", "updateKVCache").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Map<String, Boolean>> updateKVsCache(final String cacheName, final Map<String, Object> updates, final String valueClassName) {
        return Mono.create(sink -> {
            Map<String, Boolean> statusMap = new HashMap<>();
            updates.keySet().stream().forEach(key -> {
                Object inputValue = updates.get(key);
                long startTimeNanos = System.nanoTime();
                try {
                    if (inputValue == null) {
                        client.delete(key, Instant.now().toEpochMilli()).get(); // TODO : join future with Mono
                        statusMap.put(key, true);
                        return;
                    }
                    Object value = getAs(inputValue, valueClassName);
                    statusMap.put(key, client.insert(key, value, Optional.of(-1), Instant.now().toEpochMilli()).get()); // TODO : join future with Mono
                } catch (Exception e) {
                    writeErrorCounter.labels("attributes", "updateKVsCache").inc();
                    LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
                    statusMap.put(key, false);
                } finally {
                    writeLatencies.labels("attributes", "updateKVsCache").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
                }
            });
            sink.success(statusMap);
        });
    }

    public Mono<Object> getKVCacheValue(final String cacheName, final String key) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                Object value = client.read(key).get();
                if (value == null) {
                    sink.success();
                } else {
                    sink.success(value);
                }
            } catch (Exception e) {
                readErrorCounter.labels("attributes", "getKVCacheValue").inc();
                sink.error(e);
            } finally {
                readLatencies.labels("attributes", "getKVCacheValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Map<String, Object>> getKVCacheValues(final String cacheName, final Collection<String> keys) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                Map<String, Object> map = new HashMap<>();
                keys.stream().map(key -> {
                    try {
                        return map.put(key, client.read(key).get());
                    } catch (Exception e) {
                        readErrorCounter.labels("attributes", "getKVCacheValues").inc();
                        throw new RuntimeException(e);
                    }
                }).count();
                sink.success(map); // TODO : join future with Mono
            } catch (Exception e) {
                readMultiErrorCounter.labels("attributes", "getKVCacheValues").inc();
                sink.error(e);
            } finally {
                readMultiLatencies.labels("attributes", "getKVCacheValues").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Boolean> deleteKVCacheValue(final String cacheName, final String key) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                client.delete(key, Instant.now().toEpochMilli()).get(); // TODO : join future with Mono
                sink.success(true);
            } catch (Exception e) {
                deleteErrorCounter.labels("attributes", "deleteKVCacheValue").inc();
                sink.error(e);
            } finally {
                deleteLatencies.labels("attributes", "deleteKVCacheValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Collection<String>> getKVKeys(final String cacheName) {
        throw new UnsupportedOperationException();
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

    public Mono<Map<String, List<Map>>> executeQueries(final String cacheName, final List<String> sqls) {
        throw new UnsupportedOperationException();
    }

    public Mono<Map<String, Boolean>> executePOJOs(final String cacheName, final List<Object> dmls) {
        throw new UnsupportedOperationException();
    }
}
