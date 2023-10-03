package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.options.DMLOperation;
import com.apple.aml.stargate.common.utils.AppConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteClosure;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import javax.cache.Cache;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.cache.utils.CacheExpiryUtils.addToCacheWithTTLAsIs;
import static com.apple.aml.stargate.cache.utils.CacheExpiryUtils.cleanUpExpiredEntries;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.CREATE_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.DELETE_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.READ_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.UPDATE_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.IgniteUtils.executeIgniteCacheDML;
import static com.apple.aml.stargate.cache.utils.IgniteUtils.executeIgniteCacheQuery;
import static com.apple.aml.stargate.common.constants.CommonConstants.MDCKeys.SOURCE_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_PROVIDER_IGNITE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE_KV;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE_RELATIONAL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.EXCEPTION;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OK;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OVERALL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_KV;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.TEMPLATE_NAME_RELATIONAL;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.As;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastReadJson;
import static com.apple.aml.stargate.common.utils.LogUtils.getFromMDCOrDefault;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.schema;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "ignite-local")
public class IgniteLocalCacheService implements CacheService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    @Autowired
    private Ignite ignite;
    private boolean isRelationalCacheLocalOnly;
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        isRelationalCacheLocalOnly = "REPLICATED".equalsIgnoreCase(AppConfig.config().getString(String.format("ignite.cacheTemplates.%s.cacheMode", TEMPLATE_NAME_RELATIONAL)));
    }

    public Mono<List<Map>> currentTopology() {
        return Mono.create(sink -> {
            IgniteCluster cluster = ignite.cluster();
            Collection<ClusterNode> nodes = cluster.topology(cluster.topologyVersion());
            sink.success(nodes.stream().map(node -> {
                Map<String, Object> details = new HashMap<>();
                details.put("addresses", node.addresses());
                details.put("hostNames", node.hostNames());
                details.put("local", node.isLocal());
                details.put("order", node.order());
                details.put("nodeId", node.id().toString());
                return details;
            }).collect(Collectors.toList()));
        });
    }

    @SuppressWarnings("unchecked")
    public <O> O getCacheValue(final String cacheName, final String key) throws Exception {
        long startTime = System.nanoTime();
        try {
            O value = (O) ignite.cache(cacheName).get(key);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getCacheValue", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            return value;
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getCacheValue", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Could not fetch value for key from KV cache", Map.of("cacheName", cacheName, "key", key), e);
            throw e;
        }
    }

    @Override
    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName) throws Exception {
        return updateCacheValue(cacheName, key, inputValue, valueClassName, null);
    }

    @SuppressWarnings("unchecked")
    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName, final Duration ttl) throws Exception {
        long startTime = System.nanoTime();
        try {
            if (inputValue == null) {
                ignite.cache(cacheName).remove(key);
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateCacheValue", "delete", SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                return true;
            }
            long castStartTime = System.nanoTime();
            Object value = getAs(inputValue, valueClassName);
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateCacheValue", "cast", SUCCESS, OK).observe((System.nanoTime() - castStartTime) / 1000000.0);
            addToCacheWithTTLAsIs(ttl, ignite.cache(cacheName), key, value);
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateCacheValue", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            return true;
        } catch (Exception e) {
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateCacheValue", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    public Mono<Boolean> updateKVCache(final String cacheName, final String key, final Object inputValue, final String valueClassName) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                if (inputValue == null) {
                    ignite.cache(cacheName).remove(key);
                    sink.success(true);
                    UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateKVCache", "delete", SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                    return;
                }
                long castStartTime = System.nanoTime();
                Object value = getAs(inputValue, valueClassName);
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateKVCache", "cast", SUCCESS, OK).observe((System.nanoTime() - castStartTime) / 1000000.0);
                ignite.cache(cacheName).put(key, value);
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateKVCache", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                sink.success(true);
            } catch (Exception e) {
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateKVCache", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
                sink.error(e);
            }
        });
    }

    public Mono<Map<String, Boolean>> updateKVsCache(final String cacheName, final Map<String, Object> updates, final String valueClassName) {
        return Mono.create(sink -> {
            long overallStartTime = System.nanoTime();
            Map<String, Boolean> statusMap = new HashMap<>();
            updates.keySet().stream().forEach(key -> {
                Object inputValue = updates.get(key);
                IgniteCache<Object, Object> cache = ignite.cache(cacheName);
                long startTime = System.nanoTime();
                try {
                    if (inputValue == null) {
                        cache.remove(key);
                        statusMap.put(key, true);
                        UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateKVCache", "delete", SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                        return;
                    }
                    long castStartTime = System.nanoTime();
                    Object value = getAs(inputValue, valueClassName);
                    UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateKVCache", "cast", SUCCESS, OK).observe((System.nanoTime() - castStartTime) / 1000000.0);
                    cache.put(key, value);
                    statusMap.put(key, true);
                    UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateKVCache", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                } catch (Exception e) {
                    UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateKVCache", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                    LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
                    statusMap.put(key, false);
                }
            });
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "updateKVsCache", OVERALL, SUCCESS, OK).observe((System.nanoTime() - overallStartTime) / 1000000.0);
            sink.success(statusMap);
        });
    }

    public Mono<Object> getKVCacheValue(final String cacheName, final String key) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                sink.success(ignite.cache(cacheName).get(key));
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getKVCacheValue", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getKVCacheValue", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Could not fetch value for key from KV cache", Map.of("cacheName", cacheName, "key", key), e);
                sink.error(e);
            }
        });
    }

    public Mono<Map<String, Object>> getKVCacheValues(final String cacheName, final Collection<String> keys) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                Map<String, Object> map = new HashMap<>();
                IgniteCache<String, Object> cache = ignite.cache(cacheName);
                keys.stream().map(key -> map.put(key, cache.get(key))).count();
                sink.success(map);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getKVCacheValues", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getKVCacheValues", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                sink.error(e);
            }
        });
    }

    public Mono<Boolean> deleteKVCacheValue(final String cacheName, final String key) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                sink.success(ignite.cache(cacheName).remove(key));
                DELETE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "deleteKVCacheValue", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                DELETE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "deleteKVCacheValue", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Could not delete key from KV cache", Map.of("cacheName", cacheName, "key", key), e);
                sink.error(e);
            }
        });
    }

    public Mono<Collection<String>> getKVKeys(final String cacheName) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                sink.success(ignite.cache(cacheName).query(new ScanQuery<>(), (IgniteClosure<Cache.Entry<String, Object>, String>) Cache.Entry::getKey).getAll());
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getKVKeys", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getKVKeys", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Could not fetch all KV keys", Map.of("cacheName", cacheName), e);
                sink.error(e);
            }
        });
    }

    public Mono<Boolean> executeDDL(final String cacheName, final String sql) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                ignite.cache(cacheName).query(new SqlFieldsQuery(sql)).getAll();
                CREATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeDDL", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                sink.success(true);
            } catch (Exception e) {
                if (String.valueOf(e.getMessage()).startsWith("Table already exists")) { // very bad way; will fix it using IgniteSQLException
                    LOGGER.warn("Table already exists. Will ignore", Map.of("cacheName", cacheName, "sql", sql), e);
                    CREATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeDDL", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                    sink.success(true);
                } else {
                    CREATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeDDL", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                    sink.error(e);
                }
            }
        });
    }

    @Override
    public Mono<Boolean> createTableDDL(final String cacheName, final String tableName, final String ddl) {
        Map<String, String> additionalParams = Map.of("TEMPLATE", TEMPLATE_NAME_RELATIONAL, "CACHE_NAME", tableName, "WRAP_KEY", "true");
        String sql = String.format("%s WITH \"%s\"", ddl, additionalParams.keySet().stream().map(k -> String.format("%s=%s", k, additionalParams.get(k))).collect(Collectors.joining(",")));
        return executeDDL(cacheName, sql);
    }

    public Mono<Boolean> createIndexDDL(final String cacheName, final String tableName, final String indexName, final String ddl) {
        return executeDDL(cacheName, ddl);
    }

    public Mono<Boolean> executeDML(final String cacheName, final String dml) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                sink.success(executeIgniteCacheDML(ignite, cacheName, dml, isRelationalCacheLocalOnly));
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeDML", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeDML", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing query", Map.of("cacheName", cacheName, "sql", dml), e);
                sink.error(e);
            }
        });
    }

    public Mono<Map<String, Boolean>> executeDMLs(final String cacheName, final List<String> dmls) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            String dml = null;
            try {
                Map<String, Boolean> returnMap = new HashMap<>();
                for (String sql : dmls) {
                    dml = sql;
                    long singleStartTime = System.nanoTime();
                    returnMap.put(dml, executeIgniteCacheDML(ignite, cacheName, dml, isRelationalCacheLocalOnly));
                    UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeDML", OVERALL, SUCCESS, OK).observe((System.nanoTime() - singleStartTime) / 1000000.0);
                }
                sink.success(returnMap);
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeDMLs", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeDMLs", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing queries", Map.of("cacheName", cacheName, "dmls", dmls, "currentSQL", String.valueOf(dml)), e);
                sink.error(e);
            }
        });
    }

    public Mono<List<Map>> executeQuery(final String cacheName, final String sql) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                List<Map> returnList = executeIgniteCacheQuery(ignite, cacheName, sql, isRelationalCacheLocalOnly);
                sink.success(returnList);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeQuery", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeQuery", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing query", Map.of("cacheName", cacheName, "sql", sql), e);
                sink.error(e);
            }
        });
    }

    public Mono<Map<String, List<Map>>> executeQueries(final String cacheName, final List<String> sqls) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            try {
                Map<String, List<Map>> returnMap = new HashMap<>();
                for (final String sql : sqls) {
                    long singleStartTime = System.nanoTime();
                    List<Map> returnList = executeIgniteCacheQuery(ignite, cacheName, sql, isRelationalCacheLocalOnly);
                    returnMap.put(sql, returnList);
                    READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeQuery", OVERALL, SUCCESS, OK).observe((System.nanoTime() - singleStartTime) / 1000000.0);
                }
                sink.success(returnMap);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeQueries", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executeQueries", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing queries", Map.of("cacheName", cacheName, "sqls", sqls), e);
                sink.error(e);
            }
        });
    }

    @Override
    public Mono<Map<String, Boolean>> executePOJOs(final String cacheName, final List<Object> pojos) {
        return Mono.create(sink -> {
            long startTime = System.nanoTime();
            String dml = null;
            try {
                Map<String, Boolean> returnMap = new HashMap<>();
                for (Object object : pojos) {
                    long singleStartTime = System.nanoTime();
                    DMLOperation operation = (object instanceof String) ? fastReadJson((String) object, DMLOperation.class) : As(object, DMLOperation.class);
                    if (operation.getSchemaId() == null) {
                        dml = operation.sql();
                    } else {
                        String schemaId = operation.getSchemaId();
                        ObjectToGenericRecordConverter converter = converterMap.computeIfAbsent(schemaId, s -> {
                            try {
                                return converter(schema(operation.getSchemaReference(), schemaId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                        GenericRecord record = converter.convert(operation.getData());
                        dml = operation.sql(record);
                    }
                    returnMap.put(dml, executeIgniteCacheDML(ignite, cacheName, dml, isRelationalCacheLocalOnly));
                    UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executePOJO", OVERALL, SUCCESS, OK).observe((System.nanoTime() - singleStartTime) / 1000000.0);
                }
                sink.success(returnMap);
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executePOJOs", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "executePOJOs", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.warn("Error executing queries", Map.of("cacheName", cacheName, "pojos", pojos, "currentSQL", String.valueOf(dml)), e);
                sink.error(e);
            }
        });
    }

    @Scheduled(fixedDelayString = "${stargate.cache.ofs.cleanup.frequency}")
    public void clearExpiredCacheEntries() {
        LOGGER.debug("Scheduled task to clear expired KV Cache Entries is starting {}", System.currentTimeMillis());
        cleanUpExpiredEntries(ignite.cache(CACHE_NAME_KV));
    }
}
