package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter;
import com.apple.aml.stargate.common.options.DMLOperation;
import com.apple.aml.stargate.common.utils.AppConfig;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
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
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import javax.cache.Cache;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.cache.utils.CacheMetrics.UPDATE_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.IgniteUtils.executeIgniteCacheDML;
import static com.apple.aml.stargate.cache.utils.IgniteUtils.executeIgniteCacheQuery;
import static com.apple.aml.stargate.common.constants.CommonConstants.MDCKeys.SOURCE_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_PROVIDER_IGNITE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE_RELATIONAL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.EXCEPTION;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.METHOD_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OK;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OVERALL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.TEMPLATE_NAME_RELATIONAL;
import static com.apple.aml.stargate.common.converters.ObjectToGenericRecordConverter.converter;
import static com.apple.aml.stargate.common.utils.ClassUtils.As;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastReadJson;
import static com.apple.aml.stargate.common.utils.LogUtils.getFromMDCOrDefault;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.bucketBuilder;
import static com.apple.aml.stargate.common.utils.SchemaUtils.schema;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "ignite-multi")
public class IgniteMultiValueCacheService implements CacheService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private ConcurrentHashMap<String, ObjectToGenericRecordConverter> converterMap = new ConcurrentHashMap<>();
    @Autowired
    private Ignite ignite;
    private boolean isRelationalCacheLocalOnly;

    private Histogram createTableLatencies;
    private Histogram execQueryLatencies;
    private Histogram execQueriesLatencies;
    private Histogram deleteLatencies;
    private Histogram readLatencies;
    private Histogram readKeysLatencies;
    private Histogram writeLatencies;

    private Counter createTableErrorCounter;
    private Counter execQueryErrorCounter;
    private Counter execQueriesErrorCounter;
    private Counter deleteErrorCounter;
    private Counter readErrorCounter;
    private Counter readKeysErrorCounter;
    private Counter writeErrorCounter;

    @PostConstruct
    public void init() {
        isRelationalCacheLocalOnly = "REPLICATED".equalsIgnoreCase(AppConfig.config().getString(String.format("ignite.cacheTemplates.%s.cacheMode", TEMPLATE_NAME_RELATIONAL)));
        createTableLatencies = bucketBuilder("ignite_create_table_latencies_ms", Histogram.build().help("Ignite create table latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        deleteLatencies = bucketBuilder("ignite_delete_latencies_ms", Histogram.build().help("Ignite readAs latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        execQueryLatencies = bucketBuilder("ignite_exec_query_latencies_ms", Histogram.build().help("Ignite execute query latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        execQueriesLatencies = bucketBuilder("ignite_exec_queries_latencies_ms", Histogram.build().help("Ignite execute queries latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readLatencies = bucketBuilder("ignite_read_latencies_ms", Histogram.build().help("Ignite read latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readKeysLatencies = bucketBuilder("ignite_read_keys_latencies_ms", Histogram.build().help("Ignite read all keys latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        writeLatencies = bucketBuilder("ignite_write_latencies_ms", Histogram.build().help("Ignite write latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();

        createTableErrorCounter = Counter.build().name("ignite_create_table_error_counter").help("Ignite create table error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        execQueryErrorCounter = Counter.build().name("ignite_exec_query_error_counter").help("Ignite exec query error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        execQueriesErrorCounter = Counter.build().name("ignite_exec_queries_error_counter").help("Ignite exec queries error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        deleteErrorCounter = Counter.build().name("ignite_delete_error_counter").help("Ignite delete error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        readErrorCounter = Counter.build().name("ignite_read_error_counter").help("Ignite read error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        readKeysErrorCounter = Counter.build().name("ignite_read_keys_error_counter").help("Ignite read keys error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        writeErrorCounter = Counter.build().name("ignite_write_error_counter").help("Ignite write error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();

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
        long startTimeNanos = System.nanoTime();
        try {
            return (O) ignite.cache(cacheName).get(key);
        } catch (Exception e) {
            readErrorCounter.labels("ignite", "getKVCacheValue").inc();
            LOGGER.warn("Could not fetch value for key from KV cache", Map.of("cacheName", cacheName, "key", key), e);
            throw e;
        } finally {
            readLatencies.labels("ignite", "getKVCacheValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName) throws Exception {
        return updateCacheValue(cacheName, key, inputValue, valueClassName, null);
    }

    public Boolean updateCacheValue(final String cacheName, final String key, final Object inputValue, final String valueClassName, final Duration ttl) throws Exception {
        long startTimeNanos = System.nanoTime();
        IgniteCache<String, Set<Object>> cache = ignite.cache(cacheName);
        try {
            if (inputValue == null) {
                //cache.remove(key); // todo - revisit
                return false;
            }
            cache.putIfAbsent(key, new HashSet<>());
            Object value = getAs(inputValue, valueClassName);
            cache.get(key).add(value);
            return true;
        } catch (Exception e) {
            writeErrorCounter.labels("ignite", "updateKVCache").inc();
            LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
            throw e;
        } finally {
            writeLatencies.labels("ignite", "updateKVCache").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    public Mono<Boolean> updateKVCache(final String cacheName, final String key, final Object inputValue, final String valueClassName) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            IgniteCache<String, Set<Object>> cache = ignite.cache(cacheName);
            try {
                if (inputValue == null) {
                    //cache.remove(key); // todo - revisit
                    sink.success(false);
                    return;
                }
                cache.putIfAbsent(key, new HashSet<>());
                Object value = getAs(inputValue, valueClassName);
                cache.get(key).add(value);
                sink.success(true);
            } catch (Exception e) {
                writeErrorCounter.labels("ignite", "updateKVCache").inc();
                LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
                sink.error(e);
            } finally {
                writeLatencies.labels("ignite", "updateKVCache").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Map<String, Boolean>> updateKVsCache(final String cacheName, final Map<String, Object> updates, final String valueClassName) {
        return Mono.create(sink -> {
            IgniteCache<String, Set<Object>> cache = ignite.cache(cacheName);
            Map<String, Boolean> statusMap = new HashMap<>();
            updates.keySet().stream().forEach(key -> {
                Object inputValue = updates.get(key);
                long startTimeNanos = System.nanoTime();
                try {
                    if (inputValue == null) {
                        //cache.remove(key); // todo - revisit
                        statusMap.put(key, false);
                        return;
                    }
                    cache.putIfAbsent(key, new HashSet<>());
                    Object value = getAs(inputValue, valueClassName);
                    cache.get(key).add(value);
                    statusMap.put(key, true);
                } catch (Exception e) {
                    writeErrorCounter.labels("ignite", "updateKVsCache").inc();
                    LOGGER.warn("Could not update kv cache for", Map.of("cacheName", cacheName, "key", key, "valueType", String.valueOf(valueClassName)), e);
                    statusMap.put(key, false);
                } finally {
                    writeLatencies.labels("ignite", "updateKVsCache").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
                }
            });
            sink.success(statusMap);
        });
    }

    public Mono<Object> getKVCacheValue(final String cacheName, final String key) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                Object value = ignite.cache(cacheName).get(key);
                if (value == null) {
                    sink.success();
                } else {
                    sink.success(value);
                }
            } catch (Exception e) {
                readErrorCounter.labels("ignite", "getKVCacheValue").inc();
                LOGGER.warn("Could not fetch value for key from KV cache", Map.of("cacheName", cacheName, "key", key), e);
                sink.error(e);
            } finally {
                readLatencies.labels("ignite", "getKVCacheValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Map<String, Object>> getKVCacheValues(final String cacheName, final Collection<String> keys) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                IgniteCache<String, Set<Object>> cache = ignite.cache(cacheName);
                Map<String, Object> result = new HashMap<>();
                keys.stream().map(key -> result.put(key, cache.get(key))).count();
                sink.success(result);
            } catch (Exception e) {
                readErrorCounter.labels("ignite", "getKVCacheValues").inc();
                sink.error(e);
            } finally {
                readLatencies.labels("ignite", "getKVCacheValues").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Boolean> deleteKVCacheValue(final String cacheName, final String key) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                IgniteCache<String, Set<Object>> cache = ignite.cache(cacheName);
                sink.success(cache.remove(key));
            } catch (Exception e) {
                deleteErrorCounter.labels("ignite", "deleteKVCacheValue").inc();
                LOGGER.warn("Could not delete key from KV cache", Map.of("cacheName", cacheName, "key", key), e);
                sink.error(e);
            } finally {
                deleteLatencies.labels("ignite", "deleteKVCacheValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Collection<String>> getKVKeys(final String cacheName) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                IgniteCache<String, Set<Object>> cache = ignite.cache(cacheName);
                sink.success(cache.query(new ScanQuery<>(), (IgniteClosure<Cache.Entry<String, Object>, String>) Cache.Entry::getKey).getAll());
            } catch (Exception e) {
                readKeysErrorCounter.labels("ignite", "getKVKeys").inc();
                LOGGER.warn("Could not fetch all KV keys", Map.of("cacheName", cacheName), e);
                sink.error(e);
            } finally {
                readKeysLatencies.labels("ignite", "getKVKeys").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Boolean> executeDDL(final String cacheName, final String sql) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                ignite.cache(cacheName).query(new SqlFieldsQuery(sql)).getAll();
                sink.success(true);
            } catch (Exception e) {
                if (String.valueOf(e.getMessage()).startsWith("Table already exists")) { // very bad way; will fix it using IgniteSQLException
                    LOGGER.warn("Table already exists. Will ignore", Map.of("cacheName", cacheName, "sql", sql), e);
                    sink.success(true);
                } else {
                    createTableErrorCounter.labels("ignite", "createTable").inc();
                    sink.error(e);
                }
            } finally {
                createTableLatencies.labels("ignite", "createTable").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
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
            long startTimeNanos = System.nanoTime();
            try {
                sink.success(executeIgniteCacheDML(ignite, cacheName, dml, isRelationalCacheLocalOnly));
            } catch (Exception e) {
                execQueryErrorCounter.labels("ignite", "executeDML").inc();
                LOGGER.warn("Error executing query", Map.of("cacheName", cacheName, "sql", dml), e);
                sink.error(e);
            } finally {
                execQueryLatencies.labels("ignite", "executeDML").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Map<String, Boolean>> executeDMLs(final String cacheName, final List<String> dmls) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            String dml = null;
            try {
                Map<String, Boolean> returnMap = new HashMap<>();
                for (String sql : dmls) {
                    dml = sql;
                    returnMap.put(dml, executeIgniteCacheDML(ignite, cacheName, dml, isRelationalCacheLocalOnly));
                }
                sink.success(returnMap);
            } catch (Exception e) {
                execQueriesErrorCounter.labels("ignite", "executeDMLs").inc();
                LOGGER.warn("Error executing queries", Map.of("cacheName", cacheName, "dmls", dmls, "currentSQL", String.valueOf(dml)), e);
                sink.error(e);
            } finally {
                execQueriesLatencies.labels("ignite", "executeDMLs").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<List<Map>> executeQuery(final String cacheName, final String sql) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                List<Map> returnList = executeIgniteCacheQuery(ignite, cacheName, sql, isRelationalCacheLocalOnly);
                sink.success(returnList);
            } catch (Exception e) {
                execQueryErrorCounter.labels("ignite", "execQuery").inc();
                LOGGER.warn("Error executing query", Map.of("cacheName", cacheName, "sql", sql), e);
                sink.error(e);
            } finally {
                execQueryLatencies.labels("ignite", "execQuery").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            }
        });
    }

    public Mono<Map<String, List<Map>>> executeQueries(final String cacheName, final List<String> sqls) {
        return Mono.create(sink -> {
            long startTimeNanos = System.nanoTime();
            try {
                Map<String, List<Map>> returnMap = new HashMap<>();
                for (final String sql : sqls) {
                    List<Map> returnList = executeIgniteCacheQuery(ignite, cacheName, sql, isRelationalCacheLocalOnly);
                    returnMap.put(sql, returnList);
                }
                sink.success(returnMap);
            } catch (Exception e) {
                execQueriesErrorCounter.labels("ignite", "execQueries").inc();
                LOGGER.warn("Error executing queries", Map.of("cacheName", cacheName, "sqls", sqls), e);
                sink.error(e);
            } finally {
                execQueriesLatencies.labels("ignite", "execQueries").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
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
}
