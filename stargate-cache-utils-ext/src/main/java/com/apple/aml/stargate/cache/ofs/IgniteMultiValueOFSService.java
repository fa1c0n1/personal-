package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.nativeml.spec.OFSService;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.google.common.base.Preconditions;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import lombok.SneakyThrows;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.cache.utils.IgniteUtils.executeIgniteCacheQuery;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.METHOD_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_KV;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_RELATIONAL;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.TEMPLATE_NAME_RELATIONAL;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.bucketBuilder;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "ignite-multi")
@Lazy
public class IgniteMultiValueOFSService implements OFSService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Autowired
    private Ignite ignite;
    private IgniteCache kvCache;
    private IgniteCache relationalCache;
    private boolean isRelationalCacheLocalOnly;

    private Histogram readLatencies;
    private Histogram readAsLatencies;
    private Histogram readRecordLatencies;
    private Histogram readRecordsLatencies;
    private Histogram writeLatencies;
    private Histogram writeAsLatencies;

    private Counter readErrorCounter;
    private Counter queryErrorCounter;
    private Counter writeErrorCounter;

    @PostConstruct
    public void init() {
        kvCache = ignite.cache(CACHE_NAME_KV);
        isRelationalCacheLocalOnly = "REPLICATED".equalsIgnoreCase(AppConfig.config().getString(String.format("ignite.cacheTemplates.%s.cacheMode", TEMPLATE_NAME_RELATIONAL)));
        relationalCache = ignite.cache(CACHE_NAME_RELATIONAL);
        readLatencies = bucketBuilder("ignite_ofs_read_latencies_ms", Histogram.build().help("Ignite OFS read latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readAsLatencies = bucketBuilder("ignite_ofs_readAs_latencies_ms", Histogram.build().help("Ignite OFS readAs latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readRecordLatencies = bucketBuilder("ignite_ofs_record_latencies_ms", Histogram.build().help("Ignite OFS record latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readRecordsLatencies = bucketBuilder("ignite_ofs_records_latencies_ms", Histogram.build().help("Ignite OFS records latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        writeLatencies = bucketBuilder("ignite_ofs_write_latencies_ms", Histogram.build().help("Ignite OFS write latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        writeAsLatencies = bucketBuilder("ignite_ofs_writeAs_latencies_ms", Histogram.build().help("Ignite OFS writeAs latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();

        readErrorCounter = Counter.build().name("ignite_ofs_read_error_counter").help("Ignite OFS read error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        queryErrorCounter = Counter.build().name("ignite_ofs_query_error_counter").help("Ignite OFS query error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        writeErrorCounter = Counter.build().name("ignite_ofs_write_error_counter").help("Ignite OFS write error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
    }

    public <O> O getValue(final String key, final Function<String, Object> lambda) {
        return getValue(key, lambda, null);
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Function<String, Object> lambda, final Duration ttl) {
        IgniteCache<String, Object> cache = kvCache;
        Object value = cache.get(key);
        if (value == null) {
            value = lambda.apply(key); // TODO : Take care of Common Cache
            if (value == null) {
                return null;
            }
            cache.put(key, value);
        }
        return (O) value;
    }

    public Map<String, Object> getValues(final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda) {
        return getValues(keys, lambda, null);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getValues(final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        IgniteCache<String, Object> cache = kvCache;
        final Map<String, Object> returnMap = new HashMap<>();
        final Set<String> missingKeys = new HashSet<>();
        keys.stream().map(key -> {
            Object value = cache.get(key);
            if (value == null) {
                missingKeys.add(key);
                return null;
            }
            return returnMap.put(key, value);
        }).count();
        Map<String, Object> missingMap = lambda.apply(missingKeys);
        if (missingMap != null) {
            cache.putAll(missingMap);
            returnMap.putAll(missingMap);
        }
        return returnMap;
    }

    public Map getRecord(final String query) {
        List<Map> returnList = executeIgniteCacheQuery(relationalCache, query, isRelationalCacheLocalOnly);
        if (returnList == null || returnList.isEmpty()) {
            return null;
        }
        return returnList.get(0);
    }

    public Collection<Map> getRecords(final String query) {
        return executeIgniteCacheQuery(relationalCache, query, isRelationalCacheLocalOnly);
    }

    @SuppressWarnings("unchecked")
    public Collection<Map> getKVRecords(final String key) {
        long startTimeNanos = System.nanoTime();
        try {
            IgniteCache<String, Collection<Map>> cache = kvCache;
            return cache.get(key);
        } catch (Exception e) {
            readErrorCounter.labels("ignite", "getKVRecords").inc();
            LOGGER.error("Error reading from ignite", Map.of("key", key), e);
            return null;
        } finally {
            readLatencies.labels("ignite", "getKVRecords").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key) {
        long startTimeNanos = System.nanoTime();
        try {
            IgniteCache<String, Set<Object>> cache = kvCache;
            return (O) cache.get(key);
        } catch (Exception e) {
            readErrorCounter.labels("ignite", "getValue").inc();
            LOGGER.error("Error reading from ignite", Map.of("key", key), e);
            return null;
        } finally {
            readLatencies.labels("ignite", "getValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public Map<String, Object> getValues(final String group, final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Class<O> returnType, final Function<String, O> lambda) {
        IgniteCache<String, O> cache = kvCache;
        O value = cache.get(key);
        if (value == null) {
            value = lambda.apply(key); // TODO : Take care of Common Cache
            if (value == null) {
                return null;
            }
            cache.put(key, value);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public <O> Map<String, O> getValues(final Collection<String> keys, final Class<O> returnType, final Function<Collection<String>, Map<String, O>> lambda) {
        IgniteCache<String, O> cache = kvCache;
        final Map<String, O> returnMap = new HashMap<>();
        final Set<String> missingKeys = new HashSet<>();
        keys.stream().map(key -> {
            O value = cache.get(key);
            if (value == null) {
                missingKeys.add(key);
                return null;
            }
            return returnMap.put(key, value);
        }).count();
        Map<String, O> missingMap = lambda.apply(missingKeys);
        if (missingMap != null) {
            cache.putAll(missingMap);
            returnMap.putAll(missingMap);
        }
        return returnMap;
    }

    @SneakyThrows
    public <O> O getRecord(final String query, final Class<O> recordType) {
        long startTimeNanos = System.nanoTime();
        try {
            List<Map> returnList = executeIgniteCacheQuery(relationalCache, query, isRelationalCacheLocalOnly);
            if (returnList == null || returnList.isEmpty()) {
                return null;
            }
            return getAs(returnList.get(0), recordType); // should we throw if resulted in multiple rows ? will decide later
        } catch (Exception e) {
            queryErrorCounter.labels("ignite", "getRecord").inc();
            LOGGER.error("Error querying from ignite", Map.of("sql", query, "type", recordType), e);
            return null;
        } finally {
            readRecordLatencies.labels("ignite", "getRecord").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public <O> Collection<O> getRecords(final String query, final Class<O> recordType) {
        long startTimeNanos = System.nanoTime();
        try {
            return executeIgniteCacheQuery(relationalCache, query, isRelationalCacheLocalOnly).stream().map(m -> {
                try {
                    return getAs(m, recordType);
                } catch (Exception e) {
                    queryErrorCounter.labels("ignite", "getRecords").inc();
                    return null; // will work on edge cases later
                }
            }).collect(Collectors.toList());// bad performance; Will work on it later
        } catch (Exception e) {
            queryErrorCounter.labels("ignite", "getRecords").inc();
            LOGGER.error("Error querying from ignite", Map.of("sql", query, "type", recordType), e);
            return null;
        } finally {
            readRecordsLatencies.labels("ignite", "getRecords").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    public Boolean setValue(final String key, final Object value) {
        Preconditions.checkArgument(key != null, "key can't be null");
        Preconditions.checkArgument(value != null, "value can't be null");
        long startTimeNanos = System.nanoTime();
        try {
            IgniteCache<String, Set<Object>> cache = kvCache;
            cache.putIfAbsent(key, new HashSet<>());
            cache.get(key).add(value);
            return true;
        } catch (Exception e) {
            writeErrorCounter.labels("ignite", "setValue").inc();
            LOGGER.error("Error writing to ignite", Map.of("key", key, "value", value), e);
            return false;
        } finally {
            writeLatencies.labels("ignite", "setValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public <O> O getValueAs(final String key, final Class<O> returnType) {
        return getValue(key, returnType);
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Class<O> returnType) {
        long startTimeNanos = System.nanoTime();
        try {
            IgniteCache<String, Set<Object>> cache = kvCache;
            return (O) cache.get(key);
        } catch (Exception e) {
            readErrorCounter.labels("ignite", "getValue").inc();
            LOGGER.error("Error reading from ignite", Map.of("key", key, "class", returnType), e);
            return null;
        } finally {
            readAsLatencies.labels("ignite", "getValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    public <O> Boolean setValueAs(final String key, final O value, final Class<O> clazz) {
        Preconditions.checkArgument(key != null, "key can't be null");
        Preconditions.checkArgument(value != null, "value can't be null");
        long startTimeNanos = System.nanoTime();
        try {
            IgniteCache<String, Set<Object>> cache = kvCache;
            cache.putIfAbsent(key, new HashSet<>());
            Object val = value;
            if (clazz == null) {
                LOGGER.warn("value type isn't defined, storing as object without explicit type conversion");
                cache.get(key).add(val);
                return true;
            }
            if (!clazz.isAssignableFrom(value.getClass())) {
                val = getAs(value, clazz);
            }
            cache.get(key).add(val);
            return true;
        } catch (Exception e) {
            writeErrorCounter.labels("ignite", "setValueAs").inc();
            LOGGER.error("Could not update kv cache for ", Map.of("cacheName", CACHE_NAME_KV, "key", key, "value", value, "valueType", clazz), e);
            return false;
        } finally {
            writeAsLatencies.labels("ignite", "setValueAs").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }
}
