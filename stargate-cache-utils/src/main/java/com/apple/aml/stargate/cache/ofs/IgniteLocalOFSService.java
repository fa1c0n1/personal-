package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.nativeml.spec.OFSService;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.google.common.base.Preconditions;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.cache.utils.CacheExpiryUtils.addToCacheWithTTL;
import static com.apple.aml.stargate.cache.utils.CacheExpiryUtils.validateCacheTTL;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.READ_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.UPDATE_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.IgniteUtils.executeIgniteCacheQuery;
import static com.apple.aml.stargate.common.constants.CommonConstants.MDCKeys.SOURCE_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_PROVIDER_IGNITE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE_KV;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE_RELATIONAL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.EXCEPTION;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.FOUND;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.HIT;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.LAMBDA;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.MISS;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.NOT_FOUND;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OK;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OVERALL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_KV;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_RELATIONAL;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CUSTOM_FIELD_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.TEMPLATE_NAME_RELATIONAL;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.LogUtils.getFromMDCOrDefault;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "ignite-local")
@Lazy
public class IgniteLocalOFSService implements OFSService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Autowired
    protected Ignite ignite;
    protected IgniteCache kvCache;
    protected IgniteCache relationalCache;
    protected boolean isRelationalCacheLocalOnly;

    @PostConstruct
    public void init() {
        kvCache = ignite.cache(CACHE_NAME_KV);
        relationalCache = ignite.cache(CACHE_NAME_RELATIONAL);
        isRelationalCacheLocalOnly = "REPLICATED".equalsIgnoreCase(AppConfig.config().getString(String.format("ignite.cacheTemplates.%s.cacheMode", TEMPLATE_NAME_RELATIONAL)));
    }

    public <O> O getValue(final String key, final Function<String, Object> lambda) {
        return getValue(key, lambda, null);
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Function<String, Object> lambda, final Duration ttl) {
        long startTime = System.nanoTime();
        IgniteCache<String, Object> cache = kvCache;
        Object value = null;
        try {
            value = validateCacheTTL(key, cache.get(key), cache);
            if (value != null) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, HIT, FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
                return (O) value;
            }
            long lambdaStartTime = System.nanoTime();
            value = lambda.apply(key);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", LAMBDA, SUCCESS, OK).observe((System.nanoTime() - lambdaStartTime) / 1000000.0);
            if (value != null) {
                value = addToCacheWithTTL(ttl, cache, key, value);
            }
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, value == null ? NOT_FOUND : FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error fetching value from cache for key", Map.of("key", key), e);
        }
        return (O) value;
    }

    public Map<String, Object> getValues(final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda) {
        return getValues(keys, lambda, null);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getValues(final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        long startTime = System.nanoTime();
        IgniteCache<String, Object> cache = kvCache;
        final Map<String, Object> returnMap = new HashMap<>();
        final Set<String> missingKeys = new HashSet<>();
        try {
            keys.stream().forEach(key -> {
                Object value = validateCacheTTL(key, cache.get(key), cache);
                if (value == null) missingKeys.add(key);
                else returnMap.put(key, value);
            });

            if (missingKeys.isEmpty()) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, HIT, FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
                return returnMap;
            }

            long lambdaStartTime = System.nanoTime();
            try {
                Map<String, Object> missingMap = lambda.apply(missingKeys);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", LAMBDA, SUCCESS, OK).observe((System.nanoTime() - lambdaStartTime) / 1000000.0);
                if (missingMap != null && !missingMap.isEmpty()) {
                    missingMap.entrySet().stream().forEach(entry -> {
                        try {
                            if (entry.getValue() == null) return;
                            String key = entry.getKey();
                            Object value = addToCacheWithTTL(ttl, cache, key, entry.getValue());
                            returnMap.put(key, value);
                            missingKeys.remove(key);
                        } catch (Exception e) {
                            LOGGER.error("Error serializing and caching value for key", Map.of("key", entry.getKey()), e);
                        }
                    });
                }
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, missingKeys.isEmpty() ? FOUND : NOT_FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", LAMBDA, ERROR, EXCEPTION).observe((System.nanoTime() - lambdaStartTime) / 1000000.0);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.error("Error adding values from lambda", Map.of("missingKeys", missingKeys), e);
            }
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error fetching values for keys using lambda ", Map.of("keys", keys), e);
        }
        return returnMap;
    }

    public Map getRecord(final String query) {
        long startTime = System.nanoTime();
        Map result = null;
        try {
            List<Map> returnList = executeIgniteCacheQuery(relationalCache, query, isRelationalCacheLocalOnly);
            if (returnList != null && !returnList.isEmpty()) {
                result = returnList.get(0);
            }
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecord", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecord", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error querying cache", Map.of("query", query), e);
        }
        return result;
    }

    public Collection<Map> getRecords(final String query) {
        long startTime = System.nanoTime();
        List<Map> result = null;
        try {
            result = executeIgniteCacheQuery(relationalCache, query, isRelationalCacheLocalOnly);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecords", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecords", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error querying from ignite", Map.of("query", query), e);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public Collection<Map> getKVRecords(final String key) {
        long startTime = System.nanoTime();
        List<Map> result = null;
        try {
            result = executeIgniteCacheQuery(relationalCache, transformToRelationalQuery(key), isRelationalCacheLocalOnly);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getKVRecords", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getKVRecords", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error reading from ignite", Map.of("key", key), e);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key) {
        long startTime = System.nanoTime();
        IgniteCache<String, Object> cache = kvCache;
        try {
            Object value = validateCacheTTL(key, cache.get(key), cache);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, value == null ? MISS : HIT, value == null ? NOT_FOUND : FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
            return (O) value;
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error reading from ignite", Map.of("key", key), e);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getValues(final String group, final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        long startTime = System.nanoTime();
        IgniteCache<String, Object> cache = kvCache;
        final Map<String, Object> returnMap = new HashMap<>();
        final Set<String> missingKeys = new HashSet<>();
        try {
            keys.stream().forEach(key -> {
                String compositeKey = compositeKey(group, key);
                Object value = validateCacheTTL(compositeKey, cache.get(compositeKey), cache);
                if (value == null) missingKeys.add(key);
                else returnMap.put(key, value);
            });

            if (missingKeys.isEmpty()) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, HIT, FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
                return returnMap;
            }

            long lambdaStartTime = System.nanoTime();
            try {
                Map<String, Object> missingMap = lambda.apply(missingKeys);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", LAMBDA, SUCCESS, OK).observe((System.nanoTime() - lambdaStartTime) / 1000000.0);
                if (missingMap != null && !missingMap.isEmpty()) {
                    missingMap.entrySet().stream().forEach(entry -> {
                        try {
                            if (entry.getValue() == null) return;
                            String key = entry.getKey();
                            String compositeKey = compositeKey(group, key);
                            Object value = addToCacheWithTTL(ttl, cache, compositeKey, entry.getValue());
                            returnMap.put(key, value);
                            missingKeys.remove(key);
                        } catch (Exception e) {
                            LOGGER.error("Error serializing and caching value for key ", Map.of("key", entry.getKey()), e);
                        }
                    });
                }
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, missingKeys.isEmpty() ? FOUND : NOT_FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", LAMBDA, ERROR, EXCEPTION).observe((System.nanoTime() - lambdaStartTime) / 1000000.0);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.error("Error adding values from lambda", Map.of("missingKeys", missingKeys), e);
            }
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error fetching values for keys using lambda ", Map.of("keys", keys), e);
        }
        return returnMap;
    }

    @SneakyThrows
    private String transformToRelationalQuery(final String query) {
        String[] queryParts = query.split(CUSTOM_FIELD_DELIMITER);
        // format <tableName>:<queryName>:<column>:......
        if (queryParts.length < 2) {
            throw new InvalidInputException("invalid query, expected format <tableName>:<queryName>:<column>:...", Map.of("query", query));
        }
        return new StringBuilder("SELECT * FROM ").append(queryParts[0]).append(" WHERE ").append(queryParts[1]).append(" = '").append(Arrays.stream(Arrays.copyOfRange(queryParts, 2, queryParts.length)).collect(Collectors.joining(CUSTOM_FIELD_DELIMITER))).append("'").toString();
    }

    // deprecated methods - cleanup later

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Class<O> returnType, final Function<String, O> lambda) {
        long startTime = System.nanoTime();
        IgniteCache<String, O> cache = kvCache;
        O value = null;
        try {
            value = (O) validateCacheTTL(key, cache.get(key), (IgniteCache<String, Object>) cache);
            if (value != null) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, HIT, FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
                return value;
            }

            long lambdaStartTime = System.nanoTime();
            try {
                value = lambda.apply(key);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", LAMBDA, SUCCESS, OK).observe((System.nanoTime() - lambdaStartTime) / 1000000.0);
                if (value != null) {
                    cache.put(key, value);
                }
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, value == null ? NOT_FOUND : FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception ex) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", LAMBDA, ERROR, EXCEPTION).observe((System.nanoTime() - lambdaStartTime) / 1000000.0);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.error("Error adding value from lambda", Map.of("key", key), ex);
            }
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error fetching value from cache for key", Map.of("key", key), e);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public <O> Map<String, O> getValues(final Collection<String> keys, final Class<O> returnType, final Function<Collection<String>, Map<String, O>> lambda) {
        long startTime = System.nanoTime();
        IgniteCache<String, O> cache = kvCache;
        final Map<String, O> returnMap = new HashMap<>();
        final Set<String> missingKeys = new HashSet<>();
        try {
            keys.stream().map(key -> {
                O value = (O) validateCacheTTL(key, cache.get(key), (IgniteCache<String, Object>) cache);
                if (value == null) {
                    missingKeys.add(key);
                    return null;
                }
                return returnMap.put(key, value);
            }).count();

            if (missingKeys.isEmpty()) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, HIT, FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
                return returnMap;
            }

            long lambdaStartTime = System.nanoTime();
            try {
                Map<String, O> missingMap = lambda.apply(missingKeys);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", LAMBDA, SUCCESS, OK).observe((System.nanoTime() - lambdaStartTime) / 1000000.0);
                if (missingMap != null) {
                    cache.putAll(missingMap);
                    returnMap.putAll(missingMap);
                    missingKeys.removeAll(missingMap.keySet());
                }
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, missingKeys.isEmpty() ? FOUND : NOT_FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
            } catch (Exception e) {
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", LAMBDA, ERROR, EXCEPTION).observe((System.nanoTime() - lambdaStartTime) / 1000000.0);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
                LOGGER.error("Error adding values from lambda", Map.of("missingKeys", missingKeys), e);
            }
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error fetching values from cache for keys", Map.of("keys", keys), e);
        }
        return returnMap;
    }

    @SneakyThrows
    public <O> O getRecord(final String query, final Class<O> recordType) {
        long startTime = System.nanoTime();
        O result = null;
        try {
            List<Map> returnList = executeIgniteCacheQuery(relationalCache, query, isRelationalCacheLocalOnly);
            if (returnList != null && !returnList.isEmpty()) {
                result = getAs(returnList.get(0), recordType); // should we throw if resulted in multiple rows ? will decide later
            }
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecord", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecord", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error querying from cache", Map.of("query", query, "type", recordType), e);
        }
        return result;
    }

    public <O> Collection<O> getRecords(final String query, final Class<O> recordType) {
        long startTime = System.nanoTime();
        List<O> result = null;
        try {
            result = executeIgniteCacheQuery(relationalCache, query, isRelationalCacheLocalOnly).stream().map(m -> {
                long castStartTime = System.nanoTime();
                try {
                    return getAs(m, recordType);
                } catch (Exception e) {
                    READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecords", "cast", ERROR, EXCEPTION).observe((System.nanoTime() - castStartTime) / 1000000.0);
                    return null; // will work on edge cases later
                }
            }).collect(Collectors.toList());// bad performance; Will work on it later
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecords", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecords", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error querying from cache", Map.of("query", query, "type", recordType), e);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public Boolean setValue(final String key, final Object value) {
        Preconditions.checkArgument(key != null, "key can't be null");
        Preconditions.checkArgument(value != null, "value can't be null");
        long startTime = System.nanoTime();
        try {
            kvCache.put(key, value);
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "setValue", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            return true;
        } catch (Exception e) {
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "setValue", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error writing to ignite", Map.of("key", key, "value", value), e);
            return false;
        }
    }

    public <O> O getValueAs(final String key, final Class<O> returnType) {
        return getValue(key, returnType);
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Class<O> returnType) {
        long startTime = System.nanoTime();
        O result = null;
        try {
            result = (O) validateCacheTTL(key, kvCache.get(key), (IgniteCache<String, Object>) kvCache);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, result == null ? MISS : HIT, result == null ? NOT_FOUND : FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error reading from ignite", Map.of("key", key, "class", returnType), e);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public <O> Boolean setValueAs(final String key, final O value, final Class<O> clazz) {
        Preconditions.checkArgument(key != null, "key can't be null");
        Preconditions.checkArgument(value != null, "value can't be null");
        long startTime = System.nanoTime();
        try {
            Object val = value;
            if (clazz == null) {
                LOGGER.warn("value type isn't defined, storing as object without explicit type conversion");
                kvCache.put(key, val);
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "setValueAs", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                return true;
            }
            if (!clazz.isAssignableFrom(value.getClass())) {
                long castStartTime = System.nanoTime();
                val = getAs(value, clazz);
                UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "setValueAs", "cast", SUCCESS, OK).observe((System.nanoTime() - castStartTime) / 1000000.0);
            }
            kvCache.put(key, val);
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "setValueAs", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            return true;
        } catch (Exception e) {
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "setValueAs", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error updating kv cache with entries", Map.of("cacheName", CACHE_NAME_KV, "key", key, "value", value, "valueType", clazz), e);
            return false;
        }
    }
}
