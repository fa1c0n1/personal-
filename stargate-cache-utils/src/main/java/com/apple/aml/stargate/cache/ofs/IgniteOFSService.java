package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.nativeml.spec.OFSService;
import com.apple.aml.stargate.cache.exception.CacheWriteException;
import lombok.SneakyThrows;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.cache.utils.CacheExpiryUtils.addToCacheWithTTL;
import static com.apple.aml.stargate.cache.utils.CacheExpiryUtils.validateCacheTTL;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.READ_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.CacheMetrics.UPDATE_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.cache.utils.IgniteUtils.executeIgniteCacheQuery;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
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
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.REMOTE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SUCCESS;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_KV;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.getFromMDCOrDefault;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;

@Service
@ConditionalOnExpression("'${stargate.cache.ofs.type}'.startsWith('ignite-backed-by-')")
@Lazy
public class IgniteOFSService extends IgniteLocalOFSService implements OFSService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Autowired
    private Ignite ignite;
    @Lazy
    @Autowired
    private RemoteCacheService remoteService;
    private String backingServiceName;

    @PostConstruct
    public void init() {
        super.init();
        backingServiceName = config().getString("stargate.cache.ofs.type").replaceFirst("ignite-backed-by-", EMPTY_STRING);
        if (isBlank(backingServiceName)) {
            backingServiceName = env("APP_OFS_DEFAULT_BACKING_SERVICE", "default");
        }
    }

    public <O> O getValue(final String key, final Function<String, Object> lambda) {
        return getValue(key, lambda, null);
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Function<String, Object> lambda, final Duration ttl) {
        long startTime = System.nanoTime();
        IgniteCache<String, Object> cache = ignite.cache(CACHE_NAME_KV);
        Object value = getValue(key, ttl);
        if (value != null) return (O) value;
        try {
            value = getValueFromLambda(key, lambda);
            if (value != null) {
                value = addToCacheWithTTL(ttl, cache, key, value);
            }
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, value == null ? NOT_FOUND : FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error fetching value from lambda", Map.of("key", key), e);
        }
        // do write back in async
        if (value != null) {
            Object finalValue = value;
            CompletableFuture.runAsync(() -> saveValuesToRemote(Map.of(key, finalValue), ttl));
        }
        return (O) value;
    }

    public Map<String, Object> getValues(final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda) {
        return getValues(keys, lambda, null);
    }

    public Map<String, Object> getValues(final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        return getValues(keys, null, false, lambda, ttl);
    }

    @SneakyThrows
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
            if (e instanceof IgniteSQLException || e.getCause() instanceof IgniteSQLException) {
                // TODO - check sql exception for table not found
                long remoteStartTime = System.nanoTime();
                final Map record = remoteService.getRecord(backingServiceName, query);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecord", "remote", SUCCESS, OK).observe((System.nanoTime() - remoteStartTime) / 1000000.0);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecord", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                return record;
            }
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecord", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error querying from ignite", Map.of("query", query), e);
        }
        return result;
    }

    @SneakyThrows
    public Collection<Map> getRecords(final String query) {
        long startTime = System.nanoTime();
        Collection<Map> result = null;
        try {
            result = executeIgniteCacheQuery(relationalCache, query, isRelationalCacheLocalOnly);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecords", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            if (e instanceof IgniteSQLException || e.getCause() instanceof IgniteSQLException) {
                // TODO - check sql exception for table not found
                long remoteStartTime = System.nanoTime();
                result = remoteService.getRecords(backingServiceName, query);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecords", "remote", SUCCESS, OK).observe((System.nanoTime() - remoteStartTime) / 1000000.0);
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecords", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
                return result;
            }
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_RELATIONAL, "getRecords", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error querying from ignite", Map.of("query", query), e);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key) {
        long startTime = System.nanoTime();
        O value = getValue(key, (Duration) null);
        if (value == null) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, NOT_FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
        }
        return value;
    }

    public Map<String, Object> getValues(final String group, final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        return getValues(keys, group, true, lambda, ttl);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public <O> O getValue(final String key, final Class<O> returnType, final Function<String, O> lambda) {
        Object value = getValue(key, (Function<String, Object>) lambda, null);
        if (value == null) return null;
        return returnType.isAssignableFrom(value.getClass()) ? (O) value : readJson(jsonString(value), returnType);
    }

    public <O> Map<String, O> getValues(final Collection<String> keys, final Class<O> returnType, final Function<Collection<String>, Map<String, O>> lambda) {
        return super.getValues(keys, returnType, lambda);
    }

    @SneakyThrows
    public <O> O getRecord(final String query, final Class<O> recordType) {
        Map record = getRecord(query);
        return record == null ? null : readJson(jsonString(record), recordType);
    }

    public <O> Collection<O> getRecords(final String query, final Class<O> recordType) {
        return getRecords(query).stream().map(m -> {
            try {
                return readJson(jsonString(m), recordType);
            } catch (Exception e) {
                return null; // will work on edge cases later
            }
        }).collect(Collectors.toList());// bad performance; Will work on it later
    }

    // an overloaded helper function to fetch values using keys or composite keys
    // isComposite may be redundant when group is available, but just will act as safeguard when group is null
    private Map<String, Object> getValues(final Collection<String> keys, final String group, final boolean isComposite, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        long startTime = System.nanoTime();
        IgniteCache<String, Object> cache = ignite.cache(CACHE_NAME_KV);
        Map<String, Object> returnMap = new HashMap<>();
        Set<String> missingKeys = new HashSet<>();
        keys.stream().forEach(key -> {
            Object value = validateCacheTTL(key, cache.get(buildKey(isComposite, key, group)), cache);
            if (value == null) {
                missingKeys.add(key);
                return;
            }
            returnMap.put(key, value);
        });
        if (missingKeys.isEmpty()) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, HIT, FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
            return returnMap;
        }
        try {
            Map<String, Object> remoteMap = getValuesFromRemote(isComposite ? missingKeys.stream().map(k -> buildKey(isComposite, k, group)).collect(Collectors.toSet()) : missingKeys);
            if (remoteMap != null && !remoteMap.isEmpty()) {
                remoteMap.forEach((key, value) -> {
                    if (value == null) {
                        return;
                    }
                    String simpleKey = extractKey(isComposite, key, group);
                    returnMap.put(simpleKey, value);
                    missingKeys.remove(simpleKey);
                    if (ttl == null) { // TODO : we don't know how much of expiry is left on the remoteCache; Hence just don't add it local cache. Need to work on this implementation later based on perf benchmarks
                        cache.put(simpleKey, value);
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.warn("Could not fetch values from backing cache! Will invoke lambda", Map.of("missingKeys", missingKeys, "backingServiceName", backingServiceName), e);
        }
        if (missingKeys.isEmpty()) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
            return returnMap;
        }
        Map<String, Object> lambdaMap;
        Map<String, Object> transformedMap = null;
        try {
            lambdaMap = getValuesFromLambda(missingKeys, lambda);
            if (lambdaMap != null && !lambdaMap.isEmpty()) {
                transformedMap = new HashMap<>(lambdaMap.size());
                Map<String, String> errors = new HashMap<>();
                Exception exception = null;
                for (Map.Entry<String, Object> entry : lambdaMap.entrySet()) {
                    if (entry.getValue() == null) {
                        continue;
                    }
                    String key = entry.getKey();
                    String cacheKey = buildKey(isComposite, key, group);
                    try {
                        Object value = addToCacheWithTTL(ttl, cache, cacheKey, entry.getValue());
                        transformedMap.put(cacheKey, value);
                        returnMap.put(key, value);
                        missingKeys.remove(key);
                    } catch (CacheWriteException ex) {
                        exception = ex;
                        errors.put(key, ex.getMessage());
                    }
                }
                if (!errors.isEmpty()) {
                    LOGGER.warn("Could not translate/cache TTL entry to local cache", Map.of("errors", errors, "backingServiceName", backingServiceName), exception);
                }
            }
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, missingKeys.isEmpty() ? FOUND : NOT_FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", OVERALL, MISS, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.error("Error fetching value(s) from lambda", Map.of("keys", missingKeys), e);
        }
        // do write back in async
        if (transformedMap != null && !transformedMap.isEmpty()) {
            Map<String, Object> kvMapToSave = transformedMap;
            CompletableFuture.runAsync(() -> saveValuesToRemote(kvMapToSave, ttl));
        }
        return returnMap;
    }

    private String buildKey(final boolean isComposite, final String key, final String group) {
        return isComposite ? compositeKey(group, key) : key;
    }

    private Map<String, Object> getValuesFromRemote(final Set<String> keys) throws Exception {
        long startTime = System.nanoTime();
        try {
            Map<String, Object> value = remoteService.getValues(backingServiceName, keys);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", REMOTE, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            return value;
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValues", REMOTE, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            throw e;
        }
    }

    private String extractKey(final boolean isComposite, final String key, final String group) {
        String replaceString = String.format("%s%s", group, compositeKeyDelimiter());
        return isComposite && key.indexOf(replaceString) == 0 ? key.substring(replaceString.length()) : key;
    }

    private Map<String, Object> getValuesFromLambda(final Set<String> keys, final Function<Collection<String>, Map<String, Object>> lambda) throws Exception {
        long startTime = System.nanoTime();
        try {
            Map<String, Object> value = lambda.apply(keys);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", LAMBDA, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            return value;
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", LAMBDA, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            throw e;
        }
    }

    // an overloaded helper function, which can't determine the end of lookup (local, remote, lambda)
    // so only hit metrics are recorded here, caller should record miss metrics or perform subsequent lookups
    @SuppressWarnings("unchecked")
    private <O> O getValue(final String key, final Duration ttl) {
        long startTime = System.nanoTime();
        IgniteCache<String, Object> cache = ignite.cache(CACHE_NAME_KV);
        Object value = validateCacheTTL(key, cache.get(key), cache);
        if (value != null) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, HIT, FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
            return (O) value;
        }
        try {
            value = getValueFromRemote(key);
            if (value != null) {
                if (ttl == null) {
                    cache.put(key, value);
                }
                READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", OVERALL, MISS, FOUND).observe((System.nanoTime() - startTime) / 1000000.0);
                return (O) value;
            }
        } catch (Exception e) {
            LOGGER.warn("Error in storing value to local cache!", Map.of("key", key, "localCache", CACHE_NAME_KV), e);
        }
        return (O) value;
    }

    private Object getValueFromLambda(final String key, final Function<String, ?> lambda) throws Exception {
        long startTime = System.nanoTime();
        try {
            Object value = lambda.apply(key);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", LAMBDA, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            return value;
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", LAMBDA, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            throw e;
        }
    }

    private void saveValuesToRemote(final Map<String, Object> kvMap, final Duration ttl) {
        long startTime = System.nanoTime();
        try {
            remoteService.saveValues(backingServiceName, kvMap, ttl);
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "saveValues", REMOTE, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
        } catch (Exception e) {
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "saveValues", REMOTE, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            LOGGER.warn("Could not save lambda value to backing cache! Local cache is updated though", Map.of("keys", kvMap.keySet(), "backingServiceName", backingServiceName), e);
        }
    }

    private Object getValueFromRemote(final String key) throws Exception {
        long startTime = System.nanoTime();
        try {
            Object value = remoteService.getValue(backingServiceName, key);
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", REMOTE, SUCCESS, OK).observe((System.nanoTime() - startTime) / 1000000.0);
            return value;
        } catch (Exception e) {
            READ_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "getValue", REMOTE, ERROR, EXCEPTION).observe((System.nanoTime() - startTime) / 1000000.0);
            throw e;
        }
    }
}

