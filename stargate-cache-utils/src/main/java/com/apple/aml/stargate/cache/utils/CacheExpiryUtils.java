package com.apple.aml.stargate.cache.utils;

import com.apple.aml.stargate.cache.exception.CacheWriteException;
import com.apple.aml.stargate.cache.pojo.CacheEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.slf4j.Logger;

import javax.cache.Cache;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.cache.utils.CacheMetrics.UPDATE_LATENCIES_HISTOGRAM;
import static com.apple.aml.stargate.common.constants.CommonConstants.MDCKeys.SOURCE_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_PROVIDER_IGNITE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE_KV;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.EXCEPTION;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OK;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.OVERALL;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.SUCCESS;
import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.LogUtils.getFromMDCOrDefault;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public final class CacheExpiryUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private CacheExpiryUtils() {
    }

    public static Object validateCacheTTL(final String key, final Object value, final IgniteCache<String, Object> cache) {
        long startTimeNanos = System.nanoTime();
        if (!(value instanceof CacheEntry)) {
            return value;
        }
        CacheEntry cachedObject = (CacheEntry) value;
        Object payload = cachedObject.getPayload();
        if (hasTTLExpired(cachedObject.getExpiry())) {
            payload = null;
            cache.remove(key);
        }
        UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "validateCacheTTL", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        return payload;
    }

    public static boolean hasTTLExpired(final long expiryTime) {
        if (System.currentTimeMillis() > expiryTime) return true;
        return false;
    }

    public static Object addToCacheWithTTL(final Duration ttl, final IgniteCache<String, Object> cache, final String key, final Object value) throws CacheWriteException {
        long startTimeNanos = System.nanoTime();
        try {
            Object checkedValue = duplicate(value, Object.class);
            if (ttl == null) {
                cache.put(key, checkedValue);
                return checkedValue;
            }
            CacheEntry cachedObject = new CacheEntry(System.currentTimeMillis() + ttl.toMillis(), checkedValue);
            cache.put(key, cachedObject);
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "addToCacheWithTTL", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            return checkedValue;
        } catch (Exception e) {
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "addToCacheWithTTL", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            throw new CacheWriteException("Error writing key to cache", Map.of("key", key, "ttl", ttl), e);
        }
    }

    public static Object addToCacheWithTTLAsIs(final Duration ttl, final IgniteCache<String, Object> cache, final String key, final Object value) throws CacheWriteException {
        long startTimeNanos = System.nanoTime();
        try {
            if (ttl == null) {
                cache.put(key, value);
                return value;
            }
            CacheEntry cachedObject = new CacheEntry(System.currentTimeMillis() + ttl.toMillis(), value);
            cache.put(key, cachedObject);
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "addToCacheWithTTLAsIs", OVERALL, SUCCESS, OK).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            return value;
        } catch (Exception e) {
            UPDATE_LATENCIES_HISTOGRAM.labels(getFromMDCOrDefault(SOURCE_ID), CACHE_PROVIDER_IGNITE, CACHE_TYPE_KV, "addToCacheWithTTLAsIs", OVERALL, ERROR, EXCEPTION).observe((System.nanoTime() - startTimeNanos) / 1000000.0);
            throw new CacheWriteException("Error writing key to cache", Map.of("key", key, "ttl", ttl), e);
        }
    }

    public static void cleanUpExpiredEntries(final IgniteCache<String, Object> cache) {
        IgniteBiPredicate<String, Object> expiredFilter = (key, cacheEntry) -> {
            if (cacheEntry instanceof CacheEntry) {
                return hasTTLExpired(((CacheEntry) cacheEntry).getExpiry());
            }
            return false;
        };
        List<Cache.Entry<String, Object>> expiredEntries = cache.query(new ScanQuery<>(expiredFilter)).getAll();
        Set<String> expiredKeys = expiredEntries.stream().map(entry -> entry.getKey()).collect(Collectors.toSet());
        cache.removeAll(expiredKeys);
        LOGGER.debug("Cleaned up {} expired entries from cache: {}", expiredKeys.size(), expiredKeys);
    }

}
