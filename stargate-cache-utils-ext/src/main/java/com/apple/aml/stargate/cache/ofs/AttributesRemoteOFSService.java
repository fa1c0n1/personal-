package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.nativeml.spec.OFSService;
import com.apple.athena.lookups.client.LookupClient;
import com.google.common.base.Preconditions;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.CACHE_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.METHOD_NAME;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.CACHE_NAME_KV;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.PrometheusUtils.bucketBuilder;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "attributes-remote")
@Lazy
public class AttributesRemoteOFSService implements OFSService {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    @Autowired
    private LookupClient client;

    private Histogram readLatencies;
    private Histogram readAsLatencies;
    private Histogram writeLatencies;
    private Histogram writeAsLatencies;

    private Counter readErrorCounter;
    private Counter writeErrorCounter;

    @PostConstruct
    public void init() {
        readLatencies = bucketBuilder("attributes_ofs_read_latencies_ms", Histogram.build().help("Attributes OFS read latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        readAsLatencies = bucketBuilder("attributes_ofs_readAs_latencies_ms", Histogram.build().help("Attributes OFS readAs latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        writeLatencies = bucketBuilder("attributes_ofs_write_latencies_ms", Histogram.build().help("Attributes OFS write latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();
        writeAsLatencies = bucketBuilder("attributes_ofs_writeAs_latencies_ms", Histogram.build().help("Attributes OFS writeAs latencies in ms").labelNames(CACHE_TYPE, METHOD_NAME)).register();

        readErrorCounter = Counter.build().name("attributes_ofs_read_error_counter").help("Attributes OFS read error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
        writeErrorCounter = Counter.build().name("attributes_ofs_write_error_counter").help("Attributes OFS write error counter").labelNames(CACHE_TYPE, METHOD_NAME).register();
    }

    @SneakyThrows
    public Boolean setValue(final String key, final Object value) {
        Preconditions.checkArgument(key != null, "key can't be null");
        Preconditions.checkArgument(value != null, "value can't be null");
        long startTimeNanos = System.nanoTime();
        try {
            return client.insert(key, value, Optional.of(-1), Instant.now().toEpochMilli()).get();
        } catch (Exception e) {
            writeErrorCounter.labels("attributes", "setValue").inc();
            LOGGER.error("Error writing to attributes", Map.of("key", key, "value", value), e);
            return false;
        } finally {
            writeLatencies.labels("attributes", "setValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public <O> O getValueAs(final String key, final Class<O> returnType) {
        return getValue(key, returnType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Class<O> returnType) {
        long startTimeNanos = System.nanoTime();
        try {
            return (O) client.read(key).get();
        } catch (Exception e) {
            readErrorCounter.labels("attributes", "getValue").inc();
            LOGGER.error("Error reading from redis", Map.of("key", key, "type", returnType), e);
            return null;
        } finally {
            readAsLatencies.labels("attributes", "getValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    @SneakyThrows
    public <O> Boolean setValueAs(final String key, final O value, final Class<O> clazz) {
        Preconditions.checkArgument(key != null, "key can't be null");
        Preconditions.checkArgument(value != null, "value can't be null");
        long startTimeNanos = System.nanoTime();
        try {
            Object val = value;
            if (clazz == null) {
                LOGGER.warn("value type isn't defined, storing as object without explicit type conversion");
                return client.insert(key, value, Optional.of(-1), Instant.now().toEpochMilli()).get();
            }

            if (!clazz.isAssignableFrom(value.getClass())) {
                val = getAs(value, clazz);
            }
            return client.insert(key, val, Optional.of(-1), Instant.now().toEpochMilli()).get();
        } catch (Exception e) {
            writeErrorCounter.labels("attributes", "setValueAs").inc();
            LOGGER.error("Could not update kv cache for ", Map.of("cacheName", CACHE_NAME_KV, "key", key, "value", value, "valueType", clazz), e);
            return false;
        } finally {
            writeAsLatencies.labels("attributes", "setValueAs").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public <O> O getRecord(final String sql, final Class<O> recordType) {
        throw new UnsupportedOperationException();
    }

    public <O> Collection<O> getRecords(final String sql, final Class<O> recordType) {
        throw new UnsupportedOperationException();
    }

    public <O> O getValue(final String key, final Class<O> returnType, final Function<String, O> lambda) {
        O value = getValue(key, returnType);
        if (value == null) {
            return lambda.apply(key);
        }
        return value;
    }

    public <O> Map<String, O> getValues(final Collection<String> keys, final Class<O> returnType, final Function<Collection<String>, Map<String, O>> lambda) {
        final Map<String, O> returnMap = new HashMap<>();
        final Set<String> missingKeys = new HashSet<>();
        keys.stream().map(key -> {
            O value = getValue(key, returnType);
            if (value == null) {
                missingKeys.add(key);
                return null;
            }
            return returnMap.put(key, value);
        }).count();
        Map<String, O> missingMap = lambda.apply(missingKeys);
        if (missingMap != null) {
            returnMap.putAll(missingMap);
        }
        return returnMap;
    }

    public <O> O getValue(final String key, final Function<String, Object> lambda) {
        return getValue(key, lambda, null);
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Function<String, Object> lambda, final Duration ttl) {
        Object value = getValue(key);
        if (value == null) {
            return (O) lambda.apply(key);
        }
        return (O) value;
    }

    public Map<String, Object> getValues(final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda) {
        return getValues(keys, lambda, null);
    }

    public Map<String, Object> getValues(final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        final Map<String, Object> returnMap = new HashMap<>();
        final Set<String> missingKeys = new HashSet<>();
        keys.stream().map(key -> {
            Object value = getValue(key);
            if (value == null) {
                missingKeys.add(key);
                return null;
            }
            return returnMap.put(key, value);
        }).count();
        Map<String, Object> missingMap = lambda.apply(missingKeys);
        if (missingMap != null) {
            returnMap.putAll(missingMap);
        }
        return returnMap;
    }

    public Map getRecord(final String query) {
        throw new UnsupportedOperationException();
    }

    public Collection<Map> getRecords(final String query) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public Collection<Map> getKVRecords(final String key) {
        long startTimeNanos = System.nanoTime();
        try {
            return (Collection<Map>) client.read(key).get();
        } catch (Exception e) {
            readErrorCounter.labels("attributes", "getKVRecords").inc();
            LOGGER.error("Error reading from attributes", Map.of("key", key), e);
            return null;
        } finally {
            readLatencies.labels("attributes", "getKVRecords").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public <O> O getValue(final String key) {
        long startTimeNanos = System.nanoTime();
        try {
            return (O) client.read(key).get();
        } catch (Exception e) {
            readErrorCounter.labels("attributes", "getValue").inc();
            LOGGER.error("Error reading from attributes", Map.of("key", key), e);
            return null;
        } finally {
            readLatencies.labels("attributes", "getValue").observe((System.nanoTime() - startTimeNanos) / 1000000.0);
        }
    }

    public Map<String, Object> getValues(final String group, final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        throw new UnsupportedOperationException();
    }
}
