package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.nativeml.spec.OFSService;
import lombok.SneakyThrows;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.apple.aml.stargate.cache.ofs.JvmLocalCacheService.JVM_LOCAL_MAP;

@Service
@ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "jvm-local")
@Lazy
public class JvmLocalOFSService implements OFSService {

    public Boolean setValue(final String key, final Object value) {
        JVM_LOCAL_MAP.put(key, value);
        return true;
    }

    public <O> O getValueAs(final String key, final Class<O> returnType) {
        return getValue(key, returnType);
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key, final Class<O> returnType) {
        return (O) JVM_LOCAL_MAP.get(key);
    }

    public <O> Boolean setValueAs(final String key, O value, final Class<O> returnType) {
        JVM_LOCAL_MAP.put(key, value);
        return true;
    }

    @SneakyThrows
    public <O> O getRecord(final String sql, final Class<O> recordType) {
        throw new UnsupportedOperationException();
    }

    public <O> Collection<O> getRecords(final String sql, final Class<O> recordType) {
        throw new UnsupportedOperationException();
    }

    public <O> O getValue(final String key, final Class<O> returnType, final Function<String, O> lambda) {
        O value = getValue(key, returnType);
        if (value == null) {
            value = lambda.apply(key);
            JVM_LOCAL_MAP.put(key, value);
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
            JVM_LOCAL_MAP.putAll(missingMap);
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
            value = lambda.apply(key);
            JVM_LOCAL_MAP.put(key, value);
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
            JVM_LOCAL_MAP.putAll(missingMap);
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
        return (Collection<Map>) JVM_LOCAL_MAP.get(key);
    }

    @SuppressWarnings("unchecked")
    public <O> O getValue(final String key) {
        return (O) JVM_LOCAL_MAP.get(key);
    }

    public Map<String, Object> getValues(final String group, final Collection<String> keys, final Function<Collection<String>, Map<String, Object>> lambda, final Duration ttl) {
        throw new UnsupportedOperationException();
    }
}
