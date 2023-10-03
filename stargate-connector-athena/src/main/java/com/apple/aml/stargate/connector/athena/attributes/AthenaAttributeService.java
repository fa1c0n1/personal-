package com.apple.aml.stargate.connector.athena.attributes;

import com.apple.aml.stargate.common.options.AttributesOptions;
import com.apple.aml.stargate.common.services.ShuriOfsService;
import com.apple.athena.attributes.IAttributes;
import com.apple.athena.attributes.ILookups;
import com.apple.athena.attributes.IShuri;
import com.apple.athena.attributes.client.AttributesClient;
import com.apple.athena.attributes.client.ShuriClientContext;
import com.apple.athena.conf.AthenaConfiguration;
import com.apple.athena.lookups.client.LookupClient;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.DEFAULT_DELIMITER;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.athena.dao.RequestDetails.emptyObject;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.util.Arrays.asList;

@SuppressWarnings("unchecked")
public class AthenaAttributeService implements ShuriOfsService {
    private static final ConcurrentHashMap<String, AthenaAttributeService> SERVICE_CACHE = new ConcurrentHashMap<>();
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final String nodeName;
    private ILookups lookups;
    private IAttributes attributes;
    private IShuri shuri;
    private String keyspace;

    public AthenaAttributeService(final String nodeName, final AttributesOptions options) {
        this.nodeName = nodeName;
        Map<String, Object> runtimeProps = new HashMap<>();
        if (options.getProps() != null) runtimeProps.putAll(options.getProps());
        Set<String> clientTypes = isBlank(options.getClientType()) ? new HashSet<>() : new HashSet<>(asList(options.getClientType().toLowerCase().trim().split(DEFAULT_DELIMITER)));
        this.keyspace = isBlank(options.getKeyspace()) ? null : options.getKeyspace().trim();
        String namespace = isBlank(options.getNamespace()) ? null : options.getNamespace().trim();
        Set<String> namespaces = options.getNamespaces() == null ? new HashSet<>() : (options.getNamespaces() instanceof Collection) ? ((Collection<Object>) options.getNamespaces()).stream().map(o -> o.toString()).collect(Collectors.toSet()) : Arrays.stream(String.valueOf(options.getNamespaces()).split(",")).collect(Collectors.toSet());
        if (namespace != null) namespaces.add(namespace);
        else if (namespaces != null && !namespaces.isEmpty()) namespace = namespaces.iterator().next();

        if (clientTypes.contains("attributes") || clientTypes.contains("athena") || clientTypes.contains(IAttributes.class.getSimpleName().toLowerCase())) {
            addRuntimeProps(keyspace != null ? String.format("athena.%s.hosted.cassandra", options.getKeyspace()) : null, runtimeProps, options);
            attributes = AttributesClient.createInstance();
        }

        if (clientTypes.contains("shuri") || clientTypes.contains("shuri-ofs") || clientTypes.contains("ofs") || clientTypes.contains(IShuri.class.getSimpleName().toLowerCase())) {
            if (namespace == null) throw new IllegalArgumentException("namespace config is missing!!");
            if (keyspace == null) throw new IllegalArgumentException("keyspace config is missing!!");
            initializeShuriClient(options, runtimeProps, namespace, namespaces);
        }

        if (clientTypes.contains("lookups") || clientTypes.contains("legacy") || clientTypes.contains("adc") || clientTypes.contains(ILookups.class.getSimpleName().toLowerCase())) {
            addRuntimeProps(keyspace != null ? String.format("athena.%s.hosted.cassandra", options.getKeyspace()) : null, runtimeProps, options);
            lookups = LookupClient.createInstance();
        }

        if (clientTypes.isEmpty()) {
            if (keyspace != null && namespace != null) initializeShuriClient(options, runtimeProps, namespace, namespaces);
            addRuntimeProps(keyspace != null ? String.format("athena.%s.hosted.cassandra", options.getKeyspace()) : null, runtimeProps, options);
            lookups = LookupClient.createInstance();
        }
    }

    private void initializeShuriClient(final AttributesOptions options, final Map<String, Object> runtimeProps, final String namespace, final Set<String> namespaces) {
        runtimeProps.put("athena.attributes.shuri.namespaces", namespaces.stream().collect(Collectors.joining(",")));
        namespaces.forEach(n -> {
            runtimeProps.put(String.format("athena.attributes.shuri.%s.keyspaces", n), keyspace);
            String prefix = String.format("athena.features.%s.cassandra", n);
            addPrefixedProps(prefix, runtimeProps, options);
        });
        runtimeProps.forEach((k, v) -> System.setProperty(k, String.valueOf(v)));
        AthenaConfiguration.initialize();
        runtimeProps.forEach((k, v) -> AthenaConfiguration.setOverrideProperty(k, v));
        ShuriClientContext.initialize();
        shuri = ShuriClientContext.getClient(namespace);
    }

    private void addRuntimeProps(final String prefix, final Map<String, Object> runtimeProps, final AttributesOptions options) {
        if (prefix != null) addPrefixedProps(prefix, runtimeProps, options);
        runtimeProps.forEach((k, v) -> System.setProperty(k, String.valueOf(v)));
        AthenaConfiguration.initialize();
        runtimeProps.forEach((k, v) -> AthenaConfiguration.setOverrideProperty(k, v));
    }

    private static void addPrefixedProps(final String prefix, final Map<String, Object> runtimeProps, final AttributesOptions options) {
        runtimeProps.put(String.format("%s.truststore.use", prefix), options.isUseTruststore());
        if (!isBlank(options.getDatacenter())) runtimeProps.put(String.format("%s.datacenter", prefix), options.getDatacenter());
        if (!isBlank(options.getEndpoints())) runtimeProps.put(String.format("%s.endpoints", prefix), options.getEndpoints());
        if (options.getPort() >= 1) runtimeProps.put(String.format("%s.port", prefix), options.getPort());
        if (!isBlank(options.getUser())) {
            runtimeProps.put(String.format("%s.authentication", prefix), true);
            runtimeProps.put(String.format("%s.user", prefix), options.getUser());
        }
        if (!isBlank(options.getPassword())) {
            runtimeProps.put(String.format("%s.authentication", prefix), true);
            runtimeProps.put(String.format("%s.password", prefix), options.getPassword());
        }
    }

    public static AthenaAttributeService attributeService(final String nodeName, final AttributesOptions options) {
        return SERVICE_CACHE.computeIfAbsent(nodeName, n -> new AthenaAttributeService(nodeName, options));
    }

    public static AthenaAttributeService attributeService(final String nodeName, final Function<String, AttributesOptions> function) {
        return SERVICE_CACHE.computeIfAbsent(nodeName, n -> new AthenaAttributeService(nodeName, function.apply(n)));
    }

    private IShuri shuri(final String namespace) {
        if (namespace == null) return shuri;
        return ShuriClientContext.getClient(namespace);
    }

    @Override
    @SneakyThrows
    public <O> O readLookup(final String key) {
        return (O) lookups.read(key).get();
    }

    @Override
    public <I, O> Map<String, O> bulkReadLookup(Collection<String> keys, Function<I, O> function) {
        return keys.stream().map(key -> {
            try {
                return lookups.read(key).thenApply(value -> Pair.of(key, function.apply((I) value)));
            } catch (Exception e) {
                LOGGER.error("Error reading lookup key ", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName, "key", key), e);
                CompletableFuture<Pair<String, O>> cFuture = CompletableFuture.completedFuture(Pair.of(key, null));
                return cFuture;
            }
        }).map(CompletableFuture::join).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    @Override
    @SneakyThrows
    public <O> boolean writeLookup(final String key, final O value, final int ttl, final long timestamp) {
        return lookups.insert(key, value, optionalTTL(ttl), timestamp < 0 ? ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now()) : timestamp).get();
    }

    @Override
    @SneakyThrows
    public <O> Map<String, Boolean> bulkWriteLookup(Map<String, O> keyValueMap, int ttl, long timestamp) {
        return keyValueMap.entrySet().stream().map(entry -> {
            try {
                return CompletableFuture.supplyAsync(() -> writeLookup(entry.getKey(), entry.getValue(), ttl, timestamp)).thenApply(value -> Pair.of(entry.getKey(), value));
            } catch (Exception e) {
                LOGGER.error("Error writing lookup key ", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName, "key", entry.getKey()), e);
                return CompletableFuture.completedFuture(Pair.of(entry.getKey(), Boolean.FALSE));
            }
        }).map(CompletableFuture::join).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private static Optional<Integer> optionalTTL(final int ttl) {
        return ttl <= 0 ? Optional.empty() : Optional.of(ttl);
    }

    @Override
    @SneakyThrows
    public boolean deleteLookup(final String key, final long timestamp) {
        return lookups.delete(key, timestamp < 0 ? ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now()) : timestamp).get();
    }

    @Override
    public Map<String, Boolean> bulkDeleteLookup(List<String> keys, long timestamp) {
        return keys.stream().map(key -> {
            try {
                return CompletableFuture.supplyAsync(() -> deleteLookup(key, timestamp)).thenApply(value -> Pair.of(key, value));
            } catch (Exception e) {
                LOGGER.error("Error deleting lookup key ", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName, "key", key), e);
                return CompletableFuture.completedFuture(Pair.of(key, Boolean.FALSE));
            }
        }).map(CompletableFuture::join).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    @Override
    @SneakyThrows
    public <O> O readFeatureGroup(final String namespace, final String featureName, final String entityId) {
        return (O) shuri(namespace).read(keyspace, attrKey(featureName, entityId)).get();
    }

    private static String attrKey(final String featureName, final String entityId) {
        return String.format("%s:%s", featureName, entityId);
    }

    @Override
    @SneakyThrows
    public <O> O readFeatureField(final String namespace, final String featureName, final String entityId, final String fieldName) {
        return (O) shuri(namespace).read(keyspace, attrKey(featureName, entityId), fieldName).get();
    }

    @Override
    @SneakyThrows
    public <O> O readFeatureGroups(final String namespace, final String featureName, final List<String> entityIds) {
        Map<String, Object> map = entityIds.stream().map(entityId -> shuri(namespace).read(keyspace, attrKey(featureName, entityId)).thenApply(o -> Pair.of(entityId, o))).map(CompletableFuture::join).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        return (O) map;
    }

    @Override
    @SneakyThrows
    public boolean writeFeatureGroup(final String namespace, final String featureName, final String entityId, final Map<String, ?> data, final int ttl) {
        return (ttl <= 0 ? shuri(namespace).insert(keyspace, attrKey(featureName, entityId), (Map<String, Object>) data) : shuri(namespace).insert(keyspace, attrKey(featureName, entityId), (Map<String, Object>) data, ttl)).get();
    }

    @Override
    @SneakyThrows
    public boolean deleteFeatureGroup(final String namespace, final String featureName, final String entityId) {
        return shuri(namespace).delete(keyspace, attrKey(featureName, entityId)).get();
    }

    @Override
    @SneakyThrows
    public boolean deleteFeatureField(final String namespace, final String featureName, final String entityId, final String fieldName) {
        return shuri(namespace).delete(keyspace, attrKey(featureName, entityId), fieldName).get();
    }

    @Override
    @SneakyThrows
    public <O> boolean write(final String key, final O value, final int ttl) {
        return writeFuture(key, value, ttl).get();
    }


    @Override
    @SneakyThrows
    public <O> Map<String, Boolean> bulkWrite(Map<String, O> keyValueMap, int ttl) {
        return keyValueMap.entrySet().stream().map(entry -> {
            try {
                return CompletableFuture.supplyAsync(() -> write(entry.getKey(), entry.getValue(), ttl)).thenApply(value -> Pair.of(entry.getKey(), value));
            } catch (Exception e) {
                LOGGER.error("Error writing attribute key ", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName, "key", entry.getKey()), e);
                return CompletableFuture.completedFuture(Pair.of(entry.getKey(), Boolean.FALSE));
            }
        }).map(CompletableFuture::join).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    @SneakyThrows
    public <O> Future<Boolean> writeFuture(final String key, final O value, final int ttl) {
        if (shuri != null) {
            if (value instanceof Map) {
                return shuri.insert(keyspace, key, (Map<String, Object>) value, ttl);
            }
            return shuri.insertLookup(keyspace, key, value, ttl);
        }
        if (attributes != null) {
            boolean isCollection = value instanceof Collection;
            Object val = isCollection ? ((((Collection<Object>) value).iterator().next() instanceof Map) ? value : readJson(jsonString(value), List.class)) : getAs(value, Map.class);
            return attributes.insert(key, val, isCollection ? List.class : Map.class, ttl <= 0 ? null : ttl, emptyObject());
        }
        return lookups.insert(key, value, optionalTTL(ttl), ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now()));
    }


    @Override
    @SneakyThrows
    public <O> boolean writeField(final String key, final String fieldName, final O fieldValue, final int ttl) {
        if (shuri != null) return shuri.insert(keyspace, key, fieldName, fieldValue, ttl).get();
        return attributes.updateMap(key, fieldName, fieldValue, ttl <= 0 ? null : ttl, emptyObject()).get();
    }

    @Override
    @SneakyThrows
    public boolean writeFields(final String key, final Map<String, ?> fields, final int ttl) {
        Integer ttlValue = ttl <= 0 ? null : ttl;
        if (shuri != null) return fields.entrySet().stream().map(e -> shuri.insert(keyspace, key, e.getKey(), e.getValue(), ttl)).map(CompletableFuture::join).allMatch(s -> s);
        return fields.entrySet().stream().map(e -> attributes.updateMap(key, e.getKey(), e.getValue(), ttlValue, emptyObject())).map(CompletableFuture::join).allMatch(s -> s);
    }

    @Override
    @SneakyThrows
    public boolean delete(final String key) {
        if (shuri != null) shuri.delete(keyspace, key).get();
        return attributes == null ? deleteLookup(key) : attributes.delete(key, null, emptyObject()).get();
    }

    @Override
    public Map<String, Boolean> bulkDelete(List<String> keys) {
        return keys.stream().map(key -> {
            try {
                return CompletableFuture.supplyAsync(() -> delete(key)).thenApply(value -> Pair.of(key, value));
            } catch (Exception e) {
                LOGGER.error("Error deleting attribute key ", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName, "key", key), e);
                return CompletableFuture.completedFuture(Pair.of(key, Boolean.FALSE));
            }
        }).map(CompletableFuture::join).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    @Override
    @SneakyThrows
    public boolean deleteField(final String key, final String fieldName) {
        if (shuri != null) return shuri.delete(keyspace, key, fieldName).get();
        return attributes.delete(key, null, fieldName, emptyObject()).get();
    }

    @Override
    @SneakyThrows
    public boolean deleteFields(final String key, final List<String> fieldNames) {
        if (attributes != null) return attributes.delete(key, null, fieldNames, emptyObject()).get();
        return fieldNames.stream().map(fieldName -> shuri.delete(keyspace, key, fieldName)).map(CompletableFuture::join).allMatch(s -> s);
    }

    @Override
    @SneakyThrows
    public <O> O read(final String key) {
        return (O) readFuture(key).get();
    }

    @Override
    public <I, O> Map<String, O> bulkRead(Collection<String> keys, Function<I, O> function) {
        return keys.stream().map(key -> {
            try {
                return readFuture(key).thenApply(value -> Pair.of(key, function.apply((I) value)));
            } catch (Exception e) {
                LOGGER.error("Error reading attribute key ", Map.of(ERROR_MESSAGE, String.valueOf(e.getMessage()), "nodeName", nodeName, "key", key), e);
                CompletableFuture<Pair<String, O>> cFuture = CompletableFuture.completedFuture(Pair.of(key, null));
                return cFuture;
            }
        }).map(CompletableFuture::join).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    @SneakyThrows
    public <O> CompletableFuture<O> readFuture(final String key) {
        if (shuri != null) return shuri.read(keyspace, key);
        return attributes == null ? lookups.read(key) : attributes.read(key, Map.class);
    }

    @Override
    @SneakyThrows
    public <O> O readList(final String key) {
        return (O) attributes.read(key, List.class).get();
    }

    @Override
    @SneakyThrows
    public <O> O readField(final String key, final String fieldName) {
        if (shuri != null) return (O) shuri.read(keyspace, key, fieldName).get();
        return (O) attributes.read(key, fieldName).get();
    }

    @Override
    @SneakyThrows
    public Map<String, ?> readFields(final String key, final List<String> fieldNames) {
        if (attributes != null) return attributes.read(key, fieldNames).get();
        return fieldNames.stream().map(fieldName -> shuri.read(keyspace, key, fieldName).thenApply(val -> Pair.of(fieldName, val))).map(CompletableFuture::join).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    @Override
    @SneakyThrows
    public Map<String, ?> readRangeFields(final String key, final String startKey, final String endKey) {
        if (shuri != null) return shuri.readFromMap(keyspace, key, startKey, endKey).get();
        return attributes.read(key, startKey, endKey).get();
    }
}
