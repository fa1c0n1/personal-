package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.pojo.KeyValuePair;
import com.apple.aml.stargate.common.utils.jackson.serializers.Utf8Serializer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;
import com.typesafe.config.ConfigValue;
import lombok.SneakyThrows;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.REDACTED_STRING;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS;
import static com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.WRITE_DOC_START_MARKER;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.apache.commons.lang3.StringUtils.isBlank;

public final class JsonUtils {
    private static final ObjectMapper OBJECT_MAPPER = objectMapper();
    private static final ObjectMapper YAML_MAPPER = yamlMapper();
    private static final ObjectMapper PROPS_MAPPER = propsMapper();
    private static final ObjectMapper ALL_MODULES_OBJECT_MAPPER = allModulesObjectMapper();
    private static final String REDACT_KEY_REGEX = "(secret|password|user|username|access|(private\\.key)|(verification\\.key)|token)";
    private static final Pattern REDACT_KEY_PATTERN = Pattern.compile(REDACT_KEY_REGEX, CASE_INSENSITIVE);
    private static final Pattern REDACT_REPLACEMENT_PATTERN = Pattern.compile("(" + REDACT_KEY_REGEX + "([^:=]*)([:=]+))([^\\r\\n=:, \\t]+)", CASE_INSENSITIVE);
    private static final String REDACTED_REPLACEMENT_STRING = "$1" + REDACTED_STRING;
    private static List<String> sensitiveKeys = null;
    private static Predicate<Object> IS_NULL = o -> o == null;
    private static Predicate<Object> IS_DEFAULT_NUMBER = o -> (o instanceof Number) && ((Number) o).doubleValue() == 0;
    private static Predicate<Object> IS_DEFAULT_BOOLEAN = o -> (o instanceof Boolean) && !((Boolean) o).booleanValue();
    @SuppressWarnings("unchecked")
    private static Predicate<Object>[] DEFAULT_SKIPS = new Predicate[]{IS_NULL, IS_DEFAULT_NUMBER, IS_DEFAULT_BOOLEAN};

    private JsonUtils() {
    }

    private static ObjectMapper allModulesObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(FAIL_ON_EMPTY_BEANS, false);
        return mapper;
    }

    private static ObjectMapper objectMapper() {
        ObjectMapper mapper = setObjectMapperDefaults(new ObjectMapper());
        return mapper;
    }

    public static <O extends ObjectMapper> O setObjectMapperDefaults(final O mapper) {
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new ParanamerModule());
        mapper.registerModule(new ParameterNamesModule());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(FAIL_ON_EMPTY_BEANS, false);
        SimpleModule module = new SimpleModule();
        module.addSerializer(Utf8.class, new Utf8Serializer());
        mapper.registerModule(module);
        return mapper;
    }

    private static ObjectMapper yamlMapper() {
        YAMLFactory factory = new YAMLFactory();
        factory.configure(WRITE_DOC_START_MARKER, false);
        ObjectMapper mapper = setObjectMapperDefaults(new ObjectMapper(factory));
        return mapper;
    }

    private static ObjectMapper propsMapper() {
        ObjectMapper mapper = setObjectMapperDefaults(new ObjectMapper(new JavaPropsFactory()));
        return mapper;
    }

    public static String fastJsonString(final Object value) {
        return JsonStream.serialize(value);
    }

    @SuppressWarnings("unchecked")
    public static Map<Object, Object> readNullableJsonMap(final String jsonString) throws Exception {
        try {
            return OBJECT_MAPPER.readValue(jsonString, Map.class);
        } catch (Exception e) {
            if (jsonString.trim().isBlank()) {
                return null;
            }
            throw e;
        }
    }

    public static String sjsonString(final Object value) throws JsonProcessingException {
        return ALL_MODULES_OBJECT_MAPPER.writeValueAsString(value);
    }

    public static <O> O sreadJson(final String jsonString, final Class<O> readAsClass) throws JsonProcessingException {
        return ALL_MODULES_OBJECT_MAPPER.readValue(jsonString, readAsClass);
    }

    @SuppressWarnings("unchecked")
    public static Map<Object, Object> sreadJsonMap(final String jsonString) throws JsonProcessingException {
        return ALL_MODULES_OBJECT_MAPPER.readValue(jsonString, Map.class);
    }

    public static String jsonToYaml(final String json) throws JsonProcessingException {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(json);
        String yaml = YAML_MAPPER.writeValueAsString(jsonNode);
        return yaml;
    }

    public static <O> O yamlToPojo(final String yamlString, final Class<O> readAsClass) throws JsonProcessingException {
        return YAML_MAPPER.readValue(yamlString, readAsClass);
    }

    @SneakyThrows
    public static Map yamlToMap(final String yamlString) {
        Object yml = YAML_MAPPER.readValue(yamlString, Object.class);
        String json = OBJECT_MAPPER.writeValueAsString(yml);
        return readJson(json, Map.class);
    }

    public static <O> O readJson(final String jsonString, final Class<O> readAsClass) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(jsonString, readAsClass);
    }

    public static <O> O fastReadJson(final String jsonString, final Class<O> readAsClass) {
        return JsonIterator.deserialize(jsonString, readAsClass);
    }

    @SneakyThrows
    public static String yamlString(final Object value) {
        return YAML_MAPPER.writeValueAsString(value);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static String simpleYamlString(final Object value) {
        Map<?, Object> map = value instanceof Map ? (Map<String, Object>) value : readJsonMap(jsonString(value));
        return map.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue())).collect(Collectors.joining("\n")) + "\n";
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static Map<Object, Object> readJsonMap(final String jsonString) {
        return OBJECT_MAPPER.readValue(jsonString, Map.class);
    }

    @SneakyThrows
    public static String jsonString(final Object value) {
        return OBJECT_MAPPER.writeValueAsString(value);
    }

    public static JsonNode jsonNode(final String jsonString, final Class<?> valueType) throws JsonProcessingException {
        return OBJECT_MAPPER.reader().forType(valueType).readTree(jsonString);
    }

    public static JsonNode jsonNode(final String jsonString) throws IOException {
        JsonFactory factory = OBJECT_MAPPER.getFactory();
        JsonParser parser = factory.createParser(jsonString);
        return OBJECT_MAPPER.readTree(parser);
    }

    @SuppressWarnings("unchecked")
    public static Map propertiesToMap(final List<KeyValuePair> pairs) throws JsonProcessingException {
        if (pairs == null) {
            return null;
        }
        if (pairs.isEmpty()) {
            return new HashMap<>();
        }
        StringBuilder builder = new StringBuilder();
        Collections.sort(pairs);
        for (KeyValuePair entry : pairs) {
            String value = entry.getValue();
            if (value == null) {
                continue;
            }
            String key = entry.getKey();
            builder.append(key).append(" = ");
            builder.append(value);
            builder.append("\n");
        }
        return PROPS_MAPPER.readValue(builder.toString(), Map.class);
    }

    @SuppressWarnings("unchecked")
    public static Map propertiesToMap(final Properties properties) throws JsonProcessingException {
        if (properties == null) {
            return null;
        }
        if (properties.isEmpty()) {
            return new HashMap<>();
        }
        StringBuilder builder = new StringBuilder();
        TreeMap<Object, Object> treeMap = new TreeMap<>(properties);
        for (Map.Entry<Object, Object> entry : treeMap.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            String key = entry.getKey().toString();
            if (key.isBlank()) {
                continue;
            }
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            builder.append(key).append(" = ");
            builder.append(value);
            builder.append("\n");
        }
        return PROPS_MAPPER.readValue(builder.toString(), Map.class);
    }

    public static <O> O propertiesToPojo(final Map<String, Object> properties, final Class<O> readAsClass) throws JsonProcessingException {
        if (properties == null) {
            return null;
        }
        String propString = propertiesToString(properties);
        return PROPS_MAPPER.readValue(propString, readAsClass);
    }

    private static String propertiesToString(final Map<String, Object> properties) {
        if (properties.isEmpty()) {
            return EMPTY_STRING;
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            Object val = entry.getValue();
            if (val == null) {
                continue;
            }
            String key = entry.getKey();
            builder.append(key).append(" = ");
            builder.append(val);
            builder.append("\n");
        }
        return builder.toString();
    }

    public static <O> Map<String, Object> pojoToProperties(final O object, final String prefix) throws JsonProcessingException {
        Map<String, Object> properties = new HashMap<>();
        Stream.of(PROPS_MAPPER.writeValueAsString(object).split("\\n")).map(x -> x.split("=")).forEach(x -> properties.put(prefix + x[0].trim(), x.length > 1 ? x[1] : EMPTY_STRING));
        return properties;
    }

    public static String propertiesString(final Object properties) throws JsonProcessingException {
        return PROPS_MAPPER.writeValueAsString(properties);
    }

    public static <O> O merge(final O object, final String jsonString) throws JsonProcessingException {
        ObjectReader reader = OBJECT_MAPPER.readerForUpdating(object);
        return reader.readValue(jsonString);
    }

    public static String dropSensitiveKeys(final Object object) {
        if (object == null) {
            return null;
        }
        try {
            return dropSensitiveKeys(jsonString(object), false);
        } catch (Exception e) {
            return REDACTED_STRING;
        }
    }

    public static String dropSensitiveKeys(final String inputJson, final boolean returnNullString) {
        if (inputJson == null) {
            return returnNullString ? "null" : null;
        }
        if (inputJson.isBlank()) {
            return inputJson;
        }
        String str = inputJson;
        for (final String key : setAndGetSensitiveKeys()) {
            str = str.replaceAll("\"([^\"]*)" + key + "\"[\\t ]?:[\\t ]?\"([^\"]*)\"[\\t ]?([,]?)", EMPTY_STRING);
            str = str.replaceAll("([\\r\\n\\t ]+)" + key + "[\\t ]?:[\\t ]?\"([^\"]*)\"[\\t ]?([,]?)", EMPTY_STRING);
        }
        str = REDACT_REPLACEMENT_PATTERN.matcher(str).replaceAll(REDACTED_REPLACEMENT_STRING);
        return str;
    }

    private static List<String> setAndGetSensitiveKeys() {
        if (sensitiveKeys != null) {
            return sensitiveKeys;
        }
        List<String> keys = new ArrayList<>();
        for (ConfigValue v : AppConfig.config().getList("stargate.sensitive.jsonKeys")) {
            keys.add(v.unwrapped().toString());
        }
        sensitiveKeys = keys;
        return keys;
    }

    public static String jsonStringFreemarker(final Object value) throws JsonProcessingException {
        if (value instanceof GenericRecord) {
            return ((GenericRecord) value).toString();
        }
        return OBJECT_MAPPER.writeValueAsString(value);
    }

    public static String dropSensitiveKeys(final String inputJson) {
        return dropSensitiveKeys(inputJson, false);
    }

    public static String redactString(final String input) {
        if (input == null) {
            return "null";
        }
        return REDACT_REPLACEMENT_PATTERN.matcher(input).replaceAll("$1****redacted****");
    }

    public static boolean isRedactable(final String input) {
        if (input == null) {
            return false;
        }
        return REDACT_KEY_PATTERN.matcher(input).find();
    }

    @SuppressWarnings("unchecked")
    public static Map<Object, Object> nonNullValueMap(final Object object) throws JsonProcessingException {
        Map<Object, Object> map = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(object), Map.class);
        for (Object key : new HashSet<>(map.keySet())) {
            Object value = map.get(key);
            if (value == null) {
                map.remove(key);
                continue;
            }
            if (value instanceof Number) {
                Number number = (Number) value;
                if (number.doubleValue() == 0) {
                    map.remove(key);
                    continue;
                }
            }
        }
        return map;
    }

    @SneakyThrows
    public static Map<Object, Object> nonNullMap(final Object object) {
        return filteredMap(object, DEFAULT_SKIPS);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static Map<Object, Object> filteredMap(final Object object, final Predicate<Object>... skips) {
        Map<Object, Object> map = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(object), Map.class);
        for (Object key : new HashSet<>(map.keySet())) {
            Object value = map.get(key);
            boolean remove = false;
            for (Predicate<Object> predicate : skips) {
                if (predicate.test(value)) {
                    remove = true;
                    break;
                }
            }
            if (remove) map.remove(key);
        }
        return map;
    }

    public static Map<String, String> toStringMap(final Map<String, Object> map) {
        return map == null ? Map.of() : map.entrySet().stream().map(e -> {
            try {
                Object value = e.getValue();
                if (value == null) return Pair.of(e.getKey(), (String) null);
                return Pair.of(e.getKey(), (value instanceof Collection || value instanceof Map) ? jsonString(value) : String.valueOf(value));
            } catch (Exception ex) {
                return Pair.of(e.getKey(), (String) null);
            }
        }).filter(p -> p.getValue() != null).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    public static String getNestedJsonField(final String jsonString, final Object... path) throws JsonProcessingException {
        if (isBlank(jsonString)) {
            return null;
        }

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode jsonNode = objectMapper.readTree(jsonString);
        for (Object arg : path) {
            if (arg instanceof String) jsonNode = jsonNode.get((String) arg);
            if (arg instanceof Integer) jsonNode = jsonNode.get((Integer) arg);
        }

        return jsonNode.toString();
    }

    public static <O> O getNestedJsonField(Map<String, Object> json, Class<O> readAs, final Object... path) throws JsonProcessingException {
        if (json == null || json.isEmpty()) {
            return null;
        }

        ObjectMapper objectMapper = new ObjectMapper();

        String jsonString = objectMapper.writeValueAsString(json);
        String nestedJsonString = getNestedJsonField(jsonString, path);
        return isBlank(nestedJsonString) ? null : readJson(nestedJsonString, readAs);
    }
}
