package com.apple.aml.stargate.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

import java.util.Map;
import java.util.Properties;

import static com.apple.aml.stargate.common.constants.CommonConstants.CONFIG_RENDER_OPTIONS_CONCISE;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.utils.JsonUtils.pojoToProperties;

public final class HoconUtils {
    private HoconUtils() {
    }

    public static <O> O hoconToPojo(final String hoconString, final Class<O> readAsClass) {
        Config config = ConfigFactory.parseString(hoconString);
        return ConfigBeanFactory.create(config, readAsClass);
    }

    public static Map hoconToMap(final String hoconString) {
        Config config = ConfigFactory.parseString(hoconString);
        Map map = config.root().unwrapped();
        return map;
    }

    public static <O> String hoconString(final O object) throws JsonProcessingException {
        Map<String, Object> map = pojoToProperties(object, EMPTY_STRING);
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            appendHoconToBuilder(entry.getKey(), entry.getValue(), builder);
        }
        return builder.toString();
    }

    public static void appendHoconToBuilder(final String key, final Object value, final StringBuilder builder) {
        if (key == null || value == null) {
            return;
        }
        builder.append(key).append(" = ");
        if (value instanceof String) {
            String val = (String) value;
            String trimmed = val.trim();
            if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                trimmed = trimmed.replaceAll("\\n", "\n").replaceAll("\\t", "\t").replaceAll("\\r", "\r"); // TODO : need to replace all \value to value
                builder.append(trimmed);
            } else if (val.contains("\n") || val.contains("^")) {
                builder.append('"').append('"').append('"').append(value).append('"').append('"').append('"');
            } else if (val.startsWith("\"") && val.endsWith("\"")) {
                builder.append(value);
            } else if (val.contains("@") || val.contains("=") || val.contains(":") || val.contains(",") || val.contains("#") || val.contains("+") || val.contains(".") || val.contains("/") || val.contains("\\") || val.isBlank()) {
                builder.append('"').append(value).append('"');
            } else {
                builder.append(value);
            }
        } else if (value instanceof Class) {
            builder.append('"').append(((Class) value).getName()).append('"');
        } else {
            builder.append(value);
        }
        builder.append("\n");
    }

    public static ConfigObject merge(final ConfigObject input, final Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return input;
        }
        String hocon = input.render(CONFIG_RENDER_OPTIONS_CONCISE);
        hocon += "\n\n" + hoconString(properties);
        return ConfigFactory.parseString(hocon).root();
    }

    public static String hoconString(final Map<?, ?> map) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry entry : map.entrySet()) {
            appendHoconToBuilder(entry.getKey().toString(), entry.getValue(), builder);
        }
        return builder.toString();
    }
}
