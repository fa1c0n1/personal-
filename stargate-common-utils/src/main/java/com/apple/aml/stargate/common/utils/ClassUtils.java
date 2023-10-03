package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.annotation.RootLevelOption;
import com.google.common.base.CaseFormat;
import com.typesafe.config.Optional;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;

import java.beans.Statement;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastJsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.fastReadJson;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.merge;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public final class ClassUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    private ClassUtils() {
    }

    public static Class fetchClassIfExists(final String className) {
        try {
            return Class.forName(className);
        } catch (Exception ignored) {
            LOGGER.trace("Could not load class", Map.of("className", className, ERROR_MESSAGE, String.valueOf(ignored.getMessage())));
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static Object getAs(final Object input, final String className) {
        if (input == null) {
            return null;
        }
        Class valueClass = className == null ? Map.class : (Class.forName(className.indexOf(".") < 0 ? "java.lang." + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, className.toLowerCase()) : className));
        Class inputClass = input.getClass();
        if (valueClass.isAssignableFrom(inputClass)) {
            return input;
        }
        return readJson(jsonString(input), valueClass);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static Object fastGetAs(final Object input, final String className) {
        if (input == null) {
            return null;
        }
        Class valueClass = className == null ? Map.class : (Class.forName(className.indexOf(".") < 0 ? "java.lang." + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, className.toLowerCase()) : className));
        Class inputClass = input.getClass();
        if (valueClass.isAssignableFrom(inputClass)) {
            return input;
        }
        return fastReadJson(fastJsonString(input), valueClass);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static <O> O As(final Object input, final Class<O> className) {
        if (input == null) {
            return null;
        }
        Class valueClass = className == null ? Map.class : className;
        Class inputClass = input.getClass();
        if (valueClass.isAssignableFrom(inputClass)) {
            return (O) input;
        }
        return (O) fastReadJson(fastJsonString(input), valueClass);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static <O> O getAs(final Object input, final Class<O> className) {
        if (input == null) {
            return null;
        }
        Class valueClass = className == null ? Map.class : className;
        Class inputClass = input.getClass();
        if (valueClass.isAssignableFrom(inputClass)) {
            return (O) input;
        }
        return (O) readJson(jsonString(input), valueClass);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public static <O> O duplicate(final Object input, final Class<O> className) {
        if (input == null) {
            return null;
        }
        return readJson(jsonString(input), className);
    }

    public static boolean parseBoolean(final Object input) {
        if (input == null) return false;
        if (input instanceof Boolean) return (Boolean) input;
        String str = input.toString().trim();
        if (str.isBlank()) return false;
        return "true".equalsIgnoreCase(str) || "yes".equalsIgnoreCase(str) || "on".equalsIgnoreCase(str) || "y".equalsIgnoreCase(str);
    }

    @SuppressWarnings("unchecked")
    public static void cascadeProperties(final Object root, final Object applyOn, final boolean override) {
        if (applyOn == null || applyOn instanceof Map || root == null) return;
        Class className = applyOn.getClass();
        FieldUtils.getAllFieldsList(root.getClass()).stream().filter(f -> f.getAnnotation(RootLevelOption.class) != null && f.getAnnotation(Optional.class) != null).forEach(field -> {
            String methodSuffix = String.valueOf(field.getName().charAt(0)).toUpperCase() + field.getName().substring(1);
            try {
                Object value;
                try {
                    value = field.get(root);
                } catch (Exception e) {
                    try {
                        value = root.getClass().getMethod("get" + methodSuffix).invoke(root);
                    } catch (Exception ex) {
                        value = root.getClass().getMethod("is" + methodSuffix).invoke(root);
                    }
                }
                if (value == null) return;
                Method setterMethod = className.getMethod("set" + methodSuffix, field.getType());
                if (setterMethod == null) return;
                if (override) {
                    setterMethod.invoke(applyOn, value);
                    return;
                }
                Method getterMethod = className.getMethod("get" + methodSuffix); // You can override only String fields for now; rest of primitive fields have defaults of thier own & hence we can't determine if user specified as override or not
                if (getterMethod == null || !getterMethod.getReturnType().equals(String.class) || getterMethod.getParameterCount() != 0) return;
                String existingValue = (String) getterMethod.invoke(applyOn);
                if (existingValue != null) return;
                setterMethod.invoke(applyOn, value);
            } catch (Exception ignored) {

            }
        });
    }

    @SuppressWarnings("unchecked")
    public static Method getMatchingMethod(final Class objectClass, final String name, final Class<?>... parameterTypes) {
        try {
            Method method = objectClass.getMethod(name, parameterTypes);
            return method;
        } catch (Exception e) {
            return null;
        }
    }

    public static byte[] UUIDToBytes(final UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    public static UUID bytesToUUID(final byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        long high = byteBuffer.getLong();
        long low = byteBuffer.getLong();
        return new UUID(high, low);
    }

    @SneakyThrows
    public static <O> O updateObject(final O object, final Map<String, Object> attributes) {
        if (object == null || attributes == null || attributes.isEmpty()) return object;
        Map<String, Object> remaining = new HashMap<>();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            String key = entry.getKey();
            CaseFormat format = key.contains("-") ? CaseFormat.LOWER_HYPHEN : (key.contains("_") ? CaseFormat.LOWER_UNDERSCORE : CaseFormat.LOWER_CAMEL);
            Statement statement = new Statement(object, String.format("set%s", format.to(CaseFormat.UPPER_CAMEL, key)), new Object[]{entry.getValue()});
            try {
                statement.execute();
            } catch (Exception ignored) {
                remaining.put(entry.getKey(), entry.getValue());
            }
        }
        if (!remaining.isEmpty()) merge(object, jsonString(remaining));
        return object;
    }
}
