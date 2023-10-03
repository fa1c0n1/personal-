package com.apple.aml.stargate.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.event.Level;

import java.util.Optional;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.jvm.commons.util.Strings.isBlank;

public final class LogUtils {
    private static final String PACKAGE_GROUP_REGEX = "com\\.apple\\.aml\\.";

    private LogUtils() {
    }

    public static Logger logger(final Class<?> className) {
        return LoggerFactory.getLogger(className.getName().replaceFirst(PACKAGE_GROUP_REGEX, EMPTY_STRING));
    }

    public static String getFromMDCOrDefault(String key) {
        return getFromMDCOrDefault(key, "unknown");
    }

    public static String getFromMDCOrDefault(String key, String defaultValue) {
        try {
            return Optional.ofNullable(MDC.get(key)).orElse(defaultValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static Level parseLevel(final String input) {
        if (isBlank(input)) return null;
        String level = input.trim();
        try {
            return Level.valueOf(level.toUpperCase());
        } catch (Exception e) {
            return parseBoolean(level) ? Level.INFO : null;
        }
    }
}
