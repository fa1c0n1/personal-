package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.PipelineConstants.ENVIRONMENT;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Long.parseLong;

public final class AppConfig {
    private static final Config BASE_CONFIG = ConfigFactory.load();
    private static final String MODE = _env("APP_MODE", BASE_CONFIG.hasPath("APP_MODE") ? BASE_CONFIG.getString("APP_MODE") : "LOCAL").toUpperCase();
    private static final Config APP_CONFIG = BASE_CONFIG.hasPath(MODE) ? BASE_CONFIG.getConfig(MODE).withFallback(BASE_CONFIG) : BASE_CONFIG;
    private static final String APP_NAME = _env("APP_NAME", BASE_CONFIG.hasPath("APP_NAME") ? BASE_CONFIG.getString("APP_NAME") : "Stargate");
    private static final String CONFIG_PREFIX = APP_NAME.toLowerCase();
    private static final long APP_ID = initAppId();

    private AppConfig() {
    }

    private static long initAppId() {
        long appId = _initAppId();
        System.setProperty(A3Constants.APP_ID, String.valueOf(appId));
        return appId;
    }

    private static long _initAppId() {
        String envValue = _env(A3Constants.APP_ID, null);
        if (envValue != null && !envValue.trim().isBlank()) {
            return parseLong(envValue.trim());
        }
        if (APP_CONFIG.hasPath(CONFIG_PREFIX + ".idms.appId")) {
            return APP_CONFIG.getLong(CONFIG_PREFIX + ".idms.appId");
        }
        if (APP_CONFIG.hasPath("appeng.aluminum.auth.idms.appId")) {
            return APP_CONFIG.getLong("appeng.aluminum.auth.idms.appId");
        }
        A3Constants.KNOWN_APP app = A3Constants.KNOWN_APP.app(APP_NAME);
        if (app != null) return app.appId();
        throw new IllegalStateException("Could not determine IDMS AppID");
    }

    private static String _env(final String propertyName, final String defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (!isBlank(propertyValue)) {
            return propertyValue.trim();
        }
        propertyValue = System.getenv(propertyName.indexOf('.') <= 0 ? propertyName : String.format("APP_%s", propertyName.replaceAll("\\.", "_").toUpperCase()));
        if (!isBlank(propertyValue)) {
            return propertyValue.trim();
        }
        return defaultValue;
    }

    public static String env(final String propertyName, final String defaultValue) {
        String value = _env(propertyName, null);
        if (value != null) return value;
        if (APP_CONFIG.hasPath(propertyName)) {
            String propertyValue = APP_CONFIG.getString(propertyName);
            if (!isBlank(propertyValue)) {
                return propertyValue.trim();
            }
        }
        return defaultValue;
    }

    public static long appId() {
        return APP_ID;
    }

    public static String mode() {
        return MODE;
    }

    public static String appName() {
        return APP_NAME;
    }

    public static String configPrefix() {
        return CONFIG_PREFIX;
    }

    public static Config config() {
        return APP_CONFIG;
    }

    public static ENVIRONMENT environment() {
        return ENVIRONMENT.environment(MODE);
    }

    public static ConfigObject configObject() {
        return APP_CONFIG.root();
    }

    public static Properties properties(final String path, final String prefix) {
        Properties properties = new Properties();
        APP_CONFIG.getConfig(path).entrySet().forEach(entry -> properties.put(prefix + "." + entry.getKey(), entry.getValue().unwrapped()));
        return properties;
    }

    public static Map<String, Object> map(final String path) {
        Map<String, Object> map = new HashMap<>();
        APP_CONFIG.getConfig(path).entrySet().forEach(entry -> map.put(entry.getKey(), entry.getValue().unwrapped()));
        return map;
    }
}
