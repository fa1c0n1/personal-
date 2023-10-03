package com.apple.aml.stargate.common.constants;

import com.apple.aml.stargate.common.configs.KnownAppConfig;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;

import java.util.concurrent.ConcurrentHashMap;

public interface A3Constants {
    String CALLER_APP_ID = "callerAppId";
    String TRACKER_APP_ID = "trackerAppId";
    String APP_ID = "APP_ID";
    String APP_DS_KEY = "APP_DS_KEY";
    String APP_DS_PASSWORD = "APP_DS_PASSWORD";

    enum A3_MODE {
        PROD, UAT
    }

    enum KNOWN_APP {
        EAI, CLOUD_CONFIG, DHARI, STARGATE, NATIVE_ML_PLATFORM, NATIVE_ML_TEST_SUITE, STRATOS, DATA_CATALOG, GBI_KEYSTONE, APP_CONFIG;
        private static final ConcurrentHashMap<String, KNOWN_APP> LOOKUP = new ConcurrentHashMap<>();

        static {
            Config cfg = AppConfig.config().getConfig(KnownAppConfig.CONFIG_NAME);
            for (final String key : cfg.root().keySet()) {
                KNOWN_APP app = KNOWN_APP.valueOf(key.toUpperCase());
                app.config = ConfigBeanFactory.create(cfg.getConfig(key), KnownAppConfig.class);
                try {
                    app.config.init();
                } catch (Exception e) {
                    throw new IllegalArgumentException();
                }
                LOOKUP.put(app.name(), app);
                LOOKUP.put(app.getConfig().getAppId() + "", app);
            }
        }

        private KnownAppConfig config = new KnownAppConfig();

        public static KNOWN_APP app(final String input) {
            try {
                if (input == null || input.isBlank()) {
                    return null;
                }
                String inputString = input.toUpperCase();
                KNOWN_APP app = KNOWN_APP.valueOf(inputString);
                if (app != null) {
                    return app;
                }
                return LOOKUP.get(inputString);
            } catch (Exception e) {
                return null;
            }
        }

        public static KNOWN_APP app(final long input) {
            try {
                return LOOKUP.get(String.valueOf(input));
            } catch (Exception e) {
                return null;
            }
        }

        public KnownAppConfig getConfig() {
            return config;
        }

        public long appId() {
            return config.getAppId();
        }

    }
}
