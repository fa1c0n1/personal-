package com.apple.aml.stargate.common.web.clients;

import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.typesafe.config.Config;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.JsonUtils.getNestedJsonField;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.WebUtils.getJsonData;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class AppConfigClient {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final Long APP_ID = appId();
    private static final String CONFIG_PREFIX = configPrefix() + ".appconfig";
    private static final Config CONFIG = AppConfig.config();
    private static final long CONFIG_SERVER_APP_ID = CONFIG.hasPath(CONFIG_PREFIX + ".appId") ? CONFIG.getLong(CONFIG_PREFIX + ".appId") : A3Constants.KNOWN_APP.APP_CONFIG.appId();
    private static final String BASE_URL = (CONFIG.hasPath(CONFIG_PREFIX + ".apiUri") ? CONFIG.getString(CONFIG_PREFIX + ".apiUri") : environment().getConfig().getAppConfigApiUrl());
    private static final String CONFIG_SERVER_A3_CONTEXT = CONFIG.hasPath(CONFIG_PREFIX + ".contextString") ? CONFIG.getString(CONFIG_PREFIX + ".contextString") : A3Constants.KNOWN_APP.APP_CONFIG.getConfig().getContextString();
    private static final String APP_MODE = AppConfig.mode();
    private static final String APPCONFIG_DEFAULT_MODULEID = CONFIG.hasPath("appconfig.default.module.id") ? CONFIG.getString("appconfig.default.module.id") : null;

    private AppConfigClient() {}

    @SuppressWarnings("unchecked")
    public static <O> Map<String, O> getProperties(final String connectId, final String moduleId, final String environment) throws Exception {
        Map<String, Object> propertiesResponse = getPropertiesResponse(connectId, moduleId, environment);
        Map<String, String> properties = getNestedJsonField(propertiesResponse, Map.class, "propertySources", 0, "source");

        Map<String, O> connectInfo = new HashMap<>();
        connectInfo.put("properties", (O) Base64.getEncoder().encodeToString(jsonString(properties).getBytes(UTF_8)));

        return connectInfo;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getPropertiesResponse(final String connectId, final String moduleId, final String environment) throws Exception {
        String acProfileFormat = connectId.replace("-", "_").toUpperCase();
        String resolvedModuleId = moduleId == null ? APPCONFIG_DEFAULT_MODULEID : moduleId;
        String resolvedEnvironment = environment == null ? APP_MODE.toUpperCase() : environment.toUpperCase();

        return getData(String.format("/%s/%s/%s", resolvedModuleId, acProfileFormat, resolvedEnvironment), Map.class);
    }

    private static <O> O getData(final String subUrl, final Class<O> readAs) throws Exception {
        return getData(subUrl, readAs, true);
    }

    @SuppressWarnings("unchecked")
    private static <O> O getData(final String subUrl, final Class<O> readAs, final boolean retry) throws Exception {
        String url = BASE_URL + subUrl;

        try {
            return getJsonData(url, APP_ID, A3Utils.getA3Token(CONFIG_SERVER_APP_ID, CONFIG_SERVER_A3_CONTEXT), A3Utils.a3Mode(), readAs);
        } catch (Exception e) {
            if (retry) {
                return getData(subUrl, readAs, false);
            }

            LOGGER.warn(e.getMessage());
            throw e;
        }
    }
}