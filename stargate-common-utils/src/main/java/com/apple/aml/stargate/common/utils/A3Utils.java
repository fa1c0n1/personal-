package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.pojo.A3Token;
import com.apple.ist.idms.i3.a3client.pub.A3TokenClientI;
import com.apple.ist.idms.i3.a3client.pub.A3TokenFactory;
import com.apple.ist.idms.i3.a3client.pub.A3TokenInfo;
import com.typesafe.config.Config;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.apple.aml.stargate.common.constants.A3Constants.APP_DS_PASSWORD;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.IDMS_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.lang.Boolean.parseBoolean;

public final class A3Utils {
    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient();
    private static final long GRACE_PERIOD = 10 * 60 * 1000; // 10minutes
    private static ConcurrentHashMap<String, A3TokenInfo> tokenInfoMap = new ConcurrentHashMap<>();
    private final long appId = AppConfig.appId();
    private String defaultContextString;
    private String mode;
    private String appAdminPassword;
    private A3TokenClientI client;

    private A3Utils() {
        try {
            init();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Unable to initialize A3TokenClient", e);
        }
    }

    private void init() throws InterruptedException {
        Config config = AppConfig.config();
        String configPrefix = configPrefix();
        String appengPrefix = "appeng.aluminum.auth";
        String mode = config.hasPath(configPrefix + ".idms.mode") ? config.getString(configPrefix + ".idms.mode") : null;
        if (mode == null) {
            mode = config.hasPath(appengPrefix + ".idms.mode") ? config.getString(appengPrefix + ".idms.mode") : null;
        }
        if (mode == null || mode.toUpperCase().startsWith(A3Constants.A3_MODE.PROD.name())) {
            mode = A3Constants.A3_MODE.PROD.name();
        } else {
            mode = A3Constants.A3_MODE.UAT.name();
        }
        this.mode = mode;
        System.setProperty("I3_ENV", A3Constants.A3_MODE.PROD.name().equals(this.mode) ? "PROD-HTTPS" : "IDMS-UAT-HTTPS");
        setDefaultContextString(config, configPrefix, appengPrefix);
        appAdminPassword = config.hasPath(configPrefix + ".idms.appAdminPassword") ? config.getString(configPrefix + ".idms.appAdminPassword") : (config.hasPath(appengPrefix + ".idms.appAdminPassword") ? config.getString(appengPrefix + ".idms.appAdminPassword") : env(APP_DS_PASSWORD, null));
        if (isBlank(appAdminPassword)) return;// "A3 Details not configured !! Will skip A3 initialization !!"
        client = A3TokenFactory.getA3TokenClient();
        client.initialize(appId, appAdminPassword);
        int maxWait = config.hasPath(configPrefix + ".idms.maxSecondsToWait") ? config.getInt(configPrefix + ".idms.maxSecondsToWait") : 15;
        int counter = 0;
        while (!client.isInitialized() && counter < maxWait) {
            Thread.sleep(1000);
            counter += 1;
        }
        if (!client.isInitialized()) {
            throw new IllegalStateException("Unable to initialize A3TokenClient within " + maxWait + " seconds");
        }
    }

    private void setDefaultContextString(final Config config, final String configPrefix, final String appengPrefix) {
        if (config.hasPath(configPrefix + ".idms.contextString")) {
            defaultContextString = config.getString(configPrefix + ".idms.contextString");
        }
        if ((defaultContextString == null || defaultContextString.isBlank()) && config.hasPath(appengPrefix + ".idms.contextString")) {
            defaultContextString = config.getString(appengPrefix + ".idms.contextString");
        }
        if ((defaultContextString == null || defaultContextString.isBlank()) && config.hasPath(configPrefix + ".idms.contextStrings")) {
            defaultContextString = config.getConfig(configPrefix + ".idms.contextStrings").getString("1");
        }
        if ((defaultContextString == null || defaultContextString.isBlank()) && config.hasPath(appengPrefix + ".idms.a3.receiver.contextStrings")) {
            defaultContextString = config.getConfig(appengPrefix + ".idms.a3.receiver.contextStrings").getString("1");
        }
    }

    public static String getA3Token(final long targetAppId) {
        return getA3Token(targetAppId, null);
    }

    public static String getA3Token(final long targetAppId, final String contextString) {
        return getA3Token(targetAppId, contextString, null);
    }

    public static String getA3Token(final long targetAppId, final String contextString, final Long contextVersion) {
        A3Utils instance = getInstance();
        String a3TokenCachingKey = String.format("%s^%s^%s^%s", instance.appId, targetAppId, contextString, contextVersion);
        A3TokenInfo cachedInfo = tokenInfoMap.get(a3TokenCachingKey);
        if (cachedInfo == null || (cachedInfo.getTokenExpirationTime() - System.currentTimeMillis() <= GRACE_PERIOD)) {
            A3TokenInfo info = getA3TokenInfo(targetAppId, contextString, contextVersion);
            info.parseToken();
            tokenInfoMap.put(a3TokenCachingKey, info);
            return info.getToken();
        }
        return cachedInfo.getToken();
    }

    public static A3Utils getInstance() {
        return Helper.INSTANCE;
    }

    public static A3TokenInfo getA3TokenInfo(final long targetAppId, final String contextString, final Long contextVersion) {
        A3Utils instance = getInstance();
        String ctxString = contextString;
        A3Constants.KNOWN_APP app = null;
        if (isBlank(ctxString)) {
            app = A3Constants.KNOWN_APP.app(targetAppId);
            ctxString = app == null ? instance.defaultContextString : app.getConfig().getContextString();
        }
        A3TokenInfo tokenInfo = new A3TokenInfo(instance.appId, targetAppId, ctxString);
        tokenInfo.setOnetimeToken(false);
        Long ctxVersion = contextVersion;
        if ((ctxVersion == null || ctxVersion <= 0) && app != null) ctxVersion = app.getConfig().getContextVersion();
        if (ctxVersion != null) tokenInfo.setContextVersion(ctxVersion);
        A3TokenInfo generatedInfo = instance.client.generateToken(tokenInfo);
        return generatedInfo;
    }

    public static A3TokenInfo getA3TokenInfo(final long targetAppId, final String contextString) {
        return getA3TokenInfo(targetAppId, contextString, null);
    }

    public static String getCachedToken(final long targetAppId, final String appAdminPassword, final String contextString) {
        A3Utils instance = getInstance();
        return getCachedToken(instance.appId, targetAppId, appAdminPassword, contextString, null, environment());
    }

    public static String getCachedToken(final long appId, final long targetAppId, final String appAdminPassword, final String contextString, final Long contextVersion, final PipelineConstants.ENVIRONMENT environment) {
        return getCachedTokenInfo(appId, targetAppId, appAdminPassword, contextString, contextVersion, environment).getToken();
    }

    public static A3TokenInfo getCachedTokenInfo(final long appId, final long targetAppId, final String appAdminPassword, final String contextString, final Long contextVersion, final PipelineConstants.ENVIRONMENT environment) {
        A3Utils instance = getInstance();
        String context = isBlank(contextString) ? instance.defaultContextString : contextString;
        String a3TokenCachingKey = String.format("%s^%s^%s^%s", appId, targetAppId, context, null);
        A3TokenInfo cachedInfo = tokenInfoMap.get(a3TokenCachingKey);
        if (cachedInfo == null || (cachedInfo.getTokenExpirationTime() - System.currentTimeMillis() <= GRACE_PERIOD)) {
            A3TokenInfo tokenInfo = new A3TokenInfo(appId, targetAppId, context);
            tokenInfo.setOnetimeToken(false);
            PipelineConstants.ENVIRONMENT env = environment == null ? environment() : environment;
            tokenInfo.setToken(getA3Token(tokenInfo.getAppId(), env.getConfig().getIdmsUrl(), appAdminPassword, tokenInfo.getOtherAppId(), tokenInfo.getContext(), contextVersion));
            tokenInfo.parseToken();
            tokenInfoMap.put(a3TokenCachingKey, tokenInfo);
            return tokenInfo;
        }
        return cachedInfo;
    }

    public static String getA3Token(final long appId, final String a3BaseUrl, final String appAdminPassword, final long targetAppId, final String contextString) throws SecurityException {
        return getA3Token(appId, a3BaseUrl, appAdminPassword, targetAppId, contextString, null);
    }

    public static String getA3Token(final long appId, final String a3BaseUrl, final String appAdminPassword, final long targetAppId, final String contextString, final Long contextVersion) throws SecurityException {
        String url = a3BaseUrl + "/generate";
        Response response = null;
        String responseString;
        int responseCode;
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put(IDMS_APP_ID, appId);
            payload.put("appPassword", appAdminPassword);
            payload.put("otherApp", targetAppId);
            payload.put("context", contextString);
            payload.put("oneTimeToken", false);
            if (contextVersion != null && contextVersion.longValue() > 0) payload.put("contextVersion", contextVersion.longValue());
            String json = jsonString(payload);
            RequestBody body = RequestBody.create(json, MEDIA_TYPE_APPLICATION_JSON);
            Request request = new Request.Builder().url(url).post(body).build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                throw new Exception("Invalid A3 generate POST request !! Response code : " + responseCode + ". Response Message : " + responseString);
            }
            if (responseString == null || responseString.isEmpty()) {
                return null;
            }
            return (String) readJson(responseString, Map.class).get("token");
        } catch (Exception e) {
            throw new SecurityException("Error getting a valid response for A3 token generate http POST request to url " + url, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public static long tokenExpiry(A3TokenInfo info) {
        return info.getTokenExpirationTime() - GRACE_PERIOD;
    }

    public static String getA3Token(final String a3BaseUrl, final long targetAppId) throws SecurityException {
        A3Utils instance = getInstance();
        return getA3Token(a3BaseUrl, instance.appAdminPassword, targetAppId, instance.defaultContextString);
    }

    public static String getA3Token(final String a3BaseUrl, final String appAdminPassword, final long targetAppId, final String contextString) throws SecurityException {
        A3Utils instance = getInstance();
        return getA3Token(instance.appId, a3BaseUrl, appAdminPassword, targetAppId, contextString);
    }

    public static String getA3Token(final String a3BaseUrl, final String appAdminPassword, final long targetAppId) throws SecurityException {
        A3Utils instance = getInstance();
        return getA3Token(a3BaseUrl, appAdminPassword, targetAppId, instance.defaultContextString);
    }

    public static boolean verify(final A3TokenInfo tokenInfo) {
        return getInstance().client.validateToken(tokenInfo);
    }

    public static boolean verify(final String a3BaseUrl, final A3TokenInfo tokenInfo) throws SecurityException {
        String url = a3BaseUrl + "/verify";
        Response response = null;
        String responseString;
        int responseCode = -1;
        try {
            String json = jsonString(tokenInfo);
            RequestBody body = RequestBody.create(json, MEDIA_TYPE_APPLICATION_JSON);
            Request request = new Request.Builder().url(url).post(body).build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                throw new Exception("Invalid A3 verify POST request !! Response code : " + responseCode + ". Response Message : " + responseString);
            }
            if (responseString == null || responseString.isEmpty()) {
                return false;
            }
            return parseBoolean(responseString) || "{}".equals(responseString.replaceAll("[\\n\\t ]", EMPTY_STRING).trim());
        } catch (Exception e) {
            throw new SecurityException("Error getting a valid response for A3 token verify http POST request to url " + url, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public static boolean validate(final long appId, final String token) {
        return validate(appId, token, getInstance().defaultContextString);
    }

    public static boolean validate(final long appId, final String token, final String context) {
        A3Utils instance = getInstance();
        A3TokenInfo tokenInfo = new A3TokenInfo(instance.appId, appId, context);
        tokenInfo.setOnetimeToken(false);
        tokenInfo.setToken(token);
        return instance.client.validateToken(tokenInfo);
    }

    public static boolean validate(final A3TokenInfo tokenInfo) {
        return getInstance().client.validateToken(tokenInfo);
    }

    public static boolean validate(final String a3BaseUrl, final A3Token tokenInfo) throws SecurityException {
        A3Utils instance = getInstance();
        String url = a3BaseUrl + "/validate";
        Response response = null;
        String responseString;
        int responseCode = -1;
        try {
            String json = jsonString(Map.of(IDMS_APP_ID, tokenInfo.getAppId(), "appPassword", tokenInfo.appPassword(instance.appAdminPassword), "otherApp", tokenInfo.getOtherAppId(), "context", tokenInfo.getContext(), "oneTimeToken", tokenInfo.isOnetimeToken(), "token", tokenInfo.getToken()));
            RequestBody body = RequestBody.create(json, MEDIA_TYPE_APPLICATION_JSON);
            Request request = new Request.Builder().url(url).post(body).build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                throw new Exception("Invalid A3 verify POST request !! Response code : " + responseCode + ". Response Message : " + responseString);
            }
            if (responseString == null || responseString.isEmpty()) {
                return false;
            }
            return parseBoolean(responseString) || "{}".equals(responseString.replaceAll("[\\n\\t ]", EMPTY_STRING).trim());
        } catch (Exception e) {
            throw new SecurityException("Error getting a valid response for A3 token verify http POST request to url " + url, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public static String a3Mode() {
        A3Utils instance = getInstance();
        return instance.mode;
    }

    public static String contextString() {
        A3Utils instance = getInstance();
        return instance.defaultContextString;
    }

    public static A3TokenClientI client() {
        return getInstance().client;
    }

    private static class Helper {
        private static final A3Utils INSTANCE = new A3Utils();
    }
}
