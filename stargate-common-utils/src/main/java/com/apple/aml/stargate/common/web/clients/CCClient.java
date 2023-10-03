package com.apple.aml.stargate.common.web.clients;

import com.apple.aml.stargate.common.constants.A3Constants.KNOWN_APP;
import com.apple.aml.stargate.common.exceptions.ResourceNotFoundException;
import com.apple.aml.stargate.common.pojo.KeyValuePair;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.AppConfig;
import com.apple.aml.stargate.common.web.spec.ConfigServiceSpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import lombok.Data;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.internal.Util;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_A3_TOKEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_CLIENT_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_MARKDOWN;
import static com.apple.aml.stargate.common.utils.A3Utils.a3Mode;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.AppConfig.appName;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.configPrefix;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.JsonUtils.dropSensitiveKeys;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.propertiesToMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getDecoder;

final class CCClient implements ConfigServiceSpec {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final String CURRENT_VERSION = "currentVersion";
    private static final String ACTIVE_VERSION = "activeVersion";
    private static final String APP_ID = String.valueOf(appId());
    private static final String CONFIG_PREFIX = configPrefix() + ".cloudconfig";
    private static final Config CONFIG = AppConfig.config();
    private static final long CONFIG_SERVER_APP_ID = CONFIG.hasPath(CONFIG_PREFIX + ".appId") ? CONFIG.getLong(CONFIG_PREFIX + ".appId") : KNOWN_APP.CLOUD_CONFIG.appId();
    private static final String BASE_URL = (CONFIG.hasPath(CONFIG_PREFIX + ".apiUri") ? CONFIG.getString(CONFIG_PREFIX + ".apiUri") : environment().getConfig().getCloudConfigApiUri()) + "/api/v1/server";
    private static final String CONFIG_SERVER_A3_CONTEXT = CONFIG.hasPath(CONFIG_PREFIX + ".contextString") ? CONFIG.getString(CONFIG_PREFIX + ".contextString") : KNOWN_APP.CLOUD_CONFIG.getConfig().getContextString();
    private static final String IDMS_MODE = CONFIG.hasPath(CONFIG_PREFIX + ".idms.mode") ? CONFIG.getString(CONFIG_PREFIX + ".idms.mode") : null;
    private static final String IDMS_MODE_PASSWORD = CONFIG.hasPath(CONFIG_PREFIX + ".idms.appAdminPassword") ? CONFIG.getString(CONFIG_PREFIX + ".idms.appAdminPassword") : null;

    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient();
    private static String a3Token = null;
    private static long a3TokenGeneratedTime = System.currentTimeMillis();

    private static String resetA3Token() {
        a3Token = IDMS_MODE == null || a3Mode().equalsIgnoreCase(IDMS_MODE) ? A3Utils.getA3Token(CONFIG_SERVER_APP_ID, CONFIG_SERVER_A3_CONTEXT) : A3Utils.getA3Token(environment().getConfig().getIdmsUrl(), IDMS_MODE_PASSWORD, CONFIG_SERVER_APP_ID, CONFIG_SERVER_A3_CONTEXT);
        a3TokenGeneratedTime = System.nanoTime();
        return a3Token;
    }

    private static String a3Token() {
        if (a3Token == null || System.nanoTime() - a3TokenGeneratedTime >= 3600000) {
            return resetA3Token();
        }
        return a3Token;
    }

    private static Request.Builder builder(final String url) {
        return new Request.Builder().url(url).addHeader(HEADER_CLIENT_APP_ID, APP_ID).addHeader(HEADER_A3_TOKEN, a3Token());
    }

    private static <O> O getData(final String subUrl, final Class<O> readAs) throws Exception {
        return getData(subUrl, readAs, true);
    }

    @SuppressWarnings("unchecked")
    private static <O> O getData(final String subUrl, final Class<O> readAs, final boolean retry) throws Exception {
        String url = BASE_URL + subUrl;
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            LOGGER.debug("Making remote http GET request to url {}", url);
            Request request = builder(url).get().build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            if (!response.isSuccessful()) {
                if (responseCode == 404) {
                    return null;
                }
                if (responseCode == 401 && retry) {
                    resetA3Token();
                    return getData(subUrl, readAs, false);
                }
                responseString = responseBody == null ? null : responseBody.string();
                throw new Exception("Non 200-OK response received for GET http request for url " + url + ". Response Code : " + responseCode + ". Error message : " + responseString);
            }
            if (responseBody != null && readAs == byte[].class) {
                byte[] bytes = responseBody.bytes();
                LOGGER.debug("http GET request successful", Map.of("url", url, "responseSize", bytes.length, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
                return (O) bytes;
            }
            responseString = responseBody == null ? null : responseBody.string();
            LOGGER.debug("http GET request successful", Map.of("url", url, "responseLength", responseString == null ? -1 : responseString.length(), "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            if (isBlank(responseString)) {
                return null;
            }
            if (readAs.isAssignableFrom(String.class)) {
                return (O) responseString;
            }
            return readJson(responseString, readAs);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http GET request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            throw new Exception("Error getting a valid response for http GET request to url " + url + ". responseCode : " + responseCode, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private static <O> O postData(final String subUrl, final Object data, final Class<O> readAs) throws Exception {
        return postData(subUrl, data, readAs, true);
    }

    @SuppressWarnings("unchecked")
    private static <O> O postData(final String subUrl, final Object data, final Class<O> readAs, final boolean retry) throws Exception {
        String url = BASE_URL + subUrl;
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            LOGGER.debug("Making remote http POST request to url {}", url);
            String json = jsonString(data);
            RequestBody body = RequestBody.create(json, MEDIA_TYPE_APPLICATION_JSON);
            Request request = builder(url).post(body).build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                if (responseCode == 401 && retry) {
                    resetA3Token();
                    return postData(subUrl, data, readAs, false);
                }
                throw new Exception("Non 200-OK response received for POST http request for url " + url + ". Response Code : " + responseCode + ". Error message : " + responseString);
            }
            LOGGER.debug("http POST request successful", Map.of("url", url, "response", dropSensitiveKeys(responseString), "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            if (isBlank(responseString)) {
                return null;
            }
            if (readAs.isAssignableFrom(String.class)) {
                return (O) responseString;
            }
            return readJson(responseString, readAs);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http POST request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            throw new Exception("Error getting a valid response for http POST request to url " + url + ". responseCode : " + responseCode, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private static <O> O postMultiPartData(final String subUrl, final Object metadata, final String fileName, final byte[] resource, final Class<O> readAs) throws Exception {
        return postMultiPartData(subUrl, metadata, fileName, resource, readAs, true);
    }

    @SuppressWarnings("unchecked")
    private static <O> O postMultiPartData(final String subUrl, final Object metadata, final String fileName, final byte[] resource, final Class<O> readAs, final boolean retry) throws Exception {
        String url = BASE_URL + subUrl;
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            LOGGER.debug("Making remote http POST request to url {}", url);
            String json = jsonString(metadata);
            InputStream inputStream = new ByteArrayInputStream(resource);
            RequestBody streamBody = RequestBodyUtil.create(MEDIA_TYPE_MARKDOWN, inputStream);
            RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM).addFormDataPart("metadata", json).addFormDataPart("file", fileName, streamBody).build();
            Request request = builder(url).post(body).build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                if (responseCode == 401 && retry) {
                    resetA3Token();
                    return postData(subUrl, metadata, readAs, false);
                }
                throw new Exception("Non 200-OK response received for POST http request for url " + url + ". Response Code : " + responseCode + ". Error message : " + responseString);
            }
            LOGGER.debug("http POST request successful", Map.of("url", url, "response", dropSensitiveKeys(responseString), "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            if (isBlank(responseString)) {
                return null;
            }
            if (readAs.isAssignableFrom(String.class)) {
                return (O) responseString;
            }
            return readJson(responseString, readAs);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http POST request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            throw new Exception("Error getting a valid response for http POST request to url " + url + ". responseCode : " + responseCode, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private static <O> O patchData(final String subUrl, final Object data, final Class<O> readAs) throws Exception {
        return patchData(subUrl, data, readAs, true);
    }

    @SuppressWarnings("unchecked")
    private static <O> O patchData(final String subUrl, final Object data, final Class<O> readAs, final boolean retry) throws Exception {
        String url = BASE_URL + subUrl;
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            LOGGER.debug("Making remote http PATCH request to url {}", url);
            String json = jsonString(data);
            RequestBody body = RequestBody.create(json, MEDIA_TYPE_APPLICATION_JSON);
            Request request = builder(url).patch(body).build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                if (responseCode == 401 && retry) {
                    resetA3Token();
                    return patchData(subUrl, data, readAs, false);
                }
                throw new Exception("Non 200-OK response received for PATCH http request for url " + url + ". Response Code : " + responseCode + ". Error message : " + responseString);
            }
            LOGGER.debug("http PATCH request successful", Map.of("url", url, "response", dropSensitiveKeys(responseString), "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            if (isBlank(responseString)) {
                return null;
            }
            if (readAs.isAssignableFrom(String.class)) {
                return (O) responseString;
            }
            return readJson(responseString, readAs);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http PATCH request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            throw new Exception("Error getting a valid response for http PATCH request to url " + url + ". responseCode : " + responseCode, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private static <O> O deleteData(final String subUrl, final Class<O> readAs) throws Exception {
        return deleteData(subUrl, readAs, true);
    }

    @SuppressWarnings("unchecked")
    private static <O> O deleteData(final String subUrl, final Class<O> readAs, final boolean retry) throws Exception {
        String url = BASE_URL + subUrl;
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            LOGGER.debug("Making remote http DELETE request to url {}", url);
            Request request = builder(url).delete().build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                if (responseCode == 404) {
                    throw new ResourceNotFoundException("404 - Not Found response received for DELETE http request for url " + url + ". Response Code : " + responseCode + ". Error message : " + responseString, Map.of("resourceUrl", url));
                }
                if (responseCode == 401 && retry) {
                    resetA3Token();
                    return deleteData(subUrl, readAs, false);
                }
                throw new Exception("Non 200-OK response received for DELETE http request for url " + url + ". Response Code : " + responseCode + ". Error message : " + responseString);
            }
            LOGGER.debug("http DELETE request successful", Map.of("url", url, "responseLength", responseString == null ? -1 : responseString.length(), "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            if (isBlank(responseString)) {
                return null;
            }
            if (readAs.isAssignableFrom(String.class)) {
                return (O) responseString;
            }
            return readJson(responseString, readAs);
        } catch (ResourceNotFoundException e) {
            throw new ResourceNotFoundException("Error getting a 404 response for http DELETE request to url " + url + ". responseCode : " + responseCode, e);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http DELETE request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            throw new Exception("Error getting a valid response for http DELETE request to url " + url + ". responseCode : " + responseCode, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private static Map activePropertySource(final String application, final String module, final String profile, final String versionLabel) throws Exception {
        Map data = Map.of("activated", true, "version", versionLabel.trim().substring(7));
        String subUrl = "/propertySources/" + application + "-" + module + "/" + profile.toLowerCase() + "/" + versionLabel;
        return patchData(subUrl, data, Map.class);
    }

    private static Map activeResources(final String application, final String module, final String profile, final String versionLabel) throws Exception {
        Map data = Map.of("activated", true, "version", versionLabel.trim().substring(7));
        String subUrl = "/resources/" + application + "-" + module + "/" + profile.toLowerCase() + "/" + versionLabel;
        return patchData(subUrl, data, Map.class);
    }

    @SuppressWarnings("unchecked")
    private static PropertySource getPropertySource(final String application, final String module, final String profile) throws Exception {
        Map<String, Object> labelsMap = getData("/applications/" + application + "/modules/" + module + "/profiles/" + profile + "/labels", Map.class);
        if (labelsMap == null || labelsMap.isEmpty()) {
            return null;
        }
        String currentVersion = (String) labelsMap.get(CURRENT_VERSION);
        if (currentVersion == null || currentVersion.isBlank()) {
            currentVersion = (String) labelsMap.get(ACTIVE_VERSION);
        }
        if (currentVersion == null || currentVersion.isBlank()) {
            try {
                currentVersion = (String) ((List) labelsMap.get("versions")).get(0);
            } catch (Exception e) {
                throw new Exception("Could not find an active version");
            }
        }
        PropertySource response = getData("/propertySources/" + application + "-" + module + "/" + profile + "/" + currentVersion, PropertySource.class);
        return response;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<KeyValuePair> getConfigProperties(final String application, final String module, final String profile) throws Exception {
        PropertySource response = getPropertySource(application, module, profile);
        if (response == null) {
            return null;
        }
        return response.getProperties();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <O> O getConfigResource(final String application, final String module, final String profile, final String resourceName, final Class<O> readAs) throws Exception {
        Map<String, Object> labelsMap = getData("/resources/" + application + "-" + module + "/" + profile + "/labels", Map.class);
        if (labelsMap == null || labelsMap.isEmpty()) {
            return null;
        }
        String currentVersion = (String) labelsMap.get(CURRENT_VERSION);
        if (currentVersion == null || currentVersion.isBlank()) {
            currentVersion = (String) labelsMap.get(ACTIVE_VERSION);
        }
        if (currentVersion == null || currentVersion.isBlank()) {
            try {
                currentVersion = (String) ((List) labelsMap.get("versions")).get(0);
            } catch (Exception e) {
                throw new Exception("Could not find an active version");
            }
        }
        return getData("/resources/" + application + "-" + module + "/" + profile + "/" + currentVersion + "/" + resourceName + "/download?version=" + currentVersion, readAs);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map getResourceMetadata(final String application, final String module, final String profile) throws Exception {
        Map<String, Object> labelsMap = getData("/resources/" + application + "-" + module + "/" + profile + "/labels", Map.class);
        if (labelsMap == null || labelsMap.isEmpty()) {
            return null;
        }
        String currentVersion = (String) labelsMap.get(CURRENT_VERSION);
        if (currentVersion == null || currentVersion.isBlank()) {
            currentVersion = (String) labelsMap.get(ACTIVE_VERSION);
        }
        if (currentVersion == null || currentVersion.isBlank()) {
            try {
                currentVersion = (String) ((List) labelsMap.get("versions")).get(0);
            } catch (Exception e) {
                throw new Exception("Could not find an active version");
            }
        }
        Map response = getData("/resources/" + application + "-" + module + "/" + profile + "/" + currentVersion + "/" + "metadata", Map.class);
        return response;
    }

    @Override
    public Map uploadResource(final String application, final String module, final String profile, final String targetFileName, final Object resource) throws Exception {
        Map response = null;
        Map metadata = Map.of("application", application + "-" + module, "profile", profile);
        if (resource instanceof String) {
            response = postMultiPartData("/resources", metadata, targetFileName, resource.toString().getBytes(UTF_8), Map.class);
        }
        if (resource instanceof byte[]) {
            response = postMultiPartData("/resources", metadata, targetFileName, (byte[]) resource, Map.class);
        }
        if (resource instanceof ByteBuffer) {
            response = postMultiPartData("/resources", metadata, targetFileName, ((ByteBuffer) resource).array(), Map.class);
        }
        if (response == null || response.isEmpty()) {
            LOGGER.debug("Failed to upload resource, because the resource type is not valid");
            return null;
        }
        if (response.containsKey("activated") && (boolean) response.get("activated")) {
            return response;
        }
        String version = null;
        if (response.containsKey("label")) {
            version = (String) response.get("label");

        } else if (response.containsKey("version")) {
            version = (String) response.get("version");
        }
        if (!isBlank(version)) {
            activeResources(application, module, profile, version);
        }
        return response;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map mergeResource(final String application, final String module, final String profile, final String targetFileName, final Object resource) throws Exception {
        Map response = null;
        Map<String, Object> metadata = getResourceMetadata(application, module, profile);
        if (metadata == null || metadata.isEmpty()) {
            return uploadResource(application, module, profile, targetFileName, resource);
        }
        if (metadata.containsKey("files")) {
            for (int i = 0; i < ((List<Map>) metadata.get("files")).size(); i++) {
                Map file = ((List<Map>) metadata.get("files")).get(i);
                if (file != null && !file.isEmpty() && file.containsKey("name")) {
                    if (file.get("name").equals(targetFileName)) {
                        ((List<?>) metadata.get("files")).remove(i);
                        break;
                    }
                }
            }
        }
        if (resource instanceof String) {
            LOGGER.debug("Resource type is String, resource: {}, fileName: {}", resource, targetFileName);
            String lastResource = getConfigResource(appName(), module, profile, targetFileName, String.class);
            if (lastResource != null && lastResource.equals(resource)) {
                LOGGER.debug("Resource exists, so skipped to upload resource");
                return null;
            }
            response = postMultiPartData("/resources", metadata, targetFileName, resource.toString().getBytes(UTF_8), Map.class);
        }
        if (resource instanceof byte[]) {
            LOGGER.debug("Resource type is Byte Array, resource: {}, fileName: {}", resource, targetFileName);
            byte[] lastResource = getConfigResource(appName(), module, profile, targetFileName, byte[].class);
            if (Arrays.equals(lastResource, (byte[]) resource)) {
                LOGGER.debug("Resource exists, so skipped to upload resource");
                return null;
            }
            response = postMultiPartData("/resources", metadata, targetFileName, (byte[]) resource, Map.class);
        }
        if (resource instanceof ByteBuffer) {
            LOGGER.debug("Resource type is Byte Buffer, resource: {}, fileName: {}", resource, targetFileName);
            byte[] lastResource = getConfigResource(appName(), module, profile, targetFileName, byte[].class);
            byte[] resourceBytes = ((ByteBuffer) resource).array();
            if (Arrays.equals(lastResource, resourceBytes)) {
                LOGGER.debug("Resource exists, so skipped to upload resource");
                return null;
            }
            response = postMultiPartData("/resources", metadata, targetFileName, resourceBytes, Map.class);
        }
        if (response == null || response.isEmpty()) {
            LOGGER.debug("Failed to upload resource, because the resource type is not valid");
            return null;
        }
        if (response.containsKey("activated") && (boolean) response.get("activated")) {
            return response;
        }
        String version = null;
        if (response.containsKey("label")) {
            version = (String) response.get("label");

        } else if (response.containsKey("version")) {
            version = (String) response.get("version");
        }
        if (!isBlank(version)) {
            activeResources(application, module, profile, version);
        }
        return response;
    }

    @Override
    public Map removeConfigSet(final String application, final String module, final String profile) throws Exception {
        return deleteData("/applications/" + application + "/modules/" + module + "/profiles/" + profile, Map.class);
    }

    @Override
    public Map getConfigSet(final String application, final String module, final String profile) throws Exception {
        Map response = getData("/applications/" + application + "/modules/" + module + "/profiles/" + profile, Map.class);
        if (response == null || response.isEmpty() || !profile.equals(response.get("name"))) {
            return null;
        }
        return response;
    }

    @Override
    public Map createConfigSet(final String application, final String module, final String profile, final String readGroupId, final String writeGroupId) throws Exception {
        Map request = Map.of("name", profile, "moduleName", module, "appName", application, "description", module + "-" + profile, "writeGroup", writeGroupId, "readOnlyGroup", readGroupId);
        return postData("/applications/" + application + "/modules/" + module + "/profiles", request, Map.class);
    }

    @Override
    public Map<String, Object> getConfigPropertiesMap(final String application, final String module, final String profile, final boolean asIs) throws Exception {
        PropertySource response = null;
        if (config().getBoolean("ccclient.localmode.enabled")) {
            String envValue = env(String.format("APP_CC_%s_%s_%s", application, module, profile).replace('-', '_').toUpperCase(), null);
            if (!isBlank(envValue)) {
                Properties props = new Properties();
                props.load(new StringReader(new String(getDecoder().decode(envValue), UTF_8)));
                response = PropertySource.from(props);
            }
        }
        if (response == null) {
            response = getPropertySource(application, module, profile);
        }
        if (response == null) {
            return null;
        }
        return response.getMap(asIs);
    }

    @Override
    public Map createConfigProperties(final String application, final String module, final String profile, final Map<String, Object> properties) throws Exception {
        Map request = Map.of("application", application + "-" + module, "profile", profile, "activated", true, "properties", PropertySource.from(properties).getProperties());
        Map response = postData("/propertySources", request, Map.class);
        if (response == null || response.isEmpty()) {
            return null;
        }
        if (response.containsKey("activated") && (boolean) response.get("activated")) {
            return response;
        }
        String version = null;
        if (response.containsKey("label")) {
            version = (String) response.get("label");

        } else if (response.containsKey("version")) {
            version = (String) response.get("version");
        }
        if (!isBlank(version)) {
            activePropertySource(application, module, profile, version);
        }
        return response;
    }

    @Data
    private static final class PropertySource implements Serializable {
        private static final long serialVersionUID = 1L;
        private Map<String, Object> auditInfo;
        private List<KeyValuePair> properties;

        @SuppressWarnings("unchecked")
        public static PropertySource from(final Map properties) {
            if (properties == null || properties.isEmpty()) {
                return null;
            }
            PropertySource source = new PropertySource();
            source.properties = new ArrayList<>(properties.size());
            for (Map.Entry entry : ((Map<Object, Object>) properties).entrySet()) {
                if (entry.getKey() == null || entry.getKey().toString().isBlank()) {
                    continue;
                }
                String key = entry.getKey().toString();
                Object value = entry.getValue();
                if (value == null) {
                    continue;
                }
                KeyValuePair pair = new KeyValuePair();
                pair.setKey(key);
                if (value instanceof String) {
                    pair.setValue((String) value);
                } else if (value.getClass().isPrimitive()) {
                    pair.setValue(String.valueOf(value));
                } else {
                    try {
                        pair.setValue(jsonString(value));
                    } catch (Exception e) {
                        pair.setValue(String.valueOf(value));
                    }
                }
                source.properties.add(pair);
            }
            return source;
        }

        @SuppressWarnings("unchecked")
        public Map<String, Object> getMap(final boolean asIs) throws JsonProcessingException {
            if (properties == null) {
                return null;
            }
            if (!asIs) {
                return propertiesToMap(this.properties);
            }
            Map<String, Object> map = new TreeMap<>();
            for (KeyValuePair entry : properties) {
                Object value = entry.value();
                if (value == null) {
                    continue;
                }
                map.put(entry.getKey(), value);
            }
            return map;
        }
    }

    private static class RequestBodyUtil {
        public static RequestBody create(final MediaType mediaType, final InputStream inputStream) {
            return new RequestBody() {
                @Override
                public long contentLength() {
                    try {
                        return inputStream.available();
                    } catch (IOException e) {
                        return 0;
                    }
                }

                @Override
                public MediaType contentType() {
                    return mediaType;
                }

                @Override
                public void writeTo(final BufferedSink sink) throws IOException {
                    Source source = null;
                    try {
                        source = Okio.source(inputStream);
                        sink.writeAll(source);
                    } finally {
                        Util.closeQuietly(source);
                    }
                }
            };
        }
    }
}
