package com.apple.aml.stargate.common.web.clients;

import com.apple.aml.stargate.common.constants.CommonConstants;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.API_PREFIX;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_APP_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_PAYLOAD_KEY;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_PUBLISHER_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_PUBLISH_FORMAT;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_SCHEMA_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_TOKEN;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.WebUtils.getHttpClient;

public final class PipelineConfigClient {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final OkHttpClient HTTP_CLIENT = getHttpClient();

    @SuppressWarnings("unchecked")
    public static Map saveState(final String pipelineId, final String pipelineToken, final String stateId, final Map<String, Object> payload) throws Exception {
        return postData(environment().getConfig().getStargateUri() + API_PREFIX + "/pipeline", "/state/save/" + pipelineId + "/" + stateId, pipelineToken, payload, Map.class);
    }

    @SuppressWarnings("unchecked")
    private static <K, I> K postData(final String rootPath, final String path, final String token, final I data, final Class<K> returnType) throws Exception {
        return postData(rootPath, path, sanitizedToken(token), data, returnType, null);
    }

    @SuppressWarnings("unchecked")
    private static <K, I> K postData(final String rootPath, final String path, final String token, final I data, final Class<K> returnType, final Map<String, String> headers) throws Exception {
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        String url = rootPath + path;
        try {
            LOGGER.debug("Making remote http POST request to url {}", url);
            String json = data instanceof String ? (String) data : jsonString(data);
            Request.Builder builder = new Request.Builder().url(url).post(RequestBody.create(json, MEDIA_TYPE_APPLICATION_JSON));
            if (token != null) {
                builder = builder.addHeader(HEADER_PIPELINE_TOKEN, token);
            }
            if (headers != null) {
                for (Map.Entry<String, String> header : headers.entrySet()) {
                    builder = builder.addHeader(header.getKey(), header.getValue());
                }
            }
            Request request = builder.build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            if (!response.isSuccessful()) {
                responseString = responseBody == null ? null : responseBody.string();
                throw new Exception("Non 200-OK response received for POST http request for url " + url + ". Response Code : " + responseCode + ". Error message : " + responseString);
            }
            if (responseBody == null) {
                LOGGER.debug("http POST request successful", Map.of("url", url, "responseSize", 0, "responseBody", "null", "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
                return null;
            }
            if (ByteBuffer.class.isAssignableFrom(returnType)) {
                byte[] bytes = responseBody.bytes();
                LOGGER.debug("http POST request successful", Map.of("url", url, "responseSize", bytes.length, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
                return (K) ByteBuffer.wrap(bytes);
            }
            responseString = responseBody == null ? null : responseBody.string();
            LOGGER.debug("http POST request successful", Map.of("url", url, "responseLength", responseString == null ? -1 : responseString.length(), "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            if (responseString == null || responseString.isEmpty()) {
                return null;
            }
            if (returnType.isAssignableFrom(String.class)) {
                return (K) responseString;
            }
            return readJson(responseString, returnType);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http POST request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            if (responseCode == 401) {
                throw new SecurityException("Unauthorized Request !!", e);
            }
            throw new Exception("Error getting a valid response for http POST request to url " + url + ". responseCode : " + responseCode, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private static String sanitizedToken(final String token) throws Exception {
        if (token == null || token.isBlank()) {
            throw new Exception("Cannot post to pipeline service. Empty token supplied!");
        }
        return token.replaceAll("\"", CommonConstants.EMPTY_STRING);
    }

    @SuppressWarnings("unchecked")
    public static List<Map> getState(final String pipelineId, final String pipelineToken, final String stateId) throws Exception {
        return getData(environment().getConfig().getStargateUri() + API_PREFIX + "/pipeline", "/state/get/" + pipelineId + "/" + stateId, pipelineToken, List.class);
    }

    @SuppressWarnings("unchecked")
    private static <K> K getData(final String rootPath, final String path, final String token, final Class<K> returnType) throws Exception {
        if (token == null || token.isBlank()) {
            throw new SecurityException("Cannot fetch pipeline details. Empty token supplied!");
        }
        String pipelineToken = token.replaceAll("\"", CommonConstants.EMPTY_STRING);
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        String url = rootPath + path;
        try {
            LOGGER.debug("Making remote http GET request to url {}", url);
            Request request = new Request.Builder().url(url).addHeader(HEADER_PIPELINE_TOKEN, pipelineToken).get().build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            if (!response.isSuccessful()) {
                responseString = responseBody == null ? null : responseBody.string();
                throw new Exception("Non 200-OK response received for GET http request for url " + url + ". Response Code : " + responseCode + ". Error message : " + responseString);
            }
            if (responseBody == null) {
                LOGGER.debug("http GET request successful", Map.of("url", url, "responseSize", 0, "responseBody", "null", "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
                return null;
            }
            if (ByteBuffer.class.isAssignableFrom(returnType)) {
                byte[] bytes = responseBody.bytes();
                LOGGER.debug("http GET request successful", Map.of("url", url, "responseSize", bytes.length, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
                return (K) ByteBuffer.wrap(bytes);
            }
            responseString = responseBody == null ? null : responseBody.string();
            LOGGER.debug("http GET request successful", Map.of("url", url, "responseLength", responseString == null ? -1 : responseString.length(), "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            if (responseString == null || responseString.isEmpty()) {
                return null;
            }
            if (returnType.isAssignableFrom(String.class)) {
                return (K) responseString;
            }
            return readJson(responseString, returnType);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http GET request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            if (responseCode == 401) {
                throw new SecurityException("Unauthorized Request !!", e);
            }
            throw new Exception("Error getting a valid response for http GET request to url " + url + ". responseCode : " + responseCode, e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static Map ingestJson(final String pipelineId, final String pipelineToken, final Long appId, final String publisherId, final String schemaId, final String payload) throws Exception {
        return postData("/ingest/json/" + pipelineId, pipelineToken, payload, Map.class, Map.of(HEADER_PIPELINE_APP_ID, appId + "", HEADER_PIPELINE_PUBLISHER_ID, publisherId, HEADER_PIPELINE_SCHEMA_ID, schemaId));
    }

    @SuppressWarnings("unchecked")
    private static <K, I> K postData(final String path, final String token, final I data, final Class<K> returnType, Map<String, String> headers) throws Exception {
        return postData(environment().getConfig().getDhariUri() + "/sg", path, sanitizedToken(token), data, returnType, headers);
    }

    @SuppressWarnings("unchecked")
    public static Map ingestJson(final String pipelineId, final String pipelineToken, final Long appId, final String publisherId, final String schemaId, final String payloadKey, final String payload, final String publishFormat) throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_PIPELINE_APP_ID, appId + "");
        headers.put(HEADER_PIPELINE_PUBLISHER_ID, publisherId);
        headers.put(HEADER_PIPELINE_SCHEMA_ID, schemaId);
        if (payloadKey != null) {
            headers.put(HEADER_PIPELINE_PAYLOAD_KEY, payloadKey);
        }
        if (publishFormat != null) {
            headers.put(HEADER_PIPELINE_PUBLISH_FORMAT, publishFormat);
        }
        return postData("/ingest/json/" + pipelineId, pipelineToken, payload, Map.class, headers);
    }
}
