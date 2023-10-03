package com.apple.aml.stargate.common.web.clients;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON_VALUE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_TEXT_JSON;
import static com.apple.aml.stargate.common.utils.JsonUtils.dropSensitiveKeys;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public final class ACIKafkaRestClient {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient();

    private ACIKafkaRestClient() {

    }

    public static Map activityDetails(final String apiUri, final String apiToken, final String activityId) throws Exception {
        return getData(apiUri, apiToken, "/activities/" + activityId);
    }

    private static Map getData(final String apiUri, final String apiToken, final String subUrl) throws Exception {
        String url = subUrl;
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            url = apiUri + subUrl;
            LOGGER.debug("ACIKafkaRestClient : Making remote http GET request to url {}", url);
            Request request = builder(apiToken, url).get().build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                if (responseCode == 404) {
                    return null;
                }
                throw new Exception("ACIKafkaRestClient : Invalid GET request for url " + url + "!! Response code : " + responseCode + ". Response Message : " + responseString);
            }
            LOGGER.debug("http GET request successful", Map.of("url", url, "response", dropSensitiveKeys(responseString), "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            if (responseString == null || responseString.isEmpty()) {
                return null;
            }
            return readJson(responseString, Map.class);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http GET request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            throw new Exception("Error getting a valid response for http GET request to url " + url + ". Reason : " + e.getMessage(), e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private static Builder builder(final String apiToken, final String url) {
        return new Builder().url(HttpUrl.get(url)).addHeader("Accept", MEDIA_TYPE_APPLICATION_JSON_VALUE).addHeader("X-API-TOKEN", apiToken).addHeader("X-API-TOKEN-VERSION", "2");
    }

    public static Map groupDetails(final String apiUri, final String apiToken, final String group) throws Exception {
        return getData(apiUri, apiToken, "/groups/" + group);
    }

    public static Map namespaceDetails(final String apiUri, final String apiToken, final String group, final String namespace) throws Exception {
        return getData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace);
    }

    public static Map topicDetails(final String apiUri, final String apiToken, final String group, final String namespace, final String topic) throws Exception {
        return getData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic);
    }

    @SuppressWarnings("unchecked")
    public static Integer topicPartitionCount(final String apiUri, final String apiToken, final String group, final String namespace, final String topic) throws Exception {
        try {
            return (Integer) ((Map) getData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic).get("entity")).getOrDefault("partition-count", -1);
        } catch (Exception e) {
            return -1;
        }
    }

    public static Map topicAccessDetails(final String apiUri, final String apiToken, final String group, final String namespace, final String topic) throws Exception {
        return getData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic + "/accesses?expanded");
    }

    public static Map clientDetails(final String apiUri, final String apiToken, final String group, final String clientId) throws Exception {
        return getData(apiUri, apiToken, "/groups/" + group + "/identities/" + clientId);
    }

    public static Map clientAccess(final String apiUri, final String apiToken, final String group, final String namespace, final String topic, final String clientId) throws Exception {
        return getData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic + "/accesses/groups/" + group + "/identities/" + clientId);
    }

    public static Map createTopic(final String apiUri, final String apiToken, final String group, final String namespace, final String topic, final Map topicProperties) throws Exception {
        return postData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics", topicProperties);
    }

    private static Map postData(final String apiUri, final String apiToken, final String subUrl, final Map data) throws Exception {
        String url = subUrl;
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            url = apiUri + subUrl;
            LOGGER.debug("ACIKafkaRestClient : Making remote http POST request to url {}", url);
            String json = jsonString(data);
            RequestBody body = RequestBody.create(json, MEDIA_TYPE_TEXT_JSON);
            Request request = builder(apiToken, url).post(body).build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                throw new Exception("ACIKafkaRestClient : Non 200-OK response received for POST http request for url " + url + ". Response code : " + responseCode + ". Response Message : " + responseString);
            }
            LOGGER.debug("http POST request successful", Map.of("url", url, "response", dropSensitiveKeys(responseString), "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            if (responseString == null || responseString.isEmpty()) {
                return null;
            }
            return readJson(responseString, Map.class);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http POST request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            throw new Exception("Error getting a valid response for http POST request to url " + url + ". Reason : " + e.getMessage(), e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public static Map deleteTopic(final String apiUri, final String apiToken, final String group, final String namespace, final String topic) throws Exception {
        return deleteData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic);
    }

    private static Map deleteData(final String apiUri, final String apiToken, final String subUrl) throws Exception {
        String url = subUrl;
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            url = apiUri + subUrl;
            LOGGER.debug("ACIKafkaRestClient : Making remote http DELETE request to url {}", url);
            Request request = builder(apiToken, url).delete().build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                if (responseCode == 404) {
                    return null;
                }
                throw new Exception("ACIKafkaRestClient : Invalid DELETE request for url " + url + "!! Response code : " + responseCode + ". Response Message : " + responseString);
            }
            LOGGER.debug("http DELETE request to url {} successful. Response : {}. Time taken : {}", url, responseString, (System.nanoTime() - startTime) / 1000000.0);
            if (responseString == null || responseString.isEmpty()) {
                return null;
            }
            return readJson(responseString, Map.class);
        } catch (Exception e) {
            LOGGER.warn("Error getting a valid response for http DELETE request to url", Map.of("url", url, "responseCode", responseCode, "responseString", String.valueOf(responseString)), e);
            throw new Exception("Error getting a valid response for http DELETE request to url " + url + ". Reason : " + e.getMessage(), e);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public static Map createClient(final String apiUri, final String apiToken, final String group, final String clientId, final String publicKey) throws Exception {
        Map requestBody = Map.of("name", clientId, "entity", Map.of("public-key", publicKey));
        return postData(apiUri, apiToken, "/groups/" + group + "/identities", requestBody);
    }

    public static Map createProducerAccess(final String apiUri, final String apiToken, final String group, final String namespace, final String topic, final String clientId, final String producerQuota) throws Exception {
        Map requestBody = Map.of("identity-id", Map.of("group", group, "identity", clientId), "entity", Map.of("produce", Map.of("bytes-per-sec", producerQuota)));
        return postData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic + "/accesses", requestBody);
    }

    public static Map createProducerAndConsumerAccess(final String apiUri, final String apiToken, final String group, final String namespace, final String topic, final String clientId, final String producerQuota, final String consumerQuota) throws Exception {
        Map requestBody = Map.of("identity-id", Map.of("group", group, "identity", clientId), "entity", Map.of("produce", Map.of("bytes-per-sec", producerQuota), "consume", Map.of("bytes-per-sec", consumerQuota)));
        return postData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic + "/accesses", requestBody);
    }

    @SuppressWarnings("unchecked")
    public static Map updateProducerAccess(final String apiUri, final String apiToken, final String group, final String namespace, final String topic, final String clientId, final Map existingStatus, final String producerQuota) throws Exception {
        ((Map) existingStatus.get("entity")).put("produce", Map.of("bytes-per-sec", producerQuota));
        return postData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic + "/accesses", existingStatus);
    }

    @SuppressWarnings("unchecked")
    public static Map updateConsumerAccess(final String apiUri, final String apiToken, final String group, final String namespace, final String topic, final String clientId, final Map existingStatus, final String consumerQuota) throws Exception {
        ((Map) existingStatus.get("entity")).put("consume", Map.of("bytes-per-sec", consumerQuota));
        return postData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic + "/accesses", existingStatus);
    }

    public static Map createConsumerAccess(final String apiUri, final String apiToken, final String group, final String namespace, final String topic, final String clientId, final String consumerQuota) throws Exception {
        Map requestBody = Map.of("identity-id", Map.of("group", group, "identity", clientId), "entity", Map.of("consume", Map.of("bytes-per-sec", consumerQuota)));
        return postData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic + "/accesses", requestBody);
    }

    public static Map deleteAccess(final String apiUri, final String apiToken, final String group, final String namespace, final String topic, final String identityGroupId, final String identity) throws Exception {
        return deleteData(apiUri, apiToken, "/groups/" + group + "/namespaces/" + namespace + "/topics/" + topic + "/accesses/groups/" + identityGroupId + "/identities/" + identity);
    }

    public static Map deleteIdentity(final String apiUri, final String apiToken, final String group, final String identity) throws Exception {
        return deleteData(apiUri, apiToken, "/groups/" + group + "/identities/" + identity);
    }

}
