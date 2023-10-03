package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.GenericException;
import com.apple.aml.stargate.common.options.HttpConnectionOptions;
import com.apple.aml.stargate.common.options.HttpOptions;
import com.apple.aml.stargate.common.options.RetryOptions;
import com.apple.aml.stargate.common.pojo.RestResponse;
import com.apple.aml.stargate.common.pojo.SSLOptions;
import io.github.resilience4j.retry.Retry;
import lombok.SneakyThrows;
import okhttp3.ConnectionPool;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.http.ssl.SSLContexts;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_A3_TOKEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_CLIENT_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_TOKEN_TYPE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_APPLICATION_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_BINARY;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.UNKNOWN;
import static com.apple.aml.stargate.common.utils.A3Utils.getA3Token;
import static com.apple.aml.stargate.common.utils.A3Utils.getCachedToken;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.JsonUtils.dropSensitiveKeys;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJson;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SneakyRetryFunction.applyWithRetry;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

public final class WebUtils {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient();
    private static final TrustManager TRUST_ALL_CERTS_MANAGER = new X509TrustManager() {
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[]{};
        }
    };
    private static final OkHttpClient HTTP_CLIENT_DISABLE_CERT_VALIDATION = getHttpClient();

    private WebUtils() {
    }

    public static <O, I> O httpPost(final OkHttpClient client, final String url, final I data, Map<String, String> headers, final Class<O> returnType, final boolean throwOnError) throws Exception {
        return httpPost(client, url, data, null, headers, returnType, throwOnError, true);
    }

    @SuppressWarnings("unchecked")
    public static <O, I> O httpPost(final OkHttpClient client, final String url, final I data, final MediaType mediaType, Map<String, String> headers, final Class<O> returnType, final boolean throwOnError, final boolean log) throws Exception {
        Response response = null;
        byte[] responseBytes = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            if (log) LOGGER.debug("Making remote http POST", Map.of("url", url));
            byte[] bodyContent = data == null ? EMPTY_STRING.getBytes(UTF_8) : data instanceof byte[] ? (byte[]) data : (data instanceof String ? (String) data : jsonString(data)).getBytes(UTF_8);
            MediaType mType = mediaType == null ? MEDIA_TYPE_APPLICATION_JSON : mediaType;
            RequestBody body = RequestBody.create(bodyContent, mType);
            Request request = requestBuilder(url, headers).post(body).build();
            response = (client == null ? HTTP_CLIENT : client).newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseBytes = responseBody == null ? null : responseBody.bytes();
            if (response.isSuccessful()) {
                if (log) LOGGER.debug("http POST request successful", Map.of("url", url, "responseCode", responseCode, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            } else {
                throw new Exception("Non 200-OK response received for POST http request for url " + url + ". Response Code : " + responseCode);
            }
            if (responseBytes == null || responseBytes.length == 0) {
                return null;
            }
            String responseString;
            if (returnType == null) {
                responseString = (new String(responseBytes)).trim();
                if (responseString.startsWith("{")) {
                    return (O) readJson(responseString, HashMap.class);
                }
                return (O) readJson(responseString, ArrayList.class);
            }
            if (ByteBuffer.class.isAssignableFrom(returnType)) {
                return (O) ByteBuffer.wrap(responseBytes);
            }
            responseString = new String(responseBytes);
            if (String.class.isAssignableFrom(returnType)) {
                return (O) responseString;
            }
            return readJson(responseString, returnType);
        } catch (Exception e) {
            String responseString = responseBytes == null || responseBytes.length == 0 ? EMPTY_STRING : new String(responseBytes);
            if (log) LOGGER.warn("Error getting a valid response for http POST request to url", Map.of("url", url, "responseCode", responseCode, "responseString", responseString, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)), e);
            if (throwOnError) {
                if (responseCode == 401) {
                    throw new SecurityException("Error getting a valid response for http POST request to url " + url + ". responseCode : " + responseCode, e);
                } else {
                    throw new Exception("Error getting a valid response for http POST request to url " + url + ". responseCode : " + responseCode, e);
                }
            } else {
                return null;
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private static Request.Builder requestBuilder(final String url, Map<String, String> headers) {
        Request.Builder requestBuilder = new Request.Builder().url(HttpUrl.get(url));
        if (headers != null && !headers.isEmpty()) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                requestBuilder = requestBuilder.addHeader(entry.getKey(), entry.getValue());
            }
        }
        return requestBuilder;
    }

    public static <O, I> O postJsonData(final String url, final I data, final long appId, final String a3Token, final String a3Mode, final Class<O> returnType) throws Exception {
        return postJsonData(url, data, appId, a3Token, a3Mode, returnType, true);
    }

    public static <O, I> O postJsonData(final String url, final I data, final long appId, final String a3Token, final String a3Mode, final Class<O> returnType, final boolean certValidation) throws Exception {
        return httpPost(url, data, null, Map.of(HEADER_CLIENT_APP_ID, String.valueOf(appId), HEADER_A3_TOKEN, a3Token, HEADER_TOKEN_TYPE, a3Mode), returnType, true, certValidation);
    }

    public static <O, I> O httpPost(final String url, final I data, final MediaType mediaType, Map<String, String> headers, final Class<O> returnType, final boolean throwOnError, final boolean certValidation) throws Exception {
        return httpPost(certValidation ? HTTP_CLIENT : HTTP_CLIENT_DISABLE_CERT_VALIDATION, url, data, mediaType, headers, returnType, throwOnError, true);
    }

    public static <O, I> O postJsonData(final String url, final I data, final Map<String, String> headers, final long appId, final String a3Token, final String a3Mode, final Class<O> returnType) throws Exception {
        return postJsonData(url, data, headers, appId, a3Token, a3Mode, returnType, true);
    }

    public static <O, I> O postJsonData(final String url, final I data, final Map<String, String> headers, final long appId, final String a3Token, final String a3Mode, final Class<O> returnType, final boolean certValidation) throws Exception {
        Map<String, String> modifiedHeaders = new HashMap<>();
        modifiedHeaders.put(HEADER_CLIENT_APP_ID, String.valueOf(appId));
        modifiedHeaders.put(HEADER_A3_TOKEN, a3Token);
        modifiedHeaders.put(HEADER_TOKEN_TYPE, a3Mode);
        if (headers != null && !headers.isEmpty()) {
            modifiedHeaders.putAll(headers);
        }
        return httpPost(url, data, null, modifiedHeaders, returnType, true, certValidation);
    }

    public static <O, I> O invokeRestApi(final String url, final I data, final Class<O> returnType) throws Exception {
        return httpPost(url, data, null, null, returnType, true);
    }

    public static <O, I> O httpPost(final String url, final I data, final MediaType mediaType, Map<String, String> headers, final Class<O> returnType, final boolean throwOnError) throws Exception {
        return httpPost(null, url, data, mediaType, headers, returnType, throwOnError, true);
    }

    public static <O> O postBinaryData(final String url, final byte[] data, final long appId, final String a3Token, final String a3Mode, final Class<O> returnType) throws Exception {
        return httpPost(url, data, MEDIA_TYPE_BINARY, Map.of(HEADER_CLIENT_APP_ID, String.valueOf(appId), HEADER_A3_TOKEN, a3Token, HEADER_TOKEN_TYPE, a3Mode), returnType, true);
    }

    public static <O, I> O postData(final String url, final I data, final MediaType mediaType, final Class<O> returnType) throws Exception {
        return httpPost(url, data, mediaType, null, returnType, true);
    }

    public static <O> O getJsonData(final String url, final long appId, final String a3Token, final String a3Mode, final Class<O> returnType) throws Exception {
        return getData(url, appId, a3Token, a3Mode, returnType);
    }

    public static <O> O getData(final String url, final long appId, final String a3Token, final String a3Mode, final Class<O> returnType) throws Exception {
        return httpGet(url, Map.of(HEADER_CLIENT_APP_ID, String.valueOf(appId), HEADER_A3_TOKEN, a3Token, HEADER_TOKEN_TYPE, a3Mode), returnType, true, true);
    }

    @SuppressWarnings("unchecked")
    public static <O> O httpGet(final String url, Map<String, String> headers, final Class<O> returnType, final boolean throwOnError, final boolean log) throws Exception {
        Response response = null;
        byte[] responseBytes = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            if (log) LOGGER.debug("Making remote http GET", Map.of("url", url));
            Request request = requestBuilder(url, headers).get().build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseBytes = responseBody == null ? null : responseBody.bytes();
            if (response.isSuccessful()) {
                if (log) LOGGER.debug("http GET request successful", Map.of("url", url, "responseCode", responseCode, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            } else {
                throw new Exception("Non 200-OK response received for GET http request for url " + url + ". Response Code : " + responseCode);
            }
            if (responseBytes == null || responseBytes.length == 0) {
                return null;
            }
            String responseString;
            if (returnType == null) {
                responseString = (new String(responseBytes)).trim();
                if (responseString.startsWith("{")) {
                    return (O) readJson(responseString, HashMap.class);
                }
                return (O) readJson(responseString, ArrayList.class);
            }
            if (ByteBuffer.class.isAssignableFrom(returnType)) {
                return (O) ByteBuffer.wrap(responseBytes);
            }
            responseString = new String(responseBytes);
            if (String.class.isAssignableFrom(returnType)) {
                return (O) responseString;
            }
            return readJson(responseString, returnType);
        } catch (Exception e) {
            String responseString = responseBytes == null || responseBytes.length == 0 ? EMPTY_STRING : new String(responseBytes);
            if (log) LOGGER.warn("Error getting a valid response for http GET request to url", Map.of("url", url, "responseCode", responseCode, "responseString", responseString, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)), e);
            if (throwOnError) {
                if (responseCode == 401) {
                    throw new SecurityException("Error getting a valid response for http GET request to url " + url + ". responseCode : " + responseCode, e);
                } else {
                    throw new Exception("Error getting a valid response for http GET request to url " + url + ". responseCode : " + responseCode, e);
                }
            } else {
                return null;
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public static <O> O getData(final String url, final Map<String, String> headers, final Class<O> returnType, final boolean throwOnError) throws Exception {
        return httpGet(url, headers, returnType, throwOnError, true);
    }

    public static <O> O deleteData(final String url, final long appId, final String a3Token, final String a3Mode, final Class<O> returnType) throws Exception {
        return httpDelete(url, Map.of(HEADER_CLIENT_APP_ID, String.valueOf(appId), HEADER_A3_TOKEN, a3Token, HEADER_TOKEN_TYPE, a3Mode), returnType, true);
    }

    @SuppressWarnings("unchecked")
    public static <O> O httpDelete(final String url, Map<String, String> headers, final Class<O> returnType, final boolean throwOnError) throws Exception {
        Response response = null;
        byte[] responseBytes = null;
        int responseCode = -1;
        long startTime = System.nanoTime();
        try {
            LOGGER.debug("Making remote http DELETE", Map.of("url", url));
            Request request = requestBuilder(url, headers).delete().build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseBytes = responseBody == null ? null : responseBody.bytes();
            if (response.isSuccessful()) {
                LOGGER.debug("http DELETE request successful", Map.of("url", url, "responseCode", responseCode, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
            } else {
                if (responseCode == 404) {
                    LOGGER.debug("404 - Not Found response received for DELETE http request for url " + url);
                    return null;
                }
                throw new Exception("Non 200-OK response received for DELETE http request for url " + url + ". Response Code : " + responseCode);
            }
            if (responseBytes == null || responseBytes.length == 0) {
                return null;
            }
            String responseString;
            if (returnType == null) {
                responseString = (new String(responseBytes)).trim();
                if (responseString.startsWith("{")) {
                    return (O) readJson(responseString, HashMap.class);
                }
                return (O) readJson(responseString, ArrayList.class);
            }
            if (ByteBuffer.class.isAssignableFrom(returnType)) {
                return (O) ByteBuffer.wrap(responseBytes);
            }
            responseString = new String(responseBytes);
            if (String.class.isAssignableFrom(returnType)) {
                return (O) responseString;
            }
            return readJson(responseString, returnType);
        } catch (Exception e) {
            String responseString = responseBytes == null || responseBytes.length == 0 ? EMPTY_STRING : new String(responseBytes);
            LOGGER.warn("Error getting a valid response for http DELETE request to url", Map.of("url", url, "responseCode", responseCode, "responseString", responseString, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)), e);
            if (throwOnError) {
                if (responseCode == 401) {
                    throw new SecurityException("Error getting a valid response for http DELETE request to url " + url + ". responseCode : " + responseCode, e);
                } else {
                    throw new Exception("Error getting a valid response for http DELETE request to url " + url + ". responseCode : " + responseCode, e);
                }
            } else {
                return null;
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public static OkHttpClient getHttpClient() {
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{TRUST_ALL_CERTS_MANAGER};
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            return new OkHttpClient.Builder().sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0]).hostnameVerifier((hostname, session) -> true).build();
        } catch (Exception e) {
            return new OkHttpClient();
        }
    }

    public static OkHttpClient createHttpClient(final Boolean useSSL, final SSLOptions sslOptions) throws KeyManagementException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException {
        OkHttpClient client;
        if (useSSL) {
            TrustManager[] trustAllCerts = new TrustManager[]{TRUST_ALL_CERTS_MANAGER};
            client = new OkHttpClient.Builder().sslSocketFactory(loadSSLContext(sslOptions).getSocketFactory(), (X509TrustManager) trustAllCerts[0]).build();
        } else {
            client = new OkHttpClient();
        }
        return client;
    }

    public static SSLContext loadSSLContext(final SSLOptions sslOptions) throws KeyManagementException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException {
        SSLContext sslcontext = SSLContexts.custom().loadKeyMaterial(new File(sslOptions.getKeystorePath()), sslOptions.getKeystorePassword().toCharArray(), sslOptions.getKeyPassword().toCharArray()).loadTrustMaterial(new File(sslOptions.getTruststorePath()), sslOptions.getTruststorePassword().toCharArray()).build();
        return sslcontext;
    }

    public static OkHttpClient newOkHttpClient(final HttpConnectionOptions options) throws Exception {
        return newOkHttpClient(options, null);
    }

    public static OkHttpClient newOkHttpClient(final HttpConnectionOptions options, final Supplier<Map<String, String>> dynamicHeaders) throws Exception {
        if (options == null) return new OkHttpClient();
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        SSLContext sslContext;
        TrustManager[] trustAllCerts = new TrustManager[]{TRUST_ALL_CERTS_MANAGER};
        if (options.getSsl() == null) {
            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        } else {
            sslContext = loadSSLContext(options.getSsl());
        }
        if (options.getHeaders() != null && !options.getHeaders().isEmpty()) {
            builder.addInterceptor(chain -> {
                final Request original = chain.request();
                Set<String> originalHeaders = original.headers().names();
                Request.Builder reqBuilder = original.newBuilder();
                options.getHeaders().forEach((k, v) -> {
                    if (!originalHeaders.contains(k)) reqBuilder.addHeader(k, v);
                });
                return chain.proceed(reqBuilder.build());
            });
        }
        if (dynamicHeaders != null) {
            builder.addInterceptor(chain -> {
                Map<String, String> headers = dynamicHeaders.get();
                if (headers == null || headers.isEmpty()) return chain.proceed(chain.request());
                final Request original = chain.request();
                Set<String> originalHeaders = original.headers().names();
                Request.Builder reqBuilder = original.newBuilder();
                headers.forEach((k, v) -> {
                    if (!originalHeaders.contains(k)) reqBuilder.addHeader(k, v);
                });
                return chain.proceed(reqBuilder.build());
            });
        }
        builder = builder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0]);
        if (options.isDisableHostnameVerifier()) builder = builder.hostnameVerifier((hostname, session) -> true);
        if (options.getCallTimeout() != null) builder = builder.callTimeout(options.getCallTimeout());
        if (options.getConnectTimeout() != null) builder = builder.connectTimeout(options.getConnectTimeout());
        if (options.getReadTimeout() != null) builder = builder.readTimeout(options.getReadTimeout());
        if (options.getWriteTimeout() != null) builder = builder.writeTimeout(options.getWriteTimeout());
        if (options.getPingInterval() != null) builder = builder.pingInterval(options.getPingInterval());
        if (options.getMaxIdleConnections() != null && options.getMaxIdleConnections() > 0) builder = builder.connectionPool(new ConnectionPool(options.getMaxIdleConnections(), options.getKeepAliveDuration().getSeconds(), TimeUnit.SECONDS));
        builder = builder.retryOnConnectionFailure(options.isRetryOnConnectionFailure());
        builder = builder.followRedirects(options.isFollowRedirects());
        builder = builder.followSslRedirects(options.isFollowSslRedirects());
        return builder.build();
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <O> O restInvoke(final OkHttpClient client, final HttpOptions httpOptions, final RetryOptions retryOptions, final Object payload, final Map<String, String> payloadHeaders, final Class<O> returnType, final boolean throwOnError, final Logger logger) {
        String url = httpOptions.url();
        Retry retry = retryOptions == null ? null : SneakyRetryFunction.retry(retryOptions, e -> {
            if (logger != null) logger.warn("http request failed!. Will retry accordingly", Map.of("url", url, "method", String.valueOf(httpOptions.getMethod()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            return true;
        });
        RestResponse response = fetchResponse(client == null ? HTTP_CLIENT : client, httpOptions, retry, url, payload, payloadHeaders, throwOnError, logger, null);
        return parseResponse(returnType, response);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <O> RestResponse fetchResponse(final OkHttpClient client, final HttpOptions httpOptions, final Retry retry, final String url, final Object payload, final Map<String, String> payloadHeaders, final boolean throwOnError, final Logger logger, final Map<String, Object> logMap) {
        Map<String, String> headers = new HashMap<>();
        httpOptions.getHeaders().forEach((k, v) -> headers.put(k, String.valueOf(v)));
        Set<String> authTypes = isBlank(httpOptions.getAuthTypes()) ? new HashSet<>() : new HashSet<>(asList(httpOptions.getAuthTypes().trim().toUpperCase().split(",")));
        if (authTypes.contains("A3")) {
            PipelineConstants.ENVIRONMENT environment = isBlank(httpOptions.getA3Mode()) ? AppConfig.environment() : PipelineConstants.ENVIRONMENT.environment(httpOptions.getA3Mode());
            long sourceAppId;
            String a3Token;
            String a3Mode;
            if (httpOptions.getA3AppId() <= 0 || isBlank(httpOptions.getA3Password())) {
                sourceAppId = appId();
                a3Mode = A3Utils.a3Mode();
                a3Token = isBlank(httpOptions.getA3ContextString()) ? getA3Token(httpOptions.getA3TargetAppId()) : getA3Token(httpOptions.getA3TargetAppId(), httpOptions.getA3ContextString(), httpOptions.getA3ContextVersion());
            } else {
                sourceAppId = httpOptions.getA3AppId();
                a3Token = getCachedToken(sourceAppId, httpOptions.getA3TargetAppId(), httpOptions.getA3Password(), httpOptions.getA3ContextString(), httpOptions.getA3ContextVersion(), environment);
                a3Mode = (environment == PipelineConstants.ENVIRONMENT.PROD ? A3Constants.A3_MODE.PROD : A3Constants.A3_MODE.UAT).name();
            }
            headers.put(httpOptions.a3AppIdHeader(), String.valueOf(sourceAppId));
            headers.put(httpOptions.a3TokenHeader(), a3Token);
            if (httpOptions.getA3TokenTypeHeader() != null) headers.put(httpOptions.a3TokenTypeHeader(), a3Mode);
        }
        if (payloadHeaders != null) headers.putAll(payloadHeaders);
        Headers reqHeaders = Headers.of(headers);
        Request request;
        if (httpOptions.getMethod() == null || "post".equalsIgnoreCase(httpOptions.getMethod())) {
            RequestBody body = _requestBody(httpOptions, payload);
            request = new Request.Builder().url(HttpUrl.get(url)).headers(reqHeaders).post(body).build();
        } else if ("get".equalsIgnoreCase(httpOptions.getMethod())) {
            request = new Request.Builder().url(HttpUrl.get(url)).headers(reqHeaders).get().build();
        } else if ("delete".equalsIgnoreCase(httpOptions.getMethod())) {
            request = new Request.Builder().url(HttpUrl.get(url)).headers(reqHeaders).delete().build();
        } else if ("put".equalsIgnoreCase(httpOptions.getMethod())) {
            RequestBody body = _requestBody(httpOptions, payload);
            request = new Request.Builder().url(HttpUrl.get(url)).headers(reqHeaders).put(body).build();
        } else if ("patch".equalsIgnoreCase(httpOptions.getMethod())) {
            RequestBody body = _requestBody(httpOptions, payload);
            request = new Request.Builder().url(HttpUrl.get(url)).headers(reqHeaders).patch(body).build();
        } else {
            request = new Request.Builder().url(HttpUrl.get(url)).headers(reqHeaders).get().build();
        }
        try {
            return retry == null ? _response(client, url, request, logger, logMap == null ? Map.of() : logMap) : applyWithRetry(retry, o -> _response(client, url, request, logger, logMap == null ? Map.of() : logMap)).apply(null);
        } catch (Exception e) {
            if (throwOnError) throw e;
            return null;
        }
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <O> O parseResponse(final Class<O> returnType, final RestResponse response) {
        byte[] responseBytes = response.getContent();
        if (responseBytes == null) return null;
        String responseString;
        if (returnType == null) {
            responseString = (new String(responseBytes)).trim();
            if (responseString.isBlank()) return (O) EMPTY_STRING;
            if (responseString.startsWith("{")) return (O) readJson(responseString, HashMap.class);
            if (responseString.startsWith("[")) return (O) readJson(responseString, ArrayList.class);
            return (O) responseString;
        }
        if (ByteBuffer.class.isAssignableFrom(returnType)) return (O) ByteBuffer.wrap(responseBytes);
        responseString = new String(responseBytes);
        if (String.class.isAssignableFrom(returnType)) return (O) responseString;
        return (O) readJson(responseString, returnType);
    }

    @NotNull
    private static RequestBody _requestBody(final HttpOptions httpOptions, final Object payload) throws Exception {
        byte[] bodyContent = payload == null ? EMPTY_STRING.getBytes(UTF_8) : payload instanceof byte[] ? (byte[]) payload : (payload instanceof String ? (String) payload : jsonString(payload)).getBytes(UTF_8);
        MediaType mType = isBlank(httpOptions.getContentType()) ? MEDIA_TYPE_APPLICATION_JSON : MediaType.get(httpOptions.getContentType());
        return RequestBody.create(bodyContent, mType);
    }

    @SneakyThrows
    private static RestResponse _response(final OkHttpClient client, final String url, final Request request, final Logger logger, final Map<String, Object> logMap) {
        Response response = null;
        try {
            long startTime = System.nanoTime();
            byte[] responseBytes;
            int responseCode;
            response = client.newCall(request).execute();
            responseCode = response.code();
            String responseMessage = response.message();
            if (responseMessage == null) responseMessage = UNKNOWN;
            ResponseBody responseBody = response.body();
            responseBytes = responseBody == null ? null : responseBody.bytes();
            final Map<String, String> headers = new HashMap<>();
            response.headers().iterator().forEachRemaining(p -> headers.put(p.getFirst(), p.getSecond()));
            RestResponse restResponse = RestResponse.builder().code(responseCode).message(responseMessage).content(responseBytes).headers(headers).build();
            if (response.isSuccessful()) {
                if (logger != null) logger.debug("http request successful", logMap, Map.of("url", url, "method", request.method(), "responseCode", responseCode, "responseMessage", responseMessage, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
                return restResponse;
            } else {
                String responseString = dropSensitiveKeys(responseBytes == null ? null : new String(responseBytes), true);
                if (logger != null) logger.warn("http request failed", logMap, Map.of("url", url, "method", request.method(), "responseCode", responseCode, "responseMessage", responseMessage, "responseString", responseString, "timeTaken", ((System.nanoTime() - startTime) / 1000000.0)));
                throw new GenericException(String.format("http request failed for url %s of method type %s. server response -  code  : %d; message: %s; body : %s" + responseCode, url, request.method(), responseCode, responseMessage, responseString), restResponse);
            }
        } finally {
            try {
                if (response != null) response.close();
            } catch (Exception ex) {
            }
        }
    }
}
