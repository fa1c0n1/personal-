package com.apple.aml.stargate.common.utils;

import com.apple.aml.stargate.common.pojo.IASToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MEDIA_TYPE_UTF_8;
import static com.apple.aml.stargate.common.utils.JsonUtils.dropSensitiveKeys;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class IASClient {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient();
    private static final String TOKEN_REQUEST_BODY = "scope=openid offline corpds:ds:profile&grant_type=client_credentials";
    private static final String REFRESH_TOKEN_REQUEST_BODY = "grant_type=refresh_token&refresh_token=%s";
    // Expiration is set to 90% of the expiration of the token to ensure that we do not run into edge cases where we might return an expired token
    private static final long IASClientIdTokenExpirationMillis = 3600 * 1000L * 90 / 100;
    private static final long IASClientRefreshTokenExpirationMillis = 86399 * 1000L * 90 / 100;
    private static final long IASClientTokenExpirationBufferMillis = 60 * 1000L * 90 / 100;
    private final long idTokenExpirationMillis;
    private final long refreshTokenExpirationMillis;
    private final long buffer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private String idToken;
    private String refreshToken;
    private long idTokenExpirationTime;
    private long refreshTokenExpirationTime;

    public IASClient() {
        this(IASClientIdTokenExpirationMillis, IASClientRefreshTokenExpirationMillis, IASClientTokenExpirationBufferMillis);
    }

    public IASClient(long idTokenExpirationMillis, long refreshTokenExpirationMillis, long buffer) {
        this.idTokenExpirationMillis = idTokenExpirationMillis;
        this.refreshTokenExpirationMillis = refreshTokenExpirationMillis;
        this.buffer = buffer;
    }

    public static void main(String[] args) throws InterruptedException {
        IASClient iasClient = new IASClient(IASClientIdTokenExpirationMillis, IASClientRefreshTokenExpirationMillis, IASClientTokenExpirationBufferMillis);
        while (true) {
            System.out.println(iasClient.getJWT("", ""));
            Thread.sleep(10 * 60 * 1000);
        }
    }

    public String getJWT(String clientId, String clientSecret) throws SecurityException {
        if (isIdTokenValid()) {
            LOGGER.debug("Spark IAS Client id token is valid");
            return idToken;
        } else if (isRefreshTokenValid()) {
            LOGGER.debug("Spark IAS Client refresh token is valid");
            handleTokenResponse(getRefreshTokenResponse(clientId, clientSecret));
            return idToken;
        } else {
            LOGGER.debug("Spark IAS Client id token and refresh token are invalid");
            handleTokenResponse(getClientCredentialsResponse(clientId, clientSecret));
            return idToken;
        }
    }

    private boolean isIdTokenValid() {
        return (System.nanoTime() + buffer) < idTokenExpirationTime;
    }

    private boolean isRefreshTokenValid() {
        return (System.nanoTime() + buffer) < refreshTokenExpirationTime;
    }

    public void handleTokenResponse(final String responseBody) throws SecurityException {
        try {
            IASToken tokenResponse = objectMapper.readValue(responseBody, IASToken.class);
            idTokenExpirationTime = System.nanoTime() + idTokenExpirationMillis;
            refreshTokenExpirationTime = System.nanoTime() + refreshTokenExpirationMillis;
            idToken = tokenResponse.getIdToken();
            refreshToken = tokenResponse.getRefreshToken();
        } catch (IOException e) {
            throw new SecurityException("Error during deserializing JWT response using IAS Client");
        }
    }

    public String getRefreshTokenResponse(String clientId, String clientSecret) throws SecurityException {
        LOGGER.debug("get refresh token response");
        return requestToken(String.format(REFRESH_TOKEN_REQUEST_BODY, refreshToken), clientId, clientSecret);
    }

    public String getClientCredentialsResponse(String clientId, String clientSecret) throws SecurityException {
        LOGGER.debug("get client credential response");
        return requestToken(TOKEN_REQUEST_BODY, clientId, clientSecret);
    }

    private String requestToken(final String requestBody, String clientId, String clientSecret) throws SecurityException {
        String url = "https://iam.corp.apple.com/oauth2/token";   //todo: conf
        Response response = null;
        String responseString = null;
        int responseCode = -1;
        try {
            LOGGER.debug("url: {}", url);
            RequestBody body = RequestBody.create(requestBody, MEDIA_TYPE_UTF_8);
            Request request = new Request.Builder().url(url).addHeader("content-type", "application/x-www-form-urlencoded").addHeader("authorization", "Basic " + Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8))).post(body).build();
            response = HTTP_CLIENT.newCall(request).execute();
            responseCode = response.code();
            ResponseBody responseBody = response.body();
            responseString = responseBody == null ? null : responseBody.string();
            if (!response.isSuccessful()) {
                throw new Exception("Non 200-OK response received for POST http request for url " + url + ". Response Code : " + responseCode + ". Error message : " + responseString);
            }
            LOGGER.debug("http POST request successful", Map.of("url", url, "response", dropSensitiveKeys(responseString), "timeTaken", System.nanoTime()));
            if (responseString == null || responseString.isEmpty()) {
                return null;
            }
            return responseString;
        } catch (Exception t) {
            throw new SecurityException("Error getting a valid JWT response using IAS Client with http POST request to url " + url, t);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }
}
