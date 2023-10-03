package com.apple.aml.stargate.common.web.clients;

import com.apple.aml.stargate.common.options.HttpConnectionOptions;
import com.apple.aml.stargate.common.options.HttpInvokerOptions;
import com.apple.aml.stargate.common.options.HttpOptions;
import com.apple.aml.stargate.common.options.RetryOptions;
import com.apple.aml.stargate.common.pojo.RestResponse;
import com.apple.aml.stargate.common.services.RestService;
import com.apple.aml.stargate.common.utils.SneakyRetryFunction;
import io.github.resilience4j.retry.Retry;
import lombok.SneakyThrows;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.MetricLabels.ERROR_MESSAGE;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.WebUtils.fetchResponse;
import static com.apple.aml.stargate.common.utils.WebUtils.newOkHttpClient;
import static com.apple.aml.stargate.common.utils.WebUtils.parseResponse;

public final class RestClient implements RestService, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private final HttpConnectionOptions connectionOptions;
    private final HttpOptions httpOptions;
    private final RetryOptions retryOptions;
    private final Logger logger;
    private final Map<String, Object> logMap;
    private final String url;
    private transient Retry retry;
    private transient OkHttpClient client;

    public RestClient(final HttpInvokerOptions invokerOptions, final Logger logger, final Map<String, Object> logMap) {
        this(invokerOptions.getConnectionOptions(), invokerOptions, invokerOptions.getRetryOptions(), logger, logMap);
    }

    public RestClient(final HttpConnectionOptions connectionOptions, final HttpOptions httpOptions, final RetryOptions retryOptions, final Logger logger, final Map<String, Object> logMap) {
        this.connectionOptions = connectionOptions;
        this.httpOptions = httpOptions;
        this.retryOptions = retryOptions;
        this.logger = logger == null ? LOGGER : logger;
        this.logMap = logMap == null ? Map.of() : logMap;
        this.url = httpOptions.url();
        this.retry = retry();
    }

    @SneakyThrows
    private Retry retry() {
        if (this.retry != null) return retry;
        this.retry = retryOptions == null ? null : SneakyRetryFunction.retry(retryOptions, e -> {
            this.logger.warn("http request failed!. Will retry accordingly", this.logMap, Map.of("url", url, "method", String.valueOf(httpOptions.getMethod()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            return true;
        });
        return retry;
    }

    public RestClient(final HttpConnectionOptions connectionOptions, final HttpOptions httpOptions, final RetryOptions retryOptions) {
        this(connectionOptions, httpOptions, retryOptions, logger(MethodHandles.lookup().lookupClass()), Map.of());
    }

    @Override
    public <O> O invoke(final Object payload, final Map<String, String> payloadHeaders, final Class<O> returnType, final boolean throwOnError) {
        RestResponse response = fetchResponse(client(), httpOptions, retry(), url, payload, payloadHeaders, throwOnError, logger, null);
        return parseResponse(returnType, response);
    }

    @SneakyThrows
    private OkHttpClient client() {
        if (this.client != null) return client;
        client = newOkHttpClient(this.connectionOptions);
        return client;
    }

    public RestResponse httpInvoke(final String url, final String payload) throws Exception {
        Retry retry = retryOptions == null ? null : SneakyRetryFunction.retry(retryOptions, e -> {
            this.logger.warn("http request failed!. Will retry accordingly", this.logMap, Map.of("url", url, "method", String.valueOf(httpOptions.getMethod()), ERROR_MESSAGE, String.valueOf(e.getMessage())), e);
            return true;
        });
        return fetchResponse(client(), httpOptions, retry, url, payload, null, true, this.logger, this.logMap);
    }
}
