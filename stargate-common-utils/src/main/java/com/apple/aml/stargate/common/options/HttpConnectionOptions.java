package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.pojo.SSLOptions;
import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

@Data
public class HttpConnectionOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private boolean disableHostnameVerifier = true;
    @Optional
    private boolean retryOnConnectionFailure = true;
    @Optional
    private boolean followRedirects = true;
    @Optional
    private boolean followSslRedirects = true;
    @Optional
    private Duration callTimeout;
    @Optional
    private Duration connectTimeout;
    @Optional
    private Duration readTimeout;
    @Optional
    private Duration writeTimeout;
    @Optional
    private Duration pingInterval;
    @Optional
    private SSLOptions ssl;
    @Optional
    private Integer maxIdleConnections;
    @Optional
    private Duration keepAliveDuration = Duration.ofMinutes(5);
    @Optional
    private Map<String, String> headers;
}
