package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.K8sPorts.RM;
import static com.apple.jvm.commons.util.Strings.isBlank;

@EqualsAndHashCode(callSuper = true)
@Data
public class ExternalTransformOptions extends HttpConnectionOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String host;
    @Optional
    private int port;
    @Optional
    private String uri;
    @Optional
    private String scheme = "grpc";
    @Optional
    private String executionType;
    @Optional
    private String initializePath;
    @Optional
    private String executePath;
    @Optional
    private Map<String, Object> grpcPoolConfig;

    public String initializeUri() {
        String subPath = isBlank(initializePath) ? String.format("/init/%s", isBlank(executionType) ? "lambda" : executionType.trim()) : initializePath.trim();
        return String.format("%s%s", this.uri(), subPath);
    }

    public String uri() {
        if (!isBlank(this.uri)) return this.uri;
        return String.format("%s://%s:%d", isBlank(scheme) ? "http" : scheme.trim(), host(), port());
    }

    public String host() {
        return isBlank(host) ? "localhost" : host.trim();
    }

    public int port() {
        return port <= 0 ? RM : port;
    }

    public String executeUri() {
        String subPath = isBlank(executePath) ? String.format("/execute/%s", isBlank(executionType) ? "lambda" : executionType.trim()) : executePath.trim();
        return String.format("%s%s", this.uri(), subPath);
    }
}
