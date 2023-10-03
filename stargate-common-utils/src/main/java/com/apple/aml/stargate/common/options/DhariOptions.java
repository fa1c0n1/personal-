package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

import static com.apple.aml.stargate.common.utils.AppConfig.environment;

@Data
@EqualsAndHashCode(callSuper = true)
public class DhariOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private boolean randomKey;
    @Optional
    private boolean grpc;
    @Optional
    private boolean validateGrpc;
    @Optional
    private String publisherId;
    @Optional
    private String publishFormat;
    @Optional
    private String payload;
    @Optional
    private String uri;
    @Optional
    private String grpcEndpoint;
    @Optional
    private String method;
    @Optional
    private String a3Token;
    @Optional
    private String transform;
    @Optional
    private long a3AppId;
    @Optional
    private String a3Password;
    @Optional
    private String a3Mode;
    @Optional
    private String a3ContextString;

    public String discoveryUrl() {
        return String.format("%s/discovery/grpc", this.getUri() == null ? environment().getConfig().getDhariUri() : this.getUri());
    }
}
