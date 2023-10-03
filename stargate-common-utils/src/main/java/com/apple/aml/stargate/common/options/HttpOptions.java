package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_A3_TOKEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_CLIENT_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_TOKEN_TYPE;
import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
@EqualsAndHashCode(callSuper = true)
public class HttpOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String baseUri;
    private String uri;
    @Optional
    private Map<String, Object> headers = new HashMap<>();
    @Optional
    private String method;
    @Optional
    private String contentType;
    @Optional
    private String authTypes;
    @Optional
    private String a3AppIdHeader;
    @Optional
    private String a3TokenHeader;
    @Optional
    private String a3TokenTypeHeader;
    @Optional
    private long a3AppId;
    @Optional
    private long a3TargetAppId;
    @Optional
    private String a3Password;
    @Optional
    private String a3Mode;
    @Optional
    private String a3ContextString;
    @Optional
    private Long a3ContextVersion;

    public String a3AppIdHeader() {
        if (isBlank(a3AppIdHeader)) return HEADER_CLIENT_APP_ID;
        return a3AppIdHeader;
    }

    public String a3TokenHeader() {
        if (isBlank(a3TokenHeader)) return HEADER_A3_TOKEN;
        return a3TokenHeader;
    }

    public String a3TokenTypeHeader() {
        if (isBlank(a3TokenTypeHeader)) return HEADER_TOKEN_TYPE;
        return a3TokenTypeHeader;
    }

    public String url() {
        return isBlank(baseUri) ? uri : (baseUri + uri);
    }
}
