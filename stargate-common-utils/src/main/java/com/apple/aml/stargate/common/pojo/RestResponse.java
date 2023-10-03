package com.apple.aml.stargate.common.pojo;

import com.typesafe.config.Optional;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
@Builder
public class RestResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private int code;
    @Optional
    private String message;
    @Optional
    private byte[] content;
    @Optional
    private Map<String, String> headers;
}
