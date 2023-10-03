package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class AttributesOpsOptions extends FreemarkerOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String keyspace;
    @Optional
    private String endpoints;
    @Optional
    private int port;
    @Optional
    private String user;
    @Optional
    private String password;
    @Optional
    private String datacenter;
    @Optional
    private boolean useTruststore;
    @Optional
    private Map<String, Object> props;
}
