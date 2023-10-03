package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class AttributesOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String clientType; // possible values - legacy/attributes/shuri
    @Optional
    private String namespace = "live"; // this maps to athena's concept of live/val
    @Optional
    private Object namespaces;
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
    @Optional
    private String operation = "read";
    @Optional
    private List<String> keys;
    @Optional
    private String lookupName;
    @Optional
    private List<String> lookupKeys;
    @Optional
    private String keysAttribute;
    @Optional
    private String lookupNameAttribute;
    @Optional
    private String lookupKeysAttribute;
    @Optional
    private boolean useLookupKey = true;
    @Optional
    private String keyAttribute; // defaults to "key"
    @Optional
    private String lookupKeyAttribute;
    @Optional
    private String valueAttribute; // defaults to "value"
    @Optional
    private int ttl = -1;
    @Optional
    private String ttlAttribute;
    @Optional
    private String timestampAttribute;
    @Optional
    private String expression;
    @Optional
    private boolean enableDirectAccess;
    @Optional
    private String transform;
}