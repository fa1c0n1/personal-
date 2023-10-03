package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.constants.CommonConstants;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class LocalOfsOptions extends WindowOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String baseUri;
    @Optional
    private String type; // kv or relational
    @Optional
    private String tableName;
    @Optional
    private boolean preferMerge = true;
    @Optional
    private boolean preferRaw = true;
    @Optional
    private String expression;
    @Optional
    private String cacheName;
    @Optional
    private String transform;
    @Optional
    private Map<String, Object> indexes;
    @Optional
    private boolean disableCertificateValidation = true;
    @Optional
    private String deleteAttributeName = CommonConstants.OfsConstants.DELETE_ATTRIBUTE_NAME;

}
