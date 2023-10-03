package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class DeploymentResourceOptions extends ResourceOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private Map<String, Object> labels;
    @Optional
    private Map<String, Object> annotations;
}