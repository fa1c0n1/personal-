package com.apple.aml.stargate.common.pojo;

import com.apple.aml.stargate.common.options.ResourceOptions;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class CoreResourceOptions extends ResourceOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private Map<String, String> labels;
    @Optional
    private Map<String, String> annotations;
}
