package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = true)
public class StateOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String stateId;
    @Optional
    private String preprocess;
    @Optional
    private String callback;
    @Optional
    private String expression;
    @Optional
    private String evaluate;
    @Optional
    private String transform;
    @Optional
    private String pipelineToken;
}
