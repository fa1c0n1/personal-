package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = true)
public class SwitchOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private Object rules;
    @Optional
    private String expression;
    @Optional
    private boolean enableMultiMatch;
    @Optional
    private String defaultRule;
}
