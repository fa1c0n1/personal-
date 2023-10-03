package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@EqualsAndHashCode(callSuper = true)
@Data
public class FreemarkerFunctionOptions extends JavaFunctionOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String expression;
}
