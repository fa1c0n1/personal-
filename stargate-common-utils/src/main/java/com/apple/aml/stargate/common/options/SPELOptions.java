package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class SPELOptions extends BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String expression;
    @Optional
    private String type = "json";
    @Optional
    private String className = Map.class.getCanonicalName();
    @Optional
    private String transform;

    public String expression() {
        if (expression != null) {
            return expression;
        }
        return transform;
    }
}
