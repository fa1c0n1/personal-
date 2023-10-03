package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = true)
public class TemplateEvaluatorOptions extends DerivedSchemaOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String expression;
    @Optional
    private String keyExpression;
    @Optional
    private String type = "json";
    @Optional
    private String transform;
    @Optional
    private String schemaIdExpression;
    @Optional
    private String referenceType;

    public String expression() {
        if (expression != null) {
            return expression;
        }
        return transform;
    }

}
