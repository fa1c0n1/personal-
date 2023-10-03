package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class ExternalFunctionOptions extends BatchLambdaOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String runtimeType;
    @Optional
    private Map<String, Object> functionOptions;
    @Optional
    private String expression;
    @Optional
    private String transform;
    @Optional
    private ExternalTransformOptions connectionOptions;

    public String expression() {
        if (expression != null) {
            return expression;
        }
        return transform;
    }

    public ExternalTransformOptions connectionOptions() {
        return this.connectionOptions;
    }

    public ExternalTransformOptions connectionOptions(final ExternalTransformOptions options) {
        this.connectionOptions = options;
        return this.connectionOptions;
    }

    public void setFunction(final Map<String, Object> options) {
        _setRuntimeOptions(null, options);
    }

    private void _setRuntimeOptions(final String type, final Map<String, Object> options) {
        if (options == null) return;
        this.runtimeType = type;
        if (functionOptions == null) {
            this.functionOptions = options;
        } else {
            this.functionOptions.putAll(options);
        }
    }

    public void setPythonOptions(final Map<String, Object> options) {
        _setRuntimeOptions("python", options);
    }

    public void setExternalJvmOptions(final Map<String, Object> options) {
        _setRuntimeOptions("jvm", options);
    }

}
