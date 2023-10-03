package com.apple.aml.stargate.common.options;

import com.apple.aml.stargate.common.annotation.RootLevelOption;
import com.typesafe.config.Optional;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static lombok.AccessLevel.NONE;

@Data
public class ErrorOptions implements Serializable {
    @Optional
    @RootLevelOption
    @Setter(AccessLevel.NONE)
    private String logOnError;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private boolean _logOnError;
    @Optional
    @RootLevelOption
    @Setter(AccessLevel.NONE)
    private String failOnError;
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private boolean _failOnError;
    @Optional
    private JavaFunctionOptions errorOptions;
    @Optional
    @RootLevelOption
    private String errorInterceptor;

    public void setLogOnError(final String logOnError) {
        this.logOnError = logOnError;
        this._logOnError = parseBoolean(this.logOnError);
    }

    public boolean logOnError() {
        return _logOnError;
    }

    public void setFailOnError(final String failOnError) {
        this.failOnError = failOnError;
        this._failOnError = parseBoolean(this.failOnError);
    }

    public boolean failOnError() {
        return _failOnError;
    }

    @Override
    public String toString() {
        return "{" + "logOnError=" + _logOnError + ", failOnError=" + _failOnError + ", errorInterceptor='" + errorInterceptor + '\'' + '}';
    }
}
