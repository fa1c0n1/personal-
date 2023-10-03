package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.time.Duration;

@Data
@EqualsAndHashCode(callSuper = true)
public class JdbiOptions extends JdbcOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String bindingNullToPrimitivesPermitted;
    @Optional
    private String preparedArgumentsEnabled;
    @Optional
    private String untypedNullArgument;
    @Optional
    private String coalesceNullPrimitivesToDefaults;
    @Optional
    private String caseChange;
    @Optional
    private String allowNoResults;
    @Optional
    private Duration queryTimeout;
    @Optional
    private String exceptionMessageRendering;
    @Optional
    private int exceptionLengthLimit;
}
