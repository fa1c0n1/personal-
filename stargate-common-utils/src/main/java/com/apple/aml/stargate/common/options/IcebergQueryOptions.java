package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.time.Duration;

@Data
@EqualsAndHashCode(callSuper = true)
public class IcebergQueryOptions extends IcebergOptions implements Serializable {
    @Optional
    private String queryAttribute;
    @Optional
    private String queryTypeAttribute;
    @Optional
    private String queryType;
    @Optional
    private String queryIdAttribute;
    @Optional
    private String queryExpression;
    @Optional
    private String resultAttribute;
    @Optional
    private String resultSizeAttribute;
    @Optional
    private String errorAttribute;
    @Optional
    private String statusAttribute;
    @Optional
    private String startTimeAttribute;
    @Optional
    private String startTimeEpocAttribute;
    @Optional
    private String endTimeAttribute;
    @Optional
    private String endTimeEpocAttribute;
    @Optional
    private String durationAttribute;
    @Optional
    private boolean latest;
    @Optional
    private boolean enableLogging;
    @Optional
    private int sparkPartitions;
    @Optional
    private Duration timeout;
    @Optional
    private String preconditionQuery;
    @Optional
    private String precondition;
}
