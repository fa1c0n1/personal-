package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = true)
public class IcebergOpsOptions extends IcebergOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String operation;
    @Optional
    private String query;
    @Optional
    private String evaluate;
}
