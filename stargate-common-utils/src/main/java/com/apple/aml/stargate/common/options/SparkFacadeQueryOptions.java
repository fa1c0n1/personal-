package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.time.Duration;

@Data
@EqualsAndHashCode(callSuper = true)
public class SparkFacadeQueryOptions extends IcebergQueryOptions implements Serializable {
    @Optional
    private int port;
    @Optional
    private String host;
}
