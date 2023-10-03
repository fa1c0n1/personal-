package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;
import java.time.Duration;

@Data
public class RetryOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private int maxAttempts;
    @Optional
    private Duration fixedInterval = Duration.ofSeconds(1);
    @Optional
    private Duration initialInterval = Duration.ofSeconds(1);
    @Optional
    private double multiplier; // if provided => assumes exponential back off
    @Optional
    private Duration maxInterval;
    @Optional
    private double randomizationFactor; // if provided => assumes exponential back off with randomization
}
