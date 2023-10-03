package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.time.Duration;

@Data
@EqualsAndHashCode(callSuper = true)
public class SchedulerOptions extends FreemarkerOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String cron;
    @Optional
    private Duration frequency;
    @Optional
    private String startAt;
    @Optional
    private String storeType;
    @Optional
    private JdbiOptions jdbcOptions;
}
