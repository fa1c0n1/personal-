package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.time.Duration;

@Data
@EqualsAndHashCode(callSuper = true)
public class SequencerOptions extends FreemarkerOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private long rate = 1;
    @Optional
    private long startRate = -1;
    @Optional
    private long endRate = -1;
    @Optional
    private Duration frequency;
    @Optional
    private long loadSize = 1;
    @Optional
    private long start = 0;
    @Optional
    private long end = 0;
    @Optional
    private String filePath;
}
